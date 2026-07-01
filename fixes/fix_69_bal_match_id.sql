/*
 * #69 aims to fix match process which was designed to have rowid as key, to access client data
 * the mistakes come from:
 * - data change w/ BAL import (address can be deleted), so rowid is not constant!
 * - BAL codes aren't uniq for a same address (municipality, street, number, extension)
 *   BAL street can own multiple codes, as {01015_0691,01015_3f68sz} for (01015,CHEMIN DU MOULIN)
 *   BAL housenumber too, {01014_0028_00001_bis, 01014_0028_00001_bis__0} for (01014,IMPASSE DES COTEAUX,1,BIS)
 *       even more, REGEX (__0)+$ can find other cases, {01024_0002_00453} w/ 3 codes!
 *
 * solution is to put client code as ID (and no ROWID as previous)
 * this procedure tries to fix data already matched, without all running again (w/ this solution)
 *
 */
SELECT drop_all_functions_if_exists('fr', 'fix_69_bal_match_id');
CREATE OR REPLACE PROCEDURE fr.fix_69_bal_match_id(
    simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _nrows      INT;
    _naddrs     INT := 0;
    _ntotal     INT := 0;
    _ncodes     INT;
    _nids       INT;
    _keys       RECORD;
    _ids        INT[];
    _codes      VARCHAR[];
    _i          INT;
BEGIN
    -- needs to prepare fr.tmp_fix_bal_id
    IF NOT table_exists(schema_name => 'fr', table_name => 'tmp_fix_bal_id') THEN
        RAISE 'table tmp_fix_bal_id non présente!'
    END IF;

    -- uniq ID
    RAISE NOTICE 'BAL matched addresses w/ uniq ID : (municipality, street, number, extension, 1)';

    WITH
    uniq_bal_id AS (
        SELECT
            municipality,
            street,
            COALESCE(number, 0) number,
            COALESCE(UPPER(extension), '') extension,
            MIN(code) code
        FROM
            fr.tmp_fix_bal_id
        GROUP BY
            municipality,
            street,
            COALESCE(number, 0),
            COALESCE(UPPER(extension), '')
        HAVING
            COUNT(*) = 1
    )
    UPDATE fr.address_match_result mre SET
        standardized_address.id = i.code
        FROM
            uniq_bal_id i, fr.address_match_request mrq
        WHERE
            SUBSTR(mrq.source_name, 5) = i.municipality
            AND
            mre.id_request = mrq.id
            AND
            (i.municipality, i.street, i.number, i.extension) =
            (
                (mre.standardized_address).municipality_code,
                (mre.standardized_address).street_name,
                COALESCE((mre.standardized_address).housenumber, 0),
                COALESCE((mre.standardized_address).extension, '')
            )
    ;

    GET DIAGNOSTICS _nrows = ROW_COUNT;
    RAISE NOTICE 'update: #%', _nrows;

    -- multiple ID
    RAISE NOTICE 'BAL matched addresses w/ multiple ID : (municipality, street, number, extension, x)';

    FOR _keys IN (
        SELECT
            municipality,
            street,
            COALESCE(number, 0) number,
            COALESCE(UPPER(extension), '') extension
        FROM
            fr.tmp_fix_bal_id
        GROUP BY
            municipality,
            street,
            COALESCE(number, 0),
            COALESCE(UPPER(extension), '')
        HAVING
            COUNT(*) > 1
    )
    LOOP
        RAISE NOTICE '(municipality, street, number, extension)=(%,%,%,%)',
            _keys.municipality, _keys.street, _keys.number, _keys.extension;

        SELECT
            ARRAY_AGG(code ORDER BY 1)
        INTO
            _codes
        FROM
            fr.tmp_fix_bal_id
        WHERE
            (municipality, street, COALESCE(number, 0), COALESCE(UPPER(extension), '')) =
            (_keys.municipality, _keys.street, _keys.number, _keys.extension)
        ;
        _ncodes := COALESCE(CARDINALITY(_codes), 0);
        RAISE NOTICE '#% code(s)', _ncodes;

        IF _ncodes > 0 THEN
            SELECT
                ARRAY_AGG(mre.id_address ORDER BY 1)
            INTO
                _ids
            FROM
                fr.address_match_request mrq
                    JOIN fr.address_match_result mre ON mrq.id = mre.id_request
            WHERE
                SUBSTR(mrq.source_name, 5) = _keys.municipality
                AND
                (
                    (mre.standardized_address).municipality_code,
                    (mre.standardized_address).street_name,
                    COALESCE((mre.standardized_address).housenumber, 0),
                    COALESCE((mre.standardized_address).extension, '')
                ) =
                (_keys.municipality, _keys.street, _keys.number, _keys.extension)
            ;
            _nids := COALESCE(CARDINALITY(_ids), 0);
            RAISE NOTICE '#% matched(s)', nids;

            IF _nids > 0 THEN
                _naddrs := 0;
                FOR _i IN 1..ARRAY_LENGTH(_ids, 1)
                LOOP
                    IF _i > ARRAY_LENGTH(_codes, 1) THEN
                        EXIT;
                    END IF;

                    UPDATE fr.address_match_result mre SET
                        standardized_address.id = _codes[_i]
                        FROM
                            fr.address_match_request mrq
                        WHERE
                            SUBSTR(mrq.source_name, 5) = _keys.municipality
                            AND
                            mre.id_request = mrq.id
                            AND
                            mre.id_address = _ids[_i]
                        ;
                    GET DIAGNOSTICS _nrows = ROW_COUNT;
                    _naddrs := _naddrs + _nrows;
                END LOOP;
                RAISE NOTICE 'update: #%/%', _naddrs,
                    CASE
                    WHEN _nids > _ncodes THEN _ncodes
                    WHEN _ncodes > _nids THEN _nids
                    ELSE _nids
                    END
                    ;
            END IF;
        END IF;
    END LOOP;
    RAISE NOTICE 'update: #%', _ntotal;
END
$proc$ LANGUAGE plpgsql;
