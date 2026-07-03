/*
 * #69 aims to fix match process which was designed to have rowid as key, to access client data
 * the mistakes come from:
 * - data change w/ BAL import (address can be deleted), so rowid is not constant!
 * - BAL codes aren't uniq for a same address (municipality, street, number, extension)
 *   BAL street can own multiple codes
 *     (01015,CHEMIN DU MOULIN) w/ 2 codes {01015_0691,01015_3f68sz}
 *   BAL housenumber too,
 *     by example, {01014_0028_00001_bis, 01014_0028_00001_bis__0}
 *     REGEX (__0)+$ can find many other cases!
 *     (62005,RUE DE COURCELLES,20,) w/ 5 codes
 *     (01416,EN COURTIOUX ROUTE DE MALIX,0,) w/ 6 codes
 *     (35161,RUE SAINT PATERN,22,) w/ 7 codes
 *     (42150,CHEMIN DU CANAL,79,) w/ 12 codes
 *     (35161,RUE SAINT PATERN,22,) w/ 28 codes
 *
 * solution is to put client code as ID (and no ROWID as previous)
 * this procedure tries to fix data already matched, without all running again (w/ this solution)
 *
 * NOTE it can happen that number of matched code(s) are inferior to total of code(s)
 *      reason is some code(s) would be append AFTER the match...
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
        RAISE 'table fr.tmp_fix_bal_id non présente!';
    END IF;

    -- uniq ID
    RAISE NOTICE 'adresses rapprochées BAL avec ID unique : (commune, voie, numéro, extension)..(code)';

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
    RAISE NOTICE 'total maj: #%', _nrows;

    -- multiple ID
    RAISE NOTICE 'adresses rapprochées BAL avec ID multiple : (commune, voie, numéro, extension)..(codes)';

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
        RAISE NOTICE '(commune, voie, numéro, extension)=(%,%,%,%)',
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

        _naddrs := 0;
        _ncodes := COALESCE(CARDINALITY(_codes), 0);
        RAISE NOTICE ' #% code(s)', _ncodes;
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
            RAISE NOTICE ' #% rapprochée(s)', _nids;

            IF _nids > 0 THEN
                FOR _i IN 1.._nids
                LOOP
                    IF _i > _ncodes THEN
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
                RAISE NOTICE ' maj: %/%', _naddrs,
                    CASE
                    WHEN _nids > _ncodes THEN _ncodes
                    WHEN _ncodes > _nids THEN _nids
                    ELSE _nids
                    END
                    ;
            END IF;
        END IF;
        _ntotal := _ntotal + _naddrs;
    END LOOP;
    RAISE NOTICE 'total maj: #%', _ntotal;
END
$proc$ LANGUAGE plpgsql;

/* TEST
adresses rapprochées BAL avec ID unique : (commune, voie, numéro, extension)..(code)
total maj: #12245227
adresses rapprochées BAL avec ID multiple : (commune, voie, numéro, extension)..(codes)
total update: #41961

exec: ~ 6 hours

check:
SELECT
    mrq.source_name,
    mre.id_request,
    mre.id_address
FROM
    fr.address_match_request mrq
        JOIN fr.address_match_result mre ON mrq.id = mre.id_request
WHERE
    mrq.source_name ~ '^BAL'
    AND
    -- old rowid, not BAL code, as <MUNICIPALITY>_<ID STREET>[_<ID HOUSENUMBER>]
    (mre.standardized_address).id ~ '^[0-9]+$'
=> #7762

studing usecases:
    - merge municipality
      => need to join LAPOSTE areas to validate INSEE code
    - number 0 ? perhaps to store geometry of the street
      => exclude number 0
    - deleted addresses
      => create a clean process to delete these addresses (from table address_match_result) ?

 */
