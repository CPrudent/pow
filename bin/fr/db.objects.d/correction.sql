/***
 * FR-CORRECTION
 */

SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_correction');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_correction(
    simulation BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _correction RECORD;
    _query TEXT;
    _nrows_found INT;
    _nrows_history INT;
    _nrows_corrected INT;
    _table_from VARCHAR;
    _column_from VARCHAR;
    _columns_to VARCHAR;
    _kind VARCHAR;
BEGIN
    FOR _correction IN (
        SELECT key, value FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_CORRECTION'
    )
    LOOP
        DROP TABLE IF EXISTS fr.tmp_address_correction;
        _query := 'CREATE UNLOGGED TABLE fr.tmp_address_correction AS';
        IF _correction.key = 'TOO_SPACE' THEN
            _table_from := 'fr.laposte_address_street';
            _column_from := 'co_cea';
            _columns_to := 'lb_voie = ac.name, lb_voie_normalise = ac.name_normalized';
            _kind := 'STREET';
            _query := CONCAT(_query
                , '
                SELECT
                    co_cea code_address
                    , REGEXP_REPLACE(lb_voie, ''[ ]{2,}'', '' '') name
                    , REGEXP_REPLACE(lb_voie_normalise, ''[ ]{2,}'', '' '') name_normalized
                FROM fr.laposte_address_street
                WHERE
                    fl_active
                    AND
                    (lb_voie ~ ''[ ]{2,}'' OR lb_voie_normalise ~ ''[ ]{2,}'')
                '
            );
        ELSIF _correction.key = 'COMPLEMENT_WITH_STREET_ERROR' THEN
            _table_from := 'fr.laposte_address';
            _column_from := 'co_cea_l3';
            _columns_to := 'co_cea_voie = ac.co_cea_voie';
            _kind := 'COMPLEMENT';
            _query := CONCAT(_query
                , '
                WITH
                housenumber_with_multiple_streets AS (
                    SELECT co_cea_numero
                    FROM fr.laposte_address
                    WHERE fl_active AND co_cea_numero IS NOT NULL
                    GROUP BY co_cea_numero
                    HAVING COUNT(DISTINCT co_cea_voie) > 1
                )
                , good_street_of_housenumber AS (
                    SELECT DISTINCT a.co_cea_numero, a.co_cea_voie
                    FROM fr.laposte_address a
                        JOIN housenumber_with_multiple_streets e ON a.co_cea_numero = e.co_cea_numero
                    WHERE
                        a.fl_active
                        AND
                        a.co_niveau = ''NUMERO''
                )
                , good_street_of_complement AS (
                    SELECT DISTINCT a.co_cea_l3 code_address, s.co_cea_voie
                    FROM fr.laposte_address a
                        JOIN housenumber_with_multiple_streets e ON a.co_cea_numero = e.co_cea_numero
                        JOIN good_street_of_housenumber s ON a.co_cea_numero = s.co_cea_numero
                    WHERE
                        a.fl_active
                        AND
                        a.co_niveau = ''L3''
                        AND
                        a.co_cea_voie != s.co_cea_voie
                )
                SELECT * FROM good_street_of_complement
                '
            );
        END IF;
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_found = ROW_COUNT;
        ELSE
            RAISE NOTICE '%', _query;
        END IF;

        _query := CONCAT('INSERT INTO fr.laposte_address_history (
                code_address
                , date_change
                , change
                , kind
                , values
            )
            SELECT
                a.', _column_from, '
                , TIMEOFDAY()::DATE
                , ', quote_literal(_correction.key)
                , ', ', quote_literal(_kind), '
                , ROW_TO_JSON(a.*)::JSONB
            FROM ', _table_from, ' a
                JOIN fr.tmp_address_correction ac ON a.', _column_from, ' = ac.code_address
            '
        );
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_history = ROW_COUNT;
        ELSE
            RAISE NOTICE '%', _query;
        END IF;

        _query := CONCAT('UPDATE ', _table_from, ' a SET
            ', _columns_to, '
            FROM fr.tmp_address_correction ac
            WHERE
                a.', _column_from, ' = ac.code_address
            '
        );
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_corrected = ROW_COUNT;

            IF _nrows_corrected = _nrows_history AND _nrows_corrected = _nrows_found THEN
                COMMIT;
            ELSE
                ROLLBACK;
                CALL public.log_info(CONCAT('%: error (found,history,corrected)=(%,%,%)', _correction.key, _nrows_found, _nrows_history, _nrows_corrected));
            END IF;
        ELSE
            RAISE NOTICE '%', _query;
        END IF;
    END LOOP;
END;
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL fr.set_laposte_address_correction();
END $$;
