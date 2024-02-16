/***
 * FR: add LAPOSTE/RAN faults
 */

/* NOTE
initialization will be done w/ constant
 */

-- fix fault of address (referential)
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault');
CREATE OR REPLACE FUNCTION fr.fix_laposte_address_fault(
    address_element IN VARCHAR                          -- AREA|STREET|HOUSENUMBER|COMPLEMENT
    , address_join_column IN VARCHAR                    -- join ADDRESS to REFERENCE
    , address_update_column IN VARCHAR                  -- column to change
    , fault_id IN INT                                   -- fault ID (or -1 if NONE)
    , column_with_new_value IN VARCHAR DEFAULT 'name'
    , address_alias IN VARCHAR DEFAULT 'a'
    , fault_alias IN VARCHAR DEFAULT 'f'
    , uniq_alias IN VARCHAR DEFAULT 'u'
    , reference_alias IN VARCHAR DEFAULT 'r'
    , simulation IN BOOLEAN DEFAULT FALSE
    , nrows OUT INT
)
AS
$func$
DECLARE
    _query TEXT;
    _address_table VARCHAR;
    _fault_table VARCHAR;
    _uniq_table VARCHAR;
    _reference_table VARCHAR;
    _fault_key VARCHAR := CONCAT(fault_alias, '.name_id');
    _uniq_key VARCHAR := CONCAT(uniq_alias, '.id');
    _reference_key VARCHAR := CONCAT(reference_alias, '.name_id');
    _join_uniq_fault VARCHAR := CONCAT(_fault_key, ' = ', _uniq_key);
    _join_uniq_reference VARCHAR := CONCAT(_reference_key, ' = ', _uniq_key);
    _address_join_column VARCHAR := CONCAT(address_alias, '.', address_join_column);
    _address_update_column VARCHAR := CONCAT(address_alias, '.', address_update_column);
    _column_with_new_value VARCHAR := CASE
        WHEN count_words(column_with_new_value) = 1 THEN
            CONCAT(uniq_alias, '.', column_with_new_value)
        ELSE
            column_with_new_value
        END
        ;
BEGIN
    IF NOT address_element = ANY('{AREA,STREET,HOUSENUMBER,COMPLEMENT}') THEN
        RAISE 'élément adresse (%) non valide!', address_element;
    END IF;

    _address_table := CONCAT('fr.laposte_address_', LOWER(address_element));
    _fault_table := CONCAT('fr.laposte_address_fault_', LOWER(address_element));
    _uniq_table := CONCAT(_address_table, '_uniq');
    _reference_table := CONCAT(_address_table, '_reference');

    /*
    UPDATE fr.laposte_address_street s SET
        lb_voie = u.name
        FROM
            fr.laposte_address_fault_street fs
                JOIN fr.laposte_address_street_uniq u ON u.id = fs.name_id
        WHERE
            fs.fault_id = _values[_fault_i]::INT
            AND
            s.lb_voie = fs.name_before
            AND
            s.lb_voie IS DISTINCT FROM u.name
    ;
     */

    _query := CONCAT('UPDATE ', _address_table, ' ', address_alias, ' SET
        ', address_update_column, ' = ', _column_with_new_value, '
        , dt_reference = TIMEOFDAY()::DATE
        FROM
        '
    );
    IF fault_id >= 0 THEN
        _query := CONCAT(_query
            , _fault_table, ' ', fault_alias, '
                JOIN ', _uniq_table, ' ', uniq_alias, ' ON ', _join_uniq_fault, '
                JOIN ', _reference_table, ' ', reference_alias, ' ON ', _join_uniq_reference, '
            WHERE
            ', fault_alias, '.fault_id = ', fault_id, '
            AND
            '
        );
    ELSE
        _query := CONCAT(_query
            , _uniq_table, ' ', uniq_alias, '
                JOIN ', _reference_table, ' ', reference_alias, ' ON ', _join_uniq_reference, '
            WHERE
            '
        );
    END IF;
    _query := CONCAT(_query
            , _address_join_column, ' = ', CONCAT(reference_alias, '.address_id'), '
            AND
            ', _address_update_column, ' IS DISTINCT FROM ', _column_with_new_value
    );

    IF NOT simulation THEN
        EXECUTE _query;
        GET DIAGNOSTICS nrows = ROW_COUNT;
    ELSE
        RAISE NOTICE ' requête=%', _query;
        nrows := 0;
    END IF;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'fix_laposte_address_fault_links');
CREATE OR REPLACE PROCEDURE fr.fix_laposte_address_fault_links(
    simulation BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _set RECORD;
    _query TEXT;
    _nrows_found INT;
    _nrows_history INT;
    _nrows_fixed INT;
    _table_from VARCHAR;
    _column_from VARCHAR;
    _columns_to VARCHAR;
    _kind VARCHAR := 'LINK';
BEGIN
    FOR _set IN (
        SELECT key FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_LINKS'
    )
    LOOP
        DROP TABLE IF EXISTS fr.tmp_address_fault_links;
        _query := 'CREATE UNLOGGED TABLE fr.tmp_address_fault_links AS';
        IF _set.key = 'COMPLEMENT_WITH_STREET_ERROR' THEN
            _table_from := 'fr.laposte_address';
            _column_from := 'co_cea_l3';
            _columns_to := 'co_cea_voie = ac.co_cea_voie';
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
                , ', quote_literal(_set.key)
                , ', ', quote_literal(_kind), '
                , ROW_TO_JSON(a.*)::JSONB
            FROM ', _table_from, ' a
                JOIN fr.tmp_address_fault_links ac ON a.', _column_from, ' = ac.code_address
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
            FROM fr.tmp_address_fault_links ac
            WHERE
                a.', _column_from, ' = ac.code_address
            '
        );
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_fixed = ROW_COUNT;

            IF _nrows_fixed = _nrows_history AND _nrows_fixed = _nrows_found THEN
                COMMIT;
            ELSE
                ROLLBACK;
                CALL public.log_info(CONCAT('%: error (found,history,fixed)=(%,%,%)', _set.key, _nrows_found, _nrows_history, _nrows_fixed));
            END IF;
        ELSE
            RAISE NOTICE '%', _query;
        END IF;
    END LOOP;
END;
$proc$ LANGUAGE plpgsql;
