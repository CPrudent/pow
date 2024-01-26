/***
 * LAPOSTE ADDRESS history
 */


CREATE TABLE IF NOT EXISTS fr.laposte_address_history (
    code_address CHAR(10) NOT NULL          -- address ID
    , date_change DATE NOT NULL
    , change VARCHAR NOT NULL               -- defined into fr.constant (LAPOSTE_ADDRESS_CORRECTION)
    , kind VARCHAR NOT NULL                 -- {ADDRESS, ZONE_ADDRESS, STREET, HOUSENUMBER, COMPLEMENT}
    , values JSONB
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_history_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_history_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_history_code_address ON fr.laposte_address_history (code_address, date_change);
END
$proc$ LANGUAGE plpgsql;

-- add history for address faults
SELECT drop_all_functions_if_exists('fr', 'add_history_address_fault');
CREATE OR REPLACE FUNCTION fr.add_history_address_fault(
    address_table IN VARCHAR
    , address_change IN VARCHAR
    , address_element IN VARCHAR                -- STREET|HOUSENUMBER|COMPLEMENT
    , address_join_column IN VARCHAR
    , address_update_column IN VARCHAR
    , fault_table IN VARCHAR
    , fault_id IN INT
    , uniq_value_column IN VARCHAR DEFAULT 'name'
    , fault_join_column IN VARCHAR DEFAULT 'name_before'
    , address_key IN VARCHAR DEFAULT 'co_cea'
    , address_alias IN VARCHAR DEFAULT 'a'
    , fault_alias IN VARCHAR DEFAULT 'f'
    , uniq_alias IN VARCHAR DEFAULT 'u'
    , simulation IN BOOLEAN DEFAULT FALSE
    , nrows OUT INT
)
AS
$proc$
DECLARE
    _query TEXT;
    _address_key VARCHAR := CONCAT(address_alias, '.', address_key);
    _uniq_key VARCHAR := CONCAT(uniq_alias, '.id');
    _fault_key VARCHAR := CONCAT(fault_alias, '.name_id');
    _join_uniq_fault VARCHAR := CONCAT(_fault_key, ' = ', _uniq_key);
    _address_join_column VARCHAR := CONCAT(address_alias, '.', address_join_column);
    _address_update_column VARCHAR := CONCAT(address_alias, '.', address_update_column);
    _fault_join_column VARCHAR := CONCAT(fault_alias, '.', fault_join_column);
    _uniq_value_column VARCHAR := CONCAT(uniq_alias, '.', uniq_value_column);
BEGIN
    _query := CONCAT('
        INSERT INTO fr.laposte_address_history (
                code_address
                , date_change
                , change
                , kind
                , values
            )
            SELECT
            ', _address_key, '
                , TIMEOFDAY()::DATE
                , ', quote_literal(address_change), '
                , ', quote_literal(address_element), '
                , ROW_TO_JSON(', address_alias, '.*)::JSONB
            FROM
            ', address_table, ' ', address_alias, '
                    JOIN ', fault_table, ' ', fault_alias, ' ON ', _address_join_column, ' = ', _fault_join_column, '
                    JOIN fr.laposte_address_street_uniq ', uniq_alias, ' ON ', _join_uniq_fault, '
            WHERE
                fs.fault_id = ', fault_id, '
                AND
                ', _address_update_column, ' IS DISTINCT FROM ', _uniq_value_column, '
                AND
                -- not already exists
                NOT EXISTS(
                    SELECT 1 FROM fr.laposte_address_history h
                    WHERE
                        h.code_address = ', _address_key, '
                        AND
                        h.change = ', quote_literal(address_change), '
                        AND
                        h.values->>', quote_literal(address_join_column), ' = ', _fault_join_column, '
                )
        ')
        ;

    IF NOT simulation THEN
        EXECUTE _query;
        GET DIAGNOSTICS nrows = ROW_COUNT;
    ELSE
        RAISE NOTICE 'requête=%', _query;
        nrows := 0;
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL fr.set_laposte_address_history_index();
END
$$;
