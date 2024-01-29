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
    address_change IN VARCHAR                            -- fault name
    , address_element IN VARCHAR                         -- AREA|STREET|HOUSENUMBER|COMPLEMENT
    , address_join_column IN VARCHAR                     -- join address table w/ fault table
    , address_update_column IN VARCHAR                   -- column to change
    , fault_id IN INT                                    -- fault ID
    , nrows OUT INT
    , column_with_new_value IN VARCHAR DEFAULT 'name'
    , column_with_old_value IN VARCHAR DEFAULT 'name_before'
    , address_key IN VARCHAR DEFAULT 'co_cea'
    , address_alias IN VARCHAR DEFAULT 'a'
    , fault_alias IN VARCHAR DEFAULT 'f'
    , uniq_alias IN VARCHAR DEFAULT 'u'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$func$
DECLARE
    _query TEXT;
    _address_table VARCHAR;
    _fault_table VARCHAR;
    _uniq_table VARCHAR;
    _address_key VARCHAR := CONCAT(address_alias, '.', address_key);
    _uniq_key VARCHAR := CONCAT(uniq_alias, '.id');
    _fault_key VARCHAR := CONCAT(fault_alias, '.name_id');
    _join_uniq_fault VARCHAR := CONCAT(_fault_key, ' = ', _uniq_key);
    _address_join_column VARCHAR := CONCAT(address_alias, '.', address_join_column);
    _address_update_column VARCHAR := CONCAT(address_alias, '.', address_update_column);
    _column_with_old_value VARCHAR := CONCAT(fault_alias, '.', column_with_old_value);
    _column_with_new_value VARCHAR := CONCAT(uniq_alias, '.', column_with_new_value);
BEGIN
    IF NOT address_element = ANY('{AREA,STREET,HOUSENUMBER,COMPLEMENT}') THEN
        RAISE 'élément adresse (%) non valide!', address_element;
    END IF;

    _address_table := CONCAT('fr.laposte_address_', LOWER(address_element));
    _fault_table := CONCAT('fr.laposte_address_fault_', LOWER(address_element));
    _uniq_table := CONCAT('fr.laposte_address_', LOWER(address_element), '_uniq');

    /*
    INSERT INTO fr.laposte_address_history (
            code_address
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            s.co_cea
            , TIMEOFDAY()::DATE
            , _keys[_fault_i]
            , 'STREET'
            , ROW_TO_JSON(s.*)::JSONB
        FROM
            fr.laposte_address_street s
                JOIN fr.laposte_address_fault_street fs ON s.lb_voie = fs.name_before
                JOIN fr.laposte_address_street_uniq u ON fs.name_id = u.id
        WHERE
            fs.fault_id = _values[_fault_i]::INT
            AND
            s.lb_voie IS DISTINCT FROM u.name
            AND
            NOT EXISTS(
                SELECT 1 FROM fr.laposte_address_history h
                WHERE
                    h.code_address = s.co_cea
                    AND
                    h.change = _keys[_fault_i]
                    AND
                    h.values->>'lb_voie' = fs.name_before
            )
    ;
     */

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
            ', _address_table, ' ', address_alias, '
                    JOIN ', _fault_table, ' ', fault_alias, ' ON ', _address_join_column
                    , ' = ', _column_with_old_value, '
                    JOIN ', _uniq_table, ' ', uniq_alias, ' ON ', _join_uniq_fault, '
            WHERE
                ', fault_alias, '.fault_id = ', fault_id, '
                AND
                ', _address_update_column, ' IS DISTINCT FROM ', _column_with_new_value, '
                AND
                -- not already exists
                NOT EXISTS(
                    SELECT 1 FROM fr.laposte_address_history h
                    WHERE
                        h.code_address = ', _address_key, '
                        AND
                        h.change = ', quote_literal(address_change), '
                        AND
                        h.values->>', quote_literal(address_join_column), ' = ', _column_with_old_value, '
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
$func$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL fr.set_laposte_address_history_index();
END
$$;
