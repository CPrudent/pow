/***
 * LAPOSTE ADDRESS history
 */

CREATE TABLE IF NOT EXISTS fr.laposte_address_history (
    code_address CHAR(10) NOT NULL          -- address ID
    , date_change DATE NOT NULL
    , change VARCHAR NOT NULL               -- defined into fr.constant (LAPOSTE_ADDRESS_CORRECTION)
    , kind VARCHAR NOT NULL                 -- {ADDRESS, AREA, STREET, HOUSENUMBER, COMPLEMENT}
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
    address_change IN VARCHAR                           -- fault name
    , address_element IN VARCHAR                        -- AREA|STREET|HOUSENUMBER|COMPLEMENT
    , address_update_column IN VARCHAR                  -- column to change
    , fault_id IN INT                                   -- fault ID (or 0 if NONE)
    , column_with_new_value IN VARCHAR DEFAULT 'name'
    , address_key IN VARCHAR DEFAULT 'co_cea'
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
    _address_key VARCHAR := CONCAT(address_alias, '.', address_key);
    _fault_key VARCHAR := CONCAT(fault_alias, '.name_id');
    _uniq_key VARCHAR := CONCAT(uniq_alias, '.id');
    _reference_key VARCHAR := CONCAT(reference_alias, '.address_id');
    _join_uniq_fault VARCHAR := CONCAT(_fault_key, ' = ', _uniq_key);
    _join_uniq_reference VARCHAR := CONCAT(reference_alias, '.name_id = ', _uniq_key);
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
                    JOIN ', _reference_table, ' ', reference_alias, ' ON ', _address_key, ' = ', _reference_key, '
                    JOIN ', _uniq_table, ' ', uniq_alias, ' ON ', _join_uniq_reference
    );
    IF fault_id > 0 THEN
        _query := CONCAT(_query
            , '
                    JOIN ', _fault_table, ' ', fault_alias, ' ON ', _join_uniq_fault
        );
    END IF;
    _query := CONCAT(_query
            , '
            WHERE
            '
    );
    IF fault_id > 0 THEN
        _query := CONCAT(_query
            , fault_alias, '.fault_id = ', fault_id, '
                AND
                '
        );
    END IF;
    _query := CONCAT(_query
                , _address_update_column, ' IS DISTINCT FROM ', _column_with_new_value, '
                AND
                -- not already exists
                NOT EXISTS(
                    SELECT 1 FROM fr.laposte_address_history h
                    WHERE
                        h.code_address = ', _address_key, '
                        AND
                        h.change = ', quote_literal(address_change), '
                        AND
                        h.values->>', quote_literal(address_update_column), ' = ', _address_update_column, '
                )
        '
    );

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
