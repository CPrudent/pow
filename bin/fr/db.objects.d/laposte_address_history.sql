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
    element IN VARCHAR                        -- AREA|STREET|HOUSENUMBER|COMPLEMENT
    , column_update IN VARCHAR                -- column to change
    , fault_name IN VARCHAR                   -- fault name
    , fault_id IN INT                         -- fault ID
    , column_with_new_value IN VARCHAR DEFAULT 'name'
    , key_address IN VARCHAR DEFAULT 'co_cea'
    , alias_address IN VARCHAR DEFAULT 'a'
    , alias_fault IN VARCHAR DEFAULT 'f'
    , alias_uniq IN VARCHAR DEFAULT 'u'
    , alias_reference IN VARCHAR DEFAULT 'r'
    , simulation IN BOOLEAN DEFAULT FALSE
    , nrows OUT INT
)
AS
$func$
DECLARE
    _query TEXT;
    _table_address VARCHAR;
    _table_uniq VARCHAR;
    _table_reference VARCHAR;
    _key_address VARCHAR := CONCAT(alias_address, '.', key_address);
    _key_fault VARCHAR := CONCAT(alias_fault, '.name_id');
    _key_uniq VARCHAR := CONCAT(alias_uniq, '.id');
    _key_reference VARCHAR := CONCAT(alias_reference, '.address_id');
    _join_uniq_fault VARCHAR := CONCAT(_key_fault, ' = ', _key_uniq);
    _join_uniq_reference VARCHAR := CONCAT(alias_reference, '.name_id = ', _key_uniq);
    _column_update VARCHAR := CONCAT(alias_address, '.', column_update);
    _column_with_new_value VARCHAR := CASE
        WHEN count_words(column_with_new_value) = 1 THEN
            CONCAT(alias_uniq, '.', column_with_new_value)
        ELSE
            column_with_new_value
        END
        ;
BEGIN
    IF NOT element = ANY('{AREA,STREET,HOUSENUMBER,COMPLEMENT}') THEN
        RAISE 'élément ADRESSE (%) non valide!', element;
    END IF;

    _table_uniq := CONCAT('fr', fr.get_table_name(element, 'UNIQ'));
    _table_reference := CONCAT('fr', fr.get_table_name(element, 'REFERENCE'));
    _table_address := CONCAT('fr', fr.get_table_name(element, 'ADDRESS'));

    _query := CONCAT('
        INSERT INTO fr.laposte_address_history (
                code_address
                , date_change
                , change
                , kind
                , values
            )
            SELECT
            ', _key_address, '
                , TIMEOFDAY()::DATE
                , ', quote_literal(fault_name), '
                , ', quote_literal(element), '
                , ROW_TO_JSON(', alias_address, '.*)::JSONB
            FROM
            ', _table_address, ' ', alias_address, '
                JOIN ', _table_reference, ' ', alias_reference, ' ON ', _key_address, ' = ', _key_reference, '
                JOIN ', _table_uniq, ' ', alias_uniq, ' ON ', _join_uniq_reference
    );
    IF fault_id >= 0 THEN
        _query := CONCAT(_query
            , '
                JOIN fr.laposte_address_fault ', alias_fault, ' ON ', _join_uniq_fault
        );
    END IF;
    _query := CONCAT(_query
            , '
            WHERE
            '
    );
    IF fault_id >= 0 THEN
        _query := CONCAT(_query
            , alias_fault, '.element = ', quote_literal(element), '
                AND
                '
            , alias_fault, '.fault_id = ', fault_id, '
                AND
                '
        );
    END IF;
    _query := CONCAT(_query
            , _column_update, ' IS DISTINCT FROM ', _column_with_new_value, '
            AND
            -- not already exists
            NOT EXISTS(
                SELECT 1 FROM fr.laposte_address_history h
                WHERE
                    h.code_address = ', _key_address, '
                    AND
                    h.change = ', quote_literal(fault_name), '
                    AND
                    h.values->>', quote_literal(column_update), ' = ', _column_update, '
            )
        '
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

DO $$
BEGIN
    CALL fr.set_laposte_address_history_index();
END
$$;
