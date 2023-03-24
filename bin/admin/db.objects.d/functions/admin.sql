/***
 * add ADMIN facilities
 */

-- get columns of table
SELECT public.drop_all_functions_if_exists('public', 'get_table_columns');
CREATE FUNCTION public.get_table_columns(
    schema_name TEXT
    , table_name TEXT
    )
RETURNS TEXT[] AS
$func$
DECLARE
    _columns TEXT[];
BEGIN
    SELECT ARRAY_AGG(column_name)
    INTO _columns
    FROM information_schema.columns c
    WHERE c.table_schema = schema_name AND c.table_name = get_table_columns.table_name;

    RETURN _columns;
END
$func$ LANGUAGE plpgsql;

-- get information of column
SELECT public.drop_all_functions_if_exists('public', 'get_column_information');
CREATE OR REPLACE FUNCTION public.get_column_information(
    schema_name TEXT
    , table_name TEXT
    , column_name TEXT
    )
RETURNS information_schema.columns AS
$func$
DECLARE
    _information_schema_column information_schema.columns%ROWTYPE;
BEGIN
    SELECT * INTO _information_schema_column
    FROM information_schema.columns c
    WHERE c.table_schema = schema_name
        AND c.table_name = get_column_information.table_name
        AND c.column_name = get_column_information.column_name;

    RETURN _information_schema_column;
END
$func$ LANGUAGE plpgsql;

-- add NOTICE with date/hour
SELECT public.drop_all_functions_if_exists('public', 'log_info');
CREATE OR REPLACE PROCEDURE public.log_info(
    message TEXT
    , stamped BOOLEAN DEFAULT TRUE
) AS
$proc$
BEGIN
    IF stamped THEN
        RAISE NOTICE '% %', TO_CHAR(clock_timestamp(), 'HH24:MI:SS.MS'), log_info.message;
    ELSE
        RAISE NOTICE '%', log_info.message;
    END IF;
END
$proc$ LANGUAGE plpgsql;

/*
 * TESTS
DO $$
BEGIN
    CALL log_info('Hello world');
    PERFORM pg_sleep(3);
    CALL log_info('That''s all, bye!');
END $$;
 */

-- get nrows from Query plan
SELECT public.drop_all_functions_if_exists('public', 'count_estimate');
CREATE OR REPLACE FUNCTION public.count_estimate(
    query TEXT
    )
RETURNS INTEGER AS
$func$
DECLARE
    _record RECORD;
    _nrows INTEGER;
BEGIN
    FOR _record IN EXECUTE 'EXPLAIN ' || query LOOP
        _nrows := SUBSTRING(_record."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
        EXIT WHEN _nrows IS NOT NULL;
    END LOOP;
 
    RETURN _nrows;
END
$func$ LANGUAGE plpgsql;

-- wait if VACUUM
SELECT public.drop_all_functions_if_exists('public', 'wait_if_vacuum');
CREATE OR REPLACE FUNCTION public.wait_if_vacuum(
    table_name IN TEXT
    , wait_seconds IN INTEGER DEFAULT 5
    )
RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	RETURN public.wait_if_vacuum(ARRAY[table_name], wait_seconds);
END
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.wait_if_vacuum(
    table_names IN TEXT[] DEFAULT NULL
    , wait_seconds IN INTEGER DEFAULT 5
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    _vacuum_inprogress BOOLEAN;
    _query TEXT;
    _counter INTEGER := 0;
    _wait_autovacuum INTEGER := 70; -- AUTOVACUUM delay
    _pattern TEXT;
BEGIN
    IF table_names IS NOT NULL THEN
        _pattern := REPLACE(CONCAT('\m(?:', ARRAY_TO_STRING(table_names, '|'), ')\M'), '.', '\.');
    END IF;

    LOOP
        BEGIN
            SELECT TRUE /*I'M NOT A VACUUM*/, query
            INTO STRICT _vacuum_inprogress, _query
            FROM pg_stat_activity
            WHERE query ILIKE '%VACUUM%'
            -- auto-exclusion
            AND query NOT LIKE '%wait_if_vacuum%'
            AND (table_names IS NULL OR query ~ _pattern)
            LIMIT 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- before AUTOVACUUM delay
                IF (_counter * wait_seconds) > _wait_autovacuum THEN
                    EXIT;
                END IF;
        END;

        IF _vacuum_inprogress THEN
            -- no more useful to wait for AUTOVACUUM
            _wait_autovacuum := 0;
            RAISE NOTICE 'VACUUM EN COURS % : %',
                (
                CASE WHEN table_names IS NOT NULL THEN
                    CONCAT('SUR ', ARRAY_TO_STRING(table_names, ', '))
                ELSE
                    ''
                END
                ), _query;
        ELSE
            RAISE NOTICE 'PAS DE VACUUM EN COURS %',
                (
                CASE WHEN table_names IS NOT NULL THEN
                    CONCAT('SUR ', ARRAY_TO_STRING(table_names, ', '))
                ELSE ''
                END
                );
        END IF;

        _counter := _counter + 1;

        RAISE NOTICE 'Attente de % secondes (x %)', wait_seconds, _counter;
        PERFORM pg_sleep(wait_seconds);
        -- refresh stats (for this transaction)
        PERFORM pg_stat_clear_snapshot();
    END LOOP;

    RAISE NOTICE 'FIN, PAS DE VACUUM EN COURS %',
        (
        CASE WHEN table_names IS NOT NULL THEN
            CONCAT('SUR ', ARRAY_TO_STRING(table_names, ', '))
        ELSE
            ''
        END
        );
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'log_table_stat');
CREATE OR REPLACE PROCEDURE public.log_table_stat(
    log_name IN TEXT
    , schema_name IN TEXT
    , table_name IN TEXT
    )
AS
$proc$
DECLARE
    _info TEXT;
BEGIN
    -- NOTE tables created in user's schema
    CREATE TABLE IF NOT EXISTS log_pg_stat_user_tables AS (
        SELECT NULL::VARCHAR, NULL::TIMESTAMP, * FROM pg_stat_user_tables LIMIT 0
    ) WITH NO DATA;

    INSERT INTO log_pg_stat_user_tables (
        SELECT log_name, now(), * FROM pg_stat_user_tables WHERE schemaname = schema_name AND relname = table_name
    );

    CREATE TABLE IF NOT EXISTS log_pg_stat_xact_all_tables AS (
        SELECT NULL::VARCHAR, NULL::TIMESTAMP, * FROM pg_stat_xact_all_tables LIMIT 0
    ) WITH NO DATA;

    INSERT INTO log_pg_stat_xact_all_tables (
        SELECT log_name, now(), * FROM pg_stat_xact_all_tables WHERE schemaname = schema_name AND relname = table_name
    );

    /* TEST
    IF table_names IS NOT NULL AND ARRAY_LENGTH(table_names, 1) = 1 THEN
        SELECT (pg_stat_user_tables.*)::TEXT INTO _info FROM pg_stat_user_tables WHERE schemaname = schema_name AND relname = table_name;

        RAISE NOTICE 'pg_stat_user_tables=%', _info;

        SELECT (pg_stat_xact_all_tables.*)::TEXT INTO _info FROM pg_stat_xact_all_tables WHERE schemaname = schema_name AND relname = table_name;
        RAISE NOTICE 'pg_stat_xact_all_tables=%', _info;
    END IF;
    */
END;
$proc$ LANGUAGE plpgsql;

/* TESTS

1) run VACUUM :
VACUUM FULL adresse_ran;

2) in an other session, call the function :
PGSQL :
PERFORM public.wait_if_vacuum();
SQL :
SELECT public.wait_if_vacuum();
-- on a table which contains "adresse_ran" (adresse_ran, adresse_ran_has_pdi, ...)
SELECT public.wait_if_vacuum('adresse_ran');
-- with 30s between each test
SELECT public.wait_if_vacuum('adresse_ran', 30);

SELECT log_table_stat('TEST', 'public', 'adresse_ran');
SELECT * FROM log_pg_stat_user_tables
SELECT * FROM log_pg_stat_xact_all_tables

TRUNCATE TABLE log_pg_stat_user_tables;
TRUNCATE TABLE log_pg_stat_xact_all_tables;
 */
