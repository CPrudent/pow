/***
 * add DROP facilities
 */

-- drop all prototypes of a function
DROP FUNCTION IF EXISTS public.drop_all_functions_if_exists(TEXT, TEXT, BOOLEAN, BOOLEAN);
CREATE OR REPLACE FUNCTION public.drop_all_functions_if_exists(
    schema_name TEXT,
    function_name TEXT,
    cascade_mode BOOLEAN DEFAULT TRUE,
    simulation_mode BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _record RECORD;
    _query VARCHAR;
BEGIN
    FOR _record IN (
        SELECT
            pg_catalog.pg_get_function_identity_arguments(pg_proc.oid) AS arguments,
            CASE WHEN prorettype = 2278 THEN 'PROCEDURE' ELSE 'FUNCTION' END AS type
        FROM pg_catalog.pg_proc
        INNER JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
        WHERE
            pg_namespace.nspname = schema_name
            AND pg_proc.proname = LOWER(function_name)
    ) LOOP
        _query := CONCAT(
            'DROP ',
            _record.type,
            ' ',
            schema_name,
            '.',
            function_name,
            '(',
            _record.arguments,
            ')'
        );
        IF cascade_mode THEN _query := CONCAT(_query, ' CASCADE'); END IF;
        RAISE NOTICE '%', _query;
        IF NOT simulation_mode THEN EXECUTE _query; END IF;
    END LOOP;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- drop table constraints
SELECT public.drop_all_functions_if_exists('public', 'drop_table_constraints');
CREATE OR REPLACE FUNCTION public.drop_table_constraints(
    schema_name TEXT,
    table_name TEXT,
    simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _record RECORD;
    _query VARCHAR;
BEGIN
    IF NOT table_exists(schema_name, table_name) THEN RETURN FALSE; END IF;

    FOR _record IN (
        SELECT conname
        FROM pg_catalog.pg_constraint
        INNER JOIN pg_catalog.pg_class AS pg_table_class ON pg_table_class.oid = pg_constraint.conrelid
        INNER JOIN pg_catalog.pg_namespace AS pg_schema_namespace ON pg_schema_namespace.oid = pg_table_class.relnamespace
        WHERE pg_schema_namespace.nspname = schema_name
            AND pg_table_class.relname = table_name
    ) LOOP
        _query := CONCAT(
            'ALTER TABLE ',
            schema_name,
            '.',
            table_name,
            ' DROP CONSTRAINT IF EXISTS "',
            _record.conname,
            '" CASCADE'
        );
        RAISE NOTICE '%', _query;
        IF NOT simulation THEN EXECUTE _query; END IF;
    END LOOP;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.drop_table_constraints('apps_ciblage', 'user_ter', TRUE)
*/

-- drop table indexes
SELECT public.drop_all_functions_if_exists('public', 'drop_table_indexes');
CREATE OR REPLACE FUNCTION public.drop_table_indexes(
    schema_name TEXT,
    table_name TEXT,
    simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _record RECORD;
    _query VARCHAR;
BEGIN
    IF NOT table_exists(schema_name, table_name) THEN RETURN FALSE; END IF;

    FOR _record IN (
        SELECT indexname
        FROM pg_catalog.pg_indexes
        WHERE schemaname = schema_name
            AND tablename = table_name
    ) LOOP
        _query := CONCAT(
            'DROP INDEX IF EXISTS ',
            schema_name,
            '.',
            _record.indexname
        );
        RAISE NOTICE '%', _query;
        IF NOT simulation THEN EXECUTE _query; END IF;
    END LOOP;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- drop table triggers
SELECT public.drop_all_functions_if_exists('public', 'drop_table_triggers');
CREATE OR REPLACE FUNCTION public.drop_table_triggers(
    schema_name TEXT,
    table_name TEXT,
    simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _record RECORD;
    _query VARCHAR;
BEGIN
    IF NOT table_exists(schema_name, table_name) THEN RETURN FALSE; END IF;

    FOR _record IN (
        SELECT trigger_schema, trigger_name
        FROM information_schema.triggers
        WHERE
            event_object_schema = schema_name
            AND event_object_table = table_name
    ) LOOP
        _query := CONCAT(
            'DROP TRIGGER IF EXISTS ',
            _record.trigger_name,
            ' ON ',
            schema_name,
            '.',
            table_name
        );
        RAISE NOTICE '%', _query;
        IF NOT simulation THEN EXECUTE _query; END IF;
    END LOOP;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;
