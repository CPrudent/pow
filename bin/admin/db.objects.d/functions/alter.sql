/***
 * add ALTER TABLE facilities
 */

 -- drop NOT NULL if set
SELECT public.drop_all_functions_if_exists('public', 'alter_column_drop_not_null');
CREATE OR REPLACE FUNCTION public.alter_column_drop_not_null(
    schema_name TEXT
    , table_name TEXT
    , column_name TEXT
    , simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _information_schema_column information_schema.columns%ROWTYPE;
    _query VARCHAR;
BEGIN
    IF column_exists(schema_name, table_name, column_name) THEN
        _information_schema_column := public.get_column_information(schema_name, table_name, column_name);
        IF _information_schema_column.is_nullable = 'NO' IS NOT NULL THEN
            _query := CONCAT(
                'ALTER TABLE ONLY '
                , schema_name
                , '.'
                , table_name
                , ' ALTER COLUMN '
                , column_name
                , ' DROP NOT NULL'
            );
            RAISE NOTICE '%', _query;
            IF NOT simulation THEN EXECUTE _query; END IF;
            RETURN TRUE;
        END IF;
    END IF;
    RETURN FALSE;
    END
$func$ LANGUAGE plpgsql;

-- drop DEFAULT if set
SELECT public.drop_all_functions_if_exists('public', 'alter_column_drop_default');
CREATE OR REPLACE FUNCTION public.alter_column_drop_default(
    schema_name TEXT
    , table_name TEXT
    , column_name TEXT
    , simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _information_schema_column information_schema.columns%ROWTYPE;
    _query VARCHAR;
BEGIN
    IF column_exists(schema_name, table_name, column_name) THEN
        _information_schema_column := public.get_column_information(schema_name, table_name, column_name);
        IF _information_schema_column.column_default IS NOT NULL THEN
            _query := CONCAT(
                'ALTER TABLE ONLY '
                , schema_name
                , '.'
                , table_name
                , ' ALTER COLUMN '
                , column_name
                , ' DROP DEFAULT'
            );
            RAISE NOTICE '%', _query;
            IF NOT simulation THEN EXECUTE _query; END IF;
            RETURN TRUE;
        END IF;
    END IF;
    RETURN FALSE;
    END
$func$ LANGUAGE plpgsql;

-- change SCHEMA (one table)
SELECT public.drop_all_functions_if_exists('public', 'alter_table_change_schema');
CREATE OR REPLACE FUNCTION public.alter_table_change_schema(
    schema_name_from TEXT
    , schema_name_to TEXT
    , table_name TEXT
    , simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query VARCHAR;
BEGIN
    IF schema_exists(schema_name_from) AND schema_exists(schema_name_to) AND table_exists(schema_name_from, table_name) THEN
        _query := CONCAT(
            'ALTER TABLE '
            , quote_ident(schema_name_from)
            , '.'
            , quote_ident(table_name)
            , ' SET SCHEMA '
            , quote_ident(schema_name_to)
        );
        RAISE NOTICE '%', _query;
        IF NOT simulation THEN EXECUTE _query; END IF;
        RETURN TRUE;
    END IF;
    RETURN FALSE;
    END
$func$ LANGUAGE plpgsql;

-- change SCHEMA (all tables)
SELECT public.drop_all_functions_if_exists('public', 'alter_tables_change_schema');
CREATE OR REPLACE FUNCTION public.alter_tables_change_schema(
    schema_name_from TEXT
    , schema_name_to TEXT
    , simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _record RECORD;
    _result BOOLEAN;
BEGIN
    IF schema_exists(schema_name_from) AND schema_exists(schema_name_to) THEN
        FOR _record IN (
            SELECT tablename FROM pg_tables WHERE schemaname = schema_name_from
        )
        LOOP
            _result := public.alter_table_change_schema(schema_name_from, schema_name_to, _record.tablename, simulation);
            IF NOT _result THEN return FALSE; END IF;
        END LOOP;
        RETURN TRUE;
    END IF;
    RETURN FALSE;
    END
$func$ LANGUAGE plpgsql;
