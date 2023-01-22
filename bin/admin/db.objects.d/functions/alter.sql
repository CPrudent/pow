/***
 * add facilities to ALTER TABLE
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
