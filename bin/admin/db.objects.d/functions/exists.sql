/***
 * add EXISTS facilities
 */

-- test if table exists
SELECT public.drop_all_functions_if_exists('public', 'table_exists');
CREATE OR REPLACE FUNCTION public.table_exists(
    schema_name TEXT
    , table_name TEXT
    , temporary_mode BOOLEAN DEFAULT FALSE
    )
RETURNS BOOLEAN AS
$func$
BEGIN
    IF NOT temporary_mode THEN
        PERFORM TRUE
        FROM information_schema.tables t
        WHERE t.table_schema = schema_name
            AND t.table_name = table_exists.table_name
            AND t.table_type = 'BASE TABLE';
    ELSE
        -- see https://stackoverflow.com/questions/11224806/how-can-i-detect-if-a-postgres-temporary-table-already-exists
        PERFORM n.nspname, c.relname
        FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE
            (n.nspname LIKE 'pg_temp_%')
            AND
            (pg_catalog.pg_table_is_visible(c.oid))
            AND
            (UPPER(relname) = UPPER(table_name))
            AND
                /*
                    r = ordinary table
                    i = index
                    S = sequence
                    v = view
                    m = materialized view
                    c = composite type
                    t = TOAST table
                    f = foreign table
                 */
            (relkind = 'r');
    END IF;
    RETURN FOUND;
END
$func$ LANGUAGE plpgsql;

/*
 * TESTS

DO $$
BEGIN
	RAISE NOTICE 'table ordinaire %.% : %', 'ran', 'za', table_exists('ran', 'za');
	RAISE NOTICE 'table ordinaire %.% : %', 'public', 'suivi_adn', table_exists('public', 'suivi_adn');

	RAISE NOTICE 'table temporaire % : %', 'tmp_test_1', table_exists('<FOO>', 'tmp_test_1', TRUE);
	CREATE TEMPORARY TABLE tmp_test_1 AS SELECT * FROM public.suivi_adn WITH NO DATA;
	RAISE NOTICE 'table temporaire % : %', 'tmp_test_1', table_exists('<FOO>', 'tmp_test_1', TRUE);
	DROP TABLE tmp_test_1;
	RAISE NOTICE 'table temporaire % : %', 'tmp_test_1', table_exists('<FOO>', 'tmp_test_1', TRUE);
END $$;

 */

-- test if view exists
SELECT public.drop_all_functions_if_exists('public', 'view_exists');
CREATE FUNCTION public.view_exists(
    schema_name TEXT
    , view_name TEXT
    )
RETURNS BOOLEAN AS
$func$
DECLARE
	_exists BOOLEAN;
BEGIN
    SELECT TRUE INTO _exists
    FROM information_schema.views v
    WHERE v.table_schema = schema_name
        AND v.table_name = view_name;
    RETURN _exists;
END
$func$ LANGUAGE plpgsql;

-- test if index exists
SELECT public.drop_all_functions_if_exists('public', 'index_exists');
CREATE FUNCTION public.index_exists(
    schema_name TEXT
    , index_name TEXT
    )
RETURNS BOOLEAN AS
$func$
DECLARE
	_exists BOOLEAN;
BEGIN
    SELECT TRUE INTO _exists
    FROM pg_catalog.pg_indexes
    WHERE indexname = index_name
    AND schemaname = schema_name;
    RETURN _exists;
END
$func$ LANGUAGE plpgsql;

-- test if column exists
SELECT public.drop_all_functions_if_exists('public', 'column_exists');
CREATE FUNCTION public.column_exists(
    schema_name TEXT
    , table_name TEXT
    , column_name TEXT
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    _exists BOOLEAN;
BEGIN
    SELECT TRUE INTO _exists
    FROM information_schema.columns c
    WHERE c.table_schema = schema_name
        AND c.table_name = table_name
        AND c.column_name = column_name;
    RETURN _exists;
END
$func$ LANGUAGE plpgsql;

-- test if function exists
SELECT public.drop_all_functions_if_exists('public', 'function_exists');
CREATE FUNCTION public.function_exists(
    schema_name TEXT
    , function_name TEXT
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    _exists BOOLEAN;
BEGIN
    SELECT TRUE INTO _exists
    FROM information_schema.routines
    WHERE routine_schema = schema_name
        AND routine_name = LOWER(function_name);
    RETURN _exists;
END
$func$ LANGUAGE plpgsql;

-- test if extension exists
SELECT public.drop_all_functions_if_exists('public', 'extension_exists');
CREATE OR REPLACE FUNCTION public.extension_exists(
    extension_name IN VARCHAR
    )
RETURNS BOOLEAN AS
$func$
DECLARE
	_extension pg_catalog.pg_extension%ROWTYPE;
BEGIN
	SELECT * INTO STRICT _extension
	FROM pg_catalog.pg_extension
	WHERE extname = extension_name;
	RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
	RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

-- test if role exists
SELECT public.drop_all_functions_if_exists('public', 'role_exists');
CREATE OR REPLACE FUNCTION public.role_exists(
    role_name IN VARCHAR
    )
RETURNS BOOLEAN AS
$func$
DECLARE
	_role pg_catalog.pg_authid%ROWTYPE;
BEGIN
	SELECT * INTO STRICT _role
	FROM pg_catalog.pg_authid
	WHERE rolname = role_name;
	RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
	RETURN FALSE;
END
$func$ LANGUAGE plpgsql;
