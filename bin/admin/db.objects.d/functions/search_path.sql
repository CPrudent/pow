/***
 * add SEARCH_PATH facilities
 */

-- set
SELECT public.drop_all_functions_if_exists('public', 'set_search_path');
CREATE OR REPLACE FUNCTION public.set_search_path(
    search_path VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query VARCHAR;
BEGIN
    -- for current session
    PERFORM SET_CONFIG('search_path', search_path, FALSE);
    -- for new sessions
    _query := CONCAT('ALTER DATABASE ', CURRENT_DATABASE(), ' SET search_path = ', search_path);
    RAISE NOTICE 'requete = %', _query;
    EXECUTE _query;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- add
SELECT public.drop_all_functions_if_exists('public', 'add_to_search_path');
CREATE OR REPLACE FUNCTION public.add_to_search_path(
    schema_name TEXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _search_path TEXT[];
BEGIN
    SELECT STRING_TO_ARRAY(REPLACE(CURRENT_SETTING('search_path'), ' ', ''), ',') INTO _search_path;
    IF NOT _search_path @> ARRAY[schema_name] THEN
        _search_path := ARRAY_APPEND(_search_path, schema_name);
        PERFORM public.set_search_path(ARRAY_TO_STRING(_search_path, ', '));
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

-- delete
SELECT public.drop_all_functions_if_exists('public', 'remove_from_search_path');
CREATE OR REPLACE FUNCTION public.remove_from_search_path(
    schema_name TEXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _search_path TEXT[];
BEGIN
    SELECT STRING_TO_ARRAY(REPLACE(CURRENT_SETTING('search_path'), ' ', ''), ',') INTO _search_path;
    IF _search_path @> ARRAY[schema_name] THEN
        _search_path := ARRAY_REMOVE(_search_path, schema_name);
        PERFORM public.set_search_path(ARRAY_TO_STRING(_search_path, ', '));
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;
