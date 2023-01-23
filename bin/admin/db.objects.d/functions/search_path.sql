/***
 * add SEARCH_PATH facilities
 */

-- set
SELECT public.drop_all_functions_if_exists('public', 'set_search_path');
CREATE OR REPLACE FUNCTION public.set_search_path(
    search_path IN VARCHAR
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
    schema_name IN TEXT
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    _search_paths TEXT[];
BEGIN
    SELECT STRING_TO_ARRAY(REPLACE(CURRENT_SETTING('search_path'), ' ', ''), ', ') INTO _search_paths;
    IF NOT _search_paths @> ARRAY[schema_name] THEN
        _search_paths := ARRAY_APPEND(_search_paths, schema_name);
        PERFORM public.set_search_path(ARRAY_TO_STRING(_search_paths, ', '));
    END IF;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- del
SELECT public.drop_all_functions_if_exists('public', 'remove_from_search_path');
CREATE OR REPLACE FUNCTION public.remove_from_search_path(
    schema_name IN TEXT
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    _search_paths TEXT[];
BEGIN
    SELECT STRING_TO_ARRAY(REPLACE(CURRENT_SETTING('search_path'), ' ', ''), ', ') INTO _search_paths;
    IF _search_paths @> ARRAY[schema_name] THEN
        _search_paths := ARRAY_REMOVE(_search_paths, schema_name);
        PERFORM public.set_search_path(ARRAY_TO_STRING(search_path, ', '));
    END IF;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;
