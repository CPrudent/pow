/***
 * TERRITORY
 */

CREATE TABLE IF NOT EXISTS public.territory (
    id SERIAL NOT NULL
    , country CHAR(2) NOT NULL                  -- code ISO-3166, like FR for France
    , level CHARACTER VARYING NOT NULL
    , code CHARACTER VARYING NOT NULL
    , name CHARACTER VARYING
    , population INT
    , area INT
    , codes_adjoining VARCHAR[]                 -- list of nearing territories (same level)
    , attributs HSTORE                          -- more attributs
    , date_last DATE
    , geom_native GEOMETRY                      -- native geography (local)
    , geom_world GEOMETRY(MULTIPOLYGON, 4326)   -- WGS84-proj & simplified geography
)
;

-- manual VACUUM
ALTER TABLE public.territory SET (
	autovacuum_enabled = FALSE
);

DO $$
BEGIN
    IF column_exists('public', 'territory', 'date_geography') THEN
        ALTER TABLE public.territory RENAME COLUMN date_geography TO date_last;
    END IF;
END $$;

/*
 * FR-attributs
 *
 * ZA
 *  L5_normalized=>normalized name
 * COM
 *  L6_normalized=>normalized name
 * EPCI
 *  type=>type of EPCI
 */

SELECT drop_all_functions_if_exists('public', 'set_territory_index');
CREATE OR REPLACE PROCEDURE public.set_territory_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_id ON public.territory (id);
    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_level_code ON public.territory (country, level, code);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_territory_index();
END
$$;

SELECT drop_all_functions_if_exists('public', 'set_territory');
CREATE OR REPLACE PROCEDURE public.set_territory(
    force BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _schema_name VARCHAR;
    _procedure_name VARCHAR := 'push_territory_to_public';
    _query TEXT;
BEGIN
    FOR _schema_name IN (
        SELECT schema_name FROM information_schema.schemata
        WHERE
            schema_name ~ '^..$'
    )
    LOOP
        IF procedure_exists(_schema_name, _procedure_name) THEN
            _query := CONCAT(
                'CALL '
                , _schema_name
                , '.'
                , _procedure_name
                , '()'
            );

            EXECUTE _query;
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

--
-- facilities to navigate through territories
--

SELECT public.drop_all_functions_if_exists('public','get_territory_from_query');
CREATE OR REPLACE FUNCTION public.get_territory_from_query(
    query TEXT
    , raise_notice BOOLEAN DEFAULT FALSE
)
RETURNS SETOF public.territory AS
$func$
DECLARE
BEGIN
    IF raise_notice THEN
        RAISE NOTICE '%', query;
    END IF;
    RETURN QUERY EXECUTE get_territory_from_query.query;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public','get_alias_from_level');
CREATE OR REPLACE FUNCTION public.get_alias_from_level(
    level_in VARCHAR
)
RETURNS TEXT AS
$func$
DECLARE
BEGIN
    RETURN CONCAT('territory_', level_in);
END
$func$ LANGUAGE plpgsql;

/*
SELECT get_alias_from_level('COM') --> territory_COM
 */

SELECT public.drop_all_functions_if_exists('public','get_alias_from_query');
CREATE OR REPLACE FUNCTION public.get_alias_from_query(
    query TEXT
)
RETURNS TEXT AS
$func$
DECLARE
BEGIN
    RETURN (REGEXP_MATCHES(query,'AS (territory_[A-Z0-9_]+)'))[1];
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public','get_level_from_query');
CREATE OR REPLACE FUNCTION public.get_level_from_query(
    query TEXT
)
RETURNS TEXT AS
$func$
DECLARE
BEGIN
	RETURN (REGEXP_MATCHES(public.get_alias_from_query(query), 'territory_([A-Z_]+)'))[1];
END
$func$ LANGUAGE plpgsql;


SELECT public.drop_all_functions_if_exists('public','get_territory_query');
SELECT public.drop_all_functions_if_exists('public','get_query_territory');
CREATE OR REPLACE FUNCTION public.get_query_territory(
    country VARCHAR
    , level_in VARCHAR
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _alias VARCHAR;
BEGIN
    _alias := public.get_alias_from_level(level_in);
    _query := CONCAT('
        SELECT *
        FROM territory AS ', _alias,'
        WHERE ', _alias, '.country = ''', UPPER(country), ''' AND '
        , _alias, '.level = ''', level_in, '''
        /* WHERE-AND */
        '
    );

    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/*
SELECT level, code FROM get_territory_from_query(get_query_territory('EPCI'))
 */

CREATE OR REPLACE FUNCTION public.get_query_territory(
    country VARCHAR
    , level_in VARCHAR
    , code VARCHAR
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _alias VARCHAR;
BEGIN
    _query := public.get_query_territory(country, level_in);
    _alias := public.get_alias_from_level(level_in);
    _query := REPLACE(
                _query
                , '/* WHERE-AND */'
                , CONCAT('AND ', _alias, '.code = ''', code,'''')
            );
    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/*
SELECT level, code FROM get_territory_from_query(get_query_territory('EPCI','245900758'))
SELECT level, code FROM public.get_territory_from_query(public.get_query_territory('COM','84033'), true)
SELECT level, code FROM public.get_territory_from_query(public.get_query_territory('COM','84033','2016-05-05'::DATE), true)
 */

CREATE OR REPLACE FUNCTION public.get_query_territory(
    country VARCHAR
    , level_in VARCHAR
    , code VARCHAR[]
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _alias VARCHAR;
BEGIN
    _query := public.get_query_territory(country, level_in);
    _alias := public.get_alias_from_level(level_in);
    _query := REPLACE(
                _query
                , '/* WHERE-AND */'
                , CONCAT('AND ', _alias, '.code = ANY(''{', ARRAY_TO_STRING(code, ','), '}''::VARCHAR[])')
            );
    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/*
SELECT level, code, name
FROM get_territory_from_query(
    get_query_territory('FR', 'COM', ARRAY['84033','84007'])
)
 */

-- list of linked territories from territory given by query, (UP or DOWN, as parents or childs)
SELECT public.drop_all_functions_if_exists('public', 'get_linked_territory_query');
SELECT public.drop_all_functions_if_exists('public', 'get_query_linked_territory');
CREATE OR REPLACE FUNCTION public.get_query_linked_territory(
    country VARCHAR
    , query TEXT
    , to_levels VARCHAR[]
    , direction VARCHAR DEFAULT 'UP'
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _from_alias VARCHAR := public.get_alias_from_query(query);
BEGIN
    _query := CONCAT('
        WITH
        list_of_territory_links AS (
            WITH
            RECURSIVE links(id_territory, id_parent, depth) AS (
                SELECT _parent.id_territory, _parent.id_parent, 1
                FROM (', query, ') ', _from_alias
                    , ' JOIN public.territory_parent _parent ON ', _from_alias, '.id = _parent.'
                , CASE WHEN direction = 'UP' THEN 'id_territory'
                ELSE 'id_parent'
                END
                , ' WHERE ', _from_alias, '.country = ''', UPPER(country), '''
                UNION
                SELECT _parent.id_territory, _parent.id_parent, links.depth '
                , CASE WHEN direction = 'UP' THEN '+1'
                ELSE '-1'
                END
                , ' FROM public.territory_parent _parent
                    JOIN links ON '
                , CASE WHEN direction = 'UP' THEN '_parent.id_territory = links.id_parent'
                ELSE '_parent.id_parent = links.id_territory'
                END
                , ')

            SELECT * FROM links
        )
        , linked_territory_ids AS (
            SELECT
                ARRAY_AGG(DISTINCT id_territory) a1
                , ARRAY_AGG(DISTINCT id_parent) a2
            FROM
                list_of_territory_links
        )
        SELECT territory.*
        FROM public.territory JOIN (
            SELECT UNNEST(array_merge(a1, a2)) id FROM linked_territory_ids
            ) t ON territory.id = t.id
        WHERE
            territory.level = ANY(''{', ARRAY_TO_STRING(to_levels, ','), '}''::VARCHAR[])'
    );

    RETURN _query;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'get_territory_query_to_level');
SELECT public.drop_all_functions_if_exists('public', 'get_query_territory_extended_to_level');
CREATE OR REPLACE FUNCTION public.get_query_territory_extended_to_level(
    country VARCHAR
    , query TEXT
    , to_level VARCHAR
    , from_level VARCHAR DEFAULT NULL
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _usecase VARCHAR;
    _to_common_level VARCHAR;
    _from_level VARCHAR := COALESCE(from_level, public.get_level_from_query(query));

    /*
    _from_alias VARCHAR := public.get_alias_from_query(query);
    _to_alias VARCHAR;
     */
BEGIN
    IF _from_level = to_level THEN
        RETURN get_query_territory_extended_to_level.query;
    END IF;

    _usecase :=
        CASE
        WHEN public.is_level_below(country, _from_level, to_level) THEN
            'UP'
        WHEN public.is_level_below(country, to_level, _from_level) THEN
            'DOWN'
        ELSE
            -- no direct/indirect links, has to find common level
            'COMMON'
        END;

    IF _usecase = 'COMMON' THEN
        _to_common_level := public.get_common_level(country, _from_level, to_level);
        _query := public.get_query_territory_extended_to_level(
            country
            , public.get_query_territory_extended_to_level(country, query, _to_common_level)
            , to_level
            , _to_common_level
        );
    ELSE
        _query := public.get_query_linked_territory(
            country
            , query
            , ARRAY[to_level]::VARCHAR[]
            , _usecase
        );
    END IF;

    RETURN _query;

    /*
        _query := CONCAT('
            WITH
            territory_parents_id AS (
                WITH
                RECURSIVE parents(id_territory, id_parent, depth) AS (
                    SELECT _parent.id_territory, _parent.id_parent, 1
                    FROM ', query, '
                        JOIN public.territory_parent _parent ON ', _from_alias, '.id = _parent.id_territory
                    WHERE
                        _from.country = ', UPPER(country), '

                    UNION

                    SELECT _parent.id_territory, _parent.id_parent, parents.depth +1
                    FROM public.territory_parent _parent
                        JOIN parents ON _parent.id_territory = parents.id_parent
                )

                SELECT * FROM parents
            )
            , territory_parents AS (
                SELECT
                    ARRAY_AGG(DISTINCT id_territory) a1
                    , ARRAY_AGG(DISTINCT id_parent) a2
                FROM
                    territory_parents_id
            )
            SELECT territory.*
            FROM public.territory JOIN (
                SELECT UNNEST(array_merge(a1, a2)) id FROM territory_parents
                ) t ON territory.id = t.id
            WHERE
                territory.level = ''', to_level, '''
            '
            );

        RETURN _query;
    ELSE
        _to_common_level := public.get_common_level(country, _from_level, to_level);
        RETURN public.get_query_territory_extended_to_level(
            country
            , public.get_query_territory_extended_to_level(country, query, _to_common_level)
            , to_level
        );
    END IF;
     */
END
$func$ LANGUAGE plpgsql;
