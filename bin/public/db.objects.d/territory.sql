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
CREATE OR REPLACE FUNCTION public.get_territory_query(
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
SELECT level, code FROM get_territory_from_query(get_territory_query('EPCI'))
 */

CREATE OR REPLACE FUNCTION public.get_territory_query(
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
    _query := public.get_territory_query(country, level_in);
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
SELECT level, code FROM get_territory_from_query(get_territory_query('EPCI','245900758'))
SELECT level, code FROM public.get_territory_from_query(public.get_territory_query('COM','84033'), true)
SELECT level, code FROM public.get_territory_from_query(public.get_territory_query('COM','84033','2016-05-05'::DATE), true)
 */

CREATE OR REPLACE FUNCTION public.get_territory_query(
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
    _query := public.get_territory_query(country, level_in);
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
    get_territory_query('FR', 'COM', ARRAY['84033','84007'])
)
 */

SELECT public.drop_all_functions_if_exists('public', 'get_territory_query_to_level');
CREATE OR REPLACE FUNCTION public.get_territory_query_to_level(
    country VARCHAR
    , query TEXT
    , to_level VARCHAR
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _to_common_level VARCHAR;
    _from_alias VARCHAR := public.get_alias_from_query(query);
    _from_level VARCHAR := public.get_level_from_query(query);
    _to_alias VARCHAR;
BEGIN
    IF _from_level = to_level THEN
        RETURN get_territory_query_to_level.query;
    END IF;

    IF (
        public.is_level_below(country, _from_level, to_level)
        OR
        public.is_level_below(country, to_level, _from_level)) THEN
        _to_alias := public.get_alias_from_level(to_level);
        -- NOTE this solution needs to declare all links (territory_parent)
        -- COM -> DEP (and not only COM -> CV & CV -> DEP)
        _query := CONCAT(
            public.get_territory_query(country, to_level),'
            AND EXISTS (
                ', query, '
                AND EXISTS (
                    SELECT 1
                    FROM public.territory _from
                        JOIN public.territory_parent _parent ON _from.id = _parent.id_territory
                        JOIN public.territory _to ON _to.id = _parent.id_parent
                    WHERE
                        _from.level = ''', _from_level, '''
                        AND _from.code = ', _from_alias, '.code
                        AND _to.level = ''', to_level, '''
                        AND _to.code = ', _to_alias, '.code
                )
            )'
        );
        RETURN _query;
    ELSE
        _to_common_level := public.get_common_level(country, _from_level, to_level);
        RETURN public.get_territory_query_to_level(
            country
            , public.get_territory_query_to_level(country, query, _to_common_level)
            , to_level
        );
    END IF;
END
$func$ LANGUAGE plpgsql;
