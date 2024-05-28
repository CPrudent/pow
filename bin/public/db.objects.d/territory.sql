/***
 * TERRITORY
 */

CREATE TABLE IF NOT EXISTS public.territory (
    id SERIAL NOT NULL,
    country CHAR(2) NOT NULL,                 -- code ISO-3166, like FR for France
    level CHARACTER VARYING NOT NULL,
    code CHARACTER VARYING NOT NULL,
    name CHARACTER VARYING,
    population INT,
    area INT,
    z_min INT,
    z_max INT,
    codes_adjoining VARCHAR[],                -- list of nearing territories (same level)
    attributs HSTORE,                         -- more attributs
    date_last DATE,
    geom_native GEOMETRY,                     -- native geography (local)
    geom_world GEOMETRY(MULTIPOLYGON, 4326)   -- WGS84-proj & simplified geography
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

    IF NOT column_exists('public', 'territory', 'z_min') THEN
        ALTER TABLE public.territory ADD COLUMN z_min INTEGER;
    END IF;
    IF NOT column_exists('public', 'territory', 'z_max') THEN
        ALTER TABLE public.territory ADD COLUMN z_max INTEGER;
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
                'CALL ',
                _schema_name,
                '.',
                _procedure_name,
                '($1)'
            );

            CALL public.log_info(CONCAT('Pays: ', UPPER(_schema_name)));
            EXECUTE _query USING force;
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

--
-- facilities to navigate through territories
--

SELECT public.drop_all_functions_if_exists('public','get_territory_from_query');
CREATE OR REPLACE FUNCTION public.get_territory_from_query(
    query TEXT,
    raise_notice BOOLEAN DEFAULT FALSE
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

/* TEST
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

SELECT public.drop_all_functions_if_exists('public','get_query_territory');
CREATE OR REPLACE FUNCTION public.get_query_territory(
    country VARCHAR,
    level_in VARCHAR
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
        FROM public.territory AS ', _alias,'
        WHERE ', _alias, '.country = ''', UPPER(country), ''' AND ',
        _alias, '.level = ''', level_in, '''
        /* WHERE-AND */
        '
    );

    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM get_territory_from_query(get_query_territory('EPCI'))
 */

CREATE OR REPLACE FUNCTION public.get_query_territory(
    country VARCHAR,
    level_in VARCHAR,
    code VARCHAR
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
                _query,
                '/* WHERE-AND */',
                CONCAT('AND ', _alias, '.code = ''', code,'''')
            );
    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM get_territory_from_query(get_query_territory('fr', 'EPCI', '245900758'))
SELECT * FROM public.get_territory_from_query(public.get_query_territory('fr', 'COM', '84033'), true)
SELECT * FROM public.get_territory_from_query(public.get_query_territory('fr', 'COM', '84033', '2016-05-05'::DATE), true)
 */

CREATE OR REPLACE FUNCTION public.get_query_territory(
    country VARCHAR,
    level_in VARCHAR,
    code VARCHAR[]
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
                _query,
                '/* WHERE-AND */',
                CONCAT('AND ', _alias, '.code = ANY(''{', ARRAY_TO_STRING(code, ','), '}''::VARCHAR[])')
            );
    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM get_territory_from_query(
    get_query_territory('FR', 'COM', ARRAY['84033','84007'])
)
 */

-- list of linked territories from territory given by query, (UP or DOWN, as parents or childs)
SELECT public.drop_all_functions_if_exists('public', 'get_query_linked_territory');
CREATE OR REPLACE FUNCTION public.get_query_linked_territory(
    country VARCHAR,
    query TEXT,
    to_levels VARCHAR[],
    direction VARCHAR DEFAULT 'UP'
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
                FROM (', query, ') ', _from_alias,
                    ' JOIN public.territory_parent _parent ON ', _from_alias, '.id = _parent.',
                CASE WHEN direction = 'UP' THEN 'id_territory'
                ELSE 'id_parent'
                END,
                ' WHERE ', _from_alias, '.country = ''', UPPER(country), '''
                UNION
                SELECT _parent.id_territory, _parent.id_parent, links.depth ',
                CASE WHEN direction = 'UP' THEN '+1'
                ELSE '-1'
                END,
                ' FROM public.territory_parent _parent
                    JOIN links ON ',
                CASE WHEN direction = 'UP' THEN '_parent.id_territory = links.id_parent'
                ELSE '_parent.id_parent = links.id_territory'
                END,
                ')

            SELECT * FROM links
        ),
        linked_territory_ids AS (
            SELECT
                ARRAY_AGG(DISTINCT id_territory) a1,
                ARRAY_AGG(DISTINCT id_parent) a2
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

SELECT public.drop_all_functions_if_exists('public', 'get_query_territory_extended_to_level');
CREATE OR REPLACE FUNCTION public.get_query_territory_extended_to_level(
    country VARCHAR,
    query TEXT,
    to_level VARCHAR,
    from_level VARCHAR DEFAULT NULL
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _usecase VARCHAR;
    _to_common_level VARCHAR;
    _from_level VARCHAR := COALESCE(from_level, public.get_level_from_query(query));
BEGIN
    IF _from_level = to_level THEN
        RETURN get_query_territory_extended_to_level.query;
    END IF;

    _usecase :=
        CASE
        WHEN public.is_level_below(country, _from_level, to_level) THEN
            -- parent links
            'UP'
        WHEN public.is_level_below(country, to_level, _from_level) THEN
            -- child links
            'DOWN'
        ELSE
            -- no direct/indirect links, has to find common level
            'COMMON'
        END;

    IF _usecase = 'COMMON' THEN
        _to_common_level := public.get_common_level(country, _from_level, to_level);
        _query := public.get_query_territory_extended_to_level(
            country,
            public.get_query_territory_extended_to_level(country, query, _to_common_level),
            to_level,
            _to_common_level
        );
    ELSE
        -- parent/child links
        _query := public.get_query_linked_territory(
            country,
            query,
            ARRAY[to_level]::VARCHAR[],
            _usecase
        );
    END IF;

    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/* TEST
-- UP
SELECT * FROM get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'COM', '84007'), 'EPCI'));
-- DOWN
SELECT * FROM get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'EPCI', '248400251'), 'COM'));
-- COMMON
SELECT * FROM get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'EPCI', '248400251'), 'DEP'));
SELECT * FROM get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'DEP', '84'), 'EPCI'));
 */

CREATE OR REPLACE FUNCTION public.get_query_territory_extended_to_level(
    country VARCHAR,
    queries TEXT[],
    to_level VARCHAR
)
RETURNS TEXT AS
$func$
DECLARE
    _from_levels VARCHAR[];
    _to_common_level VARCHAR;
    _from_territories RECORD;
    _from_queries_extended_to_common_level TEXT[];
    _to_alias VARCHAR;
    _query TEXT;
BEGIN
    IF ARRAY_LENGTH(queries, 1) = 1 THEN
        RETURN public.get_query_territory_extended_to_level(country, queries[1], to_level);
    END IF;

    SELECT ARRAY_AGG(public.get_level_from_query(query))
    INTO _from_levels
    FROM UNNEST(queries) AS query;
    _to_common_level := public.get_common_level(country, _from_levels);

    FOR _from_territories IN (
        SELECT
            from_territories.query,
            public.get_level_from_query(from_territories.query) AS level
        FROM UNNEST(queries) WITH ORDINALITY AS from_territories(query, i)
    )
    LOOP
        _from_queries_extended_to_common_level := ARRAY_APPEND(
            _from_queries_extended_to_common_level,
            public.get_query_territory_extended_to_level(
                country,
                _from_territories.query,
                _to_common_level
            )
        );
    END LOOP;

    _to_alias := public.get_alias_from_level(_to_common_level);
    _query := CONCAT(
        'SELECT * FROM (',
        ARRAY_TO_STRING(
            _from_queries_extended_to_common_level,
            ' UNION '
        ),
        ') AS ', _to_alias, ' WHERE TRUE'
    );
    IF _to_common_level = to_level THEN
        RETURN _query;
    ELSE
        RETURN public.get_query_territory_extended_to_level(
            country,
            _query,
            to_level
        );
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM get_territory_from_query(get_query_territory_extended_to_level('fr', ARRAY[get_query_territory('fr', 'COM', '84007'), get_query_territory('fr', 'COM', '84033')], 'EPCI'));
 */

CREATE OR REPLACE FUNCTION public.get_query_territory_extended_to_level(
    country VARCHAR,
    queries TEXT[],
    to_levels VARCHAR[]
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _to_level VARCHAR;
    _queries TEXT[];
BEGIN
    FOREACH _to_level IN ARRAY to_levels
    LOOP
        _queries := ARRAY_APPEND(_queries,
            CONCAT('(',
                public.get_query_territory_extended_to_level(country, queries, _to_level),
                ')'
            )
        );
    END LOOP;
    _query := CONCAT(
        'SELECT * FROM (',
        ARRAY_TO_STRING(
            _queries,
            ' UNION '
        ),
        ') AS _x_ WHERE TRUE'
    );

    RETURN _query;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM get_territory_from_query(get_query_territory_extended_to_level('fr', ARRAY[get_query_territory('fr', 'COM', '84007'), get_query_territory('fr', 'COM', '84033')], ARRAY['EPCI', 'DEP', 'COM']));
 */
