/***
 * TERRITORY level definition
 */

CREATE TABLE IF NOT EXISTS public.territory_level (
    country CHAR(2) NOT NULL,
    level CHARACTER VARYING NOT NULL,
    name CHARACTER VARYING,
    name_short CHARACTER VARYING,
    name_plural CHARACTER VARYING,
    article CHARACTER VARYING,
    hierarchy INTEGER,
    sublevels VARCHAR[]                            -- direct sublevels
    -- levels_below
    -- levels_above
);

-- create indexes
SELECT drop_all_functions_if_exists('public', 'set_territory_level_index');
CREATE OR REPLACE PROCEDURE public.set_territory_level_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_level_level ON public.territory_level (country, level);
END
$proc$ LANGUAGE plpgsql;

-- initialize all levels (from countries)
DO $$
DECLARE
    _schema_name VARCHAR;
    _procedure_name VARCHAR := 'set_territory_level';
    _query TEXT;
BEGIN
    -- for each country
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
                '()'
            );

            EXECUTE _query;
        END IF;
    END LOOP;

    CALL public.set_territory_level_index();
END
$$;

-- exists level
SELECT public.drop_all_functions_if_exists('public', 'exists_level');
CREATE OR REPLACE FUNCTION public.exists_level(
    country VARCHAR,
    level VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _exists INT;
BEGIN
    SELECT 1 INTO _exists FROM public.territory_level tl
    WHERE tl.country = exists_level.country AND tl.level = exists_level.level;
    RETURN FOUND;
END
$func$ LANGUAGE plpgsql;

-- is level A a sublevel of level B
SELECT public.drop_all_functions_if_exists('public', 'is_level_below');
CREATE OR REPLACE FUNCTION public.is_level_below(
    country VARCHAR,
    level_a VARCHAR,
    level_b VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _immediate_sublevels_b VARCHAR[];
    _immediate_sublevel_b VARCHAR;
BEGIN
    IF (level_a = level_b) THEN RETURN FALSE; END IF;
    SELECT sublevels INTO _immediate_sublevels_b FROM public.territory_level tl
        WHERE level = level_b AND tl.country = UPPER(is_level_below.country);
    IF (_immediate_sublevels_b IS NOT NULL) THEN
        -- level A is an immediate sublevel from leval B
        IF (level_a = ANY(_immediate_sublevels_b)) THEN RETURN TRUE; END IF;
        FOREACH _immediate_sublevel_b IN ARRAY _immediate_sublevels_b
        LOOP
            -- level A is a deeper sublevel from leval B (no immediate) : it's a sublevel of a sublevel from level B
            IF (public.is_level_below(
                country,
                level_a,
                _immediate_sublevel_b
            )) THEN RETURN TRUE; END IF;
        END LOOP;
    END IF;
    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.is_level_below('fr', 'COM', 'EPCI'); --> TRUE
SELECT public.is_level_below('fr', 'EPCI', 'COM'); --> FALSE
SELECT public.is_level_below('fr', 'IRIS', 'COM'); --> TRUE
SELECT public.is_level_below('fr', 'CP', 'COM'); --> FALSE
 */

SELECT public.drop_all_functions_if_exists('public', 'get_common_level');
CREATE OR REPLACE FUNCTION public.get_common_level(
    country VARCHAR,
    level_a VARCHAR,
    level_b VARCHAR
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    EXECUTE CONCAT('SELECT ', LOWER(country), '.get_common_level(
            level_a => $1,
            level_b => $2
        )')
        INTO _level
        USING level_a, level_b;
    RETURN _level;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.get_common_level('fr', 'COM', 'COM') --> COM
SELECT public.get_common_level('fr', 'EPCI', 'COM') --> COM
SELECT public.get_common_level('fr', 'COM', 'EPCI') --> COM
SELECT public.get_common_level('fr', 'DEP', 'REG') --> DEP
SELECT public.get_common_level('fr', 'DEP', 'ARR') --> ARR
SELECT public.get_common_level('fr', 'DEP', 'EPCI') --> COM
SELECT public.get_common_level('fr', 'ARR', 'EPCI') --> COM
SELECT public.get_common_level('fr', 'DEP', 'CP') --> ZA
SELECT public.get_common_level('fr', 'DEP', 'DSCC') --> ZA
 */
-- get common level between a set of levels
CREATE OR REPLACE FUNCTION public.get_common_level(
    country VARCHAR,
    levels VARCHAR[]
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
    _common_level VARCHAR;
BEGIN
    FOREACH _level IN ARRAY levels
    LOOP
        IF _common_level IS NULL THEN
            _common_level := _level;
        ELSE
            _common_level := public.get_common_level(country, _level, _common_level);
        END IF;
    END LOOP;
    RETURN _common_level;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.get_common_level('fr', ARRAY['DEP', 'DSCC', 'CP', 'COM']) --> ZA
SELECT public.get_common_level('fr', ARRAY['DEP', 'EPCI', 'REG']) --> COM
SELECT public.get_common_level('fr', ARRAY['DEP', 'REG', 'PAYS']) --> DEP
 */

-- get list of levels
SELECT public.drop_all_functions_if_exists('public', 'get_levels');
CREATE OR REPLACE FUNCTION public.get_levels(
    country VARCHAR,
    order_in VARCHAR DEFAULT 'ASC',
    subfilter VARCHAR DEFAULT NULL,         -- result list from this level (smaller one)
    among_levels VARCHAR[] DEFAULT NULL     -- list of levels to order
)
RETURNS VARCHAR[] AS
$func$
DECLARE
    _levels VARCHAR[];
    _level VARCHAR;
BEGIN
    IF order_in = 'DESC' THEN
        SELECT ARRAY_AGG(tl.level ORDER BY tl.hierarchy DESC)
        INTO _levels
        FROM public.territory_level tl
        WHERE tl.country = UPPER(get_levels.country);
    ELSE
        SELECT ARRAY_AGG(tl.level ORDER BY tl.hierarchy ASC)
        INTO _levels
        FROM public.territory_level tl
        WHERE tl.country = UPPER(get_levels.country);
    END IF;

    IF subfilter IS NOT NULL THEN
        FOREACH _level IN ARRAY _levels
        LOOP
            IF _level = subfilter
            OR NOT public.is_level_below(country, subfilter, _level)
            THEN
                _levels := ARRAY_REMOVE(_levels, _level);
            END IF;
        END LOOP;
    END IF;

    IF among_levels IS NOT NULL THEN
        FOREACH _level IN ARRAY _levels
        LOOP
            IF NOT(_level = ANY(among_levels))
            THEN
                _levels := ARRAY_REMOVE(_levels, _level);
            END IF;
        END LOOP;
    END IF;

    RETURN _levels;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'get_bigger_sublevel');
CREATE OR REPLACE FUNCTION public.get_bigger_sublevel(
    country VARCHAR,
    level_in VARCHAR,
    among_levels VARCHAR[] DEFAULT NULL
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    FOR _level IN (
        --On récupère les niveaux intermédiaires (pouvant servir à construire un autre niveau)
        --En les ordonnant par ordre du plus grand niveau parent et par ordre d'apparition dans la liste des sous niveaux directs sur niveau parent
        SELECT level FROM (
            SELECT level, ROW_NUMBER() OVER() AS hierarchy FROM (
                SELECT UNNEST(tl.sublevels) AS level FROM public.territory_level tl
                WHERE tl.country = UPPER(get_bigger_sublevel.country)
                ORDER BY tl.hierarchy DESC
            ) AS sq
        ) AS sq2
        GROUP BY level
        ORDER BY MAX(hierarchy)
    )
    LOOP
        IF among_levels IS NOT NULL AND NOT(_level = ANY(among_levels)) THEN CONTINUE; END IF;
        IF public.is_level_below(country, _level, level_in) THEN
            RETURN _level;
        END IF;
    END LOOP;
    RETURN NULL;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.get_bigger_sublevel('fr', 'PAYS', ARRAY['DEP', 'EPCI', 'REG']) --> REG
 */

SELECT public.drop_all_functions_if_exists('public', 'get_smaller_sublevel');
CREATE OR REPLACE FUNCTION public.get_smaller_sublevel(
    country VARCHAR,
    level_in VARCHAR,
    among_levels VARCHAR[] DEFAULT NULL
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    FOR _level IN (
        --On récupère les niveaux intermédiaires (pouvant servir à construire un autre niveau)
        --En les ordonnant par ordre du plus grand niveau parent et par ordre d'apparition dans la liste des sous niveaux directs sur niveau parent
        SELECT level FROM (
            SELECT level, ROW_NUMBER() OVER() AS hierarchy FROM (
                SELECT UNNEST(tl.sublevels) AS level FROM public.territory_level tl
                WHERE tl.country = UPPER(get_smaller_sublevel.country)
                ORDER BY tl.hierarchy DESC
            ) AS sq
        ) AS sq2
        GROUP BY level
        ORDER BY MAX(hierarchy) DESC
    )
    LOOP
        IF among_levels IS NOT NULL AND NOT(_level = ANY(among_levels)) THEN CONTINUE; END IF;
        IF public.is_level_below(country, _level, level_in) THEN
            RETURN _level;
        END IF;
    END LOOP;
    RETURN NULL;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.get_smaller_sublevel('fr', 'DEP')
 */
