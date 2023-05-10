/***
 * TERRITORY level definition
 */

CREATE TABLE IF NOT EXISTS public.territory_level (
    country CHAR(2) NOT NULL
    , level CHARACTER VARYING NOT NULL
    , name CHARACTER VARYING
    , name_short CHARACTER VARYING
    , name_plural CHARACTER VARYING
    , article CHARACTER VARYING
    , hierarchy INTEGER

    , sublevels VARCHAR[]                            -- direct sublevels
    -- levels_below
    -- levels_above
);

SELECT drop_all_functions_if_exists('public', 'set_territory_level_index');
CREATE OR REPLACE PROCEDURE public.set_territory_level_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_level_level ON public.territory_level (country, level);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    PERFORM drop_all_functions_if_exists('public', 'territory_level');

    -- for each country
    CALL fr.set_territory_level();

    CALL public.set_territory_level_index();
END
$$;

-- is level A a sublevel of level B
SELECT public.drop_all_functions_if_exists('public', 'is_level_below');
CREATE OR REPLACE FUNCTION public.is_level_below(
    country VARCHAR
    , level_a IN VARCHAR
    , level_b IN VARCHAR
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
                country
                , level_a
                , _immediate_sublevel_b
            )) THEN RETURN TRUE; END IF;
        END LOOP;
    END IF;
    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'get_common_level');
CREATE OR REPLACE FUNCTION public.get_common_level(
    country VARCHAR
    , level_a VARCHAR
    , level_b VARCHAR
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    EXECUTE CONCAT('SELECT ', LOWER(country), '.get_common_level(
            level_a => $1
            , level_b => $2
        )')
        INTO _level
        USING level_a, level_b;
    RETURN _level;
END
$func$ LANGUAGE plpgsql;
-- get common level between a set of levels
CREATE OR REPLACE FUNCTION public.get_common_level(
    country VARCHAR
    , levels IN VARCHAR[]
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    EXECUTE CONCAT('SELECT ', LOWER(country), '.get_common_level(
            levels => $1
        )')
        INTO _level
        USING levels;
    RETURN _level;
END
$func$ LANGUAGE plpgsql;
