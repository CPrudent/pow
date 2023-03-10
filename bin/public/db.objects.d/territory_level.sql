/***
 * TERRITORY level definition
 */

CREATE TABLE IF NOT EXISTS public.territory_level (
    country CHAR(2) NOT NULL
    , level CHARACTER VARYING NOT NULL
    , name CHARACTER VARYING /*NOT*/ NULL
    , name_short CHARACTER VARYING /*NOT*/ NULL
    , name_plural CHARACTER VARYING /*NOT*/ NULL
    , article CHARACTER VARYING /*NOT*/ NULL
    , hierarchy INTEGER /*NOT*/ NULL
    , sublevels VARCHAR[]                            -- direct sublevels
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
