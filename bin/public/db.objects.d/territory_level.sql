/***
 * TERRITORY level definition
 */

CREATE TABLE IF NOT EXISTS public.territory_level
(
    country CHAR(2) NOT NULL
    , level CHARACTER VARYING NOT NULL
    , name CHARACTER VARYING /*NOT*/ NULL
    --, name_article CHARACTER VARYING /*NOT*/ NULL
    , name_short CHARACTER VARYING /*NOT*/ NULL
    , name_plural CHARACTER VARYING /*NOT*/ NULL
    , order INTEGER /*NOT*/ NULL
    , sublevels VARCHAR[]                            -- direct sublevels
);

CREATE UNIQUE INDEX iux_territory_level_level ON public.territory_level (country, level);
