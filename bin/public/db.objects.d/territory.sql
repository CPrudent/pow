/***
 * territory management
 */

CREATE TABLE IF NOT EXISTS public.territory
(
    id SERIAL NOT NULL
    , id_parent INT
    , country CHAR(2)       -- code ISO-3166, like FR for France
    , level CHARACTER VARYING NOT NULL
    , code CHARACTER VARYING NOT NULL
    , date_geography DATE /*NOT*/ NULL
    , name CHARACTER VARYING
    , type CHARACTER VARYING
    , population INT
    , area INT
    , geom_native GEOMETRY                       -- native (local) geography
    , geom_world GEOMETRY(MULTIPOLYGON, 4326)    -- simplified geography (reprojected WGS84)
    , codes_adjoining VARCHAR[] NULL
)
;

-- manual VACUUM
ALTER TABLE public.territoire SET (
	autovacuum_enabled = FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_id ON public.territory (id) ;
CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_level_code ON public.territory (country, level, code);
