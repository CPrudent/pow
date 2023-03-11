/***
 * TERRITORY management
 */

CREATE TABLE IF NOT EXISTS public.territory (
    id SERIAL NOT NULL
    , id_parent INT
    , country CHAR(2) NOT NULL                  -- code ISO-3166, like FR for France
    , level CHARACTER VARYING NOT NULL
    , code CHARACTER VARYING NOT NULL
    , name CHARACTER VARYING
    , population INT
    , area INT
    , codes_adjoining VARCHAR[]                 -- list of nearing territories (same level)
    , attr HSTORE                               -- more attributs
    , date_geography DATE /*NOT*/ NULL
    , geom_native GEOMETRY                      -- native geography (local)
    , geom_world GEOMETRY(MULTIPOLYGON, 4326)   -- WGS84-proj & simplified geography
)
;

-- manual VACUUM
ALTER TABLE public.territory SET (
	autovacuum_enabled = FALSE
);

/*
 * FR-attributs
 *
 * ZA
 *  L5=>name of old municipality
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
    CREATE INDEX IF NOT EXISTS ix_territory_id_parent ON public.territory (id_parent);
    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_level_code ON public.territory (country, level, code);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_territory_index();
END
$$;
