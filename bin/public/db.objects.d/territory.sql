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
