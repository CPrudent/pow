/***
 * ADDRESS (XY)
 */

CREATE TABLE IF NOT EXISTS public.address_xy (
    id SERIAL NOT NULL
    , id_address INT NOT NULL
    , kind VARCHAR NOT NULL                     -- INSPIRE style (ENTRANCE, ...)
    , source VARCHAR NOT NULL
    , geom GEOMETRY NOT NULL
)
;

-- manual VACUUM
ALTER TABLE public.address_xy SET (
	autovacuum_enabled = FALSE
);


SELECT drop_all_functions_if_exists('public', 'set_address_xy_index');
CREATE OR REPLACE PROCEDURE public.set_address_xy_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_xy_id ON public.address_xy (id);
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_xy_id_address ON public.address_xy (id_address, kind, source);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_xy_index();
END
$$;
