/***
 * ADDRESS (XY)
 */

CREATE TABLE IF NOT EXISTS public.address_xy (
    id SERIAL NOT NULL,
    id_address INT NOT NULL,
    kind VARCHAR NOT NULL,                     -- INSPIRE style (ENTRANCE, ...)
    source VARCHAR NOT NULL,
    geom GEOMETRY NOT NULL
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

SELECT drop_all_functions_if_exists('public', 'drop_address_xy_index');
CREATE OR REPLACE PROCEDURE public.drop_address_xy_index(
    drop_case VARCHAR DEFAULT 'ALL'             -- ALL | EXCEPT_UPSERT
)
AS
$proc$
BEGIN
    DROP INDEX IF EXISTS iux_address_xy_id;

    IF drop_case = 'ALL' THEN
        DROP INDEX IF EXISTS iux_address_xy_id_address;
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_xy_index();
END
$$;
