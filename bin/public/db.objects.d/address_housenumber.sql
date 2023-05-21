/***
 * ADDRESS (housenumber)
 */

CREATE TABLE IF NOT EXISTS public.address_housenumber (
    id SERIAL NOT NULL
    , number VARCHAR NOT NULL
    , extension VARCHAR
    , country VARCHAR
)
;

-- manual VACUUM
ALTER TABLE public.address_housenumber SET (
	autovacuum_enabled = FALSE
);

DO $$
BEGIN
    IF NOT column_exists('public', 'address_housenumber', 'country') THEN
        ALTER TABLE public.address_housenumber ADD COLUMN country VARCHAR;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('public', 'set_address_housenumber_index');
CREATE OR REPLACE PROCEDURE public.set_address_housenumber_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_housenumber_id ON public.address_housenumber (id);
    CREATE INDEX IF NOT EXISTS ix_address_housenumber_number ON public.address_housenumber (number, extension, country);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_housenumber_index();
END
$$;
