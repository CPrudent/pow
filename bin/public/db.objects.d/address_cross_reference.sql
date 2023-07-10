/***
 * ADDRESS cross reference
 */

CREATE TABLE IF NOT EXISTS public.address_cross_reference (
    id SERIAL NOT NULL
    , id_address INT
    , source VARCHAR
    , id_source VARCHAR
)
;

-- manual VACUUM
ALTER TABLE public.address_cross_reference SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('public', 'set_address_cross_reference_index');
CREATE OR REPLACE PROCEDURE public.set_address_cross_reference_index()
AS
$proc$
BEGIN
    CREATE /*UNIQUE*/ INDEX IF NOT EXISTS iux_address_cross_reference_id_address ON public.address_cross_reference (id_address, source);
    CREATE /*UNIQUE*/ INDEX IF NOT EXISTS iux_address_cross_reference_id_source ON public.address_cross_reference (source, id_source);
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'drop_address_cross_reference_index');
CREATE OR REPLACE PROCEDURE public.drop_address_cross_reference_index()
AS
$proc$
BEGIN
    DROP INDEX IF EXISTS iux_address_cross_reference_id_address;
    DROP INDEX IF EXISTS iux_address_cross_reference_id_source;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_cross_reference_index();
END
$$;
