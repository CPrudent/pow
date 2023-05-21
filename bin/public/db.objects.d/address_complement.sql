/***
 * ADDRESS (complement)
 */

CREATE TABLE IF NOT EXISTS public.address_complement (
    id SERIAL NOT NULL
    , name VARCHAR NOT NULL
    , name_normalized VARCHAR
    , country VARCHAR
)
;

-- manual VACUUM
ALTER TABLE public.address_complement SET (
	autovacuum_enabled = FALSE
);

DO $$
BEGIN
    IF NOT column_exists('public', 'address_complement', 'country') THEN
        ALTER TABLE public.address_complement ADD COLUMN country VARCHAR;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('public', 'set_address_complement_index');
CREATE OR REPLACE PROCEDURE public.set_address_complement_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_complement_id ON public.address_complement (id);
    CREATE INDEX IF NOT EXISTS ix_address_complement_name ON public.address_complement USING GIN(name GIN_TRGM_OPS, country);
    CREATE INDEX IF NOT EXISTS ix_address_complement_name_normalized ON public.address_complement USING GIN(name_normalized GIN_TRGM_OPS, country);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_complement_index();
END
$$;
