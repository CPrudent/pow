/***
 * ADDRESS (street)
 */

CREATE TABLE IF NOT EXISTS public.address_street (
    id SERIAL NOT NULL
    , name VARCHAR NOT NULL
    , name_normalized VARCHAR
    , typeof VARCHAR
)
;

-- manual VACUUM
ALTER TABLE public.address_street SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('public', 'set_address_street_index');
CREATE OR REPLACE PROCEDURE public.set_address_street_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_street_id ON public.address_street (id);
    CREATE INDEX IF NOT EXISTS ix_address_street_name ON public.address_street USING GIN(name GIN_TRGM_OPS);
    CREATE INDEX IF NOT EXISTS ix_address_street_name_normalized ON public.address_street USING GIN(name_normalized GIN_TRGM_OPS);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_street_index();
END
$$;
