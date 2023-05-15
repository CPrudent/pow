/***
 * ADDRESS
 */

CREATE TABLE IF NOT EXISTS public.address (
    id SERIAL NOT NULL
    , id_parent INT
    , id_territory INT
    , id_street INT
    , id_housenumber INT
    , id_complement INT
)
;

-- manual VACUUM
ALTER TABLE public.address SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('public', 'set_address_index');
CREATE OR REPLACE PROCEDURE public.set_address_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_id ON public.address (id);
    CREATE INDEX IF NOT EXISTS ix_address_id_territory ON public.address (id_territory);
    CREATE INDEX IF NOT EXISTS ix_address_id_street ON public.address (id_street);
    CREATE INDEX IF NOT EXISTS ix_address_id_housenumber ON public.address (id_housenumber);
    CREATE INDEX IF NOT EXISTS ix_address_id_complement ON public.address (id_complement);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_index();
END
$$;
