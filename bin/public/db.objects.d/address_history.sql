/***
 * ADDRESS history
 */

CREATE TABLE IF NOT EXISTS public.address_history (
    id INT NOT NULL
    , date_change DATE NOT NULL
    , change CHAR(1) NOT NULL               -- {-, !} for (DELETE, UPDATE)
    , kind VARCHAR NOT NULL                 -- {STREET, HOUSENUMBER, COMPLEMENT, ADDRESS}
    , values JSONB
)
;

SELECT drop_all_functions_if_exists('public', 'set_address_history_index');
CREATE OR REPLACE PROCEDURE public.set_address_history_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_address_history_id_address ON public.address_history (id, date_change);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_history_index();
END
$$;
