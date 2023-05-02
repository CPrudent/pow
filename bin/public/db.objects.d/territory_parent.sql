/***
 * TERRITORY parent
 */

DO $$
BEGIN
    IF column_exists('public', 'territory_parent', 'id_address') THEN
        DROP TABLE public.territory_parent;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.territory_parent (
    id_territory INT NOT NULL
    , id_parent INT NOT NULL
)
;

SELECT drop_all_functions_if_exists('public', 'set_territory_parent_index');
CREATE OR REPLACE PROCEDURE public.set_territory_parent_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_territory_parent_id_territory ON public.territory_parent (id_territory);
    CREATE INDEX IF NOT EXISTS ix_territory_parent_id_parent ON public.territory_parent (id_parent);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_territory_parent_index();
END
$$;
