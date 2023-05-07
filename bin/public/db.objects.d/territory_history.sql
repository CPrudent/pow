/***
 * TERRITORY history
 */

CREATE TABLE IF NOT EXISTS public.territory_history (
    id_territory INT NOT NULL
    , date_change DATE NOT NULL
    , change CHAR(1) NOT NULL               -- {-, !} for (DELETE, UPDATE)
    , kind VARCHAR NOT NULL                 -- {VALUE, LINK} for {old values, old links}
    , values JSONB
)
;

DO $$
BEGIN
    IF NOT column_exists('public', 'territory_history', 'change') THEN
        ALTER TABLE public.territory_history ADD COLUMN "change" CHAR(1) NOT NULL;
    END IF;
    IF NOT column_exists('public', 'territory_history', 'kind') THEN
        ALTER TABLE public.territory_history ADD COLUMN kind VARCHAR NOT NULL;
    END IF;
    IF column_exists('public', 'territory_history', 'date_territory') THEN
        ALTER TABLE public.territory_history RENAME COLUMN date_territory TO date_change;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('public', 'set_territory_history_index');
CREATE OR REPLACE PROCEDURE public.set_territory_history_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_territory_history_id_territory ON public.territory_history (id_territory, date_change);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_territory_history_index();
END
$$;
