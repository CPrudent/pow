/***
 * DB: deal w/ permissions
 */

DO $PERMS$
BEGIN
    -- https://stackoverflow.com/questions/17338621/what-does-grant-usage-on-schema-do-exactly
    -- gives SELECT|EXECUTE access to public's objects
    GRANT USAGE ON SCHEMA public TO fr;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO fr;
    GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO fr;
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO fr;

    -- more specific objects
    IF public.table_exists('public', 'io_history') THEN
        GRANT ALL PRIVILEGES ON public.io_history TO fr;
        GRANT ALL PRIVILEGES ON SEQUENCE io_history_id_seq TO fr;
    END IF;
    IF public.table_exists('public', 'territory') THEN
        GRANT ALL PRIVILEGES ON public.territory TO fr;
        GRANT ALL PRIVILEGES ON SEQUENCE territory_id_seq TO fr;
    END IF;
    IF public.table_exists('public', 'territory_level') THEN
        GRANT ALL PRIVILEGES ON public.territory_level TO fr;
    END IF;
END $PERMS$;
