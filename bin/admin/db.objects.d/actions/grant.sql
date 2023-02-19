/***
 * deal w/ permissions
 */

DO $PERMS$
BEGIN
    -- https://stackoverflow.com/questions/17338621/what-does-grant-usage-on-schema-do-exactly
    GRANT USAGE ON SCHEMA public TO bal, divers, geopad, ign, insee, ran;

    -- gives SELECT|EXECUTE access to public's objects
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO bal, divers, geopad, ign, insee, ran;
    GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO bal, divers, geopad, ign, insee, ran;
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO bal, divers, geopad, ign, insee, ran;

    -- more specific objects
    IF public.table_exists('public', 'io_history') THEN
        GRANT ALL PRIVILEGES ON public.io_history TO bal, divers, geopad, ign, insee, ran;
        GRANT ALL PRIVILEGES ON SEQUENCE io_history_id_seq TO bal, divers, geopad, ign, insee, ran;
    END IF;
    IF public.view_exists('public', 'za_ran_view') THEN
        GRANT ALL PRIVILEGES ON public.za_ran_view TO bal;
    END IF;
END $PERMS$;
