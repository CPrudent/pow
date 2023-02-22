/***
 * DB: deal w/ permissions
 */

DO $PERMS$
DECLARE
    _schema_name VARCHAR;
    _query VARCHAR;
BEGIN
    -- https://stackoverflow.com/questions/17338621/what-does-grant-usage-on-schema-do-exactly
    -- gives SELECT|EXECUTE access to public's objects
    GRANT USAGE ON SCHEMA public TO fr;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO fr;
    GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO fr;
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO fr;

    -- gives access to restored data (time only for relocation)
    FOR _schema_name IN (
        SELECT
            *
        FROM (
            VALUES ('bal'), ('divers'), ('geopad'), ('ran')
        ) AS t(schema)
    )
    LOOP
        IF schema_exists(_schema_name) THEN
            _query := CONCAT(
                'GRANT USAGE ON SCHEMA '
                , quote_ident(_schema_name)
                , ' TO fr'
            );
            EXECUTE _query;
            _query := CONCAT(
                'GRANT SELECT ON ALL TABLES IN SCHEMA '
                , quote_ident(_schema_name)
                , ' TO fr'
            );
            EXECUTE _query;
        END IF;
    END LOOP;

    -- more specific objects
    IF public.table_exists('public', 'io_history') THEN
        GRANT ALL PRIVILEGES ON public.io_history TO fr;
        GRANT ALL PRIVILEGES ON SEQUENCE io_history_id_seq TO fr;
    END IF;
    IF public.view_exists('public', 'za_ran_view') THEN
        GRANT ALL PRIVILEGES ON public.za_ran_view TO fr;
    END IF;
END $PERMS$;
