/***
 * DB: deal w/ purge
 */

DO $PURGE$
DECLARE
    _record RECORD;
    _query TEXT;
BEGIN
    -- drop table w/ 'backup' 1 month older
    FOR _record IN (
        SELECT table_schema, table_name, pg_stat_file.*
        FROM information_schema.tables
        CROSS JOIN pg_stat_file(pg_relation_filepath(CONCAT(table_schema, '.', table_name))) AS pg_stat_file
        WHERE LOWER(table_name) LIKE '%backup%'
        AND table_type = 'BASE TABLE'
        AND pg_stat_file.modification < (NOW() - INTERVAL '1 month')
    )
    LOOP
        RAISE NOTICE 'Suppression automatique de la table %.%', _record.table_schema, _record.table_name;
        _query := CONCAT(
            'DROP TABLE ',
            _record.table_schema,
            '.',
            _record.table_name,
            ';'
        );
        --RAISE NOTICE '%', _query;
        EXECUTE _query;
    END LOOP;

    DROP TABLE IF EXISTS fr.laposte_address_fault_street_correction;
END $PURGE$;
