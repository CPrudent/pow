/***
 * DB: deal w/ purge (after relocate)
 */

DO $PURGE$
DECLARE
    _schema_name VARCHAR;
    _ntables INT;
    _query VARCHAR;
BEGIN
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
                'SELECT COUNT(*) FROM pg_tables WHERE schemaname = '
                , quote_literal(_schema_name)
            );
            EXECUTE _query USING _schema_name INTO _ntables;
            IF _ntables = 0 THEN
                _query := CONCAT(
                    'DROP SCHEMA '
                    , _schema_name
                    , ' CASCADE'
                );
                EXECUTE _query;
            ELSE
                RAISE NOTICE 'Ne peut supprimer le schéma(%), il existe encore #% table(s)', _schema_name, _ntables;
            END IF;
        END IF;
    END LOOP;
END $PURGE$;
