/***
 * DB: deal w/ relocations (after restore)
 */

DO $RELOCATE$
DECLARE
    _schema_name VARCHAR;
BEGIN
    FOR _schema_name IN (
        SELECT
            *
        FROM (
            VALUES ('bal'), ('divers'), ('geopad'), ('ran')
        ) AS t(schema)
    )
    LOOP
        IF alter_tables_change_schema(
            schema_name_from => _schema_name
            , schema_name_to => 'fr'
        ) THEN
            IF _schema_name = 'bal' THEN
                ALTER TABLE fr.communes_summary RENAME TO bal_municipality;
                ALTER TABLE fr.voie RENAME TO bal_street;
                ALTER TABLE fr.numero RENAME TO bal_housenumber;
            ELSIF _schema_name = 'divers' THEN
                ALTER TABLE fr.source_orga RENAME TO laposte_organization;
                ALTER TABLE fr.source_orga_complement RENAME TO laposte_organization_fix;
            ELSIF _schema_name = 'geopad' THEN
                ALTER TABLE fr.pdi RENAME TO laposte_delivery_point;
            ELSIF _schema_name = 'ran' THEN
                ALTER TABLE fr.adresse RENAME TO laposte_address;
                ALTER TABLE fr.za RENAME TO laposte_zone_address;
                ALTER TABLE fr.voie RENAME TO laposte_street;
                ALTER TABLE fr.numero RENAME TO laposte_housenumber;
                ALTER TABLE fr.l3 RENAME TO laposte_complement;
                ALTER TABLE fr.coord RENAME TO laposte_xy;
            END IF;
        END IF;
    END LOOP;
END $RELOCATE$;
