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
                IF table_exists('fr', 'communes_summary') THEN
                    ALTER TABLE fr.communes_summary RENAME TO bal_municipality;
                    ALTER TABLE fr.bal_municipality OWNER TO fr;
                END IF;
                IF table_exists('fr', 'voie') THEN
                    ALTER TABLE fr.voie RENAME TO bal_street;
                    ALTER TABLE fr.bal_street OWNER TO fr;
                END IF;
                IF table_exists('fr', 'numero') THEN
                    ALTER TABLE fr.numero RENAME TO bal_housenumber;
                    ALTER TABLE fr.bal_housenumber OWNER TO fr;
                END IF;
            ELSIF _schema_name = 'divers' THEN
                IF table_exists('fr', 'source_orga') THEN
                    ALTER TABLE fr.source_orga RENAME TO laposte_organization_all;
                    ALTER TABLE fr.laposte_organization_all OWNER TO fr;
                END IF;
                IF table_exists('fr', 'source_orga_complement') THEN
                    ALTER TABLE fr.source_orga_complement RENAME TO laposte_organization_fix;
                    ALTER TABLE fr.laposte_organization_fix OWNER TO fr;
                END IF;
            ELSIF _schema_name = 'geopad' THEN
                IF table_exists('fr', 'pdi') THEN
                    ALTER TABLE fr.pdi RENAME TO laposte_delivery_point;
                    ALTER TABLE fr.laposte_delivery_point OWNER TO fr;
                END IF;
            ELSIF _schema_name = 'ran' THEN
                IF table_exists('fr', 'adresse') THEN
                    ALTER TABLE fr.adresse RENAME TO laposte_address;
                    ALTER TABLE fr.laposte_address OWNER TO fr;
                END IF;
                IF table_exists('fr', 'za') THEN
                    ALTER TABLE fr.za RENAME TO laposte_zone_address;
                    ALTER TABLE fr.laposte_zone_address OWNER TO fr;
                END IF;
                IF table_exists('fr', 'voie') THEN
                    ALTER TABLE fr.voie RENAME TO laposte_street;
                    ALTER TABLE fr.laposte_street OWNER TO fr;
                END IF;
                IF table_exists('fr', 'numero') THEN
                    ALTER TABLE fr.numero RENAME TO laposte_housenumber;
                    ALTER TABLE fr.laposte_housenumber OWNER TO fr;
                END IF;
                IF table_exists('fr', 'l3') THEN
                    ALTER TABLE fr.l3 RENAME TO laposte_complement;
                    ALTER TABLE fr.laposte_complement OWNER TO fr;
                END IF;
                IF table_exists('fr', 'coord') THEN
                    ALTER TABLE fr.coord RENAME TO laposte_xy;
                    ALTER TABLE fr.laposte_xy OWNER TO fr;
                END IF;
                IF table_exists('fr', 'perennite') THEN
                    ALTER TABLE fr.perennite RENAME TO laposte_address_redirect;
                    ALTER TABLE fr.laposte_address_redirect OWNER TO fr;
                END IF;
            END IF;
        END IF;
    END LOOP;
END $RELOCATE$;
