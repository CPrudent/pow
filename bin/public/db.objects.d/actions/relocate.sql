/***
 * DB: deal w/ relocations (after restore)
 */

DO $RELOCATE$
BEGIN
    IF alter_table_change_schema(
        schema_name_from => 'public'
        , schema_name_to => 'fr'
        , table_name => 'adresse_ran_has_rao'
    ) THEN
        IF table_exists('fr', 'adresse_ran_has_rao') THEN
            ALTER TABLE fr.adresse_ran_has_rao RENAME TO laposte_delivery_address;
            ALTER TABLE fr.laposte_delivery_address OWNER TO fr;
        END IF;
    END IF;

    IF alter_table_change_schema(
        schema_name_from => 'public'
        , schema_name_to => 'fr'
        , table_name => 'territoire'
    ) THEN
        IF table_exists('fr', 'territoire') THEN
            ALTER TABLE fr.territoire RENAME TO bcaa_territory;
            ALTER TABLE fr.bcaa_territory OWNER TO fr;
        END IF;
    END IF;

    IF alter_table_change_schema(
        schema_name_from => 'public'
        , schema_name_to => 'fr'
        , table_name => 'source_orga_laposte'
    ) THEN
        IF table_exists('fr', 'source_orga_laposte') THEN
            ALTER TABLE fr.source_orga_laposte RENAME TO laposte_organization;
            ALTER TABLE fr.laposte_organization OWNER TO fr;
        END IF;
    END IF;
END $RELOCATE$;
