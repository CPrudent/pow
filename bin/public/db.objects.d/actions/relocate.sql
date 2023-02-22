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
        ALTER TABLE fr.adresse_ran_has_rao RENAME TO laposte_delivery_address;
    END IF;

    IF alter_table_change_schema(
        schema_name_from => 'public'
        , schema_name_to => 'fr'
        , table_name => 'territoire'
    ) THEN
        ALTER TABLE fr.territoire RENAME TO bcaa_territory;
    END IF;

    IF alter_table_change_schema(
        schema_name_from => 'public'
        , schema_name_to => 'fr'
        , table_name => 'source_orga_laposte'
    ) THEN
        ALTER TABLE fr.source_orga_laposte RENAME TO bcaa_organization;
    END IF;
END $RELOCATE$;
