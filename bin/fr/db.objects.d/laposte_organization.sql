/***
 * FR: add LAPOSTE/SOURCE-ORGA organization (restored from backup)
 */

SELECT drop_all_functions_if_exists('fr', 'set_laposte_organization_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_organization_index()
AS
$proc$
BEGIN
    -- laposte_organization
    -- code
    IF index_exists('fr', 'ix_source_orga_laposte_code') AND NOT index_exists('fr', 'ix_laposte_organization_code') THEN
        ALTER INDEX ix_source_orga_laposte_code RENAME TO ix_laposte_organization_code;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_organization_code ON fr.laposte_organization (code);
    END IF;

    -- laposte_organization_all
    -- code
    IF index_exists('fr', 'ix_source_orga_code') AND NOT index_exists('fr', 'ix_laposte_organization_all_code') THEN
        ALTER INDEX ix_source_orga_code RENAME TO ix_laposte_organization_all_code;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_organization_all_code ON fr.laposte_organization_all (code);
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_laposte_organization_index();
END
$$;
