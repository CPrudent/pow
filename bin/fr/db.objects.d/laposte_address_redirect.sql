/***
 * FR: add LAPOSTE/RAN sustainability (restored from backup)
 */

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_redirect_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_redirect_index()
AS
$proc$
BEGIN
    -- old code
    CREATE INDEX IF NOT EXISTS ix_laposte_address_redirect_old ON fr.laposte_address_redirect (address_code_old);
    -- new code
    CREATE INDEX IF NOT EXISTS ix_laposte_address_redirect_new ON fr.laposte_address_redirect (address_code_new);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_laposte_address_redirect_index();
END
$$;
