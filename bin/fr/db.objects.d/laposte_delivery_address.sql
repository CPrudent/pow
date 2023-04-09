/***
 * FR: add LAPOSTE/RAO delivery (restored from backup)
 */

SELECT drop_all_functions_if_exists('fr', 'set_laposte_delivery_address_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_delivery_address_index()
AS
$proc$
BEGIN
    -- code
    IF index_exists('fr', 'idx_adresse_ran_has_rao_01') AND NOT index_exists('fr', 'iux_laposte_delivery_address_code') THEN
        ALTER INDEX idx_adresse_ran_has_rao_01 RENAME TO iux_laposte_delivery_address_code;
    ELSE
        CREATE INDEX IF NOT EXISTS iux_laposte_delivery_address_code ON fr.laposte_delivery_address (co_adr);
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_laposte_delivery_address_index();
END
$$;
