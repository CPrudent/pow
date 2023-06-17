/***
 * LAPOSTE ADDRESS history
 */


CREATE TABLE IF NOT EXISTS fr.laposte_address_history (
    code_address CHAR(10) NOT NULL          -- address ID
    , date_change DATE NOT NULL
    , change VARCHAR NOT NULL               -- defined into fr.constant (LAPOSTE_ADDRESS_CORRECTION)
    , kind VARCHAR NOT NULL                 -- {ADDRESS, ZONE_ADDRESS, STREET, HOUSENUMBER, COMPLEMENT}
    , values JSONB
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_history_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_history_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_history_code_address ON fr.laposte_address_history (code_address, date_change);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL fr.set_laposte_address_history_index();
END
$$;
