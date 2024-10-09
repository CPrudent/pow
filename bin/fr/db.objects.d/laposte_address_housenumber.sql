/***
 * FR: add LAPOSTE/RAN housenumber
 */

DO $HOUSENUMBER$
BEGIN
    ALTER TABLE IF EXISTS fr.laposte_housenumber RENAME TO laposte_address_housenumber;
    ALTER INDEX IF EXISTS fr.iux_laposte_housenumber_co_cea RENAME TO iux_laposte_address_housenumber_co_cea;
END $HOUSENUMBER$;

-- address-housenumber with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_address_housenumber (
    co_cea CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    no_voie INTEGER NOT NULL,
    lb_ext CHARACTER VARYING(10) NULL,
    lb_abr_nn CHARACTER VARYING(1) NULL --FIXME : rename to lb_abr ?
)
;

-- rename indexes after restore (original LAPOSTE data) or create them
SELECT drop_all_functions_if_exists('fr', 'set_laposte_housenumber_index');
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_housenumber_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_housenumber_index()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_numero_co_cea') AND NOT index_exists('fr', 'iux_laposte_address_housenumber_co_cea') THEN
        ALTER INDEX idx_numero_co_cea RENAME TO iux_laposte_address_housenumber_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_housenumber_co_cea ON fr.laposte_address_housenumber (co_cea);
    END IF;

    DROP INDEX IF EXISTS fr.idx_numero_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_numero_histo_key ON fr.laposte_address_housenumber_histo (co_cea, dt_reference);

    CREATE INDEX IF NOT EXISTS ix_laposte_address_housenumber_number ON fr.laposte_address_housenumber (no_voie);
END
$proc$ LANGUAGE plpgsql;

DO $$
DECLARE
    _query TEXT;
BEGIN
    -- manage indexes
    CALL fr.set_laposte_address_housenumber_index();

    -- create views
    _query := '
        SELECT
            -- HOUSENUMBER
            dict.id,
            dict.number,
            dict.extension,
            dict.occurs,

            -- ADDRESS
            address.co_cea_determinant AS co_adr,
            address.co_cea_za AS co_adr_za,
            address.co_cea_voie AS co_adr_voie
        FROM
            fr.laposte_address_housenumber_uniq dict
                JOIN fr.laposte_address_housenumber_reference ref ON dict.id = ref.number_id
                JOIN fr.laposte_address address ON address.co_cea_determinant = ref.address_id
    ';
    DROP VIEW IF EXISTS fr.housenumber_dict_view CASCADE;
    EXECUTE CONCAT_WS(
        ' ',
        'CREATE VIEW fr.housenumber_dict_view AS',
        _query
    );
END
$$;
