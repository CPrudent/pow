/***
 * FR: add LAPOSTE/RAN housenumber
 */

-- address-housenumber with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_housenumber
(
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

SELECT drop_all_functions_if_exists('fr', 'setLaPosteIndexHousenumber');
CREATE OR REPLACE PROCEDURE fr.setLaPosteIndexHousenumber()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_numero_co_cea') AND NOT index_exists('fr', 'iux_laposte_housenumber_co_cea') THEN
        ALTER INDEX idx_numero_co_cea RENAME TO iux_laposte_housenumber_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_housenumber_co_cea ON fr.laposte_housenumber (co_cea);
    END IF;

    DROP INDEX IF EXISTS fr.idx_numero_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_numero_histo_key ON fr.laposte_housenumber_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.setLaPosteIndexHousenumber();
END
$$;
