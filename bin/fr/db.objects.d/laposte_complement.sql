/***
 * FR: add LAPOSTE/RAN complement
 */

-- address-complement with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_complement
(
    co_cea CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    lb_standard_nn CHARACTER VARYING(38) NOT NULL,
    id_type_groupe1_l3 INTEGER,
    lb_type_groupe1_l3 CHARACTER VARYING(38),
    lb_abrev_g1_an CHARACTER VARYING(10),
    lb_abrev_g1_nn CHARACTER VARYING(10),
    lb_groupe1 CHARACTER VARYING(38),
    id_type_groupe2_l3 INTEGER,
    lb_type_groupe2_l3 CHARACTER VARYING(38),
    lb_abrev_g2_an CHARACTER VARYING(10),
    lb_abrev_g2_nn CHARACTER VARYING(10),
    lb_groupe2 CHARACTER VARYING(38),
    id_type_groupe3_l3 INTEGER,
    lb_type_groupe3_l3 CHARACTER VARYING(38),
    lb_abrev_g3_an CHARACTER VARYING(10),
    lb_abrev_g3_nn CHARACTER VARYING(10),
    lb_groupe3 CHARACTER VARYING(38),
    lb_descr_an_groupe1 CHARACTER VARYING(10),
    lb_descr_nn_groupe1 CHARACTER VARYING(10),
    lb_mot_dir_groupe1 CHARACTER VARYING(38),
    lb_descr_an_groupe2 CHARACTER VARYING(10),
    lb_descr_nn_groupe2 CHARACTER VARYING(10),
    lb_mot_dir_groupe2 CHARACTER VARYING(38),
    lb_descr_an_groupe3 CHARACTER VARYING(10),
    lb_descr_nn_groupe3 CHARACTER VARYING(10),
    lb_mot_dir_groupe3 CHARACTER VARYING(38)
)
;

-- manual VACUUM
ALTER TABLE fr.laposte_complement SET (
    AUTOVACUUM_ENABLED = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'setLaPosteIndexComplement');
CREATE OR REPLACE PROCEDURE fr.setLaPosteIndexComplement()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_l3_co_cea') AND NOT index_exists('fr', 'iux_laposte_complement_co_cea') THEN
        ALTER INDEX idx_l3_co_cea RENAME TO iux_laposte_complement_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_complement_co_cea ON fr.laposte_complement (co_cea);
    END IF;

    -- similar labels
    -- lb_standard_nn
    IF index_exists('fr', 'idx_l3_lb_standard_nn') AND NOT index_exists('fr', 'ix_laposte_complement_lb_standard_nn') THEN
        ALTER INDEX idx_l3_lb_standard_nn RENAME TO ix_laposte_complement_lb_standard_nn;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_complement_lb_standard_nn ON fr.laposte_complement USING GIN(lb_standard_nn GIN_TRGM_OPS);
    END IF;

    DROP INDEX IF EXISTS fr.idx_l3_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_l3_histo_key ON fr.laposte_complement_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.setLaPosteIndexComplement();
END
$$;
