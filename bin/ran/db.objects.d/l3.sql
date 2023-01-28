/***
 * RAN : add COMPLEMENT data
 */

/*
-- data from RAN-RA34 file
CREATE TABLE IF NOT EXISTS ran.l3_ra34(
    co_cea CHAR(10) NOT NULL,
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
    lb_mot_dir_groupe3 CHARACTER VARYING(38),
    fl_zone CHARACTER VARYING(1),
    lb_standard_an CHARACTER VARYING(32),
    lb_standard_nn CHARACTER VARYING(38) NOT NULL,
    fl_etat_adresse INTEGER NOT NULL,
    fl_diffusable INTEGER NOT NULL
)
;

ALTER TABLE ran.l3_ra34 SET (
    AUTOVACUUM_ENABLED = FALSE
);

COMMENT ON TABLE ran.l3_ra34 IS 'Adresses ligne 3';
 */

-- address-complement with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.l3
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

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.l3 SET (
    AUTOVACUUM_ENABLED = FALSE
);

/*
CREATE TABLE IF NOT EXISTS ran.l3_histo
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

ALTER TABLE ran.l3_histo SET (
    AUTOVACUUM_ENABLED = FALSE
);
 */

SELECT drop_all_functions_if_exists('ran', 'setIndexComplement');
CREATE OR REPLACE PROCEDURE ran.setIndexComplement()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_l3_co_cea') AND NOT index_exists('ran', 'iux_l3_co_cea') THEN
        ALTER INDEX idx_l3_co_cea RENAME TO iux_l3_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_l3_co_cea ON ran.l3 (co_cea);
    END IF;

    -- similar labels
    -- lb_standard_nn
    IF index_exists('ran', 'idx_l3_lb_standard_nn') AND NOT index_exists('ran', 'ix_l3_lb_standard_nn') THEN
        ALTER INDEX idx_l3_lb_standard_nn RENAME TO ix_l3_lb_standard_nn;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_l3_lb_standard_nn ON ran.l3 USING GIN(lb_standard_nn GIN_TRGM_OPS);
    END IF;

    DROP INDEX IF EXISTS idx_l3_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_l3_histo_key ON ran.l3_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- create indexes
    PERFORM ran.setIndexComplement();
END
$$;
