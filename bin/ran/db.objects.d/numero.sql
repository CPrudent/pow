/***
 * RAN : add HOUSENUMBER data
 */

/*
-- data from RAN-RA33 file
CREATE TABLE IF NOT EXISTS ran.numero_ra33(
    co_cea CHAR(10) NOT NULL,
    no_voie INTEGER NOT NULL,
    lb_ext CHARACTER VARYING(10) NULL,
    lb_abr_an CHARACTER VARYING(1) NULL,
    lb_abr_nn CHARACTER VARYING(1) NULL,
    fl_etat INTEGER NOT NULL,
    fl_diffusable INTEGER NOT NULL
)
;

ALTER TABLE ran.numero_ra33 SET (
    AUTOVACUUM_ENABLED = FALSE
);

COMMENT ON TABLE ran.numero_ra33 IS 'Adresses numéro';
 */

-- address-housenumber with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.numero
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

/*
CREATE TABLE IF NOT EXISTS ran.numero_histo
(
    co_cea CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    no_voie INTEGER NOT NULL,
    lb_ext CHARACTER VARYING(10) NULL,
    lb_abr_nn CHARACTER VARYING(1) NULL
)
;

ALTER TABLE ran.numero_histo SET (
    AUTOVACUUM_ENABLED = FALSE
);
 */

SELECT drop_all_functions_if_exists('ran', 'setIndexHousenumber');
CREATE OR REPLACE PROCEDURE ran.setIndexHousenumber()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_ran_numero_co_cea') AND NOT index_exists('ran', 'iux_numero_co_cea') THEN
        ALTER INDEX idx_ran_numero_co_cea RENAME TO iux_numero_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_numero_co_cea ON ran.numero (co_cea);
    END IF;

    DROP INDEX IF EXISTS ran.idx_numero_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_numero_histo_key ON ran.numero_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL ran.setIndexHousenumber();
END
$$;
