/***
 * RAN : add STREET data
 */

/*
-- data from RAN-RA41 file
CREATE TABLE IF NOT EXISTS ran.voie_ra41(
    co_cea CHAR(10),
    co_voie NUMERIC(8,0) NOT NULL,
    co_insee CHARACTER VARYING(5) NOT NULL,
    lb_voie CHARACTER VARYING(60) NOT NULL,
    lb_voie_an CHARACTER VARYING(27),
    lb_voie_nn CHARACTER VARYING(32),
    lb_abr_an CHARACTER VARYING(4) NULL,
    lb_abr_nn CHARACTER VARYING(4) NULL,
    lb_desc_an CHARACTER VARYING(10) /*NOT*/ NULL,
    lb_desc_nn CHARACTER VARYING(10) /*NOT*/ NULL,
    lb_md CHARACTER VARYING(20) NULL,
    co_insee_anc CHARACTER VARYING(5),
    fl_etat NUMERIC(1,0) NOT NULL,
    fl_adr NUMERIC(1,0) NOT NULL,
    lb_in_ext_typ_voie CHARACTER VARYING(38) NULL,
    fl_diffusable NUMERIC(1,0) NOT NULL
)
;

ALTER TABLE ran.voie_ra41 SET (
    AUTOVACUUM_ENABLED = FALSE
);

COMMENT ON TABLE ran.voie_ra41 IS 'Adresses voie';
 */

-- address-street with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.voie
(
    co_cea CHAR(10),
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    co_voie NUMERIC(8,0) NOT NULL,
    lb_voie CHARACTER VARYING(60) NOT NULL,
    lb_voie_normalise CHARACTER VARYING(32) NOT NULL,
    lb_type CHARACTER VARYING(38) NULL,
    lb_type_abrege CHARACTER VARYING(4) NULL,
    lb_md CHARACTER VARYING(20) NULL,
    lb_desc CHARACTER VARYING(10) NOT NULL,
    co_insee_commune CHAR(5) NOT NULL, --FIXME : previously necessary for index on strong word (by district)
    co_cea_za CHAR(10) --NOTE : useful for index idx_voie_co_cea_za_lb_voie (trigrams)
)
;

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.voie SET (
    AUTOVACUUM_ENABLED = FALSE
);

/*
CREATE TABLE IF NOT EXISTS ran.voie_histo
(
    co_cea CHAR(10),
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    co_voie NUMERIC(8,0) NOT NULL,
    lb_voie CHARACTER VARYING(60) NOT NULL,
    lb_voie_normalise CHARACTER VARYING(32) NOT NULL,
    lb_type CHARACTER VARYING(38) NULL,
    lb_type_abrege CHARACTER VARYING(4) NULL,
    lb_md CHARACTER VARYING(20) NULL,
    lb_desc CHARACTER VARYING(10) NOT NULL
)
;

ALTER TABLE ran.voie_histo SET (
    AUTOVACUUM_ENABLED = FALSE
);
 */

SELECT drop_all_functions_if_exists('ran', 'setIndexStreet');
CREATE OR REPLACE PROCEDURE ran.setIndexStreet()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_voie_co_cea') AND NOT index_exists('ran', 'iux_voie_co_cea') THEN
        ALTER INDEX idx_voie_co_cea RENAME TO iux_voie_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_voie_co_cea ON ran.voie (co_cea);
    END IF;

    --CREATE INDEX IF NOT EXISTS idx_voie_co_insee_commune_lb_md ON ran.voie USING GIST(co_insee_commune, lb_md GIST_TRGM_OPS);
    --CREATE INDEX IF NOT EXISTS idx_voie_co_postal_lb_md ON ran.voie USING GIST(co_postal, lb_md GIST_TRGM_OPS);
    DROP INDEX IF EXISTS ran.idx_voie_co_insee_commune_lb_md;
    DROP INDEX IF EXISTS ran.idx_voie_co_postal_lb_md;

    --CREATE INDEX IF NOT EXISTS idx_voie_co_insee_departement ON ran.voie (public.get_department_code_from_district_code(co_insee_commune));
    DROP INDEX IF EXISTS ran.idx_voie_co_insee_departement;

    -- similar labels
    -- lb_voie
    IF index_exists('ran', 'idx_voie_lb_voie') AND NOT index_exists('ran', 'ix_voie_lb_voie') THEN
        ALTER INDEX idx_voie_lb_voie RENAME TO ix_voie_lb_voie;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_voie_lb_voie ON ran.voie USING GIN(lb_voie GIN_TRGM_OPS);
    END IF;

    -- lb_md
    --CREATE INDEX ix_voie_lb_md ON ran.voie USING GIN(lb_md GIN_TRGM_OPS);

    DROP INDEX IF EXISTS ran.idx_voie_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_voie_histo_key ON ran.voie_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL ran.setIndexStreet();
END
$$;
