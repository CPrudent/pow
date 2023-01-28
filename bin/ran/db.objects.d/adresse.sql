/***
 * RAN : add ADDRESS data
 */

/*
-- data from RAN-RA49 file
CREATE TABLE IF NOT EXISTS ran.adresse_ra49(
    co_cea_voie CHAR(10),
    co_cea_numero CHAR(10),
    co_cea_l3 CHAR(10),
    co_cea_za CHAR(10) NOT NULL,
    fl_diffusable INTEGER NOT NULL
)
;

ALTER TABLE ran.adresse_ra49 SET (
    AUTOVACUUM_ENABLED = FALSE
);

COMMENT ON TABLE ran.adresse_ra49 IS 'Adresses RAN';
 */

CREATE TABLE IF NOT EXISTS ran.adresse
(
    co_cea_determinant CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    co_cea_parent CHAR(10) NULL,
    co_niveau VARCHAR(10) NOT NULL,
    co_cea_l3 CHAR(10) NULL,
    dt_reference_l3 DATE NULL,
    co_cea_numero CHAR(10) NULL,
    dt_reference_numero DATE NULL,
    co_cea_voie CHAR(10) NULL,
    dt_reference_voie DATE NULL,
    co_cea_za CHAR(10) NOT NULL,
    dt_reference_za DATE NOT NULL
)
;

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.adresse SET (
    AUTOVACUUM_ENABLED = FALSE
);

/*
CREATE TABLE IF NOT EXISTS ran.adresse_histo
(
    co_cea_determinant CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    co_cea_parent CHAR(10) NULL,
    co_niveau VARCHAR(10) NOT NULL,
    co_cea_l3 CHAR(10) NULL,
    dt_reference_l3 DATE NULL,
    co_cea_numero CHAR(10) NULL,
    dt_reference_numero DATE NULL,
    co_cea_voie CHAR(10) NULL,
    dt_reference_voie DATE NULL,
    co_cea_za CHAR(10) NOT NULL,
    dt_reference_za DATE NOT NULL
)
;

ALTER TABLE ran.adresse_histo SET (
    AUTOVACUUM_ENABLED = FALSE
);
 */

SELECT drop_all_functions_if_exists('ran', 'setIndexAddress');
CREATE OR REPLACE PROCEDURE ran.setIndexAddress()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_adresse_co_cea_determinant') AND NOT index_exists('ran', 'iux_adresse_co_cea_determinant') THEN
        ALTER INDEX idx_adresse_co_cea_determinant RENAME TO iux_adresse_co_cea_determinant;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_adresse_co_cea_determinant ON ran.adresse (co_cea_determinant);
    END IF;

    -- level
    IF index_exists('ran', 'idx_adresse_niveau') AND NOT index_exists('ran', 'ix_adresse_niveau') THEN
        ALTER INDEX idx_adresse_niveau RENAME TO ix_adresse_niveau;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_adresse_niveau ON ran.adresse (co_niveau);
    END IF;

    -- parent
    IF index_exists('ran', 'idx_adresse_co_cea_parent') AND NOT index_exists('ran', 'ix_adresse_co_cea_parent') THEN
        ALTER INDEX idx_adresse_co_cea_parent RENAME TO ix_adresse_co_cea_parent;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_adresse_co_cea_parent ON ran.adresse (co_cea_parent);
    END IF;

    -- co_cea_l3
    IF index_exists('ran', 'idx_adresse_co_cea_l3') AND NOT index_exists('ran', 'iux_adresse_co_cea_l3') THEN
        ALTER INDEX idx_adresse_co_cea_l3 RENAME TO iux_adresse_co_cea_l3;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_adresse_co_cea_l3 ON ran.adresse (co_cea_l3); --WHERE co_cea_l3 IS NOT NULL ?
    END IF;

    -- co_cea_numero
    IF index_exists('ran', 'idx_adresse_co_cea_numero') AND NOT index_exists('ran', 'ix_adresse_co_cea_numero') THEN
        ALTER INDEX idx_adresse_co_cea_numero RENAME TO ix_adresse_co_cea_numero;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_adresse_co_cea_numero ON ran.adresse (co_cea_numero); --WHERE co_cea_numero IS NOT NULL ?
    END IF;

    -- co_cea_voie
    IF index_exists('ran', 'idx_adresse_co_cea_voie') AND NOT index_exists('ran', 'ix_adresse_co_cea_voie') THEN
        ALTER INDEX idx_adresse_co_cea_voie RENAME TO ix_adresse_co_cea_voie;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_adresse_co_cea_voie ON ran.adresse (co_cea_voie);
    END IF;

    -- co_cea_za
    IF index_exists('ran', 'idx_adresse_co_cea_za') AND NOT index_exists('ran', 'ix_adresse_co_cea_za') THEN
        ALTER INDEX idx_adresse_co_cea_za RENAME TO ix_adresse_co_cea_za;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_adresse_co_cea_za ON ran.adresse (co_cea_za);
    END IF;

    DROP INDEX IF EXISTS idx_adresse_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_adresse_histo_key ON ran.adresse_histo (co_cea_determinant, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL ran.setIndexAddress();
END
$$;
