/***
 * RAN : add ZA data
 */

/*
-- data from RAN-RA18 file
CREATE TABLE IF NOT EXISTS ran.ra18(
    id CHARACTER VARYING(12) NOT NULL
    , co_cea CHAR(10) NOT NULL
    , co_insee CHAR(5) NOT NULL
    , lb_in_ext_loc CHARACTER VARYING(72) NOT NULL
    , lb_an CHARACTER VARYING(32) NOT NULL
    , lb_nn CHARACTER VARYING(38) NOT NULL
    , id_typ_loc INTEGER NOT NULL
    , lb_l5_an CHARACTER VARYING(32)
    , lb_l5_nn CHARACTER VARYING(38)
    , co_postal CHARACTER VARYING(5) NOT NULL
    , lb_ach_an CHARACTER VARYING(32) NOT NULL
    , lb_ach_nn CHARACTER VARYING(38) NOT NULL
    , co_insee_r CHAR(5)
    , fl_etat INTEGER NOT NULL
);

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.ra18 SET (
    AUTOVACUUM_ENABLED = FALSE
);

COMMENT ON TABLE ran.ra18 IS 'Zones d''adresses (INSEE*, CP*, L5, L6*)';
 */

-- address-ZA with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.za
(
    co_cea CHAR(10) NOT NULL
    , dt_reference DATE NOT NULL
    , co_mouvement CHAR(1) NOT NULL
    , fl_active BOOLEAN NOT NULL
    , co_postal CHARACTER VARYING(5) NOT NULL
    , co_insee_commune CHAR(5) NOT NULL
    , co_insee_commune_precedente CHAR(5)
    , lb_in_ext_loc CHARACTER VARYING(72) NOT NULL
    , lb_nn CHARACTER VARYING(38) NOT NULL
    , lb_l5_nn CHARACTER VARYING(38) NULL
    , lb_ach_nn CHARACTER VARYING(38) NOT NULL
    , dt_reference_commune DATE NOT NULL
    , co_insee_commune_ran CHAR(5) NOT NULL
    , co_insee_commune_precedente_ran CHAR(5)
    , co_insee_departement VARCHAR(3) NOT NULL
);

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.za SET (
    AUTOVACUUM_ENABLED = FALSE
);

/*
CREATE TABLE IF NOT EXISTS ran.za_histo
(
    co_cea CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    co_postal CHARACTER VARYING(5) NOT NULL,
    co_insee_commune CHAR(5) NOT NULL,
    co_insee_commune_precedente CHAR(5),
    lb_in_ext_loc CHARACTER VARYING(72) NOT NULL,
    lb_nn CHARACTER VARYING(38) NOT NULL,
    lb_l5_nn CHARACTER VARYING(38),
    lb_ach_nn CHARACTER VARYING(38) NOT NULL,
    dt_reference_commune DATE NOT NULL, -- update date
    co_insee_commune_ran CHAR(5) NOT NULL, -- mode DELTA
    co_insee_commune_precedente_ran CHAR(5), -- mode DELTA
    co_insee_departement VARCHAR(3) NOT NULL
)
;

ALTER TABLE ran.za_histo SET (
    AUTOVACUUM_ENABLED = FALSE
);
 */

SELECT drop_all_functions_if_exists('ran', 'setIndexZa');
CREATE OR REPLACE PROCEDURE ran.setIndexZa()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_za_co_cea') AND NOT index_exists('ran', 'iux_za_co_cea') THEN
        ALTER INDEX idx_za_co_cea RENAME TO iux_za_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_za_co_cea ON ran.za (co_cea);
    END IF;

    -- INSEE
    IF index_exists('ran', 'idx_za_co_insee_com_arr') AND NOT index_exists('ran', 'ix_za_co_insee_commune') THEN
        ALTER INDEX idx_za_co_insee_com_arr RENAME TO ix_za_co_insee_commune;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_co_insee_commune ON ran.za (co_insee_commune);
    END IF;

    -- old INSEE (used by IRISation)
    --	TEST : EXPLAIN SELECT * FROM ran.za AS za WHERE za.co_insee_commune = 'XXXXX' AND za.co_insee_commune_precedente = 'XXXXX'
    --	necessary COALESCE(commune_precedente, '') for use w/ NULL values
    IF index_exists('ran', 'idx_za_co_insee_com_arr_anc') AND NOT index_exists('ran', 'ix_za_co_insee_commune_anc') THEN
        ALTER INDEX idx_za_co_insee_com_arr_anc RENAME TO ix_za_co_insee_commune_anc;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_co_insee_commune_anc ON ran.za (co_insee_commune, COALESCE(co_insee_commune_precedente, ''));
    END IF;

    -- department (not useful)
    DROP INDEX IF EXISTS ran.idx_za_co_insee_dep;
    --CREATE INDEX IF NOT EXISTS idx_za_co_insee_departement ON ran.za (co_insee_departement);

    -- zip code
    IF index_exists('ran', 'idx_za_co_postal') AND NOT index_exists('ran', 'ix_za_co_postal') THEN
        ALTER INDEX idx_za_co_postal RENAME TO ix_za_co_postal;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_co_postal ON ran.za (co_postal);
    END IF;

    -- similar labels
    -- lb_l5_nn
    IF index_exists('ran', 'idx_za_lb_l5_nn') AND NOT index_exists('ran', 'ix_za_lb_l5_nn') THEN
        ALTER INDEX idx_za_lb_l5_nn RENAME TO ix_za_lb_l5_nn;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_l5_nn ON ran.za USING GIN(lb_l5_nn GIN_TRGM_OPS);
    END IF;
    -- lb_in_ext_loc
    IF index_exists('ran', 'idx_za_lb_in_ext_loc') AND NOT index_exists('ran', 'ix_za_lb_in_ext_loc') THEN
        ALTER INDEX idx_za_lb_in_ext_loc RENAME TO ix_za_lb_in_ext_loc;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_in_ext_loc ON ran.za USING GIN(lb_in_ext_loc GIN_TRGM_OPS);
    END IF;
    -- lb_nn
    IF index_exists('ran', 'idx_za_lb_nn') AND NOT index_exists('ran', 'ix_za_lb_nn') THEN
        ALTER INDEX idx_za_lb_nn RENAME TO ix_za_lb_nn;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_nn ON ran.za USING GIN(lb_nn GIN_TRGM_OPS);
    END IF;
    -- lb_ach_nn
    IF index_exists('ran', 'idx_za_lb_ach_nn') AND NOT index_exists('ran', 'ix_za_lb_ach_nn') THEN
        ALTER INDEX idx_za_lb_ach_nn RENAME TO ix_za_lb_ach_nn;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_ach_nn ON ran.za USING GIN(lb_ach_nn GIN_TRGM_OPS);
    END IF;

    -- date history
    DROP INDEX IF EXISTS ran.idx_za_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_za_histo_key ON ran.za_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    PERFORM ran.setIndexZa();
END
$$;
