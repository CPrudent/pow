/***
 * FR: add LAPOSTE/RAN street
 */

DO $STREET$
BEGIN
    ALTER TABLE IF EXISTS fr.laposte_street RENAME TO laposte_address_street;
    ALTER INDEX IF EXISTS fr.iux_laposte_street_co_cea RENAME TO iux_laposte_address_street_co_cea;
    ALTER INDEX IF EXISTS fr.ix_laposte_street_lb_voie RENAME TO ix_laposte_address_street_lb_voie;
END $STREET$;

-- address-street with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_address_street (
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
    co_insee_commune CHAR(5) NOT NULL, --FIXME : previously necessary for index on strong word (by municipality)
    co_cea_za CHAR(10) --NOTE : useful for index ix_laposte_address_street_co_cea_za_lb_voie (trigrams)
)
;

-- manual VACUUM
ALTER TABLE fr.laposte_address_street SET (
    AUTOVACUUM_ENABLED = FALSE
);

-- rename indexes after restore (original LAPOSTE data) or create them
SELECT drop_all_functions_if_exists('fr', 'set_laposte_street_index');
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_index()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_voie_co_cea') AND NOT index_exists('fr', 'iux_laposte_address_street_co_cea') THEN
        ALTER INDEX idx_voie_co_cea RENAME TO iux_laposte_address_street_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_street_co_cea ON fr.laposte_address_street (co_cea);
    END IF;

    --CREATE INDEX IF NOT EXISTS idx_voie_co_insee_commune_lb_md ON fr.laposte_address_street USING GIST(co_insee_commune, lb_md GIST_TRGM_OPS);
    --CREATE INDEX IF NOT EXISTS idx_voie_co_postal_lb_md ON fr.laposte_address_street USING GIST(co_postal, lb_md GIST_TRGM_OPS);
    --CREATE INDEX IF NOT EXISTS idx_voie_co_cea_za_lb_voie ON fr.laposte_address_street USING GIST(co_cea_za, lb_voie GIST_TRGM_OPS);
    DROP INDEX IF EXISTS fr.idx_voie_co_insee_commune_lb_md;
    DROP INDEX IF EXISTS fr.idx_voie_co_postal_lb_md;
    DROP INDEX IF EXISTS fr.idx_voie_co_cea_za_lb_voie;

    --CREATE INDEX IF NOT EXISTS idx_voie_co_insee_departement ON fr.laposte_address_street (public.get_department_code_from_district_code(co_insee_commune));
    DROP INDEX IF EXISTS fr.idx_voie_co_insee_departement;

    -- similar labels
    -- lb_voie
    IF index_exists('fr', 'idx_voie_lb_voie') AND NOT index_exists('fr', 'ix_laposte_address_street_lb_voie') THEN
        ALTER INDEX idx_voie_lb_voie RENAME TO ix_laposte_address_street_lb_voie;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_address_street_lb_voie ON fr.laposte_address_street USING GIN(lb_voie GIN_TRGM_OPS);
    END IF;

    -- lb_md
    --CREATE INDEX ix_voie_lb_md ON fr.laposte_address_street USING GIN(lb_md GIN_TRGM_OPS);

    DROP INDEX IF EXISTS fr.idx_voie_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_voie_histo_key ON fr.laposte_address_street_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_laposte_address_street_index();
END
$$;
