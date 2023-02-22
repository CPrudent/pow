/***
 * FR: add LAPOSTE/RAN address
 */

CREATE TABLE IF NOT EXISTS fr.laposte_address
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

-- manual VACUUM
ALTER TABLE fr.laposte_address SET (
    AUTOVACUUM_ENABLED = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'setLaPosteIndexAddress');
CREATE OR REPLACE PROCEDURE fr.setLaPosteIndexAddress(
    simulation BOOLEAN DEFAULT FALSE
    )
AS
$proc$
DECLARE
    _query VARCHAR;
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_adresse_co_cea_determinant') AND NOT index_exists('fr', 'iux_laposte_address_co_cea_determinant') THEN
        _query := 'ALTER INDEX idx_adresse_co_cea_determinant RENAME TO iux_laposte_address_co_cea_determinant';
    ELSE
        _query := 'CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_co_cea_determinant ON fr.laposte_address (co_cea_determinant)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    -- level
    IF index_exists('fr', 'idx_adresse_niveau') AND NOT index_exists('fr', 'ix_laposte_address_niveau') THEN
        _query := 'ALTER INDEX idx_adresse_niveau RENAME TO ix_laposte_address_niveau';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_address_niveau ON fr.laposte_address (co_niveau)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;
    -- parent
    IF index_exists('fr', 'idx_adresse_co_cea_parent') AND NOT index_exists('fr', 'ix_laposte_address_co_cea_parent') THEN
        _query := 'ALTER INDEX idx_adresse_co_cea_parent RENAME TO ix_laposte_address_co_cea_parent';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_address_co_cea_parent ON fr.laposte_address (co_cea_parent)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;
    -- co_cea_l3
    IF index_exists('fr', 'idx_adresse_co_cea_l3') AND NOT index_exists('fr', 'iux_laposte_address_co_cea_l3') THEN
        _query := 'ALTER INDEX idx_adresse_co_cea_l3 RENAME TO iux_laposte_address_co_cea_l3';
    ELSE
        _query := 'CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_co_cea_l3 ON fr.laposte_address (co_cea_l3)'; --WHERE co_cea_l3 IS NOT NULL ?
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;
    -- co_cea_numero
    IF index_exists('fr', 'idx_adresse_co_cea_numero') AND NOT index_exists('fr', 'ix_laposte_address_co_cea_numero') THEN
        _query := 'ALTER INDEX idx_adresse_co_cea_numero RENAME TO ix_laposte_address_co_cea_numero';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_address_co_cea_numero ON fr.laposte_address (co_cea_numero)'; --WHERE co_cea_numero IS NOT NULL ?
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;
    -- co_cea_voie
    IF index_exists('fr', 'idx_adresse_co_cea_voie') AND NOT index_exists('fr', 'ix_laposte_address_co_cea_voie') THEN
        _query := 'ALTER INDEX idx_adresse_co_cea_voie RENAME TO ix_laposte_address_co_cea_voie';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_address_co_cea_voie ON fr.laposte_address (co_cea_voie)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;
    -- co_cea_za
    IF index_exists('fr', 'idx_adresse_co_cea_za') AND NOT index_exists('fr', 'ix_laposte_address_co_cea_za') THEN
        _query := 'ALTER INDEX idx_adresse_co_cea_za RENAME TO ix_laposte_address_co_cea_za';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_address_co_cea_za ON fr.laposte_address (co_cea_za)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    DROP INDEX IF EXISTS fr.idx_adresse_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_laposte_address_histo_key ON fr.laposte_address_histo (co_cea_determinant, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.setLaPosteIndexAddress();
END
$$;
