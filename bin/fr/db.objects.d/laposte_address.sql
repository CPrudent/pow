/***
 * FR: add LAPOSTE/RAN address
 */

CREATE TABLE IF NOT EXISTS fr.laposte_address (
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

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_index(
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
DECLARE
    _query TEXT;
BEGIN
    -- manage indexes
    CALL fr.set_laposte_address_index();

    -- create views
    _query := '
        SELECT
            -- ADDRESS
            adresse.co_cea_determinant AS co_adr,
            adresse.dt_reference AS dt_reference_adr,
            adresse.co_niveau,
            adresse.co_cea_parent AS co_adr_parent,
            adresse.co_cea_l3 AS co_adr_l3,
            adresse.co_cea_numero AS co_adr_numero,
            adresse.co_cea_voie AS co_adr_voie,
            adresse.co_cea_za AS co_adr_za,
            adresse.fl_diffusable,
            adresse.fl_active,

            -- COMPLEMENT
            cdict.name AS lb_ligne3,
            cdict.name_normalized AS lb_ligne3_normalise,
            cdict.descriptors AS lb_ligne3_desc,

            -- HOUSENUMBER
            numero.no_voie AS no_numero,
            numero.lb_ext AS lb_extension_numero,
            numero.lb_abr_nn AS lb_extension_numero_abrege,

            -- STREET
            sdict.name AS lb_voie,
            sdict.name_normalized AS lb_voie_normalise,
            sdict.descriptors AS lb_voie_desc,

            -- AREA
            za.co_postal AS co_postal,
            za.lb_l5_nn AS lb_ligne5,
            za.lb_ach_nn AS lb_acheminement,
            za.co_insee_commune,
            za.co_insee_commune_precedente,
            za.co_insee_departement,
            za.fl_active AS fl_active_za,

            -- XY
            coord.co_cea AS co_coord,
            ''RAN''::VARCHAR AS co_source_coord,
            coord.dt_reference AS dt_reference_coord,
            coord.no_type_localisation AS no_type_localisation_coord,
            coord.va_x AS x_natif_coord,
            coord.va_y AS y_natif_coord,
            coord.gm_coord,

            -- DELIVERY
            rao.co_type AS rao_co_type,
            rao.lb_libelle AS rao_lb_libelle,
            rao.co_roc_site,
            source_orga.code_regate AS rao_co_regate,
            source_orga.libelle AS rao_libelle_site,
            NULLIF(CONCAT(rao.co_type, rao.lb_libelle), '''') AS rao_co_tournee
        FROM
            fr.laposte_address adresse
                LEFT OUTER JOIN fr.laposte_address_area za ON za.co_cea = adresse.co_cea_za
                --LEFT OUTER JOIN fr.laposte_address_street voie ON voie.co_cea = adresse.co_cea_voie
                LEFT OUTER JOIN fr.laposte_address_housenumber numero ON numero.co_cea = adresse.co_cea_numero
                --LEFT OUTER JOIN fr.laposte_address_complement l3 ON l3.co_cea = adresse.co_cea_l3
                LEFT OUTER JOIN fr.laposte_address_xy coord ON coord.co_cea = adresse.co_cea_determinant

                LEFT OUTER JOIN fr.laposte_address_street_reference sref ON adresse.co_cea_voie = sref.address_id
                LEFT OUTER JOIN fr.laposte_address_street_uniq sdict ON sref.name_id = sdict.id

                LEFT OUTER JOIN fr.laposte_address_complement_reference cref ON adresse.co_cea_l3 = cref.address_id
                LEFT OUTER JOIN fr.laposte_address_complement_uniq cdict ON cref.name_id = cdict.id

                LEFT OUTER JOIN fr.laposte_delivery_address rao ON rao.co_adr = adresse.co_cea_determinant
                    --TODO : modifier le type de la colonne source_orga.code : alter table source_orga alter column code type CHAR(6);
                    -- Pour éviter de devoir faire un CAST en VARCHAR et rendre exploitable l''index sur source_orga.code
                LEFT OUTER JOIN fr.laposte_organization source_orga ON source_orga.code = rao.co_roc_site::VARCHAR
    ';

    DROP VIEW IF EXISTS fr.address_all_view CASCADE;
    EXECUTE CONCAT_WS(
        ' ',
        'CREATE VIEW fr.address_all_view AS',
        _query
    );

    DROP VIEW IF EXISTS fr.address_view CASCADE;
    EXECUTE CONCAT_WS(
        ' ',
        'CREATE VIEW fr.address_view AS',
        _query,
        'WHERE adresse.fl_active AND adresse.fl_diffusable'
    );
END $$;
