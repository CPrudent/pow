/***
 * FR: add LAPOSTE/GEOPAD delivery point (PDI)
 */

-- delivery points
CREATE TABLE IF NOT EXISTS fr.laposte_delivery_point (
    id_import INTEGER NOT NULL
    , id_import_dernier_init_en_delta INTEGER NULL
    , pdi_id INTEGER NOT NULL
    , pdi_id_rattachement INTEGER NULL
    , pdi_etat SMALLINT NOT NULL
    , pdi_dt_creation TIMESTAMP WITHOUT TIME ZONE
    , pdi_dt_modification TIMESTAMP WITHOUT TIME ZONE NOT NULL
    , pdi_source CHARACTER VARYING NOT NULL
    , pdi_nature_code CHAR(3) NULL
    , pdi_nature CHARACTER VARYING
    , pdi_statut VARCHAR(10)
    , pdi_model CHAR(2) NULL
    , pdi_visible BOOLEAN NOT NULL
    , pdi_particularite CHARACTER VARYING
    , pdi_etablissement_regate CHAR(6) NULL
    , pdi_etablissement_roc CHAR(6) NULL
    , pdi_bureau_instance CHAR(6) NULL
    , pdi_pre1 INTEGER NULL
    , pdi_pre2 INTEGER NULL
    , pdi_pre3 INTEGER NULL
    , pdi_pre4 INTEGER NULL
    , pdi_pre5 INTEGER NULL
    , pdi_pre6 INTEGER NULL
    , pdi_pre7 INTEGER NULL
    , pdi_pre8 INTEGER NULL
    , pdi_pre9 INTEGER NULL
    , pdi_pre10 INTEGER NULL
    , pdi_pre11 INTEGER NULL
    , pdi_localisation CHARACTER VARYING
    , pdi_id_batterie_cidex CHARACTER VARYING
    , pdi_distance NUMERIC NULL
    , pdi_type_acces CHARACTER VARYING
    , pdi_nb_bal_normalisees INTEGER NULL
    , pdi_nb_bal_non_normalisees INTEGER NULL
    , pdi_nb_bal_etiquetees INTEGER NULL
    , pdi_ind_presence CHARACTER VARYING NULL
    , pdi_ind_presence_gardien INTEGER
    , pdi_ind_presence_presse INTEGER
    , pdi_ind_presence_num_rue INTEGER
    , pdi_ind_presence_plaque_rue INTEGER
    , pdi_ind_presence_depot_relais INTEGER
    , pdi_ind_presence_productif INTEGER
    , pdi_ind_presence_tab_indicateur INTEGER
    , ip_id CHARACTER VARYING
    , ip_stop_pub INTEGER NULL
    , ip_potentiel_ip INTEGER NULL
    , ip_code_udb CHARACTER VARYING
    , ip_poids_main CHARACTER VARYING
    , ip_comment CHARACTER VARYING
    , distri_etablissement_or CHARACTER VARYING
    , distri_etablissement_os CHARACTER VARYING
    , distri_etablissement_pr CHARACTER VARYING
    , distri_etablissement_co CHARACTER VARYING
    , distri_etablissement_ip CHARACTER VARYING
    , adresse_id CHAR(10) NULL
    , adresse_x DOUBLE PRECISION
    , adresse_y DOUBLE PRECISION
    , adresse_geocode SMALLINT
    , agg_adresse_id CHAR(10) NULL
    , agg_nb_pdi INTEGER NULL
    , nb_pre_par_per_log_ind INTEGER NULL
    , nb_pre_par_per_log_col INTEGER NULL
    , agg_nb_pdi_repositionnes INTEGER NULL
    , geom GEOMETRY(POINT, 3857) NULL
);

SELECT drop_all_functions_if_exists('fr', 'setLaPosteIndexDeliveryPoint');
SELECT drop_all_functions_if_exists('fr', 'set_laposte_delivery_point_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_delivery_point_index(
    simulation BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _query VARCHAR;
BEGIN
    -- link PDI-ID/CEA
    IF index_exists('fr', 'idx_pdi_adresse_id') AND NOT index_exists('fr', 'ix_laposte_delivery_point_adresse_id') THEN
        _query := 'ALTER INDEX idx_pdi_adresse_id RENAME TO ix_laposte_delivery_point_adresse_id';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_delivery_point_adresse_id ON fr.laposte_delivery_point (adresse_id)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    -- geometry
    IF index_exists('fr', 'idx_pdi_geom') AND NOT index_exists('fr', 'ix_laposte_delivery_point_geom') THEN
        _query := 'ALTER INDEX idx_pdi_geom RENAME TO ix_laposte_delivery_point_geom';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_delivery_point_geom ON fr.laposte_delivery_point USING GIST (geom)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    -- PDI-ID
    IF index_exists('fr', 'idx_pdi_pdi_id') AND NOT index_exists('fr', 'iux_laposte_delivery_point_pdi_id') THEN
        _query := 'ALTER INDEX idx_pdi_pdi_id RENAME TO iux_laposte_delivery_point_pdi_id';
    ELSE
        _query := 'CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_delivery_point_pdi_id ON fr.laposte_delivery_point (pdi_id)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    -- link PDI/PDI-parent
    IF index_exists('fr', 'idx_pdi_pdi_id_rattachement') AND NOT index_exists('fr', 'ix_laposte_delivery_point_pdi_id_rattachement') THEN
        _query := 'ALTER INDEX idx_pdi_pdi_id_rattachement RENAME TO ix_laposte_delivery_point_pdi_id_rattachement';
    ELSE
        _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_delivery_point_pdi_id_rattachement ON fr.laposte_delivery_point (pdi_id_rattachement) WHERE pdi_id_rattachement IS NOT NULL';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    -- aggregate CEA
    IF index_exists('fr', 'idx_pdi_agg_adresse_id') AND NOT index_exists('fr', 'iux_laposte_delivery_point_agg_adresse_id') THEN
        _query := 'ALTER INDEX idx_pdi_agg_adresse_id RENAME TO iux_laposte_delivery_point_agg_adresse_id';
    ELSE
        _query := 'CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_delivery_point_agg_adresse_id ON fr.laposte_delivery_point (agg_adresse_id)';
    END IF;
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    -- date of modification
    _query := 'CREATE INDEX IF NOT EXISTS ix_laposte_delivery_point_dt_modification ON fr.laposte_delivery_point (pdi_dt_modification)';
    IF NOT simulation THEN
        EXECUTE _query;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
DECLARE
    _query TEXT;
BEGIN
    -- manage indexes
    CALL fr.set_laposte_delivery_point_index();

    _query := '
        SELECT
            pdi.pdi_id
            , pdi.pdi_etat
            , pdi.pdi_visible
            , pdi.pdi_id_rattachement
            , pdi.pdi_dt_creation
            , pdi.pdi_dt_modification
            , pdi.pdi_source
            , pdi.adresse_id AS pdi_co_adr
            , pdi.adresse_x AS pdi_x_natif
            , pdi.adresse_y AS pdi_y_natif
            , pdi.geom AS pdi_coord
            , ST_SetSRID(
                ST_MakePoint(pdi.adresse_x, pdi.adresse_y)
                , fr.get_srid_from_department_code(za.co_insee_departement)
            ) AS pdi_coord_native
            , pdi.adresse_geocode AS pdi_no_type_localisation_coord
            , pdi.pdi_etablissement_regate
            , pdi.pdi_etablissement_roc
            , pdi.pdi_bureau_instance
            , pdi.distri_etablissement_or
            , pdi.distri_etablissement_os
            , pdi.distri_etablissement_ip
            , pdi.distri_etablissement_pr
            , pdi.distri_etablissement_co
            , pdi.ip_code_udb AS pdi_ip_code_udb
            , pdi.pdi_id_batterie_cidex
            , pdi.ip_id AS pdi_ip_id
            , pdi.pdi_nature_code
            , pdi.pdi_nature
            , pdi.pdi_model
            , (COALESCE(pdi.pdi_pre1, 0)
                +COALESCE(pdi.pdi_pre2, 0)
                +COALESCE(pdi.pdi_pre3, 0)
                +COALESCE(pdi.pdi_pre4, 0)
                +COALESCE(pdi.pdi_pre5, 0)
                +COALESCE(pdi.pdi_pre6, 0)
                +COALESCE(pdi.pdi_pre7, 0)
                +COALESCE(pdi.pdi_pre8, 0)
                +COALESCE(pdi.pdi_pre9, 0)
                +COALESCE(pdi.pdi_pre10, 0)
                +COALESCE(pdi.pdi_pre11, 0))
                AS pdi_nb_pre
            , (COALESCE(pdi.pdi_pre1, 0)
                +COALESCE(pdi.pdi_pre3, 0)
                +COALESCE(pdi.pdi_pre5, 0)
                +COALESCE(pdi.pdi_pre7, 0)
                +COALESCE(pdi.pdi_pre9, 0))
                AS pdi_nb_pre_bal
            , (COALESCE(pdi.pdi_pre2, 0)
                +COALESCE(pdi.pdi_pre4, 0)
                +COALESCE(pdi.pdi_pre6, 0)
                +COALESCE(pdi.pdi_pre8, 0)
                +COALESCE(pdi.pdi_pre10, 0))
                AS pdi_nb_pre_mainp
            , (COALESCE(pdi.pdi_pre1, 0)
                +COALESCE(pdi.pdi_pre2, 0)
                +COALESCE(pdi.pdi_pre3, 0)
                +COALESCE(pdi.pdi_pre4, 0)
                +COALESCE(pdi.pdi_pre5, 0)
                +COALESCE(pdi.pdi_pre6, 0))
                AS pdi_nb_pre_par
            , COALESCE(pdi.ip_potentiel_ip, 0) AS pdi_nb_pre_potentiel_ip
            , pdi.pdi_distance
            , pdi.pdi_ind_presence_gardien
            , pdi.pdi_ind_presence_presse
            , pdi.pdi_ind_presence_plaque_rue
            , pdi.pdi_ind_presence_num_rue
            , pdi.pdi_ind_presence_tab_indicateur
            , pdi.pdi_type_acces
            , pdi.pdi_localisation
            , COALESCE(pdi.pdi_nb_bal_normalisees, 0) AS pdi_nb_bal_normalisees
            , COALESCE(pdi.pdi_nb_bal_non_normalisees, 0) AS pdi_nb_bal_non_normalisees
            , COALESCE(pdi.pdi_nb_bal_etiquetees, 0) AS pdi_nb_bal_etiquetees
            , COALESCE(pdi.ip_stop_pub, 0) AS pdi_nb_bal_stop_pub
            , pdi.ip_comment AS pdi_ip_comment

            --adresse du PDI
            , adresse.co_cea_determinant AS co_adr
            , adresse.dt_reference AS dt_reference_adr
            , adresse.co_niveau
            , adresse.co_cea_parent AS co_adr_parent
            , adresse.co_cea_l3 AS co_adr_l3
            , adresse.co_cea_numero AS co_adr_numero
            , adresse.co_cea_voie AS co_adr_voie
            , adresse.co_cea_za AS co_adr_za
            , l3.lb_standard_nn AS lb_ligne3
            , numero.no_voie AS no_numero
            , numero.lb_ext AS lb_extension_numero
            , voie.co_voie
            , voie.lb_type AS lb_type_voie
            , voie.lb_type_abrege AS lb_type_voie_abrege
            , voie.lb_voie
            , voie.lb_voie_normalise
            , voie.lb_md AS lb_voie_mot_directeur
            , voie.lb_desc AS lb_voie_desc
            , za.co_postal AS co_postal
            , za.lb_l5_nn AS lb_ligne5
            , za.lb_in_ext_loc AS lb_localite
            , za.lb_nn AS lb_localite_normalise
            , za.lb_ach_nn AS lb_acheminement
            , za.co_insee_commune
            , za.co_insee_commune_precedente
            , za.co_insee_departement
            , adresse.fl_diffusable
            , adresse.fl_active

            --INFORMATIONS DE DISTRIBUTION
            , CASE WHEN adresse.co_cea_determinant IS NOT NULL THEN TRUE ELSE FALSE END AS fl_distribuee

            --COORDONNEES
            --, coord.co_cea AS co_coord
            --, ''RAN''::VARCHAR(10) AS co_source_best_coord
            , coord.dt_reference AS adr_dt_reference_coord
            , coord.gm_coord AS adr_coord
            , coord.no_type_localisation AS adr_no_type_localisation_coord
            , coord.va_x AS adr_x_natif
            , coord.va_y AS adr_y_natif

            --RAO
            , rao.co_type AS rao_co_type
            , rao.lb_libelle AS rao_lb_libelle

            --SOURCE-ORGA
            , org.code_regate AS rao_co_regate
        FROM fr.laposte_delivery_point pdi
            LEFT OUTER JOIN fr.laposte_address adresse ON adresse.co_cea_determinant = pdi.adresse_id
            LEFT OUTER JOIN fr.laposte_address_area za ON za.co_cea = adresse.co_cea_za
            LEFT OUTER JOIN fr.laposte_address_street voie ON voie.co_cea = adresse.co_cea_voie
            LEFT OUTER JOIN fr.laposte_address_housenumber numero ON numero.co_cea = adresse.co_cea_numero
            LEFT OUTER JOIN fr.laposte_address_complement l3 ON l3.co_cea = adresse.co_cea_l3
            LEFT OUTER JOIN fr.laposte_address_xy coord ON coord.co_cea = adresse.co_cea_determinant
            LEFT OUTER JOIN fr.laposte_delivery_address rao ON rao.co_adr = adresse.co_cea_determinant
            LEFT OUTER JOIN fr.laposte_organization org ON org.code = rao.co_roc_site::VARCHAR
    ';

    DROP VIEW IF EXISTS fr.delivery_point_view CASCADE;
    EXECUTE CONCAT_WS(
        ' '
        , 'CREATE VIEW fr.delivery_point_view AS'
        , _query
    );
END
$$;
