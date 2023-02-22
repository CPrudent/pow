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
CREATE OR REPLACE PROCEDURE fr.setLaPosteIndexDeliveryPoint(
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
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.setLaPosteIndexDeliveryPoint();
END
$$;
