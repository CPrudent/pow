/***
 * add GEOPAD PDI (delivery point)
 */

/*
temporary table to insert raw data of PDI
these data are checked (and improved) by trigger
finaly:
    if OK inserted into geopad.pdi_adr
    else remain in place w/ error & reject mentions
 */
CREATE TABLE IF NOT EXISTS geopad.pdi_tmp(
    id_import INTEGER NOT NULL
    , co_type_import VARCHAR(50) NOT NULL
    , dt_debut_donnees_import TIMESTAMP NOT NULL
    , dt_fin_donnees_import TIMESTAMP NOT NULL
    --id_unique_pdi CHARACTER VARYING, --uniquement présent dans WS DELTA, pas intéressant
    , pdi_id CHARACTER VARYING, --{pdi}->{id} ou 1er champs CSV dans fichier INIT, on préfère le champs CSV plus souvent renseigné. Présent dans flux XML pdi/delta/v1 et pdi/v1
    , pdi_id_rattachement CHARACTER VARYING, --3ème champs CSV dans fichier INIT. Non présent dans flux XML pdi/delta/v1. Présent dans flux XML pdi/v1 (pdiAttache)
    , --pdi_libelle CHARACTER VARYING, --uniquement présent dans WS DELTA et valeur bidon "Libellé du PDI" -> on abandonne donc le champs
    , pdi_etat CHARACTER VARYING, --{pdi}->{etat} ou 5ème champs CSV dans fichier INIT, on préfère le champs CSV plus souvent renseigné. Présent dans flux XML pdi/delta/v1 et pdi/v1.
    , pdi_dt_creation CHARACTER VARYING, --{pdi}->{published} dans fichier INIT. Présent dans flux XML pdi/delta/v1 et pdi/v1 (published).
    , pdi_dt_modification CHARACTER VARYING, --4ème champs CSV dans fichier INIT. Présent dans flux XML pdi/delta/v1 et pdi/v1 (updated).
    , pdi_source CHARACTER VARYING, --Non présent dans fichier INIT mais de source GEOPAD uniquement, idem pour flux XML pdi/v1. Présent dans flux XML pdi/delta/v1.
    --pdi_mouvement CHARACTER VARYING, --Non présent dans fichier INIT mais déductible à partir de l'état et des dates de création/modification, idem pour flux XML pdi/v1. -> on abandonne donc le champs Présent dans ancien flux XML pdi/delta/v1, S=Suppression M=Modification, C=Création.
    , pdi_nature_code CHARACTER VARYING, --Présent dans fichier INIT. Non présent dans flux XML pdi/delta/v1. Présent dans flux XML pdi/v1.
    , pdi_nature CHARACTER VARYING, --Non présent dans fichier INIT mais déductible à partir du code nature GEOPAD, idem pour flux XML pdi/v1. Présent dans flux XML pdi/delta/v1.
    , pdi_statut CHARACTER VARYING, --Présent dans fichier INIT, dans flux XML pdi/delta/v1 et dans flux XML pdi/v1.
    --pdi_type CHARACTER VARYING, --Présent dans fichier INIT, dans flux XML pdi/delta/v1 et dans flux XML pdi/v1. Information venant de GEOROUTE, mise à jour régulièrement sans mouvement de modification, et pas intéressant pour la BCAA -> on abandonne donc le champs (C  = PDI à distribuer chaque jour ouvré de la semaine, A ou B = PDI à distribuer 1 fois sur 2e en cas de nécessiter dans l’organisation)
    , pdi_model CHARACTER VARYING, --Présent dans fichier INIT et dans flux XML pdi/v1. Non présent dans flux XML pdi/delta/v1.
    , pdi_visible CHARACTER VARYING, --Présent dans fichier INIT et dans flux XML pdi/v1. Non présent dans flux XML pdi/delta/v1 mais par défaut tous les pdi retournés sont uniquement ceux qui sont visibles.
    --pdi_etag CHARACTER VARYING, --Présent dans fichier INIT, dans flux XML pdi/delta/v1 et dans flux XML pdi/v1. A priori c'est un champs technique inutile pour BCAA, qui s'incrémente au fur et à mesure que le PDI est modifié  -> on abandonne donc le champs
    , pdi_particularite CHARACTER VARYING, --Présent dans fichier INIT, dans flux XML pdi/delta/v1 et dans flux XML pdi/v1.
    , pdi_etablissement_regate CHARACTER VARYING, --{etablissement}->{regate} ou 2ème champs CSV dans fichier INIT, on préfère le champs CSV plus souvent renseigné. Présent dans flux XML pdi/delta/v1 et dans flux XML pdi/v1.
    , pdi_etablissement_roc CHARACTER VARYING
    , pdi_bureau_instance CHARACTER VARYING
    , pdi_pre1 CHARACTER VARYING
    , pdi_pre2 CHARACTER VARYING
    , pdi_pre3 CHARACTER VARYING
    , pdi_pre4 CHARACTER VARYING
    , pdi_pre5 CHARACTER VARYING
    , pdi_pre6 CHARACTER VARYING
    , pdi_pre7 CHARACTER VARYING
    , pdi_pre8 CHARACTER VARYING
    , pdi_pre9 CHARACTER VARYING
    , pdi_pre10 CHARACTER VARYING
    , pdi_pre11 CHARACTER VARYING
    , pdi_localisation CHARACTER VARYING
    , pdi_id_batterie_cidex CHARACTER VARYING
    , pdi_distance CHARACTER VARYING
    , pdi_type_acces CHARACTER VARYING
    --pdi_moloc CHARACTER VARYING, -- INIT : tableau présent dans {pdi}->{moloc}, non traité ne sachant à quoi il correspond, dans contrat de service WS DELTA indiqué "moyen de locomotion" ne pas utiliser non mis à jour -> on abandonne donc le champs
    , pdi_nb_bal_normalisees CHARACTER VARYING
    , pdi_nb_bal_non_normalisees CHARACTER VARYING
    , pdi_nb_bal_etiquetees CHARACTER VARYING
    , pdi_ind_presence CHARACTER VARYING
    , pdi_ind_presence_gardien BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "gardien", sinon FALSE
    , pdi_ind_presence_presse BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "presse", sinon FALSE
    , pdi_ind_presence_num_rue BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "numRue", sinon FALSE
    , pdi_ind_presence_plaque_rue BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "plaqueRue", sinon FALSE
    , pdi_ind_presence_depot_relais BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "depotRelais", sinon FALSE
    , pdi_ind_presence_productif BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "productif", sinon FALSE
    , pdi_ind_presence_tab_indicateur BOOLEAN, -- INIT : TRUE si le tableau {pdi}->{ind_presence} contient la valeur "tabIndicateur", sinon FALSE
    /* Uniquement présents dans flux XML pdi/delta/v1 et flux XML pdi/v1. Pour être exploitables il faudrait qu'ils soient présents dans fichier INIT -> on abandonne donc ces champs pour le moment
    pdi_inhabite CHARACTER VARYING,
    pdi_saisonnier CHARACTER VARYING
     */
    , ip_id CHARACTER VARYING
    , ip_stop_pub CHARACTER VARYING
    , ip_potentiel_ip CHARACTER VARYING
    , ip_code_udb CHARACTER VARYING
    , ip_poids_main CHARACTER VARYING, --Présent dans fichier INIT et dans flux XML pdi/v1. Non présent dans flux XML pdi/delta/v1.
    , ip_comment CHARACTER VARYING
    , distri_etablissement_or CHARACTER VARYING
    , distri_etablissement_os CHARACTER VARYING
    , distri_etablissement_pr CHARACTER VARYING
    , distri_etablissement_co CHARACTER VARYING
    , distri_etablissement_ip CHARACTER VARYING
    /* Champs non présents dans flux XML pdi/v1 ni dans flux XML pdi/delta/v1. Utilité pas évidente -> on abandonne donc ces champs pour le moment
    , distri_bureau_instance_or_code CHARACTER VARYING
    , distri_bureau_instance_or_libelle CHARACTER VARYING
    , distri_bureau_instance_os_code CHARACTER VARYING
    , distri_bureau_instance_os_libelle CHARACTER VARYING
    , distri_bureau_instance_pr_code CHARACTER VARYING
    , distri_bureau_instance_pr_libelle CHARACTER VARYING
    , distri_bureau_instance_co_code CHARACTER VARYING
    , distri_bureau_instance_co_libelle CHARACTER VARYING
    , distri_bureau_instance_ip_code CHARACTER VARYING
    , distri_bureau_instance_ip_libelle CHARACTER VARYING
     */
    , adresse_id CHARACTER VARYING
    , adresse_x CHARACTER VARYING
    , adresse_y CHARACTER VARYING
    , adresse_geocode CHARACTER VARYING
    /* Présents dans INIT mais non présent dans WS DELTA mais récupérable par consultation de RAN, on ne les prend pas pour gagner en espace disque
    , adresse_mot_directeur CHARACTER VARYING
    , adresse_complement_identification CHARACTER VARYING
    , adresse_cp CHARACTER VARYING
    , adresse_commune CHARACTER VARYING
    , adresse_cea_voie CHARACTER VARYING
    , adresse_id_pdi_distri CHARACTER VARYING
    , adresse_za CHARACTER VARYING
     */
    --adresse_source CHARACTER VARYING, --Non présent dans flux XML pdi/v1 ni dans flux XML pdi/delta/v1. Utilité pas évidente -> on abandonne donc ce champ pour le moment
    , errors TEXT[]
    , rejet BOOLEAN DEFAULT TRUE
);

-- tables to update PDI w/ delta (from web service)
CREATE TABLE IF NOT EXISTS geopad.pdi_ws_delta AS TABLE geopad.pdi_tmp WITH NO DATA;
CREATE INDEX IF NOT EXISTS ix_pdi_ws_delta_id_import ON geopad.pdi_ws_delta(id_import);

-- delivery points
CREATE TABLE IF NOT EXISTS geopad.pdi (
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
    --abandonné au profit de agg_adresse_id, car pour l'instant PostgreSQL n'est pas optimal sur un index unique à multiples colonnes
    --agg_adresse BOOLEAN NULL, --Indique si le PDI est un aggrégat à l'adresse (c'est un PDI unique à l'adresse, ou bien un aggrégat de PDI à l'adresse)
    , agg_adresse_id CHAR(10) NULL
    , agg_nb_pdi INTEGER NULL
    , nb_pre_par_per_log_ind INTEGER NULL
    , nb_pre_par_per_log_col INTEGER NULL
    , agg_nb_pdi_repositionnes INTEGER NULL
    , geom GEOMETRY(POINT,3857) NULL
);

CREATE OR REPLACE FUNCTION geopad.pdi_est_identique(
    v_pdi_a IN geopad.pdi
    , v_pdi_b IN geopad.pdi
    )
RETURNS BOOLEAN AS
$func$
BEGIN
    RETURN NOT (
        COALESCE(v_pdi_a.pdi_id_rattachement,-1000) != COALESCE(v_pdi_b.pdi_id_rattachement,-1000)
        OR COALESCE(v_pdi_a.pdi_etat,-9) != COALESCE(v_pdi_b.pdi_etat,-9)
        OR COALESCE(v_pdi_a.pdi_dt_creation,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP) != COALESCE(v_pdi_b.pdi_dt_creation,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP)
        --On ignore pour permettre la comparaison de mouvements à date différentes
        --OR COALESCE(v_pdi_a.pdi_dt_modification,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP) != COALESCE(v_pdi_b.pdi_dt_modification,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP)
        OR COALESCE(v_pdi_a.pdi_source,'NULL') != COALESCE(v_pdi_b.pdi_source,'NULL')
        OR COALESCE(v_pdi_a.pdi_nature_code,'ZZZ') != COALESCE(v_pdi_b.pdi_nature_code,'ZZZ')
        OR COALESCE(v_pdi_a.pdi_nature,'NULL') != COALESCE(v_pdi_b.pdi_nature,'NULL')
        OR COALESCE(v_pdi_a.pdi_statut,'NULL') != COALESCE(v_pdi_b.pdi_statut,'NULL')
        --Champs abandonnée : OR COALESCE(v_pdi_a.pdi_type,'NULL') != COALESCE(v_pdi_b.pdi_type,'NULL')
        OR COALESCE(v_pdi_a.pdi_model,'NULL') != COALESCE(v_pdi_b.pdi_model,'NULL')
        OR COALESCE(v_pdi_a.pdi_visible::INTEGER,-9) != COALESCE(v_pdi_b.pdi_visible::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_particularite,'NULL') != COALESCE(v_pdi_b.pdi_particularite,'NULL')
        OR COALESCE(v_pdi_a.pdi_etablissement_regate,'NULL') != COALESCE(v_pdi_b.pdi_etablissement_regate,'NULL')
        OR COALESCE(v_pdi_a.pdi_etablissement_roc,'NULL') != COALESCE(v_pdi_b.pdi_etablissement_roc,'NULL')
        OR COALESCE(v_pdi_a.pdi_bureau_instance,'NULL') != COALESCE(v_pdi_b.pdi_bureau_instance,'NULL')
        OR COALESCE(v_pdi_a.pdi_pre1,-1000) != COALESCE(v_pdi_b.pdi_pre1,-1000)
        OR COALESCE(v_pdi_a.pdi_pre2,-1000) != COALESCE(v_pdi_b.pdi_pre2,-1000)
        OR COALESCE(v_pdi_a.pdi_pre3,-1000) != COALESCE(v_pdi_b.pdi_pre3,-1000)
        OR COALESCE(v_pdi_a.pdi_pre4,-1000) != COALESCE(v_pdi_b.pdi_pre4,-1000)
        OR COALESCE(v_pdi_a.pdi_pre5,-1000) != COALESCE(v_pdi_b.pdi_pre5,-1000)
        OR COALESCE(v_pdi_a.pdi_pre6,-1000) != COALESCE(v_pdi_b.pdi_pre6,-1000)
        OR COALESCE(v_pdi_a.pdi_pre7,-1000) != COALESCE(v_pdi_b.pdi_pre7,-1000)
        OR COALESCE(v_pdi_a.pdi_pre8,-1000) != COALESCE(v_pdi_b.pdi_pre8,-1000)
        OR COALESCE(v_pdi_a.pdi_pre9,-1000) != COALESCE(v_pdi_b.pdi_pre9,-1000)
        OR COALESCE(v_pdi_a.pdi_pre10,-1000) != COALESCE(v_pdi_b.pdi_pre10,-1000)
        OR COALESCE(v_pdi_a.pdi_pre11,-1000) != COALESCE(v_pdi_b.pdi_pre11,-1000)
        OR COALESCE(v_pdi_a.pdi_localisation,'NULL') != COALESCE(v_pdi_b.pdi_localisation,'NULL')
        OR COALESCE(v_pdi_a.pdi_id_batterie_cidex,'NULL') != COALESCE(v_pdi_b.pdi_id_batterie_cidex,'NULL')
        OR COALESCE(v_pdi_a.pdi_distance,-1000) != COALESCE(v_pdi_b.pdi_distance,-1000)
        OR COALESCE(v_pdi_a.pdi_type_acces,'NULL') != COALESCE(v_pdi_b.pdi_type_acces,'NULL')
        OR COALESCE(v_pdi_a.pdi_nb_bal_normalisees,-1000) != COALESCE(v_pdi_b.pdi_nb_bal_normalisees,-1000)
        OR COALESCE(v_pdi_a.pdi_nb_bal_non_normalisees,-1000) != COALESCE(v_pdi_b.pdi_nb_bal_non_normalisees,-1000)
        OR COALESCE(v_pdi_a.pdi_nb_bal_etiquetees,-1000) != COALESCE(v_pdi_b.pdi_nb_bal_etiquetees,-1000)
        OR COALESCE(v_pdi_a.pdi_ind_presence,'NULL') != COALESCE(v_pdi_b.pdi_ind_presence,'NULL')
        OR COALESCE(v_pdi_a.pdi_ind_presence_gardien::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_gardien::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_ind_presence_presse::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_presse::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_ind_presence_num_rue::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_num_rue::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_ind_presence_plaque_rue::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_plaque_rue::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_ind_presence_depot_relais::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_depot_relais::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_ind_presence_productif::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_productif::INTEGER,-9)
        OR COALESCE(v_pdi_a.pdi_ind_presence_tab_indicateur::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_tab_indicateur::INTEGER,-9)
        OR COALESCE(v_pdi_a.ip_id,'NULL') != COALESCE(v_pdi_b.ip_id,'NULL')
        OR COALESCE(v_pdi_a.ip_stop_pub,-1000) != COALESCE(v_pdi_b.ip_stop_pub,-1000)
        OR COALESCE(v_pdi_a.ip_potentiel_ip,-1000) != COALESCE(v_pdi_b.ip_potentiel_ip,-1000)
        OR COALESCE(v_pdi_a.ip_code_udb,'NULL') != COALESCE(v_pdi_b.ip_code_udb,'NULL')
        OR COALESCE(v_pdi_a.ip_poids_main,'NULL') != COALESCE(v_pdi_b.ip_poids_main,'NULL')
        --On ignore ce champs, utilisé par BCAA pour flaguer les mouvements générés suite à une détection de MODIFICATION
        --OR COALESCE(v_pdi_a.ip_comment,'NULL') != COALESCE(v_pdi_b.ip_comment,'NULL')
        OR COALESCE(v_pdi_a.distri_etablissement_or,'NULL') != COALESCE(v_pdi_b.distri_etablissement_or,'NULL')
        OR COALESCE(v_pdi_a.distri_etablissement_os,'NULL') != COALESCE(v_pdi_b.distri_etablissement_os,'NULL')
        OR COALESCE(v_pdi_a.distri_etablissement_pr,'NULL') != COALESCE(v_pdi_b.distri_etablissement_pr,'NULL')
        OR COALESCE(v_pdi_a.distri_etablissement_co,'NULL') != COALESCE(v_pdi_b.distri_etablissement_co,'NULL')
        OR COALESCE(v_pdi_a.distri_etablissement_ip,'NULL') != COALESCE(v_pdi_b.distri_etablissement_ip,'NULL')
        OR COALESCE(v_pdi_a.adresse_id,'NULL') != COALESCE(v_pdi_b.adresse_id,'NULL')
        OR COALESCE(v_pdi_a.adresse_x,-1000) != COALESCE(v_pdi_b.adresse_x,-1000)
        OR COALESCE(v_pdi_a.adresse_y,-1000) != COALESCE(v_pdi_b.adresse_y,-1000)
        OR COALESCE(v_pdi_a.adresse_geocode,-1000) != COALESCE(v_pdi_b.adresse_geocode,-1000)
    );
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('geopad','create_pdi_agg_adr');
CREATE OR REPLACE FUNCTION geopad.create_pdi_agg_adr(
    v_adresse_id IN CHAR(10)
    , v_liste_pdi_id IN INTEGER[] DEFAULT NULL
    )
RETURNS BOOLEAN AS
$func$
BEGIN
    INSERT INTO geopad.pdi (
        id_import
        ,pdi_id
        ,pdi_etat
        ,pdi_dt_creation
        ,pdi_dt_modification
        ,pdi_source
        ,pdi_statut
        ,pdi_visible
        ,pdi_pre1
        ,pdi_pre2
        ,pdi_pre3
        ,pdi_pre4
        ,pdi_pre5
        ,pdi_pre6
        ,pdi_pre7
        ,pdi_pre8
        ,pdi_pre9
        ,pdi_pre10
        ,pdi_pre11
        ,pdi_nb_bal_normalisees
        ,pdi_nb_bal_non_normalisees
        ,pdi_nb_bal_etiquetees
        ,pdi_ind_presence_gardien
        ,pdi_ind_presence_presse
        ,pdi_ind_presence_num_rue
        ,pdi_ind_presence_plaque_rue
        ,pdi_ind_presence_depot_relais
        ,pdi_ind_presence_productif
        ,pdi_ind_presence_tab_indicateur
        ,adresse_id
        /* en fin de colonnes pour les déduire de la geom en une deuxième passe
        ,adresse_x
        ,adresse_y
            */
        ,adresse_geocode
        --,agg_adresse
        ,agg_adresse_id
        ,agg_nb_pdi
        ,nb_pre_par_per_log_ind
        ,nb_pre_par_per_log_col
        ,agg_nb_pdi_repositionnes
        ,geom
        ,adresse_x
        ,adresse_y
    )
    (
        SELECT
            *
            ,ST_X(
                ST_Transform(
                    geom
                    ,getSridCoordRanFromCodeInseeDepartement(getCodeInseeDepartementFromCodeInseeCommune(agg_adresse_id))
                )
            )
            ,ST_Y(
                ST_Transform(
                    geom
                    ,getSridCoordRanFromCodeInseeDepartement(getCodeInseeDepartementFromCodeInseeCommune(agg_adresse_id))
                )
            )
        FROM (
            SELECT	MAX(id_import) AS id_import
                    ,-(FIRST(pdi_id)) AS pdi_id
                    ,1 AS pdi_etat
                    ,MIN(pdi_dt_creation) AS pdi_dt_creation
                    ,MAX(pdi_dt_modification) AS pdi_dt_modification
                    ,'BCAA' AS pdi_source
                    ,'PRODUCTION' AS pdi_statut
                    ,FALSE AS pdi_visible
                    ,SUM(pdi_pre1) AS pdi_pre1
                    ,SUM(pdi_pre2) AS pdi_pre2
                    ,SUM(pdi_pre3) AS pdi_pre3
                    ,SUM(pdi_pre4) AS pdi_pre4
                    ,SUM(pdi_pre5) AS pdi_pre5
                    ,SUM(pdi_pre6) AS pdi_pre6
                    ,SUM(pdi_pre7) AS pdi_pre7
                    ,SUM(pdi_pre8) AS pdi_pre8
                    ,SUM(pdi_pre9) AS pdi_pre9
                    ,SUM(pdi_pre10) AS pdi_pre10
                    ,SUM(pdi_pre11) AS pdi_pre11
                    ,SUM(pdi_nb_bal_normalisees) AS pdi_nb_bal_normalisees
                    ,SUM(pdi_nb_bal_non_normalisees) AS pdi_nb_bal_non_normalisees
                    ,SUM(pdi_nb_bal_etiquetees) AS pdi_nb_bal_etiquetees
                    ,SUM(pdi_ind_presence_gardien) AS pdi_ind_presence_gardien
                    ,SUM(pdi_ind_presence_presse) AS pdi_ind_presence_presse
                    ,SUM(pdi_ind_presence_num_rue) AS pdi_ind_presence_num_rue
                    ,SUM(pdi_ind_presence_plaque_rue) AS pdi_ind_presence_plaque_rue
                    ,SUM(pdi_ind_presence_depot_relais) AS pdi_ind_presence_depot_relais
                    ,SUM(pdi_ind_presence_productif) AS pdi_ind_presence_productif
                    ,SUM(pdi_ind_presence_tab_indicateur) AS pdi_ind_presence_tab_indicateur
                    ,NULL::CHAR(10) AS adresse_id
                    ,CASE WHEN EXISTS_AGG(pdi.adresse_geocode = 9) THEN 9 END AS adresse_geocode
                    --,TRUE AS agg_adresse
                    ,adresse_id AS agg_adresse_id
                    ,COUNT(*) AS agg_nb_pdi
                    --Nombre total de particulier permanents habitant en logement collectif sur l'ensemble des PDI de l'adresse
                    ,SUM(nb_pre_par_per_log_ind) AS nb_pre_par_per_log_ind
                    --Nombre total de particulier permanents habitant en logement collectif sur l'ensemble des PDI de l'adresse
                    ,SUM(nb_pre_par_per_log_col) AS nb_pre_par_per_log_col
                    ,SUM(agg_nb_pdi_repositionnes) AS agg_nb_pdi_repositionnes
                    ,CASE WHEN EXISTS_AGG(pdi.adresse_geocode = 9) THEN
                        --Barycentre
                        ST_Centroid(
                            ST_Collect(
                                --des coordonnées natives des PDI repositionnés
                                CASE WHEN pdi.adresse_geocode = 9 THEN
                                    pdi.geom
                                    --ST_MakePoint(pdi.adresse_x,pdi.adresse_y)
                                END
                            )
                        )
                    END AS geom
            FROM geopad.pdi AS pdi
            -- En statut production (pas en projet), non supprimé, et visible
            WHERE pdi.pdi_etat = 1 AND pdi.pdi_visible = TRUE
            AND (
                (v_liste_pdi_id IS NULL AND pdi.adresse_id = v_adresse_id)
                OR
                (v_liste_pdi_id IS NOT NULL AND pdi.pdi_id = ANY(v_liste_pdi_id))
            )
            GROUP BY pdi.adresse_id
        ) AS sous_requete
    );
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('geopad','update_pdi_agg_adr');
CREATE OR REPLACE FUNCTION geopad.update_pdi_agg_adr(
    v_adresse_id IN CHAR(10)
    , v_raise_info IN BOOLEAN DEFAULT FALSE
    )
RETURNS BOOLEAN AS
$func$
DECLARE
	v_pdi_agg RECORD;
BEGIN
    SELECT
        SUM(CASE WHEN pdi_etat = 1 AND pdi_visible = TRUE THEN 1 ELSE 0 END) AS nb_pdi
        ,MAX(CASE WHEN pdi_etat = 1 AND pdi_visible = TRUE THEN pdi_dt_modification END) AS max_pdi_dt_modification
        ,COALESCE(SUM(agg_nb_pdi),0) AS agg_nb_pdi
        ,EXISTS_AGG(agg_nb_pdi = 1) AS agg_unique_exists
        ,EXISTS_AGG(agg_nb_pdi > 1) AS agg_multiple_exists
        ,FIRST(CASE WHEN agg_nb_pdi IS NOT NULL THEN pdi_dt_modification END) AS agg_pdi_dt_modification
    INTO v_pdi_agg
    FROM geopad.pdi
    --WHERE adresse_id = '4602122223' OR agg_adresse_id = '4602122223'
    WHERE adresse_id = v_adresse_id OR agg_adresse_id = v_adresse_id;

    --Le nombre de pdi a changé OU bien les pdi de l'adresse ont été modifiés
    IF v_pdi_agg.nb_pdi != v_pdi_agg.agg_nb_pdi
    OR v_pdi_agg.max_pdi_dt_modification != v_pdi_agg.agg_pdi_dt_modification
    THEN
        IF v_raise_info = TRUE THEN
            RAISE NOTICE 'Adresse % : aggrégat à mettre à jour (% != % ET/OU % != %)'
                ,v_adresse_id
                ,v_pdi_agg.nb_pdi
                ,v_pdi_agg.agg_nb_pdi
                ,v_pdi_agg.max_pdi_dt_modification
                ,v_pdi_agg.agg_pdi_dt_modification;
        END IF;

        --Un aggrégat de PDI unique à l'adresse : on le supprime
        IF v_pdi_agg.agg_unique_exists = TRUE THEN
            UPDATE geopad.pdi
            SET --agg_adresse = NULL
                agg_adresse_id = NULL
                ,agg_nb_pdi = NULL
            WHERE adresse_id = v_adresse_id AND agg_nb_pdi = 1;
        END IF;
        --Un aggrégat de PDI multiples à l'adresse : on le supprime
        IF v_pdi_agg.agg_multiple_exists = TRUE THEN
            DELETE FROM geopad.pdi
            WHERE agg_adresse_id = v_adresse_id AND agg_nb_pdi > 1;
        END IF;

        --Aucun PDI en PRODUCTION et VISIBLE à l'adresse : rien à faire
        /*IF v_pdi_agg.nb_pdi = 0 THEN
        --Un seul PDI en PRODUCTION et VISIBLE à l'adresse
        ELS*/IF v_pdi_agg.nb_pdi = 1 THEN
            --On ajoute l'indicateur d'aggrégat sur l'unique PDI
            UPDATE geopad.pdi
            SET	--agg_adresse = TRUE
                agg_adresse_id = v_adresse_id
                ,agg_nb_pdi = 1
            WHERE adresse_id = v_adresse_id
                AND pdi_etat = 1 AND pdi_visible = TRUE;
        --Plusieurs PDI en PRODUCTION et VISIBLEs à l'adresse
        ELSIF v_pdi_agg.nb_pdi > 1 THEN
            --Enregistrement du PDI aggrégé à l'adresse
            PERFORM geopad.create_pdi_agg_adr(v_adresse_id);
        END IF;
    END IF;

    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('geopad','pdi_rattachement_est_a_ignorer');
CREATE OR REPLACE FUNCTION geopad.pdi_rattachement_est_a_ignorer(
    v_pdi IN geopad.pdi
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    v_nb_pre_rattachement INTEGER;
    v_nb_pre_rattaches INTEGER;
BEGIN
    IF v_pdi.pdi_nature_code = 'CIP' THEN
        RETURN TRUE;
    ELSIF v_pdi.pdi_nature_code IN ('CLP','BPP') THEN
        /* Le PDI de rattachement est à ignorer s'il n'a pas de gardien
            * Dans ce cas on considère que les PRE du rattachement sont à compter en plus des PRE des rattachés
            * Même si la somme des PRE des rattachés est égale au nombre de PRE du rattachement (cas à priori rare)
            * Exemple PDI de rattachement avec 50 PRE et un gardien, ayant 2 PDI rattachés de 25 PRE chacun
            */
        IF v_pdi.pdi_ind_presence_gardien >= 1 THEN
            RETURN FALSE;
        END IF;

        SELECT 	SUM(
                (COALESCE(pdi.pdi_pre1,0)
                +COALESCE(pdi.pdi_pre2,0)
                +COALESCE(pdi.pdi_pre3,0)
                +COALESCE(pdi.pdi_pre4,0)
                +COALESCE(pdi.pdi_pre5,0)
                +COALESCE(pdi.pdi_pre6,0)
                +COALESCE(pdi.pdi_pre7,0)
                +COALESCE(pdi.pdi_pre8,0)
                +COALESCE(pdi.pdi_pre9,0)
                +COALESCE(pdi.pdi_pre10,0)
                +COALESCE(pdi.pdi_pre11,0))
            )
        INTO v_nb_pre_rattaches
        FROM geopad.pdi
        WHERE pdi_id_rattachement = v_pdi.pdi_id
        AND pdi_etat = 1 AND pdi_visible = TRUE;

        v_nb_pre_rattachement := (
            COALESCE(v_pdi.pdi_pre1,0)
            +COALESCE(v_pdi.pdi_pre2,0)
            +COALESCE(v_pdi.pdi_pre3,0)
            +COALESCE(v_pdi.pdi_pre4,0)
            +COALESCE(v_pdi.pdi_pre5,0)
            +COALESCE(v_pdi.pdi_pre6,0)
            +COALESCE(v_pdi.pdi_pre7,0)
            +COALESCE(v_pdi.pdi_pre8,0)
            +COALESCE(v_pdi.pdi_pre9,0)
            +COALESCE(v_pdi.pdi_pre10,0)
            +COALESCE(v_pdi.pdi_pre11,0)
        );

        IF (
            /* Le PDI de rattachement a des PDI rattachés */
            v_nb_pre_rattachement IS NOT NULL
            AND (
                /* Le PDI de rattachement a autant de PRE que l'ensemble de ses PDI rattachés
                    * Exemple PDI de rattachement avec 50 PRE, ayant 2 PDI rattachés de 25 PRE chacun
                    */
                v_nb_pre_rattaches = v_nb_pre_rattachement

                /* Ou bien le PDI de rattachement n'a aucun ou un seul PRE, et a des PDI/PRE rattachés
                    * On suppose alors que le nombre PRE n'a pas été correctement renseigné, renseigné à 0 ou 1
                    */
                OR (v_nb_pre_rattachement <= 1 AND v_nb_pre_rattaches >= 1)
            )
        )
        THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('geopad','setPdiRattachementIgnore');
CREATE OR REPLACE FUNCTION geopad.setPdiRattachementIgnore(
    v_id_import IN INTEGER DEFAULT NULL
    , v_update_pdi_agg IN BOOLEAN DEFAULT TRUE
    )
RETURNS BOOLEAN AS
$func$
DECLARE
    v_pdi geopad.pdi%ROWTYPE;
    v_nb_pdi_a_verifier INTEGER := 0;
    v_nb_pdi_verifies INTEGER := 0;
    v_nb_pdi_mis_a_jour INTEGER := 0;
    v_raise_compteur_modulo INTEGER :=
        CASE
        WHEN public.getEnv() IN ('PPRD','PROD') THEN 10000
        ELSE 1000
        END;
BEGIN
    RAISE NOTICE '% : début traitement PDI de rattachement', TO_CHAR(clock_timestamp(),'HH24:MI:SS');

    DROP TABLE IF EXISTS tmp_set_pdi_rattachement_ignore;
    --IF v_id_import IS NULL THEN
        CREATE TEMPORARY TABLE tmp_set_pdi_rattachement_ignore AS (
            SELECT *
            FROM geopad.pdi
            WHERE pdi_nature_code IN ('CLP','BPP','CIP')
            AND pdi_etat = 1
        );
    /* désactivé pour gérer le cas particulier d'un PDI dont le rattachement change : il faudrait alors vérifier le nouveau rattachement (FAIT), mais aussi l'ancien (PAS FAIT)
        * ce traitement étant lancé une fois par semaine au moment de l'intégration des mises à jour en DELTA des PDI, on peut se permettre de refaire une vérification complète (durée d'environ 15mn)
    ELSE
        CREATE TEMPORARY TABLE tmp_set_pdi_rattachement_ignore AS (
            WITH pdi_rattachement_affectes AS (
                --Id des PDI de rattachement affectés par un import spécifique (exemple : DELTA par WS ou DELTA par FICHIER INIT)
                SELECT DISTINCT COALESCE(pdi_id_rattachement, pdi_id) AS pdi_id
                FROM geopad.pdi
                WHERE id_import = v_id_import
                AND (
                    --PDI de rattachement mis à jour
                    pdi_nature_code IN ('CLP','BPP','CIP')
                    --ou PDI de rattachement d'un PDI rattaché mis à jour
                    OR pdi_id_rattachement IS NOT NULL
                )
            )
            SELECT pdi.*
            FROM geopad.pdi
            INNER JOIN pdi_rattachement_affectes ON pdi_rattachement_affectes.pdi_id = pdi.pdi_id
            WHERE pdi.pdi_nature_code IN ('CLP','BPP','CIP')
            AND pdi.pdi_etat = 1
        );
    END IF;
    */
    GET DIAGNOSTICS v_nb_pdi_a_verifier = ROW_COUNT;
    RAISE NOTICE '% : % PDI de rattachement à vérifier', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_nb_pdi_a_verifier;

    FOR v_pdi IN (
        SELECT * FROM tmp_set_pdi_rattachement_ignore
    )
    LOOP
        /* Si la visibilité du PDI de rattachement est incohérente avec le fait qu'il faut l'ignorer ou non, c'est-a-dire
            * Si le PDI est à ignorer : le PDI doit être non visible
            * Si le PDI n'est pas à ignorer : le PDI doit être visible
            */
        IF v_pdi.pdi_visible != (NOT geopad.pdi_rattachement_est_a_ignorer(v_pdi)) THEN
            --On met à jour sa visiblité
            UPDATE geopad.pdi
            SET pdi_visible = (NOT pdi_visible)
            WHERE pdi_id = v_pdi.pdi_id;
            --On met à jour l'aggrégat PDI à l'adresse
            IF v_update_pdi_agg = TRUE THEN
                PERFORM geopad.update_pdi_agg_adr(v_pdi.adresse_id);
            END IF;

            v_nb_pdi_mis_a_jour := v_nb_pdi_mis_a_jour + 1;
        END IF;

        v_nb_pdi_verifies := v_nb_pdi_verifies + 1;
        IF v_nb_pdi_verifies % v_raise_compteur_modulo = 0 THEN
            RAISE NOTICE '% : % PDI vérifiés, % PDI mis à jour', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_nb_pdi_verifies, v_nb_pdi_mis_a_jour;
        END IF;
    END LOOP;

    RAISE NOTICE '% : fin traitement PDI de rattachement, % PDI vérifiés, % PDI mis à jour', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_nb_pdi_verifies, v_nb_pdi_mis_a_jour;

    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

--Fonction d'intégration des mouvements de PDI en WS DELTA
SELECT drop_all_functions_if_exists('geopad','integrationPdiWsDelta');
CREATE OR REPLACE FUNCTION geopad.integrationPdiWsDelta()
RETURNS BOOLEAN AS
$func$
DECLARE
    v_import RECORD;
    v_historique_import RECORD;
    v_new_id_import INTEGER;
BEGIN
    RAISE NOTICE '% Début traitement integrationPdiWsDelta', TO_CHAR(clock_timestamp(),'HH24:MI:SS');
    --Si on est pas en PROD, c'est que les mouvements ont été importés de la PROD, il faut donc générer de nouveaux historiques d'import
    IF public.getEnv() != 'PROD' THEN
        FOR v_import IN (
            WITH historique_import_pdi_ws_delta AS (
                SELECT	id_import AS id_import
                        ,FIRST(dt_debut_donnees_import) AS dt_debut_donnees
                        ,FIRST(dt_fin_donnees_import) AS dt_fin_donnees
                        ,COUNT(*) AS nb_enregistrements_a_traiter
                FROM geopad.pdi_ws_delta
                GROUP BY id_import
            )
            SELECT *
            FROM historique_import_pdi_ws_delta
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.historique_import
                WHERE historique_import.id = historique_import_pdi_ws_delta.id_import
                AND historique_import.co_type = 'GEOPAD_PDI'
                AND historique_import.dt_debut_donnees = historique_import_pdi_ws_delta.dt_debut_donnees
                AND historique_import.dt_fin_donnees = historique_import_pdi_ws_delta.dt_fin_donnees
            )
        ) LOOP
            INSERT INTO public.historique_import
            (
                co_type
                ,co_etat
                ,dt_debut_donnees
                ,dt_fin_donnees
                ,nb_enregistrements_a_traiter
                ,nb_enregistrements_traites
                ,dt_fin_execution
            )
            VALUES
            (
                'GEOPAD_PDI'
                ,'SUCCES'
                ,v_import.dt_debut_donnees
                ,v_import.dt_fin_donnees
                ,v_import.nb_enregistrements_a_traiter
                ,v_import.nb_enregistrements_a_traiter
                ,now()
            )
            RETURNING id INTO v_new_id_import;

            UPDATE geopad.pdi_ws_delta
            SET id_import = v_new_id_import
            WHERE id_import = v_import.id_import;

            RAISE NOTICE '% Création historique import n°% en remplacement du n°% issu d''une autre plateforme', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_new_id_import, v_import.id_import;
        END LOOP;
    END IF;

    --Pour chaque import de GEOPAD qui s'est correctement terminé, mais non intégré (donc importé par webservice delta, dans un ordre croissant de date de début des données
    FOR v_historique_import IN (
        SELECT *
        FROM public.historique_import
        WHERE co_type = 'GEOPAD_PDI'
        AND co_etat = 'SUCCES'
        AND co_etat_integration IS NULL
        ORDER BY dt_debut_donnees ASC
    ) LOOP
        INSERT INTO geopad.pdi_tmp
        (
            SELECT *
            FROM geopad.pdi_ws_delta
            WHERE id_import = v_historique_import.id
        );

        RAISE NOTICE '% Fin intégration de l''import n°%', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_historique_import.id;

        UPDATE public.historique_import
        SET co_etat_integration = 'SUCCES'
            ,nb_enregistrements_valides = (
                SELECT COUNT(*)
                FROM geopad.pdi
                WHERE pdi.id_import = v_historique_import.id
                --seulement les PDI venant de GEOPAD = hors PDI créés par BCAA (exemple : PDI aggrégé à l'adresse)
                AND pdi.pdi_source = 'GEOPAD'
                --Equivalent à : AND (pdi.agg_nb_pdi IS NULL OR pdi.agg_nb_pdi = 1)
            )
        WHERE id = v_historique_import.id;

        RAISE NOTICE '% Fin comptage du nombre d''enregistrements valides de l''import n°%', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_historique_import.id;
    END LOOP;

    PERFORM setPdiRattachementIgnore(v_historique_import.id);
    TRUNCATE TABLE geopad.pdi_ws_delta;

    RAISE NOTICE '% Fin traitement integrationPdiWsDelta', TO_CHAR(clock_timestamp(),'HH24:MI:SS');

    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION geopad.pdi_ws_controle() RETURNS TRIGGER
AS
$$
DECLARE
    lr_pdi_ws_corrige geopad.pdi%ROWTYPE;
    lr_pdi_histo_conflict geopad.pdi%ROWTYPE;
    error_text TEXT;

    v_md5_correction_pdi CHAR(32);
    --v_id_import_derniere_correction_pdi historique_import.id%TYPE;

    lr_deleted_pdi geopad.pdi%ROWTYPE;

    v_pdi_coord_native GEOMETRY;
    v_pdi_coord_srid INTEGER;
BEGIN
    /* Indicateur positionné à VRAI si on rencontre une erreur fatale : impossibilité d'exploiter le mouvement de PDI minimalement, en cas de problème sur les informations suivantes :
        - identifiant de PDI
        - date de dernière modification
        - statut
        - mouvement
        - visible (vrai/faux)
    */
    NEW.rejet := FALSE;

    lr_pdi_ws_corrige.id_import := NEW.id_import;

    BEGIN
        lr_pdi_ws_corrige.pdi_id := NULLIF(TRIM(NEW.pdi_id),'')::INTEGER;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_id',error_text));
        NEW.rejet := TRUE;
    END;

    BEGIN lr_pdi_ws_corrige.pdi_id_rattachement := NULLIF(TRIM(NEW.pdi_id_rattachement),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_id_rattachement',error_text)); END;
    /* Si le PDI est rattaché à lui-même on supprime le lien de rattachement */
    lr_pdi_ws_corrige.pdi_id_rattachement := NULLIF(lr_pdi_ws_corrige.pdi_id_rattachement,lr_pdi_ws_corrige.pdi_id);

    BEGIN lr_pdi_ws_corrige.pdi_etat := NULLIF(TRIM(NEW.pdi_etat),'')::SMALLINT;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_etat',error_text));  END;
    IF lr_pdi_ws_corrige.pdi_etat IS NOT NULL AND lr_pdi_ws_corrige.pdi_etat NOT IN (0,1,2) THEN
        NEW.errors := array_append(NEW.errors, 'pdi_etat renseigné mais pas avec 0, 1 ou 2');
        NEW.rejet := TRUE; --on rejette le mouvement
    END IF;

    BEGIN lr_pdi_ws_corrige.pdi_dt_creation := NULLIF(TRIM(NEW.pdi_dt_creation),'')::TIMESTAMP WITHOUT TIME ZONE;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_dt_creation',error_text)); END;

    BEGIN
        lr_pdi_ws_corrige.pdi_dt_modification := NULLIF(TRIM(NEW.pdi_dt_modification),'')::TIMESTAMP WITHOUT TIME ZONE;
        --Si la date de dernière modification est ultérieure à la date de fin des données de l'import + 1 jour (tolérance nécessaire car ne connait pas l'heure exacte de chaque INIT GEOPAD)
        --Ce cas peut se produire en cas de problème dans GEOPAD à l'enregistrement de la date de dernière modification conduisant à une date erronnée
        IF lr_pdi_ws_corrige.pdi_dt_modification > (NEW.dt_fin_donnees_import + INTERVAL '1 day') THEN
            NEW.errors := array_append(NEW.errors, 'pdi_dt_modification renseigné mais avec une date ultérieure à la date de fin des données de l''import + 1 jour');
            NEW.rejet := TRUE; --on rejette le mouvement
            --On préfère appliquer une correction : finnalement non, afin d'eviter des interferences avec le controle des PDI disparus entre INIT (pdi_histo_generation_geopad.sql), car GEOPAD pourra finnalement envoyer une date corrigée antérieure à celle précédement corrigée
            --lr_pdi_ws_corrige.pdi_dt_modification := NEW.dt_fin_donnees_import;
        END IF;
        /* FIXME : trop tot pour vérifier que on bien reçu ce mouvement en doublon des précédents INIT
        --Idem si la date de dernière modification est antérieure à la date de début des données de l'import - 1 jour
        --Ce cas peut se produire en cas de correction manuelle de la date de dernière modification dans GEOPAD
        IF lr_pdi_ws_corrige.pdi_dt_modification < (NEW.dt_debut_donnees_import - INTERVAL '1 day') THEN
            NEW.errors := array_append(NEW.errors, 'pdi_dt_modification renseigné mais avec une date antérieure à la date de début des données de l''import - 1 jour');
            --On préfère appliquer une correction NEW.rejet := TRUE; --on rejette le mouvement
            lr_pdi_ws_corrige.pdi_dt_modification := NEW.dt_fin_donnees_import;
        END IF;
        */
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_dt_modification',error_text));
        NEW.rejet := TRUE;
    END;

    lr_pdi_ws_corrige.pdi_source := NULLIF(TRIM(NEW.pdi_source),'');
    IF lr_pdi_ws_corrige.pdi_source IS NOT NULL AND lr_pdi_ws_corrige.pdi_source NOT IN ('GEOPAD','GEODIS') THEN
        NEW.errors := array_append(NEW.errors, 'pdi_source renseigné mais pas avec GEOPAD ou GEODIS');
        lr_pdi_ws_corrige.pdi_source := NULL; --on ignore l'information renseignée
    END IF;

    lr_pdi_ws_corrige.pdi_nature := NULLIF(TRIM(NEW.pdi_nature),'');
    lr_pdi_ws_corrige.pdi_nature_code := NULLIF(TRIM(NEW.pdi_nature_code),'');
    IF lr_pdi_ws_corrige.pdi_nature_code IS NOT NULL AND LENGTH(TRIM(NEW.pdi_nature_code)) != 3 THEN
        NEW.errors := array_append(NEW.errors, 'pdi_nature_code renseigné mais pas avec 3 caractères exactement');
        lr_pdi_ws_corrige.pdi_nature_code := NULL; --on ignore l'information renseignée
    END IF;
    IF lr_pdi_ws_corrige.pdi_nature_code = 'CLO' AND lr_pdi_ws_corrige.pdi_id_rattachement IS NOT NULL THEN
        NEW.errors := array_append(NEW.errors, 'pdi_nature_code CLO mais avec un rattachement');
    END IF;

    lr_pdi_ws_corrige.pdi_statut := NULLIF(TRIM(NEW.pdi_statut),'');
    IF lr_pdi_ws_corrige.pdi_statut IS NOT NULL AND lr_pdi_ws_corrige.pdi_statut NOT IN ('PRODUCTION','PROJET') THEN
        NEW.errors := array_append(NEW.errors, 'pdi_statut renseigné mais pas avec PRODUCTION ou PROJET');
        NEW.rejet := TRUE; --on rejette le mouvement
    END IF;

    /* champs abandonné
    lr_pdi_ws_corrige.pdi_type := NULLIF(TRIM(NEW.pdi_type),'');
    IF lr_pdi_ws_corrige.pdi_type IS NOT NULL AND LENGTH(TRIM(NEW.pdi_type)) != 1 THEN
        NEW.errors := array_append(NEW.errors, 'pdi_type renseigné mais pas avec 1 caractère exactement');
        lr_pdi_ws_corrige.pdi_type := NULL; --on ignore l'information renseignée
    END IF;
    */

    IF NEW.pdi_visible IS NOT NULL AND NEW.pdi_visible NOT IN ('0','1') THEN
        NEW.errors := array_append(NEW.errors, 'pdi_visible renseigné mais pas avec 0 ou 1');
        lr_pdi_ws_corrige.pdi_visible := NULL; --on ignore l'information renseignée
        NEW.rejet := TRUE; --on rejette le mouvement
    ELSE
        BEGIN lr_pdi_ws_corrige.pdi_visible := NEW.pdi_visible::INTEGER::BOOLEAN;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_visible',error_text));
            NEW.rejet := TRUE; --on rejette le mouvement
        END;
    END IF;

    lr_pdi_ws_corrige.pdi_model := NULLIF(TRIM(NEW.pdi_model),'');
    IF lr_pdi_ws_corrige.pdi_model IS NOT NULL AND LENGTH(TRIM(NEW.pdi_model)) != 2 THEN
        NEW.errors := array_append(NEW.errors, 'pdi_model renseigné mais pas avec 2 caractères exactement');
        lr_pdi_ws_corrige.pdi_model := NULL; --on ignore l'information renseignée
    END IF;

    lr_pdi_ws_corrige.pdi_particularite := NULLIF(TRIM(NEW.pdi_particularite),'');

    lr_pdi_ws_corrige.pdi_etablissement_regate := NULLIF(TRIM(NEW.pdi_etablissement_regate),'');
    IF lr_pdi_ws_corrige.pdi_etablissement_regate IS NOT NULL AND LENGTH(TRIM(NEW.pdi_etablissement_regate)) != 6 THEN
        NEW.errors := array_append(NEW.errors, 'pdi_etablissement_regate renseigné mais pas avec 6 caractères exactement');
        lr_pdi_ws_corrige.pdi_etablissement_regate := NULL; --on ignore l'information renseignée
    END IF;

    lr_pdi_ws_corrige.pdi_etablissement_roc := NULLIF(TRIM(NEW.pdi_etablissement_roc),'');
    IF lr_pdi_ws_corrige.pdi_etablissement_roc IS NOT NULL AND LENGTH(TRIM(NEW.pdi_etablissement_roc)) != 6 THEN
        NEW.errors := array_append(NEW.errors, 'pdi_etablissement_roc renseigné mais pas avec 6 caractères exactement');
        lr_pdi_ws_corrige.pdi_etablissement_roc := NULL; --on ignore l'information renseignée
    END IF;

    lr_pdi_ws_corrige.pdi_bureau_instance := NULLIF(TRIM(NEW.pdi_bureau_instance),'');
    IF lr_pdi_ws_corrige.pdi_bureau_instance IS NOT NULL AND LENGTH(TRIM(NEW.pdi_bureau_instance)) != 6 THEN
        NEW.errors := array_append(NEW.errors, 'pdi_bureau_instance renseigné mais pas avec 6 caractères exactement');
        lr_pdi_ws_corrige.pdi_bureau_instance := NULL; --on ignore l'information renseignée
    END IF;

    BEGIN lr_pdi_ws_corrige.pdi_pre1 := NULLIF(TRIM(NEW.pdi_pre1),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre1',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre2 := NULLIF(TRIM(NEW.pdi_pre2),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre2',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre3 := NULLIF(TRIM(NEW.pdi_pre3),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre3',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre4 := NULLIF(TRIM(NEW.pdi_pre4),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre4',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre5 := NULLIF(TRIM(NEW.pdi_pre5),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre5',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre6 := NULLIF(TRIM(NEW.pdi_pre6),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre6',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre7 := NULLIF(TRIM(NEW.pdi_pre7),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre7',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre8 := NULLIF(TRIM(NEW.pdi_pre8),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre8',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre9 := NULLIF(TRIM(NEW.pdi_pre9),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre9',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre10 := NULLIF(TRIM(NEW.pdi_pre10),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre10',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_pre11 := NULLIF(TRIM(NEW.pdi_pre11),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_pre11',error_text)); END;

    lr_pdi_ws_corrige.pdi_localisation := NULLIF(TRIM(NEW.pdi_localisation),'');

    lr_pdi_ws_corrige.pdi_id_batterie_cidex := NULLIF(TRIM(NEW.pdi_id_batterie_cidex),'');

    BEGIN lr_pdi_ws_corrige.pdi_distance := NULLIF(TRIM(NEW.pdi_distance),'')::NUMERIC;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_distance',error_text)); END;

    lr_pdi_ws_corrige.pdi_type_acces := NULLIF(TRIM(NEW.pdi_type_acces),'');

    --,lr_pdi_ws_corrige.pdi_moloc := NULLIF(TRIM(NEW.pdi_moloc),'');
    BEGIN lr_pdi_ws_corrige.pdi_nb_bal_normalisees := NULLIF(TRIM(NEW.pdi_nb_bal_normalisees),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_nb_bal_normalisees',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_nb_bal_non_normalisees := NULLIF(TRIM(NEW.pdi_nb_bal_non_normalisees),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_nb_bal_non_normalisees',error_text)); END;
    BEGIN lr_pdi_ws_corrige.pdi_nb_bal_etiquetees := NULLIF(TRIM(NEW.pdi_nb_bal_etiquetees),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','pdi_nb_bal_etiquetees',error_text)); END;

    lr_pdi_ws_corrige.pdi_ind_presence := NULLIF(TRIM(NEW.pdi_ind_presence),'');
    lr_pdi_ws_corrige.pdi_ind_presence_gardien := NEW.pdi_ind_presence_gardien::INTEGER;
    lr_pdi_ws_corrige.pdi_ind_presence_presse := NEW.pdi_ind_presence_presse::INTEGER;
    lr_pdi_ws_corrige.pdi_ind_presence_num_rue := NEW.pdi_ind_presence_num_rue::INTEGER;
    lr_pdi_ws_corrige.pdi_ind_presence_plaque_rue := NEW.pdi_ind_presence_plaque_rue::INTEGER;
    lr_pdi_ws_corrige.pdi_ind_presence_depot_relais := NEW.pdi_ind_presence_depot_relais::INTEGER;
    lr_pdi_ws_corrige.pdi_ind_presence_productif := NEW.pdi_ind_presence_productif::INTEGER;
    lr_pdi_ws_corrige.pdi_ind_presence_tab_indicateur := NEW.pdi_ind_presence_tab_indicateur::INTEGER;

    BEGIN lr_pdi_ws_corrige.ip_id := NULLIF(TRIM(NEW.ip_id),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','ip_id',error_text)); END;

    BEGIN lr_pdi_ws_corrige.ip_stop_pub := NULLIF(TRIM(NEW.ip_stop_pub),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','ip_stop_pub',error_text)); END;

    BEGIN lr_pdi_ws_corrige.ip_potentiel_ip := NULLIF(TRIM(NEW.ip_potentiel_ip),'')::INTEGER;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','ip_potentiel_ip',error_text)); END;

    lr_pdi_ws_corrige.ip_code_udb := NULLIF(TRIM(NEW.ip_code_udb),''); --minlength=, maxlength=, pct_nonnull=0.00, pct_distinct=0.00, liste_distinct=
    lr_pdi_ws_corrige.ip_poids_main := NULLIF(TRIM(NEW.ip_poids_main),''); --minlength=, maxlength=, pct_nonnull=0.00, pct_distinct=0.00
    lr_pdi_ws_corrige.ip_comment := NULLIF(TRIM(NEW.ip_comment),'');
    lr_pdi_ws_corrige.distri_etablissement_or := NULLIF(TRIM(NEW.distri_etablissement_or),'');
    lr_pdi_ws_corrige.distri_etablissement_os := NULLIF(TRIM(NEW.distri_etablissement_os),'');
    lr_pdi_ws_corrige.distri_etablissement_pr := NULLIF(TRIM(NEW.distri_etablissement_pr),'');
    lr_pdi_ws_corrige.distri_etablissement_co := NULLIF(TRIM(NEW.distri_etablissement_co),'');
    lr_pdi_ws_corrige.distri_etablissement_ip := NULLIF(TRIM(NEW.distri_etablissement_ip),'');

    lr_pdi_ws_corrige.adresse_id := NULLIF(TRIM(NEW.adresse_id),'');
    IF lr_pdi_ws_corrige.adresse_id IS NOT NULL AND LENGTH(TRIM(NEW.adresse_id)) != 10 THEN
        NEW.errors := array_append(NEW.errors, 'adresse_id renseigné mais pas avec 10 caractères exactement');
        lr_pdi_ws_corrige.adresse_id := NULL; --on ignore l'information renseignée
    END IF;

    BEGIN lr_pdi_ws_corrige.adresse_x := REPLACE(NULLIF(TRIM(NEW.adresse_x),''),',','.')::DOUBLE PRECISION;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','adresse_x',error_text)); END;
    BEGIN lr_pdi_ws_corrige.adresse_y := REPLACE(NULLIF(TRIM(NEW.adresse_y),''),',','.')::DOUBLE PRECISION;
        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','adresse_y',error_text)); END;

    /* a voir avec équipe GEOPAD : valeur -1 et 123456 normale ? */
    IF NULLIF(TRIM(NEW.adresse_geocode),'') IS NOT NULL AND TRIM(NEW.adresse_geocode) NOT IN ('1','2','3','4','5','6','7','8','9') THEN
        NEW.errors := array_append(NEW.errors, 'adresse_geocode renseigné mais pas avec 1, 2, 3, 4, 5, 6, 7, 8 ou 9');
        lr_pdi_ws_corrige.adresse_geocode := NULL; --on ignore l'information renseignée
    ELSE
        BEGIN lr_pdi_ws_corrige.adresse_geocode := NULLIF(TRIM(NEW.adresse_geocode),'')::SMALLINT;
            EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT; NEW.errors := array_append(NEW.errors, CONCAT_WS(':','adresse_geocode',error_text)); END;
    END IF;

    /** CONTROLES DE COHERENCES ***/
    --Correctif spécial pour un état à 1 incohérent avec un statut PROJET, on considère que l'état est mal renseigné et qu'il faut le mettre à 2
    IF lr_pdi_ws_corrige.pdi_etat = 1 AND lr_pdi_ws_corrige.pdi_statut = 'PROJET' THEN
        NEW.errors := array_append(NEW.errors, 'Etat renseigné à 1 incohérent avec le statut PROJET, état corrigé à 2');
        lr_pdi_ws_corrige.pdi_etat = 2;
    END IF;
    --Si l'état est renseigné à 1, le pdi doit être en statut PRODUCTION
    IF lr_pdi_ws_corrige.pdi_etat = 1 AND lr_pdi_ws_corrige.pdi_statut != 'PRODUCTION' THEN
        NEW.errors := array_append(NEW.errors, 'Etat renseigné à 1 incohérent avec le statut (devrait être PRODUCTION)');
        NEW.rejet := TRUE; -- on rejette le mouvement
    END IF;
    --Si l'état est renseigné à 2, le pdi doit être en statut PROJET
    IF lr_pdi_ws_corrige.pdi_etat = 2 AND lr_pdi_ws_corrige.pdi_statut != 'PROJET' THEN
        NEW.errors := array_append(NEW.errors, 'Etat renseigné à 2 incohérent avec le statut (devrait être PROJET)');
        NEW.rejet := TRUE; -- on rejette le mouvement
    END IF;

    --Toutes les informations sur les coordonnées doivent être renseignées pour être correctement exploitables
    IF (lr_pdi_ws_corrige.adresse_x IS NOT NULL OR lr_pdi_ws_corrige.adresse_y IS NOT NULL OR lr_pdi_ws_corrige.adresse_geocode IS NOT NULL)
        AND (lr_pdi_ws_corrige.adresse_x IS NULL OR lr_pdi_ws_corrige.adresse_y IS NULL OR lr_pdi_ws_corrige.adresse_geocode IS NULL) THEN
        NEW.errors := array_append(NEW.errors, 'Information sur les coordonnées partiellement renseignées');
        --on ignore toutes les informations sur les coordonnées
        lr_pdi_ws_corrige.adresse_x := NULL;
        lr_pdi_ws_corrige.adresse_y := NULL;
        lr_pdi_ws_corrige.adresse_geocode := NULL;
    END IF;

    lr_pdi_ws_corrige.nb_pre_par_per_log_ind :=
        --Si il n'y a qu'un particulier sur le PDI
        CASE WHEN (COALESCE(lr_pdi_ws_corrige.pdi_pre1,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre2,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre3,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre4,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre5,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre6,0)) = 1
            --On considère qu'il s'agit d'un logement individuel, et on compte le nombre de particulier permanents
            THEN COALESCE(lr_pdi_ws_corrige.pdi_pre1,0)+COALESCE(lr_pdi_ws_corrige.pdi_pre2,0)
            ELSE 0
        END;
    lr_pdi_ws_corrige.nb_pre_par_per_log_col :=
        --Si il y a plus d'un particulier sur le PDI
        CASE WHEN (COALESCE(lr_pdi_ws_corrige.pdi_pre1,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre2,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre3,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre4,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre5,0)
                    +COALESCE(lr_pdi_ws_corrige.pdi_pre6,0)) > 1
            --On considère qu'il s'agit d'un logement collectif, et on compte le nombre de particulier permanents
            THEN COALESCE(lr_pdi_ws_corrige.pdi_pre1,0)+COALESCE(lr_pdi_ws_corrige.pdi_pre2,0)
            ELSE 0
        END;
    lr_pdi_ws_corrige.agg_nb_pdi_repositionnes := CASE WHEN lr_pdi_ws_corrige.adresse_geocode = 9 THEN 1 ELSE 0 END;

    IF lr_pdi_ws_corrige.adresse_x IS NOT NULL
    AND lr_pdi_ws_corrige.adresse_y IS NOT NULL
    AND lr_pdi_ws_corrige.adresse_id IS NOT NULL THEN
        v_pdi_coord_srid := public.getSridCoordRanFromCodeInseeDepartement(public.getCodeInseeDepartementFromCodeInseeCommune(LEFT(lr_pdi_ws_corrige.adresse_id,5)));
        v_pdi_coord_native :=
            ST_SetSRID(
                ST_MakePoint(lr_pdi_ws_corrige.adresse_x,lr_pdi_ws_corrige.adresse_y)
                ,v_pdi_coord_srid
            );
        IF public.coordIsInSridBounds(v_pdi_coord_native) = FALSE THEN
            NEW.errors := array_append(NEW.errors, 'x / y renseignés mais en dehors de l''étendue des limites de la projection prévue pour l''adresse');
            --Si on est en projection 2154
            IF v_pdi_coord_srid = 2154 THEN
                --Pas de correction possible connue à ce jour : on ignore les coordonnées
                lr_pdi_ws_corrige.adresse_x := NULL;
                lr_pdi_ws_corrige.adresse_y := NULL;
            ELSE
                v_pdi_coord_native :=
                    ST_Transform(
                        ST_SetSRID(
                            ST_MakePoint(lr_pdi_ws_corrige.adresse_x,lr_pdi_ws_corrige.adresse_y)
                            --On tente une projection forcée en 2154
                            ,2154
                        )
                        --Puis transformée dans la projection attendue
                        ,v_pdi_coord_srid
                    );
                --Si on est désormais dans les limites de la projection attendue
                IF public.coordIsInSridBounds(v_pdi_coord_native) = TRUE THEN
                    lr_pdi_ws_corrige.adresse_x := ST_X(lr_pdi_ws_corrige.geom);
                    lr_pdi_ws_corrige.adresse_y := ST_Y(lr_pdi_ws_corrige.geom);
                    lr_pdi_ws_corrige.geom := ST_Transform(v_pdi_coord_native,3857);
                ELSE
                    lr_pdi_ws_corrige.adresse_x := NULL;
                    lr_pdi_ws_corrige.adresse_y := NULL;
                    lr_pdi_ws_corrige.geom := NULL;
                END IF;
            END IF;
        ELSE
            lr_pdi_ws_corrige.geom := ST_Transform(v_pdi_coord_native,3857);
        END IF;
    END IF;

    --RAISE NOTICE 'Nombre d''erreur : %',COALESCE(array_length(NEW.errors,1),0);

    --Si le mouvement n'est pas rejeté
    --Sur les plateformes de DEV et REC, on n'intègre que les mouvements sur les PDI associés à des adresses de la GIRONDE
    IF NEW.rejet = FALSE AND (public.getEnvDepLimit() IS NULL OR lr_pdi_ws_corrige.adresse_id LIKE CONCAT(public.getEnvDepLimit(),'%')) THEN
        --ON INTEGRE LE MOUVEMENT controlé / corrigé

        /** TRANSCODAGES ***/
        --Si le libellé nature n'est pas renseigné, mais que le code l'est
        IF lr_pdi_ws_corrige.pdi_nature IS NULL AND lr_pdi_ws_corrige.pdi_nature_code IS NOT NULL THEN
            lr_pdi_ws_corrige.pdi_nature :=
                --Transcodage du code nature (issu de GEOPAD) en libellé compatible GEODIS
                CASE lr_pdi_ws_corrige.pdi_nature_code
                    WHEN 'CLO' THEN 'Classique'
                    --PDI "Père"
                    WHEN 'CLP' THEN 'Classique' --PDI de rattachement (Père) classique
                    WHEN 'BPP' THEN 'Batterie Privée' --PDI de rattachement (Père) batterie privée
                    WHEN 'CIP' THEN 'Batterie Cidex' --PDI de rattachement (Père) batterie CIDEX
                    --PDI "Fils"
                    WHEN 'CLF' THEN 'Classique' --PDI rattaché (fils) à un PDI (Père) classique
                    WHEN 'BPF' THEN 'Cidex' --PDI rattaché (fils) à un PDI batterie privée
                    WHEN 'CIF' THEN 'Cidex' --PDI rattaché (fils) à un PDI batterie CIDEX
                END;
        END IF;

        /* A ETUDIER
        --Si le code nature n'est pas renseigné, mais que le libellé l'est
        IF lr_pdi_ws_corrige.pdi_nature_code IS NULL AND lr_pdi_ws_corrige.pdi_nature IS NOT NULL THEN
                --Transcodage du libellé nature (issu de GEODIS) en code compatible GEOPAD
                lr_pdi_ws_corrige.pdi_nature_code :=
                CASE lr_pdi_ws_corrige.pdi_nature
                    WHEN 'Classique' THEN
                        CASE
                            --PDI rattaché = "Fils" : ne peut être qu'un CLF
                            WHEN lr_pdi_ws_corrige.pdi_id_rattachement != lr_pdi_ws_corrige.pdi_id THEN 'CLF' --PDI rattaché (fils) à un PDI (Père) classique
                            --Sinon, il n'est pas contre à priori pas possible de déterminer s'il s'agit d'un CLO ou CLP, à part en cherchant à savoir si le PDI a des PDI qui lui sont rattachés
                        END
                    --Il n'est pas contre à priori pas possible de déterminer s'il s'agit d'un BPF ou CIF,
                    --à part en cherchant la nature du PDI de rattachement (si BPP alors BPF, sinon si CIP alors CIF)
                    --WHEN 'Cidex'
                    --	CASE
                    --		--PDI rattaché = "Fils" : peut être un BPF ou un CIF
                    --		WHEN lr_pdi_ws_corrige.pdi_id_rattachement != lr_pdi_ws_corrige.pdi_id AND ??? THEN 'BPF' --PDI rattaché (fils) à un PDI batterie privée (BPP)
                    --		WHEN lr_pdi_ws_corrige.pdi_id_rattachement != lr_pdi_ws_corrige.pdi_id AND ??? THEN 'CIF' --PDI rattaché (fils) à un PDI batterie CIDEX (CIP)
                    --	END
                    WHEN 'Batterie Cidex' THEN 'CIP' --PDI de rattachement (Père) batterie CIDEX
                    WHEN 'Batterie Privée' THEN 'BPP' --PDI de rattachement (Père) batterie privée
                END;
        END IF;
        */
        --Si INIT
        IF NEW.dt_debut_donnees_import = TO_DATE('01/01/1970','DD/MM/YYYY') THEN
            IF lr_pdi_ws_corrige.pdi_etat = 1
                AND lr_pdi_ws_corrige.pdi_visible = TRUE
                AND lr_pdi_ws_corrige.adresse_id IS NOT NULL
            THEN
                --lr_pdi_ws_corrige.agg_adresse := TRUE;
                lr_pdi_ws_corrige.agg_adresse_id := lr_pdi_ws_corrige.adresse_id;
                lr_pdi_ws_corrige.agg_nb_pdi := 1;
            END IF;
            INSERT INTO geopad.pdi VALUES (lr_pdi_ws_corrige.*);
        ELSE
            --DELTA, par fichier INIT ou par web service

            --Suppression de l'éventuelle version précédente du PDI
            DELETE FROM geopad.pdi WHERE pdi_id = lr_pdi_ws_corrige.pdi_id RETURNING * INTO lr_deleted_pdi;

            --Si le PDI était sur une adresse, a changé d'adresse ou n'est plus sur une adresse, qu'il était en PRODUCTION et VISIBLE
            IF lr_deleted_pdi.adresse_id IS NOT NULL
                AND lr_deleted_pdi.adresse_id != COALESCE(lr_pdi_ws_corrige.adresse_id,'AUCUNE')
                AND lr_deleted_pdi.pdi_etat = 1 --et qu'il était en PRODUCTION
                AND lr_deleted_pdi.pdi_visible = TRUE --et qu'il était VISIBLE
            THEN
                --Il faut mettre à jour l'aggrégat PDI de l'ancienne adresse
                PERFORM geopad.update_pdi_agg_adr(lr_deleted_pdi.adresse_id);
            END IF;

            --Si le PDI est toujours sur la même adresse, est toujours en PRODUCTION et VISIBLE
            --et était unique à l'adresse, il l'est donc toujours (cas le plus courant)
            IF lr_pdi_ws_corrige.adresse_id IS NOT NULL
                AND lr_pdi_ws_corrige.pdi_etat = 1
                AND lr_pdi_ws_corrige.pdi_visible = TRUE
                AND lr_pdi_ws_corrige.adresse_id = COALESCE(lr_deleted_pdi.adresse_id,'AUCUNE')
                --AND lr_deleted_pdi.agg_adresse = TRUE
                AND lr_deleted_pdi.agg_adresse_id = lr_deleted_pdi.adresse_id
                /* implicite
                AND lr_deleted_pdi.pdi_etat = 1
                AND lr_deleted_pdi.pdi_visible = TRUE
                AND lr_deleted_pdi.agg_nb_pdi = 1
                */
            THEN
                --Insertion de la nouvelle version du PDI, unique à l'adresse
                lr_pdi_ws_corrige.agg_adresse_id := lr_pdi_ws_corrige.adresse_id;
                lr_pdi_ws_corrige.agg_nb_pdi := 1;
                INSERT INTO geopad.pdi VALUES (lr_pdi_ws_corrige.*);
            ELSE
                --Insertion de la nouvelle version du PDI, qui peut être unique ou multiple ou non lié à l'adresse
                INSERT INTO geopad.pdi VALUES (lr_pdi_ws_corrige.*);
                --Il faut mettre à jour l'aggrégat PDI de l'adresse
                IF lr_pdi_ws_corrige.adresse_id IS NOT NULL THEN
                    PERFORM geopad.update_pdi_agg_adr(lr_pdi_ws_corrige.adresse_id);
                END IF;
            END IF;
        END IF;
    END IF;

    --Si il n'y a pas eu d'erreurs
    IF NEW.errors IS NULL THEN
        --On n'enregistre rien dans geopad.pdi_tmp
        RETURN NULL;
    --Il y a eu des erreurs
    ELSE
        --On enregistre dans geopad.pdi_tmp les données brutes avec les erreurs rencontrées
        RETURN NEW;
    END IF;
EXCEPTION
    /* Cette erreur est théoriquement plus possible depuis la gestion ON CONFLICT
    WHEN unique_violation THEN
        RETURN NULL;
    */
    WHEN OTHERS THEN
        --En cas d'erreur innatendue, on enregistre les données brutes avec les erreurs rencontrées
        GET STACKED DIAGNOSTICS error_text = MESSAGE_TEXT;
        NEW.errors := array_append(NEW.errors, error_text);
        NEW.rejet := TRUE;
        RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS pdi_ws_trg_controle ON geopad.pdi_tmp;
CREATE TRIGGER pdi_ws_trg_controle
    BEFORE INSERT ON geopad.pdi_tmp
    FOR EACH ROW
    EXECUTE PROCEDURE geopad.pdi_ws_controle();

SELECT set_table_comment(
    'geopad'
    ,'pdi_tmp'
    ,'PDI'
    ,'Points de distibution issus d'' fichier INIT PDI et du web service DELTA PDI'
);

SELECT set_column_comment('geopad','pdi_tmp','id_import',
'Id import B2CA','Identifiant de l''import dans BC2A','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_id',
'Id PDI','Identifiant du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_id_rattachement',
'Id PDI rattachement','PDI de rattachement dans le cas où le PDI est un fils','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_etat',
'Etat PDI','Etat du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_dt_creation',
'Date création PDI','Date de création du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_dt_modification',
'Date modif. PDI','Date de modification du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_nature',
'Nature PDI','Nature du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_statut',
'Statut PDI','Statut du point de distribution','');

/* champs abandonné
SELECT set_column_comment('geopad','pdi_tmp','pdi_type',
'Type PDI','Type du point de distribution','');
*/

SELECT set_column_comment('geopad','pdi_tmp','pdi_model',
'Model PDI','Type du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','pdi_visible',
'Visible PDI','???','');

/*
SELECT set_column_comment('geopad','pdi_tmp','nom_personne',
'Nom personne','Nom de la dernière personne ayant modifiée (ou créée) la ressource','');

SELECT set_column_comment('geopad','pdi_tmp','code_regate_site_prop_pdi',
'Code REGATE site','Code REGATE du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','code_roc_site_prop_pdi',
'Code ROC site','Code ROC identifiant le site du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','code_roc_bur_instance_pdi',
'Code ROC du bureau d''instance','Code ROC du bureau d''instance où recupérer l''OS, colis, ...','');

SELECT set_column_comment('geopad','pdi_tmp','"code_roc_etab_resp_OO"',
'Code ROC établissement responsable OO','Code ROC établissement responsable des objets ordinaires','');

SELECT set_column_comment('geopad','pdi_tmp','"code_roc_etab_resp_OS"',
'Code ROC établissement responsable OS','Code ROC établissement responsable des objets à remettre contre une signature','');

SELECT set_column_comment('geopad','pdi_tmp','"code_roc_etab_resp_IP"',
'Code ROC établissement responsable IP','Code ROC établissement responsable des imprimés publicitaires','');

SELECT set_column_comment('geopad','pdi_tmp','"code_roc_etab_resp_PR"',
'Code ROC établissement responsable PR','Code ROC établissement responsable des objets de presse','');

SELECT set_column_comment('geopad','pdi_tmp','"code_roc_etab_resp_CO"',
'Code ROC établissement responsable CO','Code ROC établissement responsable des colis','');

SELECT set_column_comment('geopad','pdi_tmp','mouvement',
'Mouvement sur le PDI','Mouvement sur le point de distribution (C:Création, M:Modification, S:Suppression)','');

SELECT set_column_comment('geopad','pdi_tmp','pre1',
'Nb PRE particuliers permanents remise BAL','Nombre de points de remise particuliers permanents de type remise en boîte aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','pre2',
'Nb PRE particuliers permanents remise main propre','Nombre de points de remise particuliers permanents de type remise en main propre','');

SELECT set_column_comment('geopad','pdi_tmp','pre3',
'Nb PRE particuliers saisonniers remise BAL','Nombre de points de remise particuliers saisonniers de type remise en boîte aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','pre4',
'Nb PRE particuliers saisonniers remise main propre','Nombre de points de remise particuliers saisonniers de type remise en main propre','');

SELECT set_column_comment('geopad','pdi_tmp','pre5',
'Nb PRE particuliers secondaires remise BAL','Nombre de points de remise particuliers secondaires de type remise en boîte aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','pre6',
'Nb PRE particuliers secondaires remise main propre','Nombre de points de remise particuliers secondaires de type remise en main propre','');

SELECT set_column_comment('geopad','pdi_tmp','pre7',
'Nb PRE professionels permanents remise BAL','Nombre de points de remise professionels permanents de type remise en boîte aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','pre8',
'Nb PRE professionels permanents remise main propre','Nombre de points de remise professionels permanents de type remise en main propre','');

SELECT set_column_comment('geopad','pdi_tmp','pre9',
'Nb PRE professionels saisonniers remise BAL','Nombre de points de remise professionels saisonniers de type remise en boîte aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','pre10',
'Nb PRE professionels saisonniers remise main propre','Nombre de points de remise professionels saisonniers de type remise en main propre','');

SELECT set_column_comment('geopad','pdi_tmp','pre11',
'Nb PRE non desservis','Nombre de points de remise non desservis','');

SELECT set_column_comment('geopad','pdi_tmp','id_batterie_cidex_music',
'Code externe','Code externe','');

SELECT set_column_comment('geopad','pdi_tmp','distance_pre',
'Distance PRE','Distance points de remise','');

SELECT set_column_comment('geopad','pdi_tmp','mode_acces_bal',
'Type d''accès BAL','Type d''accès aux boîtes aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','localisation_bal',
'Localisation BAL','Localisation des boîtes aux lettres','Les boîtes aux lettres peuvent être soit dans la propriété, soit en limite de voirie');

SELECT set_column_comment('geopad','pdi_tmp','gardien_present',
'PDI ayant gardien effectuant distribution','Point de distribution ayant un gardien effectuant la distribution','');

SELECT set_column_comment('geopad','pdi_tmp','presse_quotidienne',
'PDI ayant presse distribuée quotidiennement','Point de distribution ayant de la presse distribuée quotidiennement','');

SELECT set_column_comment('geopad','pdi_tmp','plaque_numero_rue_presente',
'PDI ayant plaque numéro rue','Point de distribution ayant une plaque indiquant le numéro dans la rue','');

SELECT set_column_comment('geopad','pdi_tmp','tableau_indicateur_present',
'PDI ayant tableau indicateur','Point de distribution ayant un tableau de correspondance entre les noms des habitants et les numéros des boîtes aux lettres','');

SELECT set_column_comment('geopad','pdi_tmp','plaque_rue_presente',
'PDI ayant plaque nom rue','Point de distribution ayant une plaque indiquant le nom de la rue','');

SELECT set_column_comment('geopad','pdi_tmp','nb_boites_normalisees',
'Nb BAL normalisées','Nombre de boîtes aux lettres normalisées','Une boîte est normalisée lorsqu''elle possède un étiquetage correct, qu''elle respecte certaines dimensions et est en bon état');

SELECT set_column_comment('geopad','pdi_tmp','nb_boites_non_normalisees',
'Nb BAL non normalisées','Nombre de boîtes aux lettres non normalisées','Une boîte est normalisée lorsqu''elle possède un étiquetage correct, qu''elle respecte certaines dimensions et est en bon état');

SELECT set_column_comment('geopad','pdi_tmp','nb_bal_etiquetees',
'Nb BAL possédent étiquette habitant','Nombre de boîtes aux lettres possédant une étiquette au nom de l''habitant','');

SELECT set_column_comment('geopad','pdi_tmp','nb_stop_pub',
'Nb BAL STOP PUB','Nombre de boîtes aux lettres possédant l''indication ''Stop PUB''','Le facteur ne doit pas distribuer d''imprimés publicitaires à cette adresse');

SELECT set_column_comment('geopad','pdi_tmp','nb_potentiel_ip',
'Nb PRE susceptibles de recevoir de l''IP','Nombre de points de remise susceptibles de recevoir des imprimés publicitaires','Ce nombre auquel on ajoute le nb de Stop PUB est inférieur ou égal au nb total de PRE du PDI (car lorsque les BAL sont abimées ou trop petites, elles ne sont pas éligibles à la distribution des imprimés publicitaires)');

SELECT set_column_comment('geopad','pdi_tmp','code_udb',
'Code UDB','Code identifiant l''unité de base du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','commentaire',
    'Commentaire sur le PDI','Commentaire sur le PDI contenant un libellé du complément d''identification, un libellé "Autres" et des observations (infos PDI)','');

SELECT set_column_comment('geopad','pdi_tmp','code_cea_adresse_geo_plus_fine',
'Code adresse géographique','Code identifiant de l''adresse géographique RAN la plus fine du point de distribution','');

SELECT set_column_comment('geopad','pdi_tmp','coord_x_gps_adresse',
'Coordonnée X','Coordonnée X GPS de l''adresse','');

SELECT set_column_comment('geopad','pdi_tmp','coord_y_gps_adresse',
'Coordonnée Y','Coordonnée Y GPS de l''adresse','');

SELECT set_column_comment('geopad','pdi_tmp','code_geocodage_adresse',
'Code géocodage','Code géocodage de l''adresse','');

*/

SELECT copy_table_comment('geopad','pdi_tmp','geopad','pdi');
SELECT copy_columns_comments('geopad','pdi_tmp','geopad','pdi');

/* TEST
INSERT INTO geopad.pdi_tmp (id_import, pdi_id, pdi_id_rattachement, pdi_etat, pdi_statut) values (1, 2, ' ', 0, 'PRODUCTION');
select errors, * from geopad.pdi_tmp where id_import = 1
select * from geopad.pdi where id_import = 1
INSERT INTO geopad.pdi_tmp (id_import, pdi_id, pdi_id_rattachement) values (1, 2, '2A');
INSERT INTO geopad.pdi_tmp (id_import, pdi_id, pdi_etablissement_regate) values (1, 2, '12345');

{"invalid input syntax for integer: \"2A\"","",""}
{"invalid input syntax for integer: \"\"","",""}
select errors, * from geopad.pdi_tmp limit 1
select * from geopad.pdi limit 1
delete from geopad.pdi_tmp where id_import = 1
select count(*) from geopad.pdi
select * from public.historique_import
delete from geopad.pdi where id_import in (select id from historique_import where co_etat = 'ERREUR');
select * from public.historique_import;
*/

CREATE OR REPLACE FUNCTION geopad.pdi_est_identique(v_pdi_a IN geopad.pdi, v_pdi_b IN geopad.pdi)
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	RETURN NOT (
		COALESCE(v_pdi_a.pdi_id_rattachement,-1000) != COALESCE(v_pdi_b.pdi_id_rattachement,-1000) 
		OR COALESCE(v_pdi_a.pdi_etat,-9) != COALESCE(v_pdi_b.pdi_etat,-9) 
		OR COALESCE(v_pdi_a.pdi_dt_creation,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP) != COALESCE(v_pdi_b.pdi_dt_creation,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP) 
		--On ignore pour permettre la comparaison de mouvements à date différentes 
		--OR COALESCE(v_pdi_a.pdi_dt_modification,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP) != COALESCE(v_pdi_b.pdi_dt_modification,TO_DATE('23/10/1981','DD/MM/YYYY')::TIMESTAMP) 
		OR COALESCE(v_pdi_a.pdi_source,'NULL') != COALESCE(v_pdi_b.pdi_source,'NULL') 
		OR COALESCE(v_pdi_a.pdi_nature_code,'ZZZ') != COALESCE(v_pdi_b.pdi_nature_code,'ZZZ') 
		OR COALESCE(v_pdi_a.pdi_nature,'NULL') != COALESCE(v_pdi_b.pdi_nature,'NULL') 
		OR COALESCE(v_pdi_a.pdi_statut,'NULL') != COALESCE(v_pdi_b.pdi_statut,'NULL') 
		--Champs abandonnée : OR COALESCE(v_pdi_a.pdi_type,'NULL') != COALESCE(v_pdi_b.pdi_type,'NULL') 
		OR COALESCE(v_pdi_a.pdi_model,'NULL') != COALESCE(v_pdi_b.pdi_model,'NULL') 
		OR COALESCE(v_pdi_a.pdi_visible::INTEGER,-9) != COALESCE(v_pdi_b.pdi_visible::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_particularite,'NULL') != COALESCE(v_pdi_b.pdi_particularite,'NULL') 
		OR COALESCE(v_pdi_a.pdi_etablissement_regate,'NULL') != COALESCE(v_pdi_b.pdi_etablissement_regate,'NULL') 
		OR COALESCE(v_pdi_a.pdi_etablissement_roc,'NULL') != COALESCE(v_pdi_b.pdi_etablissement_roc,'NULL') 
		OR COALESCE(v_pdi_a.pdi_bureau_instance,'NULL') != COALESCE(v_pdi_b.pdi_bureau_instance,'NULL') 
		OR COALESCE(v_pdi_a.pdi_pre1,-1000) != COALESCE(v_pdi_b.pdi_pre1,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre2,-1000) != COALESCE(v_pdi_b.pdi_pre2,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre3,-1000) != COALESCE(v_pdi_b.pdi_pre3,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre4,-1000) != COALESCE(v_pdi_b.pdi_pre4,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre5,-1000) != COALESCE(v_pdi_b.pdi_pre5,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre6,-1000) != COALESCE(v_pdi_b.pdi_pre6,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre7,-1000) != COALESCE(v_pdi_b.pdi_pre7,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre8,-1000) != COALESCE(v_pdi_b.pdi_pre8,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre9,-1000) != COALESCE(v_pdi_b.pdi_pre9,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre10,-1000) != COALESCE(v_pdi_b.pdi_pre10,-1000) 
		OR COALESCE(v_pdi_a.pdi_pre11,-1000) != COALESCE(v_pdi_b.pdi_pre11,-1000) 
		OR COALESCE(v_pdi_a.pdi_localisation,'NULL') != COALESCE(v_pdi_b.pdi_localisation,'NULL') 
		OR COALESCE(v_pdi_a.pdi_id_batterie_cidex,'NULL') != COALESCE(v_pdi_b.pdi_id_batterie_cidex,'NULL') 
		OR COALESCE(v_pdi_a.pdi_distance,-1000) != COALESCE(v_pdi_b.pdi_distance,-1000) 
		OR COALESCE(v_pdi_a.pdi_type_acces,'NULL') != COALESCE(v_pdi_b.pdi_type_acces,'NULL') 
		OR COALESCE(v_pdi_a.pdi_nb_bal_normalisees,-1000) != COALESCE(v_pdi_b.pdi_nb_bal_normalisees,-1000) 
		OR COALESCE(v_pdi_a.pdi_nb_bal_non_normalisees,-1000) != COALESCE(v_pdi_b.pdi_nb_bal_non_normalisees,-1000) 
		OR COALESCE(v_pdi_a.pdi_nb_bal_etiquetees,-1000) != COALESCE(v_pdi_b.pdi_nb_bal_etiquetees,-1000) 
		OR COALESCE(v_pdi_a.pdi_ind_presence,'NULL') != COALESCE(v_pdi_b.pdi_ind_presence,'NULL') 
		OR COALESCE(v_pdi_a.pdi_ind_presence_gardien::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_gardien::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_ind_presence_presse::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_presse::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_ind_presence_num_rue::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_num_rue::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_ind_presence_plaque_rue::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_plaque_rue::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_ind_presence_depot_relais::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_depot_relais::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_ind_presence_productif::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_productif::INTEGER,-9) 
		OR COALESCE(v_pdi_a.pdi_ind_presence_tab_indicateur::INTEGER,-9) != COALESCE(v_pdi_b.pdi_ind_presence_tab_indicateur::INTEGER,-9) 
		OR COALESCE(v_pdi_a.ip_id,'NULL') != COALESCE(v_pdi_b.ip_id,'NULL') 
		OR COALESCE(v_pdi_a.ip_stop_pub,-1000) != COALESCE(v_pdi_b.ip_stop_pub,-1000) 
		OR COALESCE(v_pdi_a.ip_potentiel_ip,-1000) != COALESCE(v_pdi_b.ip_potentiel_ip,-1000) 
		OR COALESCE(v_pdi_a.ip_code_udb,'NULL') != COALESCE(v_pdi_b.ip_code_udb,'NULL') 
		OR COALESCE(v_pdi_a.ip_poids_main,'NULL') != COALESCE(v_pdi_b.ip_poids_main,'NULL') 
		--On ignore ce champs, utilisé par BCAA pour flaguer les mouvements générés suite à une détection de MODIFICATION
		--OR COALESCE(v_pdi_a.ip_comment,'NULL') != COALESCE(v_pdi_b.ip_comment,'NULL') 
		OR COALESCE(v_pdi_a.distri_etablissement_or,'NULL') != COALESCE(v_pdi_b.distri_etablissement_or,'NULL') 
		OR COALESCE(v_pdi_a.distri_etablissement_os,'NULL') != COALESCE(v_pdi_b.distri_etablissement_os,'NULL') 
		OR COALESCE(v_pdi_a.distri_etablissement_pr,'NULL') != COALESCE(v_pdi_b.distri_etablissement_pr,'NULL') 
		OR COALESCE(v_pdi_a.distri_etablissement_co,'NULL') != COALESCE(v_pdi_b.distri_etablissement_co,'NULL') 
		OR COALESCE(v_pdi_a.distri_etablissement_ip,'NULL') != COALESCE(v_pdi_b.distri_etablissement_ip,'NULL') 
		OR COALESCE(v_pdi_a.adresse_id,'NULL') != COALESCE(v_pdi_b.adresse_id,'NULL') 
		OR COALESCE(v_pdi_a.adresse_x,-1000) != COALESCE(v_pdi_b.adresse_x,-1000) 
		OR COALESCE(v_pdi_a.adresse_y,-1000) != COALESCE(v_pdi_b.adresse_y,-1000) 
		OR COALESCE(v_pdi_a.adresse_geocode,-1000) != COALESCE(v_pdi_b.adresse_geocode,-1000)
	);
END
$func$ LANGUAGE plpgsql;

--Fonction d'alimentation
SELECT drop_all_functions_if_exists('geopad','setPdiAgg');
CREATE OR REPLACE FUNCTION geopad.setPdiAgg()
  RETURNS BOOLEAN AS
$func$
DECLARE
	v_adresse_avec_multiples_pdi RECORD;
	--v_pdi_agg RECORD;
	v_nb_adresses_traitees INTEGER := 0;
BEGIN
	RAISE NOTICE '% Début traitement setPdiAgg', TO_CHAR(clock_timestamp(),'HH24:MI:SS');
	FOR v_adresse_avec_multiples_pdi IN (
		SELECT pdi.adresse_id, ARRAY_AGG(pdi_id) AS liste_pdi_id
		FROM geopad.pdi AS pdi
		-- En statut production (pas en projet), non supprimé, et visible
		WHERE pdi.pdi_etat = 1 AND pdi.pdi_visible = TRUE
		-- Associé à une adresse
		AND pdi.adresse_id IS NOT NULL
		/* nécessite l'index sur la référence adresse
		-- Et ayant au moins un autre pdi sur cette même adresse
		AND EXISTS (
			SELECT 1
			FROM geopad.pdi AS autre_pdi
			WHERE autre_pdi.pdi_etat = 1 AND autre_pdi.pdi_visible = TRUE
			AND autre_pdi.adresse_id = pdi.adresse_id
		)
		*/
		GROUP BY pdi.adresse_id
		HAVING COUNT(*) > 1
	)
	LOOP
		--Les PDI de l'adresse ne sont pas unique à l'adresse
		UPDATE geopad.pdi 
		SET	--agg_adresse = NULL
			agg_adresse_id = NULL
			,agg_nb_pdi = NULL
		WHERE pdi_id = ANY(v_adresse_avec_multiples_pdi.liste_pdi_id)
			AND pdi_etat = 1 AND pdi_visible = TRUE;
		
		--Création du PDI aggrégé à l'adresse
		PERFORM geopad.create_pdi_agg_adr(v_adresse_avec_multiples_pdi.adresse_id, v_adresse_avec_multiples_pdi.liste_pdi_id);
		
		v_nb_adresses_traitees := v_nb_adresses_traitees + 1;
		IF v_nb_adresses_traitees%10000 = 0 THEN
			RAISE NOTICE '%, % adresses traitées', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_nb_adresses_traitees;
		END IF;
	END LOOP;
	
	RAISE NOTICE '% Fin traitement setPdiAgg, % adresses traitées', TO_CHAR(clock_timestamp(),'HH24:MI:SS'), v_nb_adresses_traitees;
		
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('geopad','resetPdiAgg');
CREATE OR REPLACE FUNCTION geopad.resetPdiAgg()
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	RAISE NOTICE '% : début de calcul des aggrégats PDI à l adresse', TO_CHAR(clock_timestamp(),'HH24:MI:SS');
	
	DROP INDEX IF EXISTS geopad.idx_distri_pdi_ws_corrige_agg_adresse_id;
	
	DELETE FROM geopad.pdi
	WHERE agg_nb_pdi > 1;
	
	UPDATE geopad.pdi
	SET agg_adresse_id = NULL
		,agg_nb_pdi = NULL
	WHERE NOT (pdi_etat = 1 AND pdi_visible = TRUE AND adresse_id IS NOT NULL);
	
	UPDATE geopad.pdi
	SET agg_adresse_id = adresse_id
		,agg_nb_pdi = 1
	WHERE pdi_etat = 1 AND pdi_visible = TRUE AND adresse_id IS NOT NULL;
	RAISE NOTICE '% : fin initialisation', TO_CHAR(clock_timestamp(),'HH24:MI:SS');
	
	PERFORM geopad.setPdiAgg(); --SELECT geopad.setPdiAgg();
	
	CREATE UNIQUE INDEX IF NOT EXISTS idx_distri_pdi_ws_corrige_agg_adresse_id ON geopad.pdi(agg_adresse_id);
	RAISE NOTICE '% : fin de création de l index unique sur agg_adresse_id', TO_CHAR(clock_timestamp(),'HH24:MI:SS');
	
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

--select geopad.resetPdiAgg()
