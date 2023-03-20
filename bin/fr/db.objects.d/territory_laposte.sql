/***
 * FR-TERRITORY postal definition
 */

CREATE TABLE IF NOT EXISTS fr.territory_postal (
    nivgeo VARCHAR
    , codgeo VARCHAR
    , libgeo VARCHAR
    , codgeo_pdc_ppdc_parent CHARACTER(6)
    , codgeo_ppdc_pdc_parent CHARACTER(6)
    , codgeo_dex_parent CHARACTER(6)
);

SELECT drop_all_functions_if_exists('fr', 'set_territory_laposte');
CREATE OR REPLACE FUNCTION fr.set_territory_laposte()
RETURNS BOOLEAN AS $$
BEGIN
    TRUNCATE TABLE public.territory_laposte;
    PERFORM public.drop_table_indexes('public', 'territory_laposte');

    INSERT INTO fr.territory_laposte (
        nivgeo
        , codgeo
        --, libgeo
        , codgeo_pdc_ppdc_parent
        , codgeo_ppdc_pdc_parent
        , codgeo_dex_parent
    )
    (
        WITH cp_has_site AS (
            SELECT
                ran.co_postal AS codgeo_postal
                , rao.co_roc_site AS codgeo_pdc_ppdc
                , COUNT(*) AS nb_adr_rao
            FROM fr.address_view AS ran
            INNER JOIN fr.laposte_delivery_address rao on rao.co_adr = ran.co_adr
            GROUP BY ran.co_postal, rao.co_roc_site
        )
        , cp_has_best_site AS (
            SELECT
                codgeo_postal
                , FIRST(codgeo_pdc_ppdc ORDER BY nb_adr_rao DESC) AS codgeo_pdc_ppdc_parent
            FROM cp_has_site
            GROUP BY codgeo_postal
        )
        , cp AS (
            SELECT
                cp_has_best_site.codgeo_postal
                , cp_has_best_site.codgeo_pdc_ppdc_parent
                , site_source_orga.code_regate_rattachement_eog AS codgeo_regate_pdc_ppdc_parent
                , site_source_orga.code_rattachement_eog AS codgeo_ppdc_pdc_parent
                , site_source_orga.code_animation_fonct_nationale AS codgeo_dex_parent
            FROM cp_has_best_site
            LEFT OUTER JOIN fr.laposte_organization AS site_source_orga
                ON site_source_orga.code = cp_has_best_site.codgeo_pdc_ppdc_parent
        )
        SELECT
            'CP' AS nivgeo
            , cp.codgeo_postal AS codgeo
            , cp.codgeo_pdc_ppdc_parent
            , cp.codgeo_ppdc_pdc_parent
            , cp.codgeo_dex_parent
        FROM cp
    );

    CREATE UNIQUE INDEX ON fr.territory_laposte (nivgeo, codgeo);

    --Mise à jour GEO, qui déclenchera un init GeoSupra le SUPRA n'existant pas, qui déclenchera l'updateGeoSupra spécifique
    PERFORM fr.set_territory_laposte_to_now();

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_territory_laposte_to_now');
CREATE OR REPLACE FUNCTION fr.set_territory_laposte_to_now()
RETURNS BOOLEAN
AS $$
BEGIN
    /*
    IF public.setTerritoireHasDataGeoToNow(
        in_table => 'territory_laposte'
        , in_set_geo_supra => TRUE
        , in_check_exists => FALSE
    ) THEN
    */
    IF fr.set_territory_supra(
        table_name => 'territory_laposte'
        , schema_name => 'fr'
        , base_level => 'CP'
    )
    THEN
        PERFORM fr.update_territory_laposte_supra();
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END $$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'update_territory_laposte_supra');
CREATE OR REPLACE FUNCTION fr.update_territory_laposte_supra()
RETURNS BOOLEAN AS $$
BEGIN
    --Codes Postaux : libellé = code
    UPDATE public.territory_laposte
    SET libgeo = codgeo
    WHERE nivgeo = 'CP';

    --Zones Postales : libellés SOURCE-ORGA (avec métier COURRIER, ELP) sinon sites manquants du RLP (réseau, enseigne)
    UPDATE public.territory_laposte
    SET libgeo =
        --On retire le mot "PARIS" qui est en préfixe du libellé de chaque DEX, sauf pour celle qui vraiment de Paris
        -- de même avec le mot "GENTILLY" en préfixe de la DEX OM (du métier ELP)
        CASE
            WHEN territory_laposte.nivgeo = 'DEX' AND source_orga.libelle LIKE 'PARIS DEX%'
                THEN source_orga.libelle
            ELSE
                REGEXP_REPLACE(source_orga.libelle, '^(PARIS|GENTILLY) ', '')
        END
    FROM fr.laposte_organization source_orga WHERE source_orga.code = territory_laposte.codgeo
    AND territory_laposte.nivgeo IN ('PDC_PPDC', 'PPDC_PDC', 'DEX');

    RETURN TRUE;
END $$ LANGUAGE plpgsql;
