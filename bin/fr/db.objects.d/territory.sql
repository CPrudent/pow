/***
 * FR-TERRITORY management
 */

CREATE TABLE IF NOT EXISTS fr.territory (
    nivgeo CHARACTER VARYING NOT NULL
    , codgeo CHARACTER VARYING NOT NULL
    , dt_reference_geo DATE /*NOT*/ NULL
    , libgeo CHARACTER VARYING
    , typgeo CHARACTER VARYING
    , population BIGINT
    , superficie BIGINT
    , gm_contour_natif GEOMETRY --Géographie native (non simplifiée, projetée dans un système local)
    , gm_contour GEOMETRY(MULTIPOLYGON, 4326) --Géographie simplifiée et reprojetée en 4326
    , codgeo_com_parent CHARACTER(5)
    , codgeo_com_globale_arm_parent CHARACTER(5)
    , codgeo_cv_parent CHARACTER VARYING
    , codgeo_arr_parent CHARACTER VARYING
    , codgeo_epci_parent CHARACTER VARYING
    , codgeo_dep_parent CHARACTER VARYING
    , codgeo_reg_parent CHARACTER VARYING
    , codgeo_metropole_dom_tom_parent CHARACTER(3) DEFAULT 'FRM'
    , codgeo_pays_parent CHARACTER(2) DEFAULT 'FR'
    , codgeo_cp_parent CHARACTER(5)
    , codgeo_pdc_ppdc_parent CHARACTER(6)
    , codgeo_ppdc_pdc_parent CHARACTER(6)
    , codgeo_dex_parent CHARACTER(6)
    , codgeo_voisins VARCHAR[]
);

ALTER TABLE fr.territory SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'set_territory');
CREATE OR REPLACE FUNCTION fr.set_territory(
    subsection VARCHAR DEFAULT 'ZA'
)
RETURNS BOOLEAN
AS $$
DECLARE
    _date_ign TIMESTAMP := (public.get_last_io(type_in => 'IGN_ADMINEXPRESS')).dt_data_end;
    _date_insee TIMESTAMP := (public.get_last_io(type_in => 'INSEE_DECOUPAGE_COMMUNAL')).dt_data_end;
    _date_ran TIMESTAMP := (public.get_last_io(type_in => 'LAPOSTE_ADDRESS')).dt_data_end;
    _query TEXT;
BEGIN
    /*
    --On vérifie que les sources sont à jour
    PERFORM public.setTerritoireIgnGeoToNow();
    PERFORM public.setTerritoireInseeGeoToNow();
    PERFORM public.setRanGeoToNow();
    /*
    PERFORM public.setTerritoireHasDataGeoToNow(
        in_table => 'territoire_has_insee'
        , in_set_geo_supra => TRUE
        -- pas nécessaire, on fait confiance à l'INSEE ? et pour garder l'indépendance avec la table territory ?
        , in_check_exists => FALSE
    );
        */
    --On considère cette table de même à jour
    PERFORM public.set_table_metadata('public', 'territory', CONCAT('{"dtrgeo":"', TO_CHAR(public.getDateMajCommuneToNow(), 'DD/MM/YYYY'), '"}'));
     */

    /*
    SELECT dt_fin_donnees INTO v_dtrgeo_source FROM historique_import WHERE co_type = 'IGN_ADMINEXPRESS' AND co_etat = 'SUCCES';
    SELECT TO_DATE(NULLIF(public.get_table_metadata('public', 'territoire_ign')->>'dtrgeo_source', ''), 'DD/MM/YYYY') INTO v_table_metadata_dtrgeo_source;
    SELECT TO_DATE(NULLIF(public.get_table_metadata('public', 'territoire_ign')->>'dtrgeo', ''), 'DD/MM/YYYY') INTO v_table_metadata_dtrgeo;
    IF v_dtrgeo_source IS NULL THEN
        RAISE NOTICE 'Territoires IGN non importés';
    ELSIF in_force = FALSE AND v_table_metadata_dtrgeo_source >= v_dtrgeo_source THEN
        RAISE NOTICE 'Recopie des territoires IGN importés inutile car pas plus récents (géo du %) que ceux intégrés (géo du % à jour au %)', v_dtrgeo_source, v_table_metadata_dtrgeo_source, v_table_metadata_dtrgeo;
    ELSE
        RAISE NOTICE 'Recopie des territoires IGN importés (géo du %)', v_dtrgeo_source;
     */

    CALL fr.check_municipality_subsection(
        subsection => subsection
        , check_territory => FALSE
    );

    CALL public.log_info('Calcul des Territoires (niveau de base: ' || subsection || ')');

    CALL public.log_info('Purge Données');
    TRUNCATE TABLE fr.territory;
    CALL public.log_info('Purge Index');
    PERFORM public.drop_table_indexes('fr', 'territory');

    /* NOTE
     Ajout des territoires du niveau le plus bas : COM_CP = croisement CP et COMMUNE, issu de RAN + RAO + INSEE + IGN
     croisement CP et COMMUNE :
     une commune peut être sous-découpée en CP, et un CP peut être la composition de plusieurs communes entières ou non
     */
    CALL public.log_info('Insertion Données');
    INSERT INTO fr.territory (
        nivgeo
        , codgeo
        , dt_reference_geo
        , libgeo
        , codgeo_com_parent
        , codgeo_com_globale_arm_parent
        , codgeo_arr_parent
        , codgeo_cv_parent
        , codgeo_epci_parent
        , codgeo_dep_parent
        , codgeo_reg_parent
        , codgeo_metropole_dom_tom_parent
        , codgeo_pays_parent
        , codgeo_cp_parent
        , codgeo_pdc_ppdc_parent
        , codgeo_ppdc_pdc_parent
        , codgeo_dex_parent
    )
    (
        WITH
        set_of_subsection AS (
            SELECT
                CONCAT_WS('-', za.co_insee_commune, za.co_postal) AS codgeo
                , MAX(za.dt_reference) AS dt_reference_geo
                , CONCAT(
                    FIRST(co_postal)
                    , ' ('
                    /* NOTE
                    L5/L6 are inverted for Polynésie & Nouvelle Calédonie (98)
                    */
                    , STRING_AGG(
                        DISTINCT CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_l5_nn ELSE lb_ach_nn END
                        , ', '
                        ORDER BY CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_l5_nn ELSE lb_ach_nn END)
                    , ')') AS libgeo
                , co_postal
                , co_insee_commune
            FROM fr.laposte_zone_address AS za
            WHERE
                subsection = 'COM_CP'
            GROUP BY co_postal, co_insee_commune

            UNION

            SELECT
                co_cea
                , dt_reference
                , CONCAT_WS('-'
                    , CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_ach_nn ELSE lb_l5_nn END
                    , co_postal
                    , CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_l5_nn ELSE lb_ach_nn END
                )
                , co_postal
                , co_insee_commune
            FROM fr.laposte_zone_address AS za
            WHERE
                subsection = 'ZA'
                AND
                fl_active
                -- exclude MONACO
                AND
                co_insee_commune !~ '^99'
        )

        /* NOTE
         on met la valeur Z... là où la parenté n'est pas connue, mais où elle devrait toujours l'être. cela est aussi utile à la remontée de données où on ne souhaite pas exclure les données dont la parenté n'est pas connue
         */
        SELECT
            subsection AS nivgeo
            , sub.codgeo
            , sub.dt_reference_geo
            , COALESCE(
                sub.libgeo
                , CASE
                    WHEN commune_ign.codgeo IS NOT NULL THEN
                        CONCAT(sub.co_postal, ' ', REPLACE(REPLACE(commune_ign.libgeo, 'œ', 'oe'), 'Œ', 'Oe'))
                END) AS libgeo
            , sub.co_insee_commune AS codgeo_com_parent
            , commune_insee.com AS codgeo_com_globale_arm_parent
            , COALESCE(
                commune_insee.arr
                , RPAD(dep_parent.codgeo, 4, 'Z') --arrondissement fictif dans le département pour les communes n'ayant pas d'arrondissement pour faciliter la remontée de données
                --, 'ZZZZ'
            ) AS codgeo_arr_parent
            , COALESCE(
                commune_insee.cv
                , RPAD(dep_parent.codgeo, 5, 'Z') --canton ville fictif dans le département pour les communes n'ayant pas d'arrondissement pour faciliter la remontée de données
                --, 'ZZZZZ'
            ) AS codgeo_cv_parent
            --EPCI DGCL BANATIC :
            , banatic_setof_epci.n_siren AS codgeo_epci_parent
            , /*COALESCE(*/dep_parent.codgeo/*, 'ZZZ')*/ AS codgeo_dep_parent
            , /*COALESCE(*/reg_parent.codgeo/*, 'ZZ')*/ AS codgeo_reg_parent
            , CASE
                WHEN LEFT(dep_parent.codgeo, 2) IN ('97', '98'/* MONACO , '99'*/) THEN 'FRO' --Note : DOM + autres RAN (98) + Monaco (99) (faut-il créer le code MCO ?)
                WHEN dep_parent.codgeo IS NOT NULL THEN 'FRM'
                /*ELSE 'ZZZ'*/
            END AS codgeo_metropole_dom_tom_parent
            , 'FR' AS codgeo_pays_parent
            , sub.co_postal AS codgeo_cp_parent
            , territory_laposte.codgeo_pdc_ppdc_parent
            , territory_laposte.codgeo_ppdc_pdc_parent
            , territory_laposte.codgeo_dex_parent
        FROM set_of_subsection sub
            -- INSEE municipalities
            LEFT OUTER JOIN fr.insee_administrative_cutting_municipality_and_district
            AS commune_insee
            ON commune_insee.codgeo = sub.co_insee_commune
            -- IGN municipalities
            LEFT OUTER JOIN (
                SELECT
                    insee_com AS codgeo
                    , nom AS libgeo
                    , insee_dep AS codgeo_dep_parent
                    , insee_reg AS codgeo_reg_parent
                FROM
                    fr.admin_express_commune
                WHERE
                    insee_com NOT IN ('75056', '13055', '69123')
                UNION
                SELECT
                    arm.insee_arm
                    , arm.nom
                    , com.insee_dep
                    , com.insee_reg
                FROM
                    fr.admin_express_arrondissement_municipal AS arm
                    INNER JOIN fr.admin_express_commune AS com
                    ON arm.insee_com = com.insee_com
            )
            AS commune_ign
            ON commune_ign.codgeo = sub.co_insee_commune
            -- LAPOSTE territories
            LEFT OUTER JOIN fr.territory_laposte
            ON territory_laposte.nivgeo = 'CP' AND territory_laposte.codgeo = sub.co_postal
            -- BANATIC EPCI
            LEFT OUTER JOIN fr.banatic_siren_insee
                /* NOTE
                 les arrondissements municipaux ne sont pas présents, il faut chercher l'EPCI de la commune globale de l'arrondissement municipal
                 */
            ON banatic_siren_insee.insee = COALESCE(commune_insee.com, commune_insee.codgeo)
            -- BANATIC EPCI (composition)
            LEFT OUTER JOIN fr.banatic_setof_epci
            ON banatic_siren_insee.siren = banatic_setof_epci.siren_membre
                AND banatic_setof_epci.nature_juridique IN ('MET69', 'CC', 'CA', 'METRO', 'CU')
            -- DEPARTMENT
            LEFT OUTER JOIN LATERAL (
                SELECT
                    COALESCE(
                        commune_ign.codgeo_dep_parent --source à priori la plus à jour
                        , commune_insee.dep --source alternative
                        , fr.get_department_code_from_municipality_code(sub.co_insee_commune) --génère des départements fictifs pour les communes fictives (collectivités d'outre mer)
                    ) AS codgeo
                    , CASE
                        WHEN commune_ign.codgeo_dep_parent IS NOT NULL THEN 'IGN'
                        WHEN commune_insee.dep IS NOT NULL THEN 'INSEE'
                        ELSE 'CALCUL'
                    END AS source
            )
            AS dep_parent
            ON TRUE
            -- REGION
            LEFT OUTER JOIN LATERAL (
                SELECT
                    COALESCE(
                        commune_ign.codgeo_reg_parent --source à priori la plus à jour
                        , commune_insee.reg --source alternative
                        , (
                            -- région IGN du département retenu
                            SELECT insee_reg
                            FROM fr.admin_express_departement
                            WHERE insee_dep = dep_parent.codgeo
                        )
                        -- c'est un département fictif, on créé une région fictive pour ce/ces départements (97/98/99)
                        , LEFT(dep_parent.codgeo, 2)
                    ) AS codgeo
                    , CASE
                        WHEN commune_ign.codgeo_reg_parent IS NOT NULL THEN 'IGN'
                        WHEN commune_insee.reg IS NOT NULL THEN 'INSEE'
                        ELSE 'CALCUL'
                    END AS source
            )
            AS reg_parent
            ON TRUE
    );

    CALL public.log_info('Création Index niveau de base');
    _query := CONCAT(
        'CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_codgeo_'
        , LOWER(subsection)
        , ' ON fr.territory (codgeo) WHERE nivgeo = '
        , quote_literal(subsection)
    );
    EXECUTE _query;

    -- initialize SUPRA levels
    PERFORM fr.set_territory_supra(
        table_name => 'territory'
        , schema_name => 'fr'
        , base_level => subsection
    );

    PERFORM fr.update_territory();

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

/*
SELECT drop_all_functions_if_exists('public', 'setTerritoireGeoToNow');
CREATE OR REPLACE FUNCTION public.setTerritoireGeoToNow()
RETURNS BOOLEAN
AS $$
BEGIN
    IF public.setTerritoireHasDataGeoToNow(
        in_table => 'territory'
        , in_nivgeo_base => 'COM_CP'
        , in_set_geo_supra => TRUE
        , in_check_exists => FALSE
    ) THEN
        PERFORM public.update_territory();
        --Recalcul du voisinage, là ou il est indéfini suite MAJ geo
        PERFORM public.update_territory_next(null_only => TRUE);
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END $$ LANGUAGE plpgsql;
 */

SELECT drop_all_functions_if_exists('fr', 'update_territory');
CREATE OR REPLACE FUNCTION fr.update_territory()
RETURNS BOOLEAN
AS $$
DECLARE
    _levels VARCHAR[] := fr.get_levels();
    _level VARCHAR;
BEGIN
    -- set population (COM level)
    IF column_exists('public', 'territoire_has_insee_histo', 'pmun') THEN
        RAISE NOTICE 'Population issue des séries historiques INSEE';
        UPDATE fr.territory
        SET population = (
            SELECT insee_histo.pmun
            FROM fr.territory_has_insee_histo AS insee_histo
            WHERE insee_histo.pmun IS NOT NULL
            AND insee_histo.nivgeo = 'COM'
            AND insee_histo.codgeo = territory.codgeo
            ORDER BY insee_histo.dt_reference_data DESC LIMIT 1
        )
        WHERE territory.nivgeo = 'COM';
    ELSE
        RAISE NOTICE 'Population issue de ADMIN-EXPRESS IGN';
        UPDATE fr.territory
        SET population = commune_ign.population
        FROM (
                SELECT
                    insee_com AS codgeo
                    , population
                FROM
                    fr.admin_express_commune
                WHERE
                    insee_com NOT IN ('75056', '13055', '69123')
                UNION
                SELECT
                    insee_arm
                    , population
                FROM
                    fr.admin_express_arrondissement_municipal
            )
            AS commune_ign
        WHERE commune_ign.codgeo = territory.codgeo AND territory.nivgeo = 'COM';
    END IF;
    -- set population (SUPRA levels)
    PERFORM fr.set_territory_supra(
        table_name => 'territory'
        , schema_name => 'fr'
        , base_level => 'COM'
        , update_mode => TRUE
        , columns_agg => ARRAY['population']
    );

    -- set link for EPCI level w/ majority DEP & REG levels
    RAISE NOTICE 'Calcul département/région majoriaire pour les EPCI';
    WITH com_groupby_epci AS (
        SELECT
            codgeo_epci_parent AS codgeo
            , (
                WITH dep_by_nb_com AS (
                    SELECT dep, COUNT(*) AS nb_com FROM UNNEST(ARRAY_AGG(codgeo_dep_parent)) AS dep GROUP BY dep
                )
                SELECT dep FROM dep_by_nb_com ORDER BY nb_com DESC LIMIT 1
            ) AS codgeo_dep_majoritaire
            , (
                WITH reg_by_nb_com AS (
                    SELECT reg, COUNT(*) AS nb_com FROM UNNEST(ARRAY_AGG(codgeo_reg_parent)) AS reg GROUP BY reg
                )
                SELECT reg FROM reg_by_nb_com ORDER BY nb_com DESC LIMIT 1
            ) AS codgeo_reg_majoritaire
        FROM fr.territory
        WHERE nivgeo = 'COM' AND codgeo_epci_parent IS NOT NULL
        GROUP BY codgeo_epci_parent
    )
    UPDATE fr.territory
    SET codgeo_dep_parent = com_groupby_epci.codgeo_dep_majoritaire
        , codgeo_reg_parent = com_groupby_epci.codgeo_reg_majoritaire
    FROM com_groupby_epci
    WHERE com_groupby_epci.codgeo = territory.codgeo AND territory.nivgeo = 'EPCI';

    -- set name (COM & COM_GLOBALE_ARM levels) from IGN ...
    RAISE NOTICE 'Libellés des territoires : COM, COM_GLOBALE_ARM';
    UPDATE fr.territory
    SET libgeo = commune_ign.libgeo
    FROM (
            SELECT
                'COM' AS nivgeo
                , insee_com AS codgeo
                , nom AS libgeo
            FROM
                fr.admin_express_commune
            WHERE
                insee_com NOT IN ('75056', '13055', '69123')
            UNION
            SELECT
                'COM' AS nivgeo
                , insee_arm
                , nom
            FROM
                fr.admin_express_arrondissement_municipal
            UNION
            SELECT
                'COM_GLOBALE_ARM' AS nivgeo
                , insee_com AS codgeo
                , nom AS libgeo
            FROM
                fr.admin_express_commune
            WHERE
                insee_com IN ('75056', '13055', '69123')
    ) AS commune_ign
    WHERE territory.nivgeo IN ('COM', 'COM_GLOBALE_ARM')
    AND commune_ign.nivgeo = territory.nivgeo
    AND commune_ign.codgeo = territory.codgeo;
    -- ... and from RAN (Polynésie française: 987* & Nouvelle Calédonie: 988*)
    UPDATE fr.territory
    SET libgeo = commune_ran.libgeo
    FROM (
        SELECT DISTINCT
            za.co_insee_commune codgeo
            , CASE
                WHEN lb_l5_nn IS NOT NULL THEN INITCAP(lb_l5_nn)
                ELSE INITCAP(lb_ach_nn)
            END libgeo
        FROM fr.laposte_zone_address AS za
        WHERE za.co_insee_commune ~ '^98'
    ) AS commune_ran
    WHERE territory.nivgeo IN ('COM')
    AND commune_ran.codgeo = territory.codgeo
    ;

    -- set name, type (EPCI level) from DGCL/BANATIC
    RAISE NOTICE 'Libellés des territoires : EPCI';
    UPDATE fr.territory
    SET libgeo = epci.nom_du_groupement
        , typgeo = epci.nature_juridique
    FROM fr.banatic_listof_epci epci
    WHERE territory.nivgeo = 'EPCI'
    AND territory.codgeo = epci.n_siren;

    -- set name (ARR & CV & DEP & REG levels) from INSEE
    RAISE NOTICE 'Libellés des territoires : ARR, CV, DEP, REG';
    UPDATE fr.territory
    SET libgeo = insee.libgeo
    FROM fr.insee_administrative_cutting_supra insee
    WHERE territory.nivgeo IN ('ARR', 'CV', 'DEP', 'REG')
    AND insee.nivgeo = territory.nivgeo
    AND insee.codgeo = territory.codgeo
    AND insee.millesime = (SELECT MAX(millesime) FROM fr.insee_administrative_cutting_supra);

    -- set name (postal levels) from LAPOSTE
    RAISE NOTICE 'Libellés des territoires : SUPRA CP';
    UPDATE fr.territory
    SET libgeo = territory_laposte.libgeo
    FROM fr.territory_laposte
    WHERE fr.is_level_below('CP', territory.nivgeo)
    AND territory_laposte.nivgeo = territory.nivgeo
    AND territory_laposte.codgeo = territory.codgeo;

    RAISE NOTICE 'Libellés des territoires : CP';
    WITH name_of_CP AS (
        SELECT
            za.codgeo_cp_parent AS codgeo
            , STRING_AGG(DISTINCT com.libgeo, ', ' ORDER BY com.libgeo) AS libgeo
        FROM fr.territory AS com
        INNER JOIN fr.territory AS za
            ON za.nivgeo = 'COM_CP'
            AND za.codgeo_com_parent = com.codgeo
        WHERE com.nivgeo = 'COM'
        GROUP BY za.codgeo_cp_parent
    )
    UPDATE fr.territory
    SET libgeo = name_of_CP.libgeo
    FROM name_of_CP
    WHERE territory.nivgeo = 'CP'
    AND territory.codgeo = name_of_CP.codgeo;

    -- set name (COUNTRY levels)
    UPDATE fr.territory
    SET libgeo = CASE CONCAT_WS(':', nivgeo, territory.codgeo)
        WHEN 'METROPOLE_DOM_TOM:FRM' THEN 'France métropolitaine'
        WHEN 'METROPOLE_DOM_TOM:FRO' THEN 'France d''outre-mer'
        WHEN 'PAYS:FR' THEN 'France'
    END
    WHERE territory.nivgeo IN ('METROPOLE_DOM_TOM', 'PAYS');

    RAISE NOTICE 'Calcul des index';
    FOREACH _level IN ARRAY _levels LOOP
        EXECUTE CONCAT(
            'CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_codgeo_', _level, ' ON fr.territory (codgeo) WHERE nivgeo = ''', _level, ''''
        );
        /*
        --TODO : voir si ces indexes sont judicieux
        IF column_exists('public', 'territory', CONCAT('codgeo_', _level, '_parent')) THEN
            EXECUTE CONCAT(
                'CREATE INDEX IF NOT EXISTS idx_territoire_codgeo_', _level, '_parent ON fr.territory (nivgeo, codgeo_', _level, '_parent)'
            );
        END IF;

         */
    END LOOP;

    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_nivgeo_codgeo ON fr.territory (nivgeo, codgeo);

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

-- eval next territories
SELECT drop_all_functions_if_exists('fr', 'update_territory_next');
CREATE OR REPLACE FUNCTION fr.update_territory_next(
    null_only BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN
AS $$
DECLARE
    _nrows_affected INTEGER;
BEGIN
    IF null_only THEN
        WITH initial_territory AS (
            SELECT nivgeo, codgeo, gm_contour
            FROM fr.territory
            -- only NULL ones
            WHERE codgeo_voisins IS NULL
            AND gm_contour IS NOT NULL
            -- to avoid backup-levels (as COM_A_XXXXXX)
            AND nivgeo = ANY(fr.get_all_levels())
        )
        , extend_territory AS (
            SELECT DISTINCT UNNEST(ARRAY[next_territory.codgeo, territory.codgeo]) AS codgeo, next_territory.nivgeo
            FROM initial_territory AS territory
            INNER JOIN fr.territory AS next_territory
                ON next_territory.nivgeo = territory.nivgeo
                AND next_territory.codgeo <> territory.codgeo
                AND ST_Touches(next_territory.gm_contour, territory.gm_contour)
        )
        UPDATE fr.territory
        SET codgeo_voisins = (
            SELECT ARRAY_AGG(next_territory.codgeo)
            FROM fr.territory next_territory
            WHERE next_territory.nivgeo = territory.nivgeo
            AND next_territory.codgeo <> territory.codgeo
            AND ST_Touches(next_territory.gm_contour, territory.gm_contour)
        )
        FROM extend_territory
        WHERE territory.codgeo = extend_territory.codgeo
        AND territory.nivgeo = extend_territory.nivgeo;
    ELSE
        UPDATE fr.territory
        SET codgeo_voisins = (
            SELECT ARRAY_AGG(next_territory.codgeo)
            FROM fr.territory AS next_territory
            WHERE next_territory.nivgeo = territory.nivgeo
            AND next_territory.codgeo <> territory.codgeo
            AND ST_Touches(next_territory.gm_contour, territory.gm_contour)
        )
        -- to avoid backup-levels (as COM_A_XXXXXX)
        WHERE nivgeo = ANY(fr.get_all_levels());
    END IF;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    RAISE NOTICE 'Calcul voisinage de territoires #%', _nrows_affected;
    RETURN _nrows_affected > 0;
END $$ LANGUAGE plpgsql;

/*
select * from (
    select row_number() over (partition by nivgeo order by codgeo) as rang_nivgeo, *
    from fr.territory
) as sr where rang_nivgeo = 1

select * from fr.territory where libgeo is null

select * from fr.territory
where (nivgeo = 'COM_CP' and (
    codgeo_com_parent is null
    OR codgeo_epci_parent is null
    OR codgeo_cv_parent is null
    OR codgeo_arr_parent is null
    OR codgeo_dep_parent is null
    OR codgeo_reg_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
    OR codgeo_cp_parent is null
    OR codgeo_ppdc_pdc_parent is null
    OR codgeo_dec_parent is null
))
OR (nivgeo = 'COM' and (
    codgeo_epci_parent is null
    OR codgeo_cv_parent is null
    OR codgeo_arr_parent is null
    OR codgeo_dep_parent is null
    OR codgeo_reg_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'EPCI' and (
    codgeo_dep_parent is null
    OR codgeo_reg_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'CV' and (
    codgeo_arr_parent is null
    OR codgeo_dep_parent is null
    OR codgeo_reg_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'ARR' and (
    codgeo_dep_parent is null
    OR codgeo_reg_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'DEP' and (
    codgeo_reg_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'REG' and (
    codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'CP' and (
    codgeo_ppdc_pdc_parent is null
    OR codgeo_dec_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'PPDC_PDC' and (
    codgeo_dec_parent is null
    OR codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))
OR (nivgeo = 'DEC' and (
    codgeo_metropole_dom_tom_parent is null
    OR codgeo_pays_parent is null
))

--Comparaison RAN - INSEE - IGN
SELECT
    COALESCE(millesime_a.nivgeo, millesime_b.nivgeo, millesime_c.nivgeo) AS nivgeo
    , COALESCE(millesime_a.codgeo, millesime_b.codgeo, millesime_c.codgeo) AS codgeo
    , COALESCE(millesime_a.libgeo, millesime_b.libgeo, millesime_c.libgeo) AS libgeo
    , CASE
        WHEN millesime_a.codgeo IS NOT NULL AND millesime_b.codgeo IS NOT NULL AND millesime_c.codgeo IS NOT NULL THEN 'RAN+INSEE+IGN'
        WHEN millesime_a.codgeo IS NOT NULL AND millesime_b.codgeo IS NOT NULL THEN 'RAN+INSEE'
        WHEN millesime_a.codgeo IS NOT NULL AND millesime_c.codgeo IS NOT NULL THEN 'RAN+IGN'
        WHEN millesime_a.codgeo IS NOT NULL THEN 'RAN'
        WHEN millesime_b.codgeo IS NOT NULL THEN 'INSEE'
        WHEN millesime_c.codgeo IS NOT NULL THEN 'IGN'
    END AS presence
FROM territory AS millesime_a
FULL OUTER JOIN territoire_insee AS millesime_b
    ON millesime_a.codgeo = millesime_b.codgeo
    AND millesime_a.nivgeo = millesime_b.nivgeo
FULL OUTER JOIN territoire_ign AS millesime_c
    ON millesime_a.codgeo = millesime_c.codgeo
    AND millesime_a.nivgeo = millesime_c.nivgeo
WHERE
    (millesime_a.codgeo IS NULL OR millesime_b.codgeo IS NULL OR millesime_c.codgeo IS NULL)
    AND (millesime_b.codgeo IS NULL OR public.getEnvDepLimit() IS NULL OR public.getCodeInseeDepartementFromCodeInseeCommune(millesime_b.codgeo) = public.getEnvDepLimit())
    AND (millesime_c.codgeo IS NULL OR public.getEnvDepLimit() IS NULL OR public.getCodeInseeDepartementFromCodeInseeCommune(millesime_c.codgeo) = public.getEnvDepLimit())
    AND (millesime_a.codgeo IS NULL OR millesime_a.nivgeo NOT IN ('CV', 'CP', 'COM_CP', 'PPDC_PDC', 'DEC', 'METROPOLE_DOM_TOM', 'PAYS'))
*/
