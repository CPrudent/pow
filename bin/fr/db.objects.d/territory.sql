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
    , codgeo_voisins VARCHAR[] NULL
);

ALTER TABLE fr.territory SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'set_territory');
CREATE OR REPLACE FUNCTION fr.set_territory()
RETURNS BOOLEAN
AS $$
DECLARE
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
        -- pas nécessaire, on fait confiance à l'INSEE ? et pour garder l'indépendance avec la table territoire ?
        , in_check_exists => FALSE
    );
        */
    --On considère cette table de même à jour
    PERFORM public.set_table_metadata('public', 'territoire', CONCAT('{"dtrgeo":"', TO_CHAR(public.getDateMajCommuneToNow(), 'DD/MM/YYYY'), '"}'));
     */

    TRUNCATE TABLE fr.territory;
    PERFORM public.drop_table_indexes('fr', 'territory');

    /* NOTE
     Ajout des territoires du niveau le plus bas : COM_CP = croisement CP et COMMUNE, issu de RAN + RAO + INSEE + IGN
     CROISEMENT CP ET COMMUNE (une commune peut être sous-découpée en CP, et un CP peut être la composition de plusieurs communes entières ou non)
     */
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
        WITH za AS (
            SELECT
                co_cea AS codgeo
                , dt_reference AS dt_reference_geo
                , CONCAT_WS('-',
                    lb_l5_nn
                    , co_postal
                    , lb_ach_nn
                ) AS libgeo
                , co_postal
                , co_insee_commune
            FROM
                fr.laposte_zone_address
            WHERE
                fl_active
        )
        /* NOTE
         on met la valeur Z... là où la parenté n'est pas connue, mais où elle devrait toujours l'être. cela est aussi utile à la remontée de données où on ne souhaite pas exclure les données dont la parenté n'est pas connue
         */
        SELECT
            'ZA' AS nivgeo
            , za.codgeo
            , za.dt_reference_geo
            , CASE
                WHEN commune_ign.codgeo IS NOT NULL THEN
                    CONCAT(za.co_postal, ' ', REPLACE(REPLACE(commune_ign.libgeo, 'œ', 'oe'), 'Œ', 'Oe'))
                ELSE
                    za.libgeo
            END AS libgeo
            , za.co_insee_commune AS codgeo_com_parent
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
            , za.co_postal AS codgeo_cp_parent
            , territory_laposte.codgeo_pdc_ppdc_parent
            , territory_laposte.codgeo_ppdc_pdc_parent
            , territory_laposte.codgeo_dex_parent
        FROM za
            -- INSEE municipalities
            LEFT OUTER JOIN fr.insee_administrative_cutting_municipality_and_district
            AS commune_insee
            ON commune_insee.codgeo = za.co_insee_commune
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
            ON commune_ign.codgeo = za.co_insee_commune
            -- LAPOSTE territories
            LEFT OUTER JOIN fr.territory_laposte
            ON territory_laposte.nivgeo = 'CP' AND territory_laposte.codgeo = za.co_postal
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
                        , fr.get_department_code_from_municipality_code(za.co_insee_commune) --génère des départements fictifs pour les communes fictives (collectivités d'outre mer)
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

    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_codgeo_za ON fr.territory (codgeo) WHERE nivgeo = 'ZA';

    --utile ? CREATE UNIQUE INDEX IF NOT EXISTS idx_territoire_key_base ON fr.territory (codgeo) WHERE nivgeo = 'COM_CP';

    /* Vérification inutile car les sources étant à jour, le résultat croisé l'est aussi
    --Mise à jour GEO + init / maj SUPRA si nécessaire + UPDATE SUPRA si nécessaire
    PERFORM public.setTerritoireGeoToNow();
        * On fait donc un simple setGeoSupra = updateGeoSupra spécifique :
        */
    PERFORM fr.set_territory_supra(
        table_name => 'territory'
        , schema_name => 'fr'
        , base_level => 'ZA'
    );

    --PERFORM updateTerritoireGeoSupra();

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

/*
SELECT drop_all_functions_if_exists('public', 'setTerritoireGeoToNow');
CREATE OR REPLACE FUNCTION public.setTerritoireGeoToNow()
RETURNS BOOLEAN
AS $$
DECLARE
BEGIN
	IF public.setTerritoireHasDataGeoToNow(
		in_table => 'territoire'
		, in_nivgeo_base => 'COM_CP'
		, in_set_geo_supra => TRUE
		, in_check_exists => FALSE
	) THEN
		PERFORM public.updateTerritoireGeoSupra();
		--Recalcul du voisinage, là ou il est indéfini suite MAJ geo
		PERFORM public.updateTerritoireVoisins(in_null_only => TRUE);
		RETURN TRUE;
	END IF;
	RETURN FALSE;
END $$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'updateTerritoireGeoSupra');
CREATE OR REPLACE FUNCTION public.updateTerritoireGeoSupra()
RETURNS BOOLEAN
AS $$
DECLARE
	v_nivgeos VARCHAR[] := public.getAllNivgeos(in_order => 'ASC');
	v_nivgeo VARCHAR;
BEGIN
	--Population sur niveau COM
	IF column_exists('public', 'territoire_has_insee_histo', 'pmun') THEN
		RAISE NOTICE 'Population issue des series historiques INSEE';
		UPDATE fr.territory
		SET population = (
			SELECT insee_histo.pmun
			FROM fr.territory_has_insee_histo AS insee_histo
			WHERE insee_histo.pmun IS NOT NULL
			AND insee_histo.nivgeo = 'COM'
			AND insee_histo.codgeo = territoire.codgeo
			ORDER BY insee_histo.dt_reference_data DESC LIMIT 1
		)
		WHERE territoire.nivgeo = 'COM';
	ELSE
		RAISE NOTICE 'Population issue de ADMIN EXPRESS IGN';
		UPDATE fr.territory
		SET population = commune_ign.population
		FROM fr.territory_ign AS commune_ign
		WHERE commune_ign.nivgeo = 'COM'
		AND commune_ign.codgeo = territoire.codgeo
		AND territoire.nivgeo = 'COM';
	END IF;
	/* EXEMPLE de différence de population sur le département 59
	2603723 (ign)
	2605238 (insee p_pop15)
	2603723 (insee p_pop16)
	SELECT SUM("D68_POP"::INTEGER), SUM("P11_POP"::INTEGER), SUM("P16_POP"::INTEGER) FROM insee.serie_historique
	*/
	--Remontée supra COM
	PERFORM public.setTerritoireHasDataGeoSupra(
		in_table => 'territoire'
		, in_nivgeo_base => 'COM'
		, in_update_mode => TRUE
		, in_columns_agg => ARRAY['population']
	);

	/* inutile depuis qu'on ne calcule les parentés qu'entre niveaux qui en sont des sous découpages (cf territoire_has_data.sql, fonction setTerritoireHasDataGeoSupra)
	--Les DEX et régions sont équivalentes, il est inutile d'étendre de l'un vers l'autre et vice-versa, on supprime les parentés
	UPDATE fr.territory
	SET codgeo_reg_parent = NULL
		, codgeo_dec_parent = NULL
	WHERE nivgeo IN ('REG', 'DEC');
	*/

	--Pour les EPCI, on souhaite enregistrer en parenté le département et la région majoritaire
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
	FROM com_groupby_epci WHERE com_groupby_epci.codgeo = territoire.codgeo
	AND territoire.nivgeo = 'EPCI';

	RAISE NOTICE 'Libellés des territoires : COM, COM_GLOBALE_ARM';
	--COMMUNE et COMMUNE globale d'arrondissements : libellé IGN par défaut, sinon libellé RAN
	UPDATE fr.territory
	SET libgeo = COALESCE(
			territoire_ign.libgeo
			, (
				SELECT STRING_AGG(DISTINCT za.lb_acheminement, ' + ') AS libgeo
				FROM public.za_ran_ad_view AS za
				WHERE za.co_insee_commune = territoire.codgeo
			)
		)
	FROM fr.territory_ign
	WHERE territoire.nivgeo IN ('COM', 'COM_GLOBALE_ARM')
	AND territoire_ign.nivgeo = territoire.nivgeo
	AND territoire_ign.codgeo = territoire.codgeo;

	/* On préfère les EPCI de l'INSEE qui cette année 2020 sont plus rapidement à jour que ADMIN EXPRESS DE L'IGN (inconvénient : maj annuelle)
	 * VOIR EVENTUELLEMENT A PRENDRE GOUV_COLLECTIVITES_LOCALES : https://www.collectivites-locales.gouv.fr/liste-et-composition-des-epci-a-fiscalite-propre
	 * Attention structure légèrement différente pour les arrondissements / communes globales composées d'arrondissements (cf territoire_epci_compare.sql)
	--EPCI : libellé et type IGN
	UPDATE fr.territory
	SET libgeo = territoire_ign.libgeo
		, typgeo = territoire_ign.typgeo
	FROM fr.territory_ign
	WHERE territoire.nivgeo = 'EPCI'
	AND territoire_ign.nivgeo = territoire.nivgeo
	AND territoire_ign.codgeo = territoire.codgeo;
	*/
	RAISE NOTICE 'Libellés des territoires : EPCI';
	/* EPCI : libellé et type de collectivites-locales.gouv.fr
	WITH collectivites_locales_epci AS (
		SELECT DISTINCT
			codgeo_epci_parent AS codgeo
			, libgeo_epci_parent AS libgeo
			, typgeo_epci_parent AS typgeo
		FROM divers.collectivites_locales_gouv_epci_com
	)
	UPDATE fr.territory
	SET libgeo = collectivites_locales_epci.libgeo
		, typgeo = collectivites_locales_epci.typgeo
	FROM collectivites_locales_epci
	WHERE territoire.nivgeo = 'EPCI'
	AND territoire.codgeo = collectivites_locales_epci.codgeo;
	*/
	--EPCI : libellé et type de DGCL/BANATIC
	UPDATE fr.territory
	SET libgeo = banatic_liste_epci.nom_du_groupement
		, typgeo = banatic_liste_epci.nature_juridique
	FROM divers.banatic_liste_epci
	WHERE territoire.nivgeo = 'EPCI'
	AND territoire.codgeo = banatic_liste_epci.n_siren;

	RAISE NOTICE 'Libellés des territoires : ARR, CV, DEP, REG';
	--DEPARTEMENT, REGION : On préfère les libellés INSEE, car les libellés IGN sont en majuscules
	--ARRONDISSEMENTS, CANTON VILLE : Libellés INSEE, on n'a pas les libellés IGN
	UPDATE fr.territory
	SET libgeo = territoire_insee.libgeo
	FROM fr.territory_insee
	WHERE territoire.nivgeo IN ('ARR', 'CV', 'DEP', 'REG')
	AND territoire_insee.nivgeo = territoire.nivgeo
	AND territoire_insee.codgeo = territoire.codgeo;

	RAISE NOTICE 'Libellés des territoires : SUPRA CP';
	--Libellés des territoires POSTAUX
	UPDATE fr.territory
	SET libgeo = territory_laposte.libgeo
	FROM fr.territory_postal
	WHERE public.nivgeoIsSousDecoupageDeNivgeo('CP', territoire.nivgeo)
	AND territory_laposte.nivgeo = territoire.nivgeo
	AND territory_laposte.codgeo = territoire.codgeo;

	RAISE NOTICE 'Libellés des territoires : CP';
	--Codes Postaux : libellé = code et libellé des communes liées (entièrement ou partiellement)
	WITH libelle_cp AS (
		SELECT
			territoire_com_cp.codgeo_cp_parent AS codgeo
			, STRING_AGG(DISTINCT territoire_com.libgeo, ', ' ORDER BY territoire_com.libgeo) AS libgeo
		FROM territoire AS territoire_com
		INNER JOIN territoire AS territoire_com_cp
			ON territoire_com_cp.nivgeo = 'COM_CP'
			AND territoire_com_cp.codgeo_com_parent = territoire_com.codgeo
		WHERE territoire_com.nivgeo = 'COM'
		GROUP BY territoire_com_cp.codgeo_cp_parent
	)
	UPDATE fr.territory
	SET libgeo = libelle_cp.libgeo
	FROM libelle_cp
	WHERE territoire.nivgeo = 'CP'
	AND territoire.codgeo = libelle_cp.codgeo;

	--Ajout libellé DVE
	WITH libelle_dve AS (
		SELECT
			code_dve 	AS codgeo,
			libelle_dve	AS libgeo
		FROM divers.referentiel_dve_dr
	)

	UPDATE fr.territory
	SET libgeo = libelle_dve.libgeo
	FROM libelle_dve
	WHERE territoire.nivgeo = 'DVE'
	AND territoire.codgeo = libelle_dve.codgeo;

	UPDATE fr.territory
	SET libgeo = CASE CONCAT_WS(':', nivgeo, territoire.codgeo)
		WHEN 'METROPOLE_DOM_TOM:FRM' THEN 'France métropolitaine'
		WHEN 'METROPOLE_DOM_TOM:FRO' THEN 'France d''outre-mer'
		WHEN 'PAYS:FR' THEN 'France'
	END
	WHERE territoire.nivgeo IN ('METROPOLE_DOM_TOM', 'PAYS');

	RAISE NOTICE 'Calcul des index';
	FOREACH v_nivgeo IN ARRAY v_nivgeos
	LOOP
		EXECUTE CONCAT(
			'CREATE UNIQUE INDEX IF NOT EXISTS idx_territoire_codgeo_', v_nivgeo, ' ON fr.territory (codgeo) WHERE nivgeo = ''', v_nivgeo, ''''
		);
		--TODO : voir si ces indexes sont judicieux
		IF column_exists('public', 'territoire', CONCAT('codgeo_', v_nivgeo, '_parent')) THEN
			EXECUTE CONCAT(
				'CREATE INDEX IF NOT EXISTS idx_territoire_codgeo_', v_nivgeo, '_parent ON fr.territory (nivgeo, codgeo_', v_nivgeo, '_parent)'
			);
		END IF;
	END LOOP;

	CREATE UNIQUE INDEX IF NOT EXISTS idx_territoire_key ON fr.territory (nivgeo, codgeo);
	CREATE INDEX IF NOT EXISTS idx_territoire_nivgeo ON fr.territory (nivgeo);
	--index pour optimiser la recherche de territoire par nom approchant
	--TODO : à revoir avec utilisation trigramme ? CREATE INDEX IF NOT EXISTS idx_territoire_libgeo ON fr.territory USING gin(libgeo gin_trgm_ops);

	RETURN TRUE;
END $$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'updateTerritoireVoisins');
CREATE OR REPLACE FUNCTION public.updateTerritoireVoisins(in_null_only BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN
AS $$
DECLARE
	v_nb_rows_affected INTEGER;
BEGIN
	IF in_null_only = TRUE THEN
		WITH selection_initiale AS (
			SELECT nivgeo, codgeo, gm_contour
			FROM fr.territory
			WHERE codgeo_voisins IS NULL
			AND gm_contour IS NOT NULL
			AND nivgeo = ANY(public.getAllNivgeos()) --Pour éviter les niveaux de sauvegarde type "COM_A_XXXXXX"
		)
		, selection_etendue AS (
			SELECT DISTINCT UNNEST(ARRAY[voisin.codgeo, territoire.codgeo]) AS codgeo, voisin.nivgeo
			FROM selection_initiale AS territoire
			INNER JOIN fr.territory voisin
				ON voisin.nivgeo = territoire.nivgeo
				AND voisin.codgeo <> territoire.codgeo
				AND ST_Touches(voisin.gm_contour, territoire.gm_contour) = TRUE
		)
		UPDATE fr.territory
		SET codgeo_voisins = (
			SELECT ARRAY_AGG(voisin.codgeo)
			FROM fr.territory voisin
			WHERE voisin.nivgeo = territoire.nivgeo
			AND voisin.codgeo <> territoire.codgeo
			AND ST_Touches(voisin.gm_contour, territoire.gm_contour) = TRUE
		)
		FROM selection_etendue
		WHERE territoire.codgeo = selection_etendue.codgeo
		AND territoire.nivgeo = selection_etendue.nivgeo;
	ELSE
		UPDATE fr.territory
		SET codgeo_voisins = (
			SELECT ARRAY_AGG(voisin.codgeo)
			FROM fr.territory voisin
			WHERE voisin.nivgeo = territoire.nivgeo
			AND voisin.codgeo <> territoire.codgeo
			AND ST_Touches(voisin.gm_contour, territoire.gm_contour) = TRUE
		)
		WHERE nivgeo = ANY(public.getAllNivgeos()); --Pour éviter les niveaux de sauvegarde type "COM_A_XXXXXX";
	END IF;
	GET DIAGNOSTICS v_nb_rows_affected = ROW_COUNT;
	RETURN v_nb_rows_affected > 0;
END $$ LANGUAGE plpgsql;
 */

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
FROM territoire AS millesime_a
FULL OUTER JOIN territoire_insee AS millesime_b
	ON millesime_a.codgeo = millesime_b.codgeo
	AND millesime_a.nivgeo = millesime_b.nivgeo
FULL OUTER JOIN territoire_ign AS millesime_c
	ON millesime_a.codgeo = millesime_c.codgeo
	AND millesime_a.nivgeo = millesime_c.nivgeo
WHERE (millesime_a.codgeo IS NULL OR millesime_b.codgeo IS NULL OR millesime_c.codgeo IS NULL)
AND (millesime_b.codgeo IS NULL OR public.getEnvDepLimit() IS NULL OR public.getCodeInseeDepartementFromCodeInseeCommune(millesime_b.codgeo) = public.getEnvDepLimit())
AND (millesime_c.codgeo IS NULL OR public.getEnvDepLimit() IS NULL OR public.getCodeInseeDepartementFromCodeInseeCommune(millesime_c.codgeo) = public.getEnvDepLimit())
AND (millesime_a.codgeo IS NULL OR millesime_a.nivgeo NOT IN ('CV', 'CP', 'COM_CP', 'PPDC_PDC', 'DEC', 'METROPOLE_DOM_TOM', 'PAYS'))

*/
