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
    , z_min INT
    , z_max INT
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

DO $$
BEGIN
    IF NOT column_exists('fr', 'territory', 'z_min') THEN
        ALTER TABLE fr.territory ADD COLUMN z_min INTEGER;
    END IF;
    IF NOT column_exists('fr', 'territory', 'z_max') THEN
        ALTER TABLE fr.territory ADD COLUMN z_max INTEGER;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_territory');
CREATE OR REPLACE FUNCTION fr.set_territory(
    io_infos HSTORE
    , municipality_subsection VARCHAR DEFAULT 'ZA'
)
RETURNS BOOLEAN
AS $$
DECLARE
    _query TEXT;
    _message_begin VARCHAR;
    _message_end VARCHAR;
    _key VARCHAR := 'FR-TERRITORY-GEOMETRY_t';
    _nrows INTEGER;
    _levels VARCHAR[] := public.get_levels('fr');
    _level VARCHAR;
BEGIN
    CALL fr.check_municipality_subsection(
        municipality_subsection => municipality_subsection
        , check_territory => FALSE
    );

    IF NOT io_infos ?& ARRAY['TODO', 'DEPENDS', _key] THEN
        RAISE 'argument IO semble erroné : %', io_infos;
    END IF;

    IF (io_infos -> _key)::BOOLEAN THEN
        _message_begin := 'Calcul';
    ELSE
        _message_begin := 'Mise à jour';
    END IF;
    _message_end := ' (niveau de base: ' || municipality_subsection || ')';
    CALL public.log_info(CONCAT_WS(' '
        , _message_begin
        , 'des Territoires'
        , _message_end
    ));

    CALL public.log_info('Purge Index');
    PERFORM public.drop_table_indexes('fr', 'territory');

    -- build all if necessary to calculate geometries
    IF (io_infos -> _key)::BOOLEAN THEN
        CALL public.log_info('Purge Données');
        TRUNCATE TABLE fr.territory;

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
                    municipality_subsection = 'COM_CP'
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
                    municipality_subsection = 'ZA'
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
                municipality_subsection AS nivgeo
                , subsection.codgeo
                , subsection.dt_reference_geo
                , COALESCE(
                    subsection.libgeo
                    , CASE
                        WHEN commune_ign.codgeo IS NOT NULL THEN
                            CONCAT(subsection.co_postal, ' ', REPLACE(REPLACE(commune_ign.libgeo, 'œ', 'oe'), 'Œ', 'Oe'))
                    END) AS libgeo
                , subsection.co_insee_commune AS codgeo_com_parent
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
                --EPCI DGCL BANATIC
                /* NOTE
                Seules quatre communes ne sont pas membres d’un EPCI à fiscalité propre. Il s'agit des quatre îles mono-communales qui bénéficient d'une dérogation :
                29083 Île-de-Sein
                29155 Ouessant
                22016 Île-de-Bréhat
                85113 L'Île-d'Yeu
                */
                , banatic_setof_epci.n_siren AS codgeo_epci_parent
                , /*COALESCE(*/dep_parent.codgeo/*, 'ZZZ')*/ AS codgeo_dep_parent
                , /*COALESCE(*/reg_parent.codgeo/*, 'ZZ')*/ AS codgeo_reg_parent
                , CASE
                    WHEN LEFT(dep_parent.codgeo, 2) IN ('97', '98'/* MONACO , '99'*/) THEN 'FRO' --Note : DOM + autres RAN (98) + Monaco (99) (faut-il créer le code MCO ?)
                    WHEN dep_parent.codgeo IS NOT NULL THEN 'FRM'
                    /*ELSE 'ZZZ'*/
                END AS codgeo_metropole_dom_tom_parent
                , 'FR' AS codgeo_pays_parent
                , subsection.co_postal AS codgeo_cp_parent
                , territory_laposte.codgeo_pdc_ppdc_parent
                , territory_laposte.codgeo_ppdc_pdc_parent
                , territory_laposte.codgeo_dex_parent
            FROM set_of_subsection subsection
                -- INSEE municipalities
                LEFT OUTER JOIN fr.insee_administrative_cutting_municipality_and_district
                AS commune_insee
                ON commune_insee.codgeo = subsection.co_insee_commune
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
                ON commune_ign.codgeo = subsection.co_insee_commune
                -- LAPOSTE territories
                LEFT OUTER JOIN fr.territory_laposte
                ON territory_laposte.nivgeo = 'CP' AND territory_laposte.codgeo = subsection.co_postal
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
                            , fr.get_department_code_from_municipality_code(subsection.co_insee_commune) --génère des départements fictifs pour les communes fictives (collectivités d'outre mer)
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
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        RAISE NOTICE 'LAPOSTE: insertion #% infra-commune(s)', _nrows;
    ELSE
        -- update base level, delete others
        DELETE FROM fr.territory
            WHERE nivgeo != municipality_subsection
            ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info('Purge Données (#' || _nrows || ') autres que ' || municipality_subsection);

        -- LAPOSTE : name, SUPRA COM/CP
        IF (io_infos -> 'FR-TERRITORY-LAPOSTE-AREA_t')::BOOLEAN THEN
            UPDATE fr.territory t SET
                dt_reference_geo = TIMEOFDAY()::DATE
                -- TODO: for ZA only (modify if COM_CP)
                , libgeo = CONCAT_WS('-'
                        , CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_ach_nn ELSE lb_l5_nn END
                        , co_postal
                        , CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_l5_nn ELSE lb_ach_nn END
                    )
                , codgeo_com_parent = area.co_insee_commune
                , codgeo_cp_parent = area.co_postal
            FROM fr.laposte_zone_address area
            WHERE
            (
                t.nivgeo = municipality_subsection
                AND
                area.co_cea = t.codgeo
            )
            AND
            (
                t.libgeo IS DISTINCT FROM
                    CONCAT_WS('-'
                        , CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_ach_nn ELSE lb_l5_nn END
                        , co_postal
                        , CASE WHEN co_insee_commune ~ '^98[78]' THEN lb_l5_nn ELSE lb_ach_nn END
                    )
                OR
                t.codgeo_com_parent IS DISTINCT FROM area.co_insee_commune
                OR
                t.codgeo_cp_parent IS DISTINCT FROM area.co_postal
            )
            ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            CALL public.log_info('LAPOSTE: mise à jour #' || _nrows || ' libellé(s), lien(s) COM/CP');
        END IF;

        -- LAPOSTE : SUPRA CP
        IF (io_infos -> 'FR-TERRITORY-LAPOSTE-SUPRA_t')::BOOLEAN THEN
            UPDATE fr.territory t SET
                dt_reference_geo = TIMEOFDAY()::DATE
                , codgeo_pdc_ppdc_parent = laposte.codgeo_pdc_ppdc_parent
                , codgeo_ppdc_pdc_parent = laposte.codgeo_ppdc_pdc_parent
                , codgeo_dex_parent = laposte.codgeo_dex_parent
            FROM fr.territory_laposte laposte
            WHERE
            (
                t.nivgeo = municipality_subsection
                AND
                laposte.nivgeo = 'CP'
                AND
                laposte.codgeo = t.codgeo_cp_parent
            )
            AND
            (
                t.codgeo_pdc_ppdc_parent IS DISTINCT FROM laposte.codgeo_pdc_ppdc_parent
                OR
                t.codgeo_ppdc_pdc_parent IS DISTINCT FROM laposte.codgeo_ppdc_pdc_parent
                OR
                t.codgeo_dex_parent IS DISTINCT FROM laposte.codgeo_dex_parent
            )
            ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            CALL public.log_info('LAPOSTE: mise à jour #' || _nrows || ' lien(s) SUPRA CP');
        END IF;

        -- INSEE : SUPRA
        IF (io_infos -> 'FR-TERRITORY-INSEE-SUPRA_t')::BOOLEAN THEN
            UPDATE fr.territory t SET
                dt_reference_geo = TIMEOFDAY()::DATE
                , codgeo_cv_parent = insee.cv
                , codgeo_arr_parent = insee.arr
                , codgeo_dep_parent = insee.dep
                , codgeo_reg_parent = insee.reg
            FROM fr.insee_administrative_cutting_municipality_and_district insee
            WHERE
            (
                t.nivgeo = municipality_subsection
                AND t.codgeo_com_parent = insee.codgeo
                AND insee.millesime = (
                    SELECT MAX(millesime) FROM fr.insee_administrative_cutting_municipality_and_district
                )
            )
            AND
            (
                t.codgeo_cv_parent IS DISTINCT FROM insee.cv
                OR
                t.codgeo_arr_parent IS DISTINCT FROM insee.arr
                OR
                t.codgeo_dep_parent IS DISTINCT FROM insee.dep
                OR
                t.codgeo_reg_parent IS DISTINCT FROM insee.reg
            )
            ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            CALL public.log_info('INSEE: mise à jour #' || _nrows || ' lien(s) SUPRA');
        END IF;

        -- BANATIC : SUPRA
        IF (io_infos -> 'FR-TERRITORY-BANATIC-SUPRA_t')::BOOLEAN THEN
            UPDATE fr.territory t SET
                dt_reference_geo = TIMEOFDAY()::DATE
                , codgeo_epci_parent = l.siren
            FROM
                fr.banatic_setof_epci s
                    JOIN fr.banatic_siren_insee l ON s.siren_membre = l.siren
            WHERE
            (
                t.nivgeo = municipality_subsection
                AND t.codgeo_com_parent = l.insee
            )
            AND
            (
                t.codgeo_epci_parent IS DISTINCT FROM l.siren
            )
            ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            CALL public.log_info('BANATIC: mise à jour #' || _nrows || ' lien(s) EPCI');
        END IF;
    END IF;

    CALL public.log_info('Création Index (niveau: ' || municipality_subsection || ')');
    _query := CONCAT(
        'CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_codgeo_'
        , LOWER(municipality_subsection)
        , ' ON fr.territory (codgeo) WHERE nivgeo = '
        , quote_literal(municipality_subsection)
    );
    EXECUTE _query;

    -- initialize SUPRA levels
    PERFORM fr.set_territory_supra(
        table_name => 'territory'
        , schema_name => 'fr'
        , base_level => municipality_subsection
    );

    -- update name, population, ...
    PERFORM fr.update_territory();

    FOREACH _level IN ARRAY _levels LOOP
        CALL public.log_info('Création Index (niveau: ' || _level || ')');
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

    CALL public.log_info('Création Index (niveau, code)');
    CREATE UNIQUE INDEX IF NOT EXISTS iux_territory_nivgeo_codgeo ON fr.territory (nivgeo, codgeo);

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'update_territory');
CREATE OR REPLACE FUNCTION fr.update_territory()
RETURNS BOOLEAN
AS $$
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
    WHERE public.is_level_below('fr', 'CP', territory.nivgeo)
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

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

-- eval next territories
SELECT drop_all_functions_if_exists('fr', 'set_territory_next');
CREATE OR REPLACE FUNCTION fr.set_territory_next(
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
            AND nivgeo = ANY(public.get_levels('fr'))
        )
        , extend_territory AS (
            SELECT DISTINCT
                UNNEST(ARRAY[next_territory.codgeo, territory.codgeo]) AS codgeo
                , next_territory.nivgeo
            FROM initial_territory AS territory
            INNER JOIN fr.territory AS next_territory
                ON next_territory.nivgeo = territory.nivgeo
                AND next_territory.codgeo <> territory.codgeo
                --AND ST_Touches(next_territory.gm_contour, territory.gm_contour)
                AND ST_Intersects(territory.gm_contour, next_territory.gm_contour)
                -- for 2 next polygonal geometries: dim[boundary(a) ∩ boundary(b)] = 1
                AND ST_Relate(territory.gm_contour, next_territory.gm_contour, '****1****')
        )
        UPDATE fr.territory
        SET codgeo_voisins = (
            SELECT ARRAY_AGG(next_territory.codgeo)
            FROM fr.territory next_territory
            WHERE next_territory.nivgeo = territory.nivgeo
            AND next_territory.codgeo <> territory.codgeo
            --AND ST_Touches(next_territory.gm_contour, territory.gm_contour)
            AND ST_Intersects(territory.gm_contour, next_territory.gm_contour)
            -- for 2 next polygonal geometries: dim[boundary(a) ∩ boundary(b)] = 1
            AND ST_Relate(territory.gm_contour, next_territory.gm_contour, '****1****')        )
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
            --AND ST_Touches(next_territory.gm_contour, territory.gm_contour)
            AND ST_Intersects(territory.gm_contour, next_territory.gm_contour)
            -- for 2 next polygonal geometries: dim[boundary(a) ∩ boundary(b)] = 1
            AND ST_Relate(territory.gm_contour, next_territory.gm_contour, '****1****')
        )
        -- to avoid backup-levels (as COM_A_XXXXXX)
        WHERE nivgeo = ANY(public.get_levels('fr'));
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

-- push properties of territory (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_territory_properties_to_public');
CREATE OR REPLACE PROCEDURE fr.push_territory_properties_to_public(
    force BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
BEGIN
    CALL public.log_info('Préparation des changements (valeurs)');
    DROP TABLE IF EXISTS tmp_fr_territory_changes;
    CREATE TEMPORARY TABLE tmp_fr_territory_changes AS (
        WITH
        territory_public AS (
            SELECT
                level
                , code
                , name
                , attributs
                , population
                , area
                , z_min
                , z_max
                , codes_adjoining
                , geom_native
                , geom_world
            FROM public.territory
            WHERE country = 'FR'
        )
        , territory_fr AS (
            SELECT
                nivgeo
                , codgeo
                , libgeo
                , CASE
                    WHEN nivgeo = 'EPCI' AND typgeo IS NOT NULL THEN (CONCAT('"TYPE" => "', typgeo, '"'))::HSTORE
                    WHEN nivgeo = 'COM' AND z.l6_norm IS NOT NULL THEN (CONCAT('"L6_NORM" => "', z.l6_norm, '"'))::HSTORE
                    WHEN nivgeo = 'ZA' AND y.l5_norm IS NOT NULL THEN (CONCAT('"L5_NORM" => "', y.l5_norm, '"'))::HSTORE
                END attributs
                , population
                , superficie
                , z_min
                , z_max
                , codgeo_voisins
                , gm_contour_natif
                , gm_contour
            FROM fr.territory t
                LEFT OUTER JOIN LATERAL (
                    SELECT DISTINCT
                        co_insee_commune
                        , CASE WHEN co_insee_commune ~ '^98' THEN lb_l5_nn ELSE lb_ach_nn END l6_norm
                    FROM fr.laposte_zone_address
                    WHERE
                        -- avoid duplicate code !
                        ((co_insee_commune ~ '^98') AND (lb_l5_nn IS NOT NULL))
                        OR
                        (co_insee_commune !~ '^98')
                ) z ON t.nivgeo = 'COM' AND t.codgeo = z.co_insee_commune
                LEFT OUTER JOIN LATERAL (
                    SELECT DISTINCT
                        co_cea
                        , CASE
                            WHEN co_insee_commune ~ '^98' AND lb_ach_nn IS NOT NULL THEN lb_ach_nn
                            WHEN co_insee_commune !~ '^98' AND lb_l5_nn IS NOT NULL THEN lb_l5_nn
                        END l5_norm
                    FROM fr.laposte_zone_address
                ) y ON t.nivgeo = 'ZA' AND t.codgeo = y.co_cea
            WHERE
                -- exclude MONACO !
                codgeo !~ '^99'
        )
        , changes AS (
            (
                SELECT '-' change, level, code FROM territory_public
                EXCEPT
                SELECT '-', nivgeo, codgeo FROM territory_fr
            )
            UNION
            (
                SELECT '+', nivgeo, codgeo FROM territory_fr
                EXCEPT
                SELECT '+', level, code FROM territory_public
            )
            UNION
            SELECT '!', territory_public.level, territory_public.code
            FROM territory_public
                JOIN territory_fr ON (territory_public.level, territory_public.code) = (territory_fr.nivgeo, territory_fr.codgeo)
            WHERE
                (territory_public.name IS DISTINCT FROM territory_fr.libgeo)
                OR
                ((territory_public.level = 'EPCI') AND (
                    ((territory_public.attributs ? 'TYPE') AND NOT (territory_fr.attributs ? 'TYPE'))
                    OR
                    (NOT (territory_public.attributs ? 'TYPE') AND (territory_fr.attributs ? 'TYPE'))
                    OR
                    ((territory_public.attributs ? 'TYPE') AND (territory_fr.attributs ? 'TYPE') AND (territory_public.attributs -> 'TYPE' IS DISTINCT FROM territory_fr.attributs -> 'TYPE'))
                    )
                )
                OR
                ((territory_public.level = 'COM') AND (
                    ((territory_public.attributs ? 'L6_NORM') AND NOT (territory_fr.attributs ? 'L6_NORM'))
                    OR
                    (NOT (territory_public.attributs ? 'L6_NORM') AND (territory_fr.attributs ? 'L6_NORM'))
                    OR
                    ((territory_public.attributs ? 'L6_NORM') AND (territory_fr.attributs ? 'L6_NORM') AND (territory_public.attributs -> 'L6_NORM' IS DISTINCT FROM territory_fr.attributs -> 'L6_NORM'))
                    )
                )
                OR
                ((territory_public.level = 'ZA') AND (
                    ((territory_public.attributs ? 'L5_NORM') AND NOT (territory_fr.attributs ? 'L5_NORM'))
                    OR
                    (NOT (territory_public.attributs ? 'L5_NORM') AND (territory_fr.attributs ? 'L5_NORM'))
                    OR
                    ((territory_public.attributs ? 'L5_NORM') AND (territory_fr.attributs ? 'L5_NORM') AND (territory_public.attributs -> 'L5_NORM' IS DISTINCT FROM territory_fr.attributs -> 'L5_NORM'))
                    )
                )
                OR
                (territory_public.population IS DISTINCT FROM territory_fr.population)
                OR
                (territory_public.area IS DISTINCT FROM territory_fr.superficie)
                OR
                (territory_public.z_min IS DISTINCT FROM territory_fr.z_min)
                OR
                (territory_public.z_max IS DISTINCT FROM territory_fr.z_max)
                OR
                (CARDINALITY(territory_public.codes_adjoining) != CARDINALITY(territory_fr.codgeo_voisins))
                OR
                (territory_public.codes_adjoining IS DISTINCT FROM territory_fr.codgeo_voisins)
                OR
                (NOT ST_Equals(territory_public.geom_native, territory_fr.gm_contour_natif))
                OR
                (NOT ST_Equals(territory_public.geom_world, territory_fr.gm_contour))
        )

        -- insert/update territories
        SELECT
            c.change
            , c.level
            , c.code
            , territory_fr.libgeo name
            , attributs
            , population
            , superficie area
            , z_min
            , z_max
            , codgeo_voisins codes_adjoining
            , gm_contour_natif geom_native
            , gm_contour geom_world
        FROM
            changes c
                JOIN territory_fr ON (c.level, c.code) = (territory_fr.nivgeo, territory_fr.codgeo)
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete old territories
        SELECT
            c.change
            , c.level
            , c.code
            , tp.name
            , tp.attributs
            , tp.population
            , tp.area
            , tp.z_min
            , tp.z_max
            , tp.codes_adjoining
            , tp.geom_native
            , tp.geom_world
        FROM
            changes c
                JOIN territory_public tp ON (c.level, c.code) = (tp.level, tp.code)
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('CHANGE: ', _nrows_affected));

    CALL public.log_info('Historique des modifications/suppressions (valeurs)');
    INSERT INTO public.territory_history (
            id_territory
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            t.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'VALUE'
            , ROW_TO_JSON(t.*)::JSONB
        FROM
            tmp_fr_territory_changes c
                JOIN public.territory t ON t.country = 'FR' AND (c.level, c.code) = (t.level, t.code)
        WHERE c.change = '!'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('UPDATE: ', _nrows_affected));
    INSERT INTO public.territory_history (
            id_territory
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            t.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'VALUE'
            , ROW_TO_JSON(t.*)::JSONB
        FROM
            tmp_fr_territory_changes c
                JOIN public.territory t ON t.country = 'FR' AND (c.level, c.code) = (t.level, t.code)
        WHERE c.change = '-'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));

    CALL public.log_info('Mise à jour des ajouts/modifications');
    INSERT INTO public.territory (
            country
            , level
            , code
            , name
            , attributs
            , population
            , area
            , z_min
            , z_max
            , codes_adjoining
            , date_last
            , geom_native
            , geom_world
        )
        SELECT
            'FR' country
            , c.level
            , c.code
            , c.name
            , c.attributs
            , c.population
            , c.area
            , c.z_min
            , c.z_max
            , c.codes_adjoining
            , TIMEOFDAY()::DATE
            , c.geom_native
            , c.geom_world
        FROM
            tmp_fr_territory_changes c
                JOIN public.territory_level l ON l.country = 'FR' AND c.level = l.level
        WHERE
            c.change = ANY('{+,!}')
        ORDER BY
            l.hierarchy
    ON CONFLICT(country, level, code) DO UPDATE
        SET
            name = EXCLUDED.name
            , attributs = EXCLUDED.attributs
            , population = EXCLUDED.population
            , area = EXCLUDED.area
            , z_min = EXCLUDED.z_min
            , z_max = EXCLUDED.z_max
            , codes_adjoining = EXCLUDED.codes_adjoining
            , date_last = EXCLUDED.date_last
            , geom_native = EXCLUDED.geom_native
            , geom_world = EXCLUDED.geom_world
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('INSERT/UPDATE: ', _nrows_affected));

    CALL public.log_info('Mise à jour des suppressions');
    DELETE FROM public.territory t
    USING tmp_fr_territory_changes c
    WHERE
        c.change = '-'
        AND
        t.country = 'FR'
        AND
        (t.level, t.code) = (c.level, c.code)

    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));
END
$proc$ LANGUAGE plpgsql;

-- push links of territory (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_territory_links_to_public');
CREATE OR REPLACE PROCEDURE fr.push_territory_links_to_public(
    force BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
    _change RECORD;
BEGIN
    CALL public.log_info('Préparation des changements (liens)');
    DROP TABLE IF EXISTS tmp_fr_territory_changes;
    CREATE TEMPORARY TABLE tmp_fr_territory_changes AS (
        WITH
        territory_public AS (
            SELECT
                t.level
                , t.code
                , tp.level level_parent
                , tp.code code_parent
            FROM public.territory t
                JOIN public.territory_parent p ON t.id = p.id_territory
                JOIN public.territory tp ON tp.id = p.id_parent
            WHERE
                t.country = 'FR'
        )
        , territory_fr AS (
            -- ZA/COM
            SELECT
                nivgeo
                , codgeo
                , 'COM' nivgeo_parent
                , codgeo_com_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'ZA'
                AND
                codgeo_com_parent IS NOT NULL

            UNION

            -- COM/COM_GLOBALE_ARM
            SELECT
                nivgeo
                , codgeo
                , 'COM_GLOBALE_ARM' nivgeo_parent
                , codgeo_com_globale_arm_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'COM'
                AND
                codgeo_com_globale_arm_parent IS NOT NULL

            UNION

            -- COM/ARR
            SELECT
                nivgeo
                , codgeo
                , 'ARR' nivgeo_parent
                , codgeo_arr_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'COM'
                AND
                codgeo_arr_parent IS NOT NULL

            UNION

            -- COM/CV
            SELECT
                nivgeo
                , codgeo
                , 'CV' nivgeo_parent
                , codgeo_cv_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'COM'
                AND
                codgeo_cv_parent IS NOT NULL

            UNION

            -- COM/EPCI
            SELECT
                nivgeo
                , codgeo
                , 'EPCI' nivgeo_parent
                , codgeo_epci_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'COM'
                AND
                codgeo_epci_parent IS NOT NULL

            UNION

            -- ARR/DEP
            SELECT
                nivgeo
                , codgeo
                , 'DEP' nivgeo_parent
                , codgeo_dep_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'ARR'
                AND
                codgeo_dep_parent IS NOT NULL

            UNION

            -- CV/DEP
            SELECT
                nivgeo
                , codgeo
                , 'DEP' nivgeo_parent
                , codgeo_dep_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'CV'
                AND
                codgeo_dep_parent IS NOT NULL

            UNION

            -- DEP/REG
            SELECT
                nivgeo
                , codgeo
                , 'REG' nivgeo_parent
                , codgeo_reg_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'DEP'
                AND
                codgeo_reg_parent IS NOT NULL

            UNION

            -- REG/METROPOLE_DOM_TOM
            SELECT DISTINCT
                'REG' nivgeo
                , codgeo_reg_parent codgeo
                , 'METROPOLE_DOM_TOM' nivgeo_parent
                , codgeo_metropole_dom_tom_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'COM'
                AND
                codgeo_metropole_dom_tom_parent IS NOT NULL

            UNION

            --METROPOLE_DOM_TOM/PAYS
            SELECT
                nivgeo
                , codgeo
                , 'PAYS' nivgeo_parent
                , codgeo_pays_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'METROPOLE_DOM_TOM'
                AND
                codgeo_pays_parent IS NOT NULL

            UNION

            -- ZA/CP
            SELECT
                nivgeo
                , codgeo
                , 'CP' nivgeo_parent
                , codgeo_cp_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'ZA'
                AND
                codgeo_cp_parent IS NOT NULL

            UNION

            -- CP/PDC_PPDC
            SELECT
                nivgeo
                , codgeo
                , 'PDC_PPDC' nivgeo_parent
                , codgeo_pdc_ppdc_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'CP'
                AND
                codgeo_pdc_ppdc_parent IS NOT NULL

            UNION

            -- PDC_PPDC/PPDC_PDC
            SELECT
                nivgeo
                , codgeo
                , 'PPDC_PDC' nivgeo_parent
                , codgeo_ppdc_pdc_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'PDC_PPDC'
                AND
                codgeo_ppdc_pdc_parent IS NOT NULL

            UNION

            -- PPDC_PDC/DEX
            SELECT
                nivgeo
                , codgeo
                , 'DEX' nivgeo_parent
                , codgeo_dex_parent codgeo_parent
            FROM
                fr.territory t
            WHERE
                nivgeo = 'PPDC_PDC'
                AND
                codgeo_dex_parent IS NOT NULL
        )
        , changes AS (
            (
                SELECT '-' change, level, code, level_parent, code_parent FROM territory_public
                EXCEPT
                SELECT '-', nivgeo, codgeo, nivgeo_parent, codgeo_parent FROM territory_fr
            )
            UNION
            (
                SELECT '+', nivgeo, codgeo, nivgeo_parent, codgeo_parent FROM territory_fr
                EXCEPT
                SELECT '+', level, code, level_parent, code_parent FROM territory_public
            )
        )
        SELECT ROW_NUMBER() OVER () id, * FROM changes
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('CHANGE: ', _nrows_affected));

    /*
     * no good check: merge of municipalities deletes some (w/o new creation)
     *
    CALL public.log_info('Contrôle des changements (liens)');
    FOR _change IN (
        WITH
        no_valid_change AS (
            SELECT c.*
            FROM tmp_fr_territory_changes c
                JOIN public.territory t ON t.country = 'FR' AND (c.level, c.code) = (t.level, t.code)
                JOIN public.territory_parent p ON t.id = p.id_territory
                JOIN public.territory tp ON p.id_parent = tp.id
            WHERE
                NOT EXISTS(
                    SELECT 1
                    FROM tmp_fr_territory_changes c2
                    WHERE
                        c2.change = CASE WHEN c.change = '+' THEN '-' ELSE '+' END
                        AND
                        (c2.level, c2.code, c2.level_parent) = (c.level, c.code, c.level_parent)
                        AND
                        c2.code_parent IS DISTINCT FROM c.code_parent
                )
        )
        SELECT * FROM no_valid_change ORDER BY 2, 3, 4
    )
    LOOP
        RAISE NOTICE 'ERREUR changement lien : %', _change;
        DELETE FROM tmp_fr_territory_changes
        WHERE
            change = _change.change
            AND
            level = _change.level
            AND
            code = _change.code
            AND
            level_parent = _change.level_parent
            AND
            code_parent = _change.code_parent
            ;
    END LOOP;
     */

    CALL public.log_info('Mise à jour des changements (liens) : ajout');
    INSERT INTO public.territory_parent
        WITH
        from_territory AS (
            SELECT
                c.id
                , t.id id_territory
            FROM tmp_fr_territory_changes c
                JOIN public.territory t ON t.country = 'FR' AND (c.level, c.code) = (t.level, t.code)
            WHERE change = '+'
        )
        , to_territory AS (
            SELECT
                c.id
                , t.id id_parent
            FROM tmp_fr_territory_changes c
                JOIN public.territory t ON t.country = 'FR' AND (c.level_parent, c.code_parent) = (t.level, t.code)
            WHERE change = '+'
        )
        SELECT f.id_territory, t.id_parent
        FROM from_territory f
            JOIN to_territory t ON f.id = t.id
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('INSERT: ', _nrows_affected));

    CALL public.log_info('Historique des suppressions (liens)');
    INSERT INTO public.territory_history (
            id_territory
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            t.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'LINK'
            , ROW_TO_JSON(tp.*)::JSONB
        FROM
            tmp_fr_territory_changes c
                JOIN public.territory t ON t.country = 'FR' AND (c.level, c.code) = (t.level, t.code)
                JOIN public.territory t2 ON t.country = 'FR' AND (c.level_parent, c.code_parent) = (t2.level, t2.code)
                JOIN public.territory_parent tp ON t.id = tp.id_territory AND t2.id = tp.id_parent
        WHERE c.change = '-'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));

    CALL public.log_info('Mise à jour des changements (liens) : suppression');
    WITH
    from_territory AS (
        SELECT
            c.id
            , t.id id_territory
        FROM tmp_fr_territory_changes c
            JOIN public.territory t ON t.country = 'FR' AND (c.level, c.code) = (t.level, t.code)
        WHERE change = '-'
    )
    , to_territory AS (
        SELECT
            c.id
            , t.id id_parent
        FROM tmp_fr_territory_changes c
            JOIN public.territory t ON t.country = 'FR' AND (c.level_parent, c.code_parent) = (t.level, t.code)
        WHERE change = '-'
    )
    DELETE FROM public.territory_parent tp
    USING from_territory f, to_territory t
    WHERE
        f.id = t.id
        AND
        tp.id_territory = f.id_territory
        AND
        tp.id_parent = t.id_parent
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));
END
$proc$ LANGUAGE plpgsql;

-- push territory (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_territory_to_public');
CREATE OR REPLACE PROCEDURE fr.push_territory_to_public(
    force BOOLEAN DEFAULT FALSE
)
AS
$proc$
BEGIN
    CALL fr.push_territory_properties_to_public(force);
    CALL fr.push_territory_links_to_public(force);
END
$proc$ LANGUAGE plpgsql;
