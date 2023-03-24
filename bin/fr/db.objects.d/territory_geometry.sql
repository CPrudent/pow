/***
 * FR-TERRITORY geometry management
 */

/* NOTE
 to read:
 http://postgis.net/workshops/postgis-intro/geometries.html
 https://postgis.net/docs/reference.html
 */

/* NOTE
 * type localisation (RAN/PDI)
 *
 1  Centre commune          Coordonnées du barycentre de la surface communale
 2  Mairie                  Coordonnées de la mairie de la commune
 3  Zone adressage          Coordonnées du barycentre de la surface du CP
 4  Centre voie             Coordonnées du milieu de la somme de tous les tronçons de la même voie
 5  Tronçon de voie         Coordonnées du centre du tronçon sur lequel se situe l'adresse
 6  Interpolation           Coordonnées du numéro en equi distance par rapport aux bornes du tronçon de rattachement
 7  Projection centroïde    Coordonnées de la projection orthogonale du barycentre de la parcelle cadastrale correspondant au numéro
 8  Projection plaque       Coordonnées de la plaque du numéro, donc l'entrée dans la voie
 9  Repositionné (PDI)
 */

SELECT drop_all_functions_if_exists('fr', 'set_territory_geometry');
CREATE OR REPLACE PROCEDURE fr.set_territory_geometry(
    simulation BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _municipality_with_many_zipcodes RECORD;
    _uniq_zipcode CHAR(5);
    _nof_zipcodes INTEGER;
    _test_department VARCHAR(3) := NULL;        -- for test: '33'
    _nrows_affected INTEGER;
    _context TEXT;
    _dir_tmp VARCHAR := '/data/app/pow/tmp/fr';
BEGIN
    CALL public.log_info('Début du calcul des Contours');

    --
    -- PART/1 : initialize native geometry (COM_CP first, then SUPRA)
    --

    DROP INDEX IF EXISTS ix_territory_gm_contour_natif;
    CALL public.log_info(
        message => 'Commande SH de suivi : %', 'watch -d -c "grep ''Contours CP avec commune partielle'' ' || _dir_tmp || '/SET_TERRITORY_GEOMETRY.notice.log | wc -l"'
        , stamped => FALSE
    );

    CALL public.log_info('Identification des Communes multi-CP');
    DROP TABLE IF EXISTS tmp_municipality_with_many_zipcodes;
    CREATE TEMPORARY TABLE tmp_municipality_with_many_zipcodes AS (
        SELECT
            territory.codgeo_com_parent AS codgeo
            , COUNT(*) AS nb_cp
            , FIRST(territory.codgeo_cp_parent) AS premier_cp
        FROM territory
        WHERE territory.nivgeo = 'COM_CP'
        --ayant une commune IGN = un contour commune IGN
        AND EXISTS (
            SELECT 1 FROM (
                SELECT
                    insee_com AS codgeo
                FROM
                    fr.admin_express_commune
                WHERE
                    insee_com NOT IN ('75056', '13055', '69123')
                UNION
                SELECT
                    insee_arm
                FROM
                    fr.admin_express_arrondissement_municipal
            ) AS commune_ign
            WHERE commune_ign.codgeo = territory.codgeo_com_parent
        )
        AND (_test_department IS NULL OR territory.codgeo_dep_parent = _test_department)
        GROUP BY territory.codgeo_com_parent
        HAVING COUNT(*) > 1
    );
    CREATE UNIQUE INDEX ON tmp_municipality_with_many_zipcodes (codgeo);

    CALL public.log_info('Reset des contours COM/CP');
    UPDATE fr.territory
    SET gm_contour_natif = NULL
    WHERE nivgeo = 'COM_CP'
    AND (_test_department IS NULL OR territory.codgeo_dep_parent = _test_department);

    CALL public.log_info('Init des contours COM/CP de communes entières');
    UPDATE fr.territory
    SET gm_contour_natif = commune_ign.geom
    FROM (
        SELECT
            insee_com AS codgeo
            , geom
        FROM
            fr.admin_express_commune
        WHERE
            insee_com NOT IN ('75056', '13055', '69123')
        UNION
        SELECT
            insee_arm
            , geom
        FROM
            fr.admin_express_arrondissement_municipal
    ) AS commune_ign
    WHERE territory.nivgeo = 'COM_CP'
    AND territory.codgeo_com_parent = commune_ign.codgeo
    --Qui n'est pas multi-cp
    AND NOT EXISTS (
        SELECT 1 FROM tmp_municipality_with_many_zipcodes WHERE tmp_municipality_with_many_zipcodes.codgeo = territory.codgeo_com_parent
    )
    AND (_test_department IS NULL OR territory.codgeo_dep_parent = _test_department)
    ;

    FOR _municipality_with_many_zipcodes IN (
        SELECT
            tmp_municipality_with_many_zipcodes.codgeo
            , tmp_municipality_with_many_zipcodes.nb_cp
            , tmp_municipality_with_many_zipcodes.premier_cp
            , commune_ign.geom AS gm_contour_natif
            , ST_Srid(commune_ign.geom) AS srid
        FROM tmp_municipality_with_many_zipcodes
        INNER JOIN (
            SELECT
                insee_com AS codgeo
                , geom
            FROM
                fr.admin_express_commune
            WHERE
                insee_com NOT IN ('75056', '13055', '69123')
            UNION
            SELECT
                insee_arm
                , geom
            FROM
                fr.admin_express_arrondissement_municipal
        ) AS commune_ign ON commune_ign.codgeo = tmp_municipality_with_many_zipcodes.codgeo
        ORDER BY 1
    )
    LOOP
        BEGIN
            CALL public.log_info('Init des contours COM/CP avec commune partielle : ' || _municipality_with_many_zipcodes.codgeo);

            -- set of all delivery points (PDI) for the current municipality
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point(
                geom GEOMETRY
                , co_postal CHAR(5)
                , no_type_localisation INTEGER
            );
            TRUNCATE TABLE tmp_geom_delivery_point;
            DROP INDEX IF EXISTS ix_tmp_geom_delivery_point_geom;
            INSERT INTO tmp_geom_delivery_point(
                geom
                , co_postal
                , no_type_localisation
            )
            (
                SELECT
                    ST_Transform(pdi_coord_native, _municipality_with_many_zipcodes.srid) AS geom
                    , co_postal
                    , pdi_no_type_localisation_coord AS no_type_localisation
                FROM fr.delivery_point_view
                WHERE co_insee_commune = _municipality_with_many_zipcodes.codgeo
                --WHERE co_insee_commune = '86281'
                --WHERE co_insee_commune = '2A240'
                AND fl_active
                AND fl_diffusable
                AND pdi_etat = 1
                AND pdi_visible
                AND pdi_no_type_localisation_coord >= 4
                AND pdi_coord_native IS NOT NULL
                /* TEST PDI très proches géographiquement :
                AND pdi_id IN (10652325, 24672957)
                AND pdi_coord && (SELECT ST_Extent(ST_Buffer(pdi_coord, 100)) AS etendue FROM pdi_view WHERE pdi_id = 10652325)
                */
            );

            --geom NULL pouvant venir de ST_Transform_BC2A
            DELETE FROM tmp_geom_delivery_point WHERE geom IS NULL
            /* a voir : suppression des points hors commune, ou sur le contour commune
            OR ST_ContainsProperly((SELECT ST_Buffer(geom, -200) FROM public.territoire_ign WHERE nivgeo = 'COM' AND codgeo = '27049'), geom) = FALSE
            */
            ;

            SELECT COUNT(DISTINCT co_postal), NULLIF(UNIQUE_AGG(co_postal), 'INIT_VALUE')
            INTO _nof_zipcodes, _uniq_zipcode
            FROM tmp_geom_delivery_point;

            --Le nombre de CP avec des points adresses est différent du nombre attendu
            IF _nof_zipcodes != _municipality_with_many_zipcodes.nb_cp THEN
                --Il n'y en a aucun
                IF _nof_zipcodes = 0 THEN
                    RAISE NOTICE 'ERREUR : aucun CP avec des points adresses sur la commune %, alors qu''on en attendait plusieurs', _municipality_with_many_zipcodes.codgeo;
                    /*TODO : Que faire ? on ne pourra afficher la commune au maillage CP
                        * ni remonter les contours COM_CP pour produire le contour COM, à moins d'attribuer le contour entier de la commune à un des CP
                        */
                    --On attribue le contour entier de la commune au premier des CP
                    UPDATE fr.territory
                    SET gm_contour_natif = _municipality_with_many_zipcodes.gm_contour_natif
                    WHERE territory.nivgeo = 'COM_CP'
                    AND territory.codgeo = CONCAT_WS('-', _municipality_with_many_zipcodes.codgeo, _municipality_with_many_zipcodes.premier_cp);

                    CONTINUE;
                --Il n'y en a qu'un
                ELSIF _nof_zipcodes = 1 THEN
                    RAISE NOTICE 'ERREUR : un seul CP (%) avec des points adresses sur la commune %, alors qu''on en attendait %', _uniq_zipcode, _municipality_with_many_zipcodes.codgeo, _municipality_with_many_zipcodes.nb_cp;
                    --On lui attribue le contour entier de la commune
                    UPDATE fr.territory
                    SET gm_contour_natif = _municipality_with_many_zipcodes.gm_contour_natif
                    WHERE territory.nivgeo = 'COM_CP'
                    AND territory.codgeo = CONCAT_WS('-', _municipality_with_many_zipcodes.codgeo, _uniq_zipcode);

                    CONTINUE;
                --Il y en a plusieurs (mais plus ou moins)
                ELSE
                    RAISE NOTICE 'ERREUR : % CP avec des points adresses sur la commune %, alors qu''on en attendait %', _nof_zipcodes, _municipality_with_many_zipcodes.codgeo, _municipality_with_many_zipcodes.nb_cp;
                END IF;
            END IF;

            CREATE INDEX ix_tmp_geom_delivery_point_geom ON tmp_geom_delivery_point USING GIST(geom);

            -- delimit polygons (Voronoi) for the set of points
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point_voronoi(
                voronoi_id SERIAL
                , geom GEOMETRY(POLYGON)
                , co_postal CHAR(5)
            );
            TRUNCATE TABLE tmp_geom_delivery_point_voronoi;
            DROP INDEX IF EXISTS ix_tmp_geom_delivery_point_voronoi_geom;
            INSERT INTO tmp_geom_delivery_point_voronoi (geom) (
                /* Génération des polygones de Voronoi
                * pour l'ensemble des point adresse de la commune
                * dans la limite (jusqu'à l'étendue) du contour de la commune
                */
                SELECT
                    (ST_Dump(
                        -- GEOMETRY ST_VoronoiPolygons(g1 GEOMETRY, tolerance FLOAT8, extend_to GEOMETRY);
                        ST_VoronoiPolygons(
                        (SELECT ST_Collect(geom) FROM tmp_geom_delivery_point)
                        , 5
                        --Etendue de la commune
                        , _municipality_with_many_zipcodes.gm_contour_natif
                        --, (SELECT geom FROM fr.admin_express_commune WHERE insee_com = '86281')
                        --Alternative : étendue agrandie de l'ensemble des points
                        --, (SELECT ST_Buffer(ST_Extend(geom, 10000) FROM tmp_geom_delivery_point)
                    ))).geom
            );
            CREATE INDEX ix_tmp_geom_delivery_point_voronoi_geom ON tmp_geom_delivery_point_voronoi USING GIST(geom);

            --SELECT ST_Transform(geom, 4326) FROM tmp_geom_delivery_point_voronoi UNION ALL (SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022')
            --SELECT ST_Transform(geom, 4326) FROM tmp_geom_delivery_point_voronoi where ST_ContainsProperly((SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022'), geom)

            /* TEST : L'UNION de tous les polygones de Voronoi recouvre bien la commune ?
            SELECT ST_AsText(ST_Difference(
                (SELECT geom FROM fr.admin_express_commune WHERE and insee_com = '86281')
                , (SELECT ST_Union(geom) FROM tmp_geom_delivery_point_voronoi)
            ));
            */

            /* TEST : on remplace le contour d'un CP par l'ensemble de polygones générés ainsi que les points adresses agrandis
            WITH voronoi_et_pdi AS (
                SELECT ST_Collect(geom) AS geoms
                FROM (
                    SELECT geom FROM tmp_geom_delivery_point_voronoi
                    UNION ALL
                    SELECT ST_Buffer(geom, 2) AS geom FROM tmp_geom_delivery_point
                ) AS sous_requete
            )
            UPDATE fr.territory
            SET gm_contour = geoms
                , gm_contour_simp = geoms
            FROM voronoi_et_pdi
            WHERE codgeo = '76600' AND nivgeo = 'CP'
            */

            -- remains to affect zipcode to each polygon
            WITH voronoi_has_co_postal AS (
                SELECT
                    tmp_geom_delivery_point_voronoi.voronoi_id
                    , tmp_geom_delivery_point.co_postal AS co_postal
                    , COUNT(*) AS nb_pdi
                    , SUM(no_type_localisation) AS sum_no_type_localisation
                FROM tmp_geom_delivery_point
                    INNER JOIN tmp_geom_delivery_point_voronoi
                        ON ST_Within(tmp_geom_delivery_point.geom, tmp_geom_delivery_point_voronoi.geom)
                GROUP BY
                    tmp_geom_delivery_point_voronoi.voronoi_id, tmp_geom_delivery_point.co_postal
            )
            , voronoi_has_best_co_postal AS (
                SELECT
                    voronoi_id
                    , FIRST(co_postal ORDER BY sum_no_type_localisation DESC) AS co_postal
                    --, FIRST(co_postal ORDER BY nb_pdi DESC) AS co_postal
                    --, ARRAY_AGG(co_postal ORDER BY co_postal)
                    --, ARRAY_AGG(nb_pdi ORDER BY co_postal)
                FROM voronoi_has_co_postal
                GROUP BY voronoi_id
                --HAVING COUNT(*) = 1
            )
            UPDATE tmp_geom_delivery_point_voronoi
            SET co_postal = voronoi_has_best_co_postal.co_postal
            FROM voronoi_has_best_co_postal
            WHERE voronoi_has_best_co_postal.voronoi_id = tmp_geom_delivery_point_voronoi.voronoi_id
            ;

            /* TEST : on remplace le contour des CP par l'ensemble de polygones générés regroupés par attribution CP
            WITH tmp_contour_cp_com AS (
                SELECT ST_Union(geom) as geom, co_postal
                FROM tmp_geom_delivery_point_voronoi
                WHERE co_postal is not null
                GROUP BY co_postal
            )
            UPDATE territory
            SET gm_contour = tmp_contour_cp_com.geom--,  gm_contour_simp = tmp_contour_cp_com.geom
            FROM tmp_contour_cp_com
            WHERE territory.nivgeo = 'COM_CP'
            AND territory.codgeo = CONCAT('33236-', tmp_contour_cp_com.co_postal)
            ;
            */
            WITH set_of_contour_by_zipcode AS (
                WITH set_of_contours_delimited_by_municipality AS (
                    SELECT
                        (ST_Dump(
                            --Les polygones obtenus sont issus de l'étendue de la commune (un rectangle / BBOX)
                            --Il faut les recouper pour qu'ils ne dépassent pas du contour de la commune
                            --A faire absolument avant réattribution CP !
                            /* NOTE
                            L'intersection avec la commune des polygones unis par CP ne donne pas le même résultat (et celui-ci n'est pas correct!)
                            * que l'intersection de chaque polygone avec la commune, ensuite unis par CP
                            * il était pourtant plus rapide de faire l'UNION par CP, puis l'intersection avec la commune
                            */

                            ST_Intersection(
                                _municipality_with_many_zipcodes.gm_contour_natif
                                --(SELECT geom FROM fr.admin_express_commune WHERE insee_com = '86281')
                                , ST_Union(geom)
                            )

                            /*
                            ST_Union(
                                ST_Intersection(
                                    _municipality_with_many_zipcodes.gm_contour_natif
                                    --(SELECT geom FROM fr.admin_express_commune WHERE insee_com = '86281')
                                    , geom
                                )
                            )
                            */
                        )).geom
                        , co_postal
                    FROM tmp_geom_delivery_point_voronoi
                    --WHERE co_postal IS NOT NULL --FIXME : possible ? si oui ne faut il pas les traiter tout de même ?
                    GROUP BY co_postal
                )

                /* TEST : y a t il une différence entre les polygones par CP et la commune ?
                SELECT ST_AsText(geom), geom, ST_AREA(geom) FROM
                (SELECT ST_SymDifference(ST_Union(geom), (SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022')) AS geom
                FROM set_of_contours_delimited_by_municipality) AS ss
                */

                /*
                , test AS (SELECT ST_Union(geom) AS geom, co_postal FROM set_of_contours_delimited_by_municipality	GROUP BY co_postal)
                UPDATE territory SET gm_contour = test.geom FROM test
                WHERE territory.nivgeo = 'COM_CP' AND territory.codgeo = CONCAT('59350-', test.co_postal)
                */

                --SELECT *, ROUND(ST_Area(geom)), ST_GeometryType(geom) FROM set_of_contours_delimited_by_municipality ORDER BY co_postal, ROUND(ST_Area(geom)) DESC

                , list_of_contours_with_included_flag AS (
                    SELECT
                        ROW_NUMBER() OVER() AS geom_id
                        --On considère qu'une géométrie est absorbante si sa superficie
                        --est au moins supérieure à la moitiée de celle de la plus grande géométrie du CP
                        , ST_Area(geom) > ((MAX(ST_Area(geom)) OVER(PARTITION BY co_postal))/2) AS est_absorbante
                        , geom
                        , co_postal
                    FROM set_of_contours_delimited_by_municipality
                )
                --SELECT *, ROUND(ST_Area(geom)), ST_GeometryType(geom) FROM list_of_contours_with_included_flag ORDER BY co_postal, ROUND(ST_Area(geom)) DESC
                , list_of_contours_with_included_id AS (
                    --Les autres polygones sont liés à une erreur de géolocalistion du PDI, ou de CP de l'adresse du PDI
                    --On leur réattribue un autre CP, en fonction de leur lien avec une des meilleures géometries
                    SELECT
                        list_of_contours_with_included_flag.geom_id
                        , list_of_contours_with_included_flag.co_postal
                        , COALESCE(
                            (
                                SELECT t.geom_id
                                FROM list_of_contours_with_included_flag AS t
                                --Une géométrie ne doit pas s'absorber elle même
                                WHERE list_of_contours_with_included_flag.geom_id != t.geom_id
                                --Une géométrie ne doit pas absorber une géométrie "absorbante"
                                AND NOT list_of_contours_with_included_flag.est_absorbante
                                AND (
                                    --Une géométrie en absorbe une autre si elle la contient entièrement
                                    ST_Within(list_of_contours_with_included_flag.geom, ST_MakePolygon(ST_ExteriorRing(t.geom)))
                                    --Une géométrie en absorbe une autre si elles se touchent
                                    OR (
                                        ST_Touches(list_of_contours_with_included_flag.geom, t.geom)
                                        --Et que la géométrie absorbante est plus grande que celle absorbée
                                        --Note : sinon on risque d'avoir une boucle infinie dans la recherche récursive suivante
                                        AND ST_Area(t.geom) > ST_Area(list_of_contours_with_included_flag.geom)::NUMERIC
                                    )
                                )
                                --Dans le cas où la géométrie aurait plusieurs géométries absorbantes, elle se fait absorber par la plus grande
                                ORDER BY ST_Area(t.geom) DESC
                                LIMIT 1
                            )
                        ) AS geom_id_absorbante
                    FROM list_of_contours_with_included_flag
                    --Cas étrange, à vérifier si toujours présent, il se peut qu'on obtienne autre chose que des polygones
                    --AND ST_GeometryType((set_of_contours_delimited_by_municipality.dump_geom).geom) NOT IN ('ST_LineString', 'ST_Point')
                    --SELECT * FROM (SELECT (ST_Dump(gm_contour)).geom, codgeo FROM fr.territory where nivgeo IN ('COM_CP', 'CP') and gm_contour IS NOT NULL) AS sous_requete WHERE ST_GeometryType(geom) != 'ST_Polygon'
                )
                --SELECT * FROM list_of_contours_with_included_id ORDER BY geom_id--, co_postal, ROUND(ST_Area(geom)) DESC
                , list_of_contours_with_final_zipcode AS (
                    /* NOTE
                    Il est possible qu'une géométrie soit absorbée par une géométrie elle même absorbée
                    Il faut donc chercher la géométrie absorbante finale (=CP final) par récursivité pour chaque géométrie
                    */
                    SELECT
                        list_of_contours_with_included_flag.geom
                        , (
                            WITH RECURSIVE search_graph(geom_id, co_postal, geom_id_absorbante, depth) AS (
                                SELECT g.geom_id, g.co_postal, g.geom_id_absorbante, 1
                                FROM list_of_contours_with_included_id g
                                WHERE g.geom_id = list_of_contours_with_included_flag.geom_id
                                UNION ALL
                                SELECT g.geom_id, g.co_postal, g.geom_id_absorbante, sg.depth + 1
                                FROM list_of_contours_with_included_id g, search_graph sg
                                WHERE g.geom_id = sg.geom_id_absorbante
                            )
                            SELECT co_postal FROM search_graph ORDER BY depth DESC LIMIT 1
                        ) AS co_postal
                        FROM list_of_contours_with_included_flag
                )
                --SELECT * FROM list_of_contours_with_final_zipcode
                SELECT ST_Union(geom) AS geom, co_postal
                FROM list_of_contours_with_final_zipcode
                GROUP BY co_postal
            )
            UPDATE fr.territory
            SET gm_contour_natif = set_of_contour_by_zipcode.geom
            FROM set_of_contour_by_zipcode
            WHERE territory.nivgeo = 'COM_CP'
            AND territory.codgeo = CONCAT_WS('-', _municipality_with_many_zipcodes.codgeo, set_of_contour_by_zipcode.co_postal);

            /* TEST : y a t il une différence entre les polygones par cp et la commune ?
            SELECT ST_AsText(geom), geom, ST_AREA(geom) FROM
                (SELECT ST_SymDifference(ST_Union(geom), (SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022')) AS geom
                FROM set_of_contour_by_zipcode) AS ss
            */
            --SELECT geom, insee_com FROM fr.admin_express_commune WHERE geom && (SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022')

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS _context = PG_EXCEPTION_CONTEXT;
            RAISE '% : Erreur sur traitement commune % : % (%)', TO_CHAR(clock_timestamp(), 'HH24:MI:SS'), _municipality_with_many_zipcodes.codgeo, SQLERRM, _context;
        END;
    END LOOP;

    CALL public.log_info('Indexation Territoire : Contours natifs (COM_CP)');
    CREATE INDEX IF NOT EXISTS ix_territory_gm_contour_natif ON fr.territory USING GIST(gm_contour_natif) WHERE nivgeo = 'COM_CP';

    FOR _municipality_with_many_zipcodes IN (
        SELECT
            tmp_municipality_with_many_zipcodes.codgeo
            , tmp_municipality_with_many_zipcodes.nb_cp
        FROM tmp_municipality_with_many_zipcodes
        ORDER BY tmp_municipality_with_many_zipcodes.codgeo
    )
    LOOP
        /* NOTE
         GEOMETRY ST_Snap(GEOMETRY input, GEOMETRY reference, FLOAT tolerance);
         Snaps the vertices and segments of a geometry to another Geometry's vertices.
         The result geometry is the input geometry with the vertices snapped.
         */

        WITH contour_of_municipality_with_many_zipcodes AS (
            SELECT ST_Union(gm_contour_natif) AS gm_contour_natif
            FROM fr.territory
            WHERE nivgeo = 'COM_CP'
            AND codgeo_com_parent = _municipality_with_many_zipcodes.codgeo
        )
        , snap_territory_around AS (
            SELECT
                territory_around.codgeo
                , ST_Snap(
                    territory_around.gm_contour_natif
                    , contour_of_municipality_with_many_zipcodes.gm_contour_natif
                    , 1.0
                ) AS gm_contour_natif
            FROM contour_of_municipality_with_many_zipcodes
            INNER JOIN fr.territory AS territory_around
                ON territory_around.nivgeo = 'COM_CP'
                AND territory_around.gm_contour_natif && contour_of_municipality_with_many_zipcodes.gm_contour_natif
                AND territory_around.codgeo_com_parent != _municipality_with_many_zipcodes.codgeo
        )
        UPDATE fr.territory
        SET gm_contour_natif = snap_territory_around.gm_contour_natif
        FROM snap_territory_around
        WHERE territory.codgeo = snap_territory_around.codgeo
        AND territory.nivgeo = 'COM_CP'
        -- If no snapping occurs then the input geometry is returned unchanged.
        AND NOT ST_Equals(territory.gm_contour_natif, snap_territory_around.gm_contour_natif);

        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT('ST_Snap autour de ', _municipality_with_many_zipcodes.codgeo, ' : #', _nrows_affected, 'traités'));
    END LOOP;

    COMMIT;

    --
    -- PART/2 : simplified geometry (COM_CP)
    --

    DROP INDEX IF EXISTS ix_territory_gm_contour;

    CALL public.log_info(
        message => 'Commande SH de suivi : %', 'watch -d -c "cat ' || _dir_tmp || '/SET_TERRITORY_GEOMETRY.notice.log | grep -o -P ''[0-9]+ traités'' | grep -o -P ''[0-9]+'' | awk ''{ SUM += \$1} END { print SUM }''"'
        , stamped => FALSE
    );

    UPDATE fr.territory SET gm_contour = NULL;

    CALL public.log_info('Calcul des contours simplifiés');
    CALL ST_SimplifyTerritory(
        levels => ARRAY['COM_CP']
        , to_srid => 4326
        , bbox_split_over => 1000
        , tolerance => 100
    );

    COMMIT;

    --
    -- PART/3 :
    --

    -- \include_relative territoire_contour_correction.sql

    COMMIT;

    --
    -- PART/4 : eval area (COM_CP first), then SUPRA for (simplified geometry, area)
    --

    UPDATE fr.territory
    SET superficie = ROUND(ST_Area(ST_Transform(gm_contour_natif, 4326)::GEOGRAPHY)/10000)
    WHERE territory.nivgeo = 'COM_CP';

    DROP INDEX IF EXISTS public.ix_territory_gm_contour;

    CALL public.log_info('remontée SUPRA pour superficie et contour simplifié');
    PERFORM fr.set_territory_supra(
        schema_name => 'fr'
        , table_name => 'territory'
        , base_level => 'COM_CP'
        , columns_agg => ARRAY['gm_contour', 'superficie']
        , update_mode => TRUE
    );

    CALL public.log_info('Indexation Territoire : Contours');
    CREATE INDEX IF NOT EXISTS ix_territory_gm_contour ON fr.territory USING GIST(nivgeo, gm_contour);

    COMMIT;
    CALL public.log_info('Fin du calcul des Contours');
END
$proc$ LANGUAGE plpgsql;

/*
SELECT ST_Collect(ST_Union(gm_contour), ST_Transform(ST_Union(gm_contour_natif), 4326))
FROM territory WHERE nivgeo = 'COM_CP'  GROUP BY codgeo_dep_parent

SELECT gm_contour, codgeo FROM territory where nivgeo = 'DEP'
SELECT gm_contour, codgeo FROM territory where nivgeo = 'ARR' AND codgeo_dep_parent = '86'
SELECT ST_Collect(gm_contour, ST_Transform(gm_contour_natif, 4326)), gm_contour, ST_Transform(gm_contour_natif, 4326), codgeo FROM territory where nivgeo = 'COM_CP' AND codgeo_arr_parent = '863'
SELECT gm_contour, codgeo_dep_parent FROM territory where nivgeo = 'ARR' limit 10

SELECT * FROM territory WHERE nivgeo = 'COM_CP' AND gm_contour && (
	SELECT ST_Extent(gm_contour) FROM territory WHERE nivgeo = 'COM_CP' AND libgeo ilike '%Mastribus%'
)
 */
