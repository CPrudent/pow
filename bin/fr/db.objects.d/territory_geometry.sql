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

SELECT drop_all_functions_if_exists('fr', 'set_municipality_subsection_geometry');
CREATE OR REPLACE PROCEDURE fr.set_municipality_subsection_geometry(
    subsection VARCHAR                      -- ZA or COM_CP
    , location_min INT DEFAULT 4
    , department_test VARCHAR DEFAULT NULL
)
AS
$proc$
DECLARE
    _municipality_with_many_subsections RECORD;
    _uniq VARCHAR;
    _nof INTEGER;
    _nrows_affected INTEGER;
    _context TEXT;
    _message VARCHAR;
BEGIN
    IF NOT subsection = ANY(ARRAY['ZA', 'COM_CP']) THEN
        RAISE 'argument subsection % non valide, choix possibles {ZA, COM_CP}', subsection;
    END IF;
    -- TODO exists level ?
    IF NOT EXISTS(
        SELECT 1 FROM fr.territory WHERE nivgeo = subsection
    ) THEN
        RAISE 'manque niveaux % dans fr.territory', subsection;
    END IF;

    CALL public.log_info('Reset des contours natifs ' || subsection);
    UPDATE fr.territory
    SET gm_contour_natif = NULL
    WHERE nivgeo = subsection
    -- TEST only evaluated for the department (others being reseted)
    AND (department_test IS NULL OR territory.codgeo_dep_parent = department_test)
    ;

    CALL public.log_info('Identification des Communes multi-' || subsection);
    DROP TABLE IF EXISTS tmp_municipality_with_many_subsections;
    CREATE TEMPORARY TABLE tmp_municipality_with_many_subsections AS (
        SELECT
            za.codgeo_com_parent co_insee_commune
            , COUNT(*) AS nb_subsections
            , CASE
                WHEN subsection = 'ZA' THEN FIRST(za.codgeo)
                WHEN subsection = 'COM_CP' THEN FIRST(za.codgeo_cp_parent)
            END first_subsection
        FROM fr.territory za
        WHERE
            nivgeo = subsection
            AND
            --ayant une commune IGN = un contour commune IGN
            EXISTS (
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
                WHERE commune_ign.codgeo = za.codgeo_com_parent
            )
            AND (
                department_test IS NULL
                OR
                za.codgeo_dep_parent = department_test
            )
        GROUP BY za.codgeo_com_parent
        HAVING COUNT(*) > 1
    );
    CREATE UNIQUE INDEX ON tmp_municipality_with_many_subsections (co_insee_commune);

    -- only 1 subsection : same contour as municipality
    CALL public.log_info('Init des contours de communes entières');
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
    WHERE
        nivgeo = subsection
        AND
        codgeo_com_parent = commune_ign.codgeo
        AND
        NOT EXISTS (
            SELECT 1 FROM tmp_municipality_with_many_subsections
            WHERE tmp_municipality_with_many_subsections.co_insee_commune = territory.codgeo_com_parent
        )
        AND (
            department_test IS NULL
            OR
            codgeo_dep_parent = department_test
        );

    -- many subsections
    FOR _municipality_with_many_subsections IN (
        SELECT
            tmp_municipality_with_many_subsections.co_insee_commune
            , tmp_municipality_with_many_subsections.nb_subsections
            , tmp_municipality_with_many_subsections.first_subsection
            , commune_ign.geom AS gm_contour_natif
            , ST_SRID(commune_ign.geom) AS srid
        FROM tmp_municipality_with_many_subsections
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
        ) AS commune_ign ON commune_ign.codgeo = tmp_municipality_with_many_subsections.co_insee_commune
        ORDER BY 1
    )
    LOOP
        BEGIN
            CALL public.log_info('Init des contours avec commune partielle : ' || _municipality_with_many_subsections.co_insee_commune);

            -- set of all delivery points (PDI) for the current municipality
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point(
                geom GEOMETRY
                , subsection_id VARCHAR
                , no_type_localisation INTEGER
            );
            TRUNCATE TABLE tmp_geom_delivery_point;
            DROP INDEX IF EXISTS ix_tmp_geom_delivery_point_geom;

            INSERT INTO tmp_geom_delivery_point(
                geom
                , subsection_id
                , no_type_localisation
            )
            (
                SELECT
                    ST_Transform(pdi_coord_native, _municipality_with_many_subsections.srid)
                    , CASE
                        WHEN subsection = 'ZA' THEN co_adr_za
                        WHEN subsection = 'COM_CP' THEN co_postal
                    END
                    , pdi_no_type_localisation_coord
                FROM fr.delivery_point_view
                WHERE
                    co_insee_commune = _municipality_with_many_subsections.co_insee_commune
                    AND fl_active
                    AND fl_diffusable
                    AND pdi_etat = 1
                    AND pdi_visible
                    -- at least street-center (=4)
                    AND pdi_no_type_localisation_coord >= location_min
                    AND pdi_coord_native IS NOT NULL
            );

            SELECT COUNT(DISTINCT subsection_id), NULLIF(UNIQUE_AGG(subsection_id), 'INIT_VALUE')
            INTO _nof, _uniq
            FROM tmp_geom_delivery_point;

            -- number of subsections different from waited one
            IF _nof != _municipality_with_many_subsections.nb_subsections THEN
                IF _nof = 0 THEN
                    RAISE NOTICE 'INFO : 0 % avec des points adresses sur la commune %, alors qu''on en attendait plusieurs'
                        , subsection
                        , _municipality_with_many_subsections.co_insee_commune;
                    -- set contour of municipality to first subsection
                    UPDATE fr.territory
                    SET gm_contour_natif = _municipality_with_many_subsections.gm_contour_natif
                    WHERE
                        nivgeo = subsection
                        AND
                        codgeo = CASE
                            WHEN subsection = 'ZA' THEN
                                _municipality_with_many_subsections.first_subsection
                            WHEN subsection = 'COM_CP' THEN
                                CONCAT_WS('-'
                                    , _municipality_with_many_subsections.co_insee_commune
                                    , _municipality_with_many_subsections.first_subsection
                                )
                            END;

                    CONTINUE;
                ELSIF _nof = 1 THEN
                    RAISE NOTICE 'INFO : 1 % (%) avec des points adresses sur la commune %, alors qu''on en attendait %'
                        , subsection
                        , _uniq
                        , _municipality_with_many_subsections.co_insee_commune
                        , _municipality_with_many_subsections.nb_subsections;
                    -- set contour of municipality to uniq subsection
                    UPDATE fr.territory
                    SET gm_contour_natif = _municipality_with_many_subsections.gm_contour_natif
                    WHERE
                        codgeo = CASE
                            WHEN subsection = 'ZA' THEN
                                _uniq
                            WHEN subsection = 'COM_CP' THEN
                                CONCAT_WS('-'
                                    , _municipality_with_many_subsections.co_insee_commune
                                    , _uniq
                                )
                            END;

                    CONTINUE;
                -- less or more
                ELSE
                    RAISE NOTICE 'INFO : % % avec des points adresses sur la commune %, alors qu''on en attendait %'
                        , _nof
                        , subsection
                        , _municipality_with_many_subsections.co_insee_commune
                        , _municipality_with_many_subsections.nb_subsections;
                END IF;
            END IF;

            CREATE INDEX ix_tmp_geom_delivery_point_geom ON tmp_geom_delivery_point USING GIST(geom);

            -- delimit polygons (Voronoi) for the set of points
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point_voronoi(
                voronoi_id SERIAL
                , geom GEOMETRY(POLYGON)
                , subsection_id VARCHAR
            );
            TRUNCATE TABLE tmp_geom_delivery_point_voronoi;
            DROP INDEX IF EXISTS ix_tmp_geom_delivery_point_voronoi_geom;
            INSERT INTO tmp_geom_delivery_point_voronoi (geom) (
                /*
                 generate Voronoi's polygons, w/ delivery points
                 extend_to contour of municipality
                 */
                SELECT
                    (ST_Dump(
                        -- GEOMETRY ST_VoronoiPolygons(g1 GEOMETRY, tolerance FLOAT8, extend_to GEOMETRY);
                        ST_VoronoiPolygons(
                            (SELECT ST_Collect(geom) FROM tmp_geom_delivery_point)
                            , 5
                            , _municipality_with_many_subsections.gm_contour_natif
                        )
                    )).geom
            );
            CREATE INDEX ix_tmp_geom_delivery_point_voronoi_geom ON tmp_geom_delivery_point_voronoi USING GIST(geom);

            -- remains to affect subsection to each polygon
            WITH
            voronoi_has_subsection AS (
                SELECT
                    tmp_geom_delivery_point_voronoi.voronoi_id
                    , tmp_geom_delivery_point.subsection_id
                    , SUM(no_type_localisation) AS sum_no_type_localisation
                FROM tmp_geom_delivery_point
                    INNER JOIN tmp_geom_delivery_point_voronoi
                        ON ST_Within(tmp_geom_delivery_point.geom, tmp_geom_delivery_point_voronoi.geom)
                GROUP BY
                    tmp_geom_delivery_point_voronoi.voronoi_id, tmp_geom_delivery_point.subsection_id
            )
            , voronoi_has_best_subsection AS (
                SELECT
                    voronoi_id
                    , FIRST(subsection_id ORDER BY sum_no_type_localisation DESC) AS subsection_id
                FROM voronoi_has_subsection
                GROUP BY voronoi_id
            )
            UPDATE tmp_geom_delivery_point_voronoi
            SET subsection_id = voronoi_has_best_subsection.subsection_id
            FROM voronoi_has_best_subsection
            WHERE voronoi_has_best_subsection.voronoi_id = tmp_geom_delivery_point_voronoi.voronoi_id;

            WITH
            set_of_contour_by_subsection AS (
                WITH
                set_of_contours_delimited_by_municipality AS (
                    SELECT
                        (ST_Dump(
                            ST_Intersection(
                                _municipality_with_many_subsections.gm_contour_natif
                                , ST_Union(geom)
                            )
                        )).geom
                        , subsection_id
                    FROM tmp_geom_delivery_point_voronoi
                    GROUP BY subsection_id
                )
                , list_of_contours_with_included_flag AS (
                    SELECT
                        ROW_NUMBER() OVER() AS geom_id
                        --On considère qu'une géométrie est absorbante si sa superficie
                        --est au moins supérieure à la moitiée de celle de la plus grande géométrie du CP
                        , ST_Area(geom) > ((MAX(ST_Area(geom)) OVER(PARTITION BY subsection_id))/2) AS est_absorbante
                        , geom
                        , subsection_id
                    FROM set_of_contours_delimited_by_municipality
                )
                , list_of_contours_with_included_id AS (
                    SELECT
                        list_of_contours_with_included_flag.geom_id
                        , list_of_contours_with_included_flag.subsection_id
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
                )
                , list_of_contours_with_final_subsection AS (
                    SELECT
                        list_of_contours_with_included_flag.geom
                        , (
                            WITH RECURSIVE search_graph(geom_id, subsection_id, geom_id_absorbante, depth) AS (
                                SELECT g.geom_id, g.subsection_id, g.geom_id_absorbante, 1
                                FROM list_of_contours_with_included_id g
                                WHERE g.geom_id = list_of_contours_with_included_flag.geom_id
                                UNION ALL
                                SELECT g.geom_id, g.subsection_id, g.geom_id_absorbante, sg.depth + 1
                                FROM list_of_contours_with_included_id g, search_graph sg
                                WHERE g.geom_id = sg.geom_id_absorbante
                            )
                            SELECT subsection_id FROM search_graph ORDER BY depth DESC LIMIT 1
                        ) AS subsection_id
                        FROM list_of_contours_with_included_flag
                )
                --SELECT * FROM list_of_contours_with_final_subsection
                SELECT ST_Union(geom) AS geom, subsection_id
                FROM list_of_contours_with_final_subsection
                GROUP BY subsection_id
            )
            UPDATE fr.territory
            SET gm_contour_natif = set_of_contour_by_subsection.geom
            FROM set_of_contour_by_subsection
            WHERE
                nivgeo = subsection
                AND
                codgeo = CASE
                    WHEN subsection = 'ZA' THEN
                        set_of_contour_by_subsection.subsection_id
                    WHEN subsection = 'COM_CP' THEN
                        CONCAT_WS('-'
                            , _municipality_with_many_subsections.co_insee_commune
                            , set_of_contour_by_subsection.subsection_id
                        )
                    END;

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS _context = PG_EXCEPTION_CONTEXT;
            RAISE '% : Erreur sur traitement % pour la commune % : % (%)'
                , TO_CHAR(clock_timestamp(), 'HH24:MI:SS')
                , subsection
                , _municipality_with_many_subsections.co_insee_commune
                , SQLERRM
                , _context;
        END;
    END LOOP;

    FOR _municipality_with_many_subsections IN (
        SELECT
            co_insee_commune
            , nb_subsections
        FROM tmp_municipality_with_many_subsections
        ORDER BY co_insee_commune
    )
    LOOP
        /* NOTE
        GEOMETRY ST_Snap(GEOMETRY input, GEOMETRY reference, FLOAT tolerance);
        Snaps the vertices and segments of a geometry to another Geometry's vertices.
        The result geometry is the input geometry with the vertices snapped.
        */

        WITH
        contour_of_municipality_with_many_subsections AS (
            SELECT ST_Union(gm_contour_natif) AS gm_contour_natif
            FROM fr.territory
            WHERE
                nivgeo = subsection
                AND
                codgeo_com_parent = _municipality_with_many_subsections.co_insee_commune
        )
        , snap_territory_around AS (
            SELECT
                territory_around.codgeo_com_parent co_insee_commune
                , ST_Snap(
                    territory_around.gm_contour_natif
                    , contour_of_municipality_with_many_subsections.gm_contour_natif
                    , 1.0
                ) AS gm_contour_natif
            FROM contour_of_municipality_with_many_subsections
            INNER JOIN fr.territory AS territory_around
                -- subsection with common border (ST_Touches) from another municipality
                ON territory_around.nivgeo = subsection
                AND territory_around.gm_contour_natif && contour_of_municipality_with_many_subsections.gm_contour_natif
                AND territory_around.codgeo_com_parent != _municipality_with_many_subsections.co_insee_commune
        )
        UPDATE fr.territory
        SET gm_contour_natif = snap_territory_around.gm_contour_natif
        FROM snap_territory_around
        WHERE
            nivgeo = subsection
            AND
            codgeo_com_parent = snap_territory_around.co_insee_commune
            -- If no snapping occurs then the input geometry is returned unchanged.
            AND NOT ST_Equals(territory.gm_contour_natif, snap_territory_around.gm_contour_natif);

        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        _message := ' traité';
        IF _nrows_affected > 1 THEN _message := _message || 's'; END IF;
        CALL public.log_info(CONCAT('ST_Snap autour de ', _municipality_with_many_subsections.co_insee_commune, ' : #', _nrows_affected, _message));
    END LOOP;

    COMMIT;
END
$proc$ LANGUAGE plpgsql;

/*
-- replaced by: set_municipality_subsection_geometry(subsection => 'ZA')
SELECT drop_all_functions_if_exists('fr', 'set_zone_address_geometry');
CREATE OR REPLACE PROCEDURE fr.set_zone_address_geometry(
    location_min INT DEFAULT 4
    , department_test VARCHAR DEFAULT NULL
)
AS
$proc$
DECLARE
    _municipality_with_many RECORD;
    _uniq CHAR(10);
    _nof INTEGER;
    _nrows_affected INTEGER;
    _context TEXT;
    _message VARCHAR;
BEGIN
    CALL public.log_info('Identification des Communes multi-ZA');

    DROP TABLE IF EXISTS fr.territory_za;
    CREATE TABLE fr.territory_za AS (
        SELECT
            co_cea
            , co_insee_commune
            , NULL::GEOMETRY gm_contour_natif
            , NULL::GEOMETRY gm_contour
        FROM fr.laposte_zone_address
        WHERE
            fl_active
    );
    CREATE UNIQUE INDEX iux_territory_za_cea ON fr.territory_za (co_cea);
    CREATE INDEX ix_territory_za_co_insee_commune ON fr.territory_za (co_insee_commune);

    DROP TABLE IF EXISTS tmp_municipality_with_many;
    CREATE TEMPORARY TABLE tmp_municipality_with_many AS (
        SELECT
            za.co_insee_commune
            , COUNT(*) AS nb_za
            , FIRST(za.co_cea) AS first_za      -- ZA which have more PDI ?
        FROM fr.territory_za za
        WHERE
        --ayant une commune IGN = un contour commune IGN
        EXISTS (
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
            WHERE commune_ign.codgeo = za.co_insee_commune
        )
        AND (
            department_test IS NULL
            OR
            fr.get_department_code_from_municipality_code(za.co_insee_commune) = department_test
        )
        GROUP BY za.co_insee_commune
        HAVING COUNT(DISTINCT co_cea) > 1
    );
    CREATE UNIQUE INDEX ON tmp_municipality_with_many (co_insee_commune);

    -- ZA same as COM
    CALL public.log_info('Init des contours de communes entières');
    UPDATE fr.territory_za
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
    WHERE
        co_insee_commune = commune_ign.codgeo
    --Qui n'est pas multi-cp
    AND NOT EXISTS (
        SELECT 1 FROM tmp_municipality_with_many
        WHERE tmp_municipality_with_many.co_insee_commune = territory_za.co_insee_commune
    )
    AND (department_test IS NULL OR fr.get_department_code_from_municipality_code(co_insee_commune) = department_test)
    ;

    -- ZA only part of COM (which is shared into many ZA)
    FOR _municipality_with_many IN (
        SELECT
            tmp_municipality_with_many.co_insee_commune
            , tmp_municipality_with_many.nb_za
            , tmp_municipality_with_many.first_za
            , commune_ign.geom AS gm_contour_natif
            , ST_SRID(commune_ign.geom) AS srid
        FROM tmp_municipality_with_many
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
        ) AS commune_ign ON commune_ign.codgeo = tmp_municipality_with_many.co_insee_commune
        ORDER BY 1
    )
    LOOP
        BEGIN
            CALL public.log_info('Init des contours avec commune partielle : ' || _municipality_with_many.co_insee_commune);

            -- set of all delivery points (PDI) for the current municipality
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point(
                geom GEOMETRY
                , co_cea CHAR(10)
                , no_type_localisation INTEGER
            );
            TRUNCATE TABLE tmp_geom_delivery_point;
            DROP INDEX IF EXISTS ix_tmp_geom_delivery_point_geom;

            INSERT INTO tmp_geom_delivery_point(
                geom
                , co_cea
                , no_type_localisation
            )
            (
                SELECT
                    ST_Transform(pdi_coord_native, _municipality_with_many.srid) AS geom
                    , co_adr_za
                    , pdi_no_type_localisation_coord AS no_type_localisation
                FROM fr.delivery_point_view
                WHERE co_insee_commune = _municipality_with_many.co_insee_commune
                AND fl_active
                AND fl_diffusable
                AND pdi_etat = 1
                AND pdi_visible
                -- at least street-center (=4)
                AND pdi_no_type_localisation_coord >= location_min
                AND pdi_coord_native IS NOT NULL
                /* TEST PDI très proches géographiquement :
                AND pdi_id IN (10652325, 24672957)
                AND pdi_coord && (SELECT ST_Extent(ST_Buffer(pdi_coord, 100)) AS etendue FROM pdi_view WHERE pdi_id = 10652325)
                */
            );

            SELECT COUNT(DISTINCT co_cea), NULLIF(UNIQUE_AGG(co_cea), 'INIT_VALUE')
            INTO _nof, _uniq
            FROM tmp_geom_delivery_point;

            --Le nombre de ZA avec des points adresses est différent du nombre attendu
            IF _nof != _municipality_with_many.nb_za THEN
                --Il n'y en a aucun
                IF _nof = 0 THEN
                    RAISE NOTICE 'ERREUR : aucune ZA avec des points adresses sur la commune %, alors qu''on en attendait plusieurs'
                    , _municipality_with_many.co_insee_commune;
                    /*TODO : Que faire ? on ne pourra afficher la commune au maillage CP
                        * ni remonter les contours COM_CP pour produire le contour COM, à moins d'attribuer le contour entier de la commune à un des CP
                        */
                    --On attribue le contour entier de la commune à la première ZA
                    UPDATE fr.territory_za
                    SET gm_contour_natif = _municipality_with_many.gm_contour_natif
                    WHERE
                        co_cea = _municipality_with_many.first_za;

                    CONTINUE;
                --Il n'y en a qu'un
                ELSIF _nof = 1 THEN
                    RAISE NOTICE 'ERREUR : une seule ZA (%) avec des points adresses sur la commune %, alors qu''on en attendait %'
                    , _uniq
                    , _municipality_with_many.co_insee_commune
                    , _municipality_with_many.nb_za;
                    --On lui attribue le contour entier de la commune
                    UPDATE fr.territory_za
                    SET gm_contour_natif = _municipality_with_many.gm_contour_natif
                    WHERE
                        co_cea = _uniq;

                    CONTINUE;
                --Il y en a plusieurs (mais plus ou moins)
                ELSE
                    RAISE NOTICE 'ERREUR : % ZA avec des points adresses sur la commune %, alors qu''on en attendait %'
                    , _nof
                    , _municipality_with_many.co_insee_commune
                    , _municipality_with_many.nb_za;
                END IF;
            END IF;

            CREATE INDEX ix_tmp_geom_delivery_point_geom ON tmp_geom_delivery_point USING GIST(geom);

            -- delimit polygons (Voronoi) for the set of points
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point_voronoi(
                voronoi_id SERIAL
                , geom GEOMETRY(POLYGON)
                , co_cea CHAR(10)
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
                            , _municipality_with_many.gm_contour_natif
                        )
                    )).geom
            );
            CREATE INDEX ix_tmp_geom_delivery_point_voronoi_geom ON tmp_geom_delivery_point_voronoi USING GIST(geom);

            -- remains to affect ZA to each polygon
            WITH
            voronoi_has_za AS (
                SELECT
                    tmp_geom_delivery_point_voronoi.voronoi_id
                    , tmp_geom_delivery_point.co_cea
                    , COUNT(*) AS nb_pdi
                    , SUM(no_type_localisation) AS sum_no_type_localisation
                FROM tmp_geom_delivery_point
                    INNER JOIN tmp_geom_delivery_point_voronoi
                        ON ST_Within(tmp_geom_delivery_point.geom, tmp_geom_delivery_point_voronoi.geom)
                GROUP BY
                    tmp_geom_delivery_point_voronoi.voronoi_id, tmp_geom_delivery_point.co_cea
            )
            , voronoi_has_best_za AS (
                SELECT
                    voronoi_id
                    , FIRST(co_cea ORDER BY sum_no_type_localisation DESC) AS co_cea
                FROM voronoi_has_za
                GROUP BY voronoi_id
            )
            UPDATE tmp_geom_delivery_point_voronoi
            SET co_cea = voronoi_has_best_za.co_cea
            FROM voronoi_has_best_za
            WHERE voronoi_has_best_za.voronoi_id = tmp_geom_delivery_point_voronoi.voronoi_id
            ;

            WITH
            set_of_contour_by_za AS (
                WITH
                set_of_contours_delimited_by_municipality AS (
                    SELECT
                        (ST_Dump(
                            ST_Intersection(
                                _municipality_with_many.gm_contour_natif
                                , ST_Union(geom)
                            )
                        )).geom
                        , co_cea
                    FROM tmp_geom_delivery_point_voronoi
                    GROUP BY co_cea
                )
                , list_of_contours_with_included_flag AS (
                    SELECT
                        ROW_NUMBER() OVER() AS geom_id
                        --On considère qu'une géométrie est absorbante si sa superficie
                        --est au moins supérieure à la moitiée de celle de la plus grande géométrie du CP
                        , ST_Area(geom) > ((MAX(ST_Area(geom)) OVER(PARTITION BY co_cea))/2) AS est_absorbante
                        , geom
                        , co_cea
                    FROM set_of_contours_delimited_by_municipality
                )
                , list_of_contours_with_included_id AS (
                    SELECT
                        list_of_contours_with_included_flag.geom_id
                        , list_of_contours_with_included_flag.co_cea
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
                )
                , list_of_contours_with_final_za AS (
                    SELECT
                        list_of_contours_with_included_flag.geom
                        , (
                            WITH RECURSIVE search_graph(geom_id, co_cea, geom_id_absorbante, depth) AS (
                                SELECT g.geom_id, g.co_cea, g.geom_id_absorbante, 1
                                FROM list_of_contours_with_included_id g
                                WHERE g.geom_id = list_of_contours_with_included_flag.geom_id
                                UNION ALL
                                SELECT g.geom_id, g.co_cea, g.geom_id_absorbante, sg.depth + 1
                                FROM list_of_contours_with_included_id g, search_graph sg
                                WHERE g.geom_id = sg.geom_id_absorbante
                            )
                            SELECT co_cea FROM search_graph ORDER BY depth DESC LIMIT 1
                        ) AS co_cea
                        FROM list_of_contours_with_included_flag
                )
                --SELECT * FROM list_of_contours_with_final_za
                SELECT ST_Union(geom) AS geom, co_cea
                FROM list_of_contours_with_final_za
                GROUP BY co_cea
            )
            UPDATE fr.territory_za
            SET gm_contour_natif = set_of_contour_by_za.geom
            FROM set_of_contour_by_za
            WHERE
                territory_za.co_cea = set_of_contour_by_za.co_cea;

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS _context = PG_EXCEPTION_CONTEXT;
            RAISE '% : Erreur sur traitement commune % : % (%)', TO_CHAR(clock_timestamp(), 'HH24:MI:SS'), _municipality_with_many.co_insee_commune, SQLERRM, _context;
        END;
    END LOOP;

    FOR _municipality_with_many IN (
        SELECT
            tmp_municipality_with_many.co_insee_commune
            , tmp_municipality_with_many.nb_za
        FROM tmp_municipality_with_many
        ORDER BY tmp_municipality_with_many.co_insee_commune
    )
    LOOP
        /* NOTE
        GEOMETRY ST_Snap(GEOMETRY input, GEOMETRY reference, FLOAT tolerance);
        Snaps the vertices and segments of a geometry to another Geometry's vertices.
        The result geometry is the input geometry with the vertices snapped.
        */

        WITH
        contour_of_municipality_with_many_za AS (
            SELECT ST_Union(gm_contour_natif) AS gm_contour_natif
            FROM fr.territory_za
            WHERE
                co_insee_commune = _municipality_with_many.co_insee_commune
        )
        , snap_territory_around AS (
            SELECT
                territory_around.co_insee_commune
                , ST_Snap(
                    territory_around.gm_contour_natif
                    , contour_of_municipality_with_many_za.gm_contour_natif
                    , 1.0
                ) AS gm_contour_natif
            FROM contour_of_municipality_with_many_za
            INNER JOIN fr.territory_za AS territory_around
                -- ZA with common border (ST_Touches) from another municipality
                ON territory_around.gm_contour_natif && contour_of_municipality_with_many_za.gm_contour_natif
                AND territory_around.co_insee_commune != _municipality_with_many.co_insee_commune
        )
        UPDATE fr.territory_za
        SET gm_contour_natif = snap_territory_around.gm_contour_natif
        FROM snap_territory_around
        WHERE territory_za.co_insee_commune = snap_territory_around.co_insee_commune
        -- If no snapping occurs then the input geometry is returned unchanged.
        AND NOT ST_Equals(territory_za.gm_contour_natif, snap_territory_around.gm_contour_natif);

        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        _message := ' traité';
        IF _nrows_affected > 1 THEN _message := _message || 's'; END IF;
        CALL public.log_info(CONCAT('ST_Snap autour de ', _municipality_with_many.co_insee_commune, ' : #', _nrows_affected, _message));
    END LOOP;

    COMMIT;
END
$proc$ LANGUAGE plpgsql;
 */

/*
eval geometry of territories
    PART/1
        based level (COM_CP) : native geometry
    PART/2
        based level (COM_CP) : simplified geometry, as (WGS-84 Long/Lat SRID 4326)
    PART/3
        deal w/ holes
    PART/4
        apply SUPRA (geometry, area)
 */
SELECT drop_all_functions_if_exists('fr', 'set_territory_geometry');
CREATE OR REPLACE PROCEDURE fr.set_territory_geometry(
    dir_tmp VARCHAR
    , part_todo INT DEFAULT 1 | 2 | 4 | 8
    , location_min INT DEFAULT 4
    , department_test VARCHAR DEFAULT NULL
)
AS
$proc$
DECLARE
    _municipality_with_many_zipcodes RECORD;
    _uniq_zipcode CHAR(5);
    _nof_zipcodes INTEGER;
    _nrows_affected INTEGER;
    _context TEXT;
    _message VARCHAR;
BEGIN
    CALL public.log_info('Début du calcul des Contours');

    --
    -- PART/1 : initialize native geometry for based level (as COM_CP)
    --
    IF part_todo & 1 = 1 THEN
        DROP INDEX IF EXISTS ix_territory_gm_contour_natif;
        CALL public.log_info(
            message => 'Commande SH de suivi : watch -d -c "grep ''contours avec commune partielle'' ' || dir_tmp || '/SET_TERRITORY_GEOMETRY.notice.log | wc -l"'
            , stamped => FALSE
        );

        /*
        CALL public.log_info('Identification des Communes multi-CP');
        DROP TABLE IF EXISTS tmp_municipality_with_many_zipcodes;
        CREATE TEMPORARY TABLE tmp_municipality_with_many_zipcodes AS (
            SELECT
                territory.codgeo_com_parent AS codgeo
                , COUNT(*) AS nb_cp
                , FIRST(territory.codgeo_cp_parent) AS premier_cp
            FROM fr.territory
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
            AND (department_test IS NULL OR territory.codgeo_dep_parent = department_test)
            GROUP BY territory.codgeo_com_parent
            HAVING COUNT(*) > 1
        );
        CREATE UNIQUE INDEX ON tmp_municipality_with_many_zipcodes (codgeo);

        CALL public.log_info('Reset des contours COM/CP');
        UPDATE fr.territory
        SET gm_contour_natif = NULL
        WHERE nivgeo = 'COM_CP'
        -- TEST only evaluated for the department (others being reseted)
        --AND (department_test IS NULL OR territory.codgeo_dep_parent = department_test)
        ;

        -- COM_CP same as COM
        CALL public.log_info('Init des contours de communes entières');
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
        AND (department_test IS NULL OR territory.codgeo_dep_parent = department_test)
        ;

        -- COM_CP only part of COM (which is shared into many zipcodes)
        FOR _municipality_with_many_zipcodes IN (
            SELECT
                tmp_municipality_with_many_zipcodes.codgeo
                , tmp_municipality_with_many_zipcodes.nb_cp
                , tmp_municipality_with_many_zipcodes.premier_cp
                , commune_ign.geom AS gm_contour_natif
                , ST_SRID(commune_ign.geom) AS srid
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
                CALL public.log_info('Init des contours avec commune partielle : ' || _municipality_with_many_zipcodes.codgeo);

                -- set of all delivery points (PDI) for the current municipality
                CREATE TEMPORARY TABLE IF NOT EXISTS tmp_geom_delivery_point(
                    geom GEOMETRY
                    , co_postal CHAR(5)
                    , no_type_localisation INTEGER
                );
                TRUNCATE TABLE tmp_geom_delivery_point;
                DROP INDEX IF EXISTS ix_tmp_geom_delivery_point_geom;

            /*
            exec: 4/2023 with IGN of 3/2023

            no actives PDI (ok IGN: +IGN) for these municipalities (w/ 2 zipcodes)
            last 3 columns are: number of addresses, nof OFF points, nof ON points
                +IGN  2  01104-01200  2017-11-18  01200 (CHEZERY FORENS)        1  0  0
                +IGN  2  01269-01460  2017-11-18  01460 (NANTUA)                5  0  0
                +IGN  2  01313-01630  2017-11-18  01630 (PREVESSIN MOENS)       0  0  0
                +IGN  2  04074-04270  2017-11-18  04270 (ENTRAGES)              33  0  0
                +IGN  2  04109-04310  2017-11-18  04310 (MALLEFOUGASSE AUGES)   0  0  0
                +IGN  2  04204-04270  2017-11-18  04270 (SENEZ)                 1  0  0
                +IGN  2  05133-05240  2017-11-18  05240 (ST CHAFFREY)           0  0  0
                +IGN  2  07140-07300  2017-11-18  07300 (LEMPS)                 0  0  0
                +IGN  2  07198-07800  2017-11-18  07800 (ROMPON)                6  3  0
                +IGN  2  13019-13170  2017-11-18  13170 (CABRIES)               0  0  0
                +IGN  2  13102-13700  2018-07-07  13700 (ST VICTORET)           0  0  0
                +IGN  2  18101-18350  2017-11-18  18350 (GERMIGNY L EXEMPT)     0  0  0
                +IGN  2  18134-36260  2017-11-18  36260 (LURY SUR ARNON)        0  0  0
                +IGN  2  24364-24120  2020-04-04  24120 (COLY ST AMAND)         191  0  0
                +IGN  2  26067-26340  2017-11-18  26340 (CHALANCON)             0  0  0
                +IGN  2  2A249-20100  2017-11-18  20100 (PROPRIANO)             0  0  0
                +IGN  2  2B043-20226  2017-11-18  20226 (BRANDO)                0  0  0
                +IGN  2  2B049-20260  2017-11-18  20260 (CALENZANA)             2  1  0
                +IGN  2  2B314-20217  2017-11-18  20217 (SANTO PIETRO DI TENDA) 2  19  0
                +IGN  2  32121-32130  2017-11-18  32130 (ENDOUFIELLE)           0  0  0
                +IGN  2  34209-34450  2017-11-18  34450 (PORTIRAGNES)           0  0  0
                +IGN  2  44074-44620  2017-11-18  44620 (INDRE)                 0  0  0
                +IGN  2  45269-45380  2017-11-18  45380 (ST AY)                 0  0  0
                +IGN  2  73013-73530  2017-11-18  73530 (ALBIEZ MONTROND)       0  0  0
                +IGN  2  73227-73600  2022-10-15  73600 (COURCHEVEL)            0  0  0
                +IGN  2  74208-74480  2018-03-17  74480 (PASSY)                 0  0  0
                +IGN  2  91665-91140  2017-11-18  91140 (LA VILLE DU BOIS)      0  0  0
                +IGN  2  95120-95760  2017-11-18  95760 (BUTRY SUR OISE)        0  0  0
                +IGN  2  97101-97142  2017-11-18  97142 (LES ABYMES)            181  93  0
                +IGN  2  97301-97353  2017-11-18  97353 (REGINA)                13  0  0
            no GEOMETRY (ko IGN: -IGN)
            particular case: 27676 : 01/01/2023 : Les Trois Lacs est rétablie (IGN not up todate!)
                -IGN  1  27676-27700  2022-12-11  27700 (LES TROIS LACS)        509  14  448
                -IGN  1  27676-27940  2022-12-11  27940 (LES TROIS LACS)        415  61  371
                -IGN  1  97501-97500  2017-11-18  97500 (ST PIERRE ET MIQUELON) 6  0  0
                -IGN  1  97502-97500  2017-11-18  97500 (ST PIERRE ET MIQUELON) 30  0  0
                -IGN  1  97701-97133  2017-11-18  97133 (ST BARTHELEMY)         1281  0  1230
                -IGN  1  97801-97150  2017-11-18  97150 (ST MARTIN)             7045  28  6727
             */
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
                    -- at least street-center (=4)
                    AND pdi_no_type_localisation_coord >= location_min
                    AND pdi_coord_native IS NOT NULL
                    /* TEST PDI très proches géographiquement :
                    AND pdi_id IN (10652325, 24672957)
                    AND pdi_coord && (SELECT ST_Extent(ST_Buffer(pdi_coord, 100)) AS etendue FROM pdi_view WHERE pdi_id = 10652325)
                    */
                );

                --geom NULL pouvant venir de ST_Transform_BC2A
                DELETE FROM tmp_geom_delivery_point WHERE geom IS NULL
                /* a voir : suppression des points hors commune, ou sur le contour commune
                OR NOT ST_ContainsProperly((SELECT ST_Buffer(geom, -200) FROM fr.admin_express_commune WHERE insee_com = '27049'), geom)
                */
                ;

                SELECT COUNT(DISTINCT co_postal), NULLIF(UNIQUE_AGG(co_postal), 'INIT_VALUE')
                INTO _nof_zipcodes, _uniq_zipcode
                FROM tmp_geom_delivery_point;

                --Le nombre de CP avec des points adresses est différent du nombre attendu
                IF _nof_zipcodes != _municipality_with_many_zipcodes.nb_cp THEN
                    --Il n'y en a aucun
                    IF _nof_zipcodes = 0 THEN
                        RAISE NOTICE 'ERREUR : aucun CP avec des points adresses sur la commune %, alors qu''on en attendait plusieurs'
                        , _municipality_with_many_zipcodes.codgeo;
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
                        RAISE NOTICE 'ERREUR : un seul CP (%) avec des points adresses sur la commune %, alors qu''on en attendait %'
                        , _uniq_zipcode
                        , _municipality_with_many_zipcodes.codgeo
                        , _municipality_with_many_zipcodes.nb_cp;
                        --On lui attribue le contour entier de la commune
                        UPDATE fr.territory
                        SET gm_contour_natif = _municipality_with_many_zipcodes.gm_contour_natif
                        WHERE territory.nivgeo = 'COM_CP'
                        AND territory.codgeo = CONCAT_WS('-', _municipality_with_many_zipcodes.codgeo, _uniq_zipcode);

                        CONTINUE;
                    --Il y en a plusieurs (mais plus ou moins)
                    ELSE
                        RAISE NOTICE 'ERREUR : % CP avec des points adresses sur la commune %, alors qu''on en attendait %'
                        , _nof_zipcodes
                        , _municipality_with_many_zipcodes.codgeo
                        , _municipality_with_many_zipcodes.nb_cp;
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
                            )
                        )).geom
                );
                CREATE INDEX ix_tmp_geom_delivery_point_voronoi_geom ON tmp_geom_delivery_point_voronoi USING GIST(geom);

                --SELECT ST_Transform(geom, 4326) FROM tmp_geom_delivery_point_voronoi UNION ALL (SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022')
                --SELECT ST_Transform(geom, 4326) FROM tmp_geom_delivery_point_voronoi WHERE ST_ContainsProperly((SELECT geom FROM fr.admin_express_commune WHERE insee_com = '27022'), geom)

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
                        --SELECT * FROM (SELECT (ST_Dump(gm_contour)).geom, codgeo FROM fr.territory WHERE nivgeo IN ('COM_CP', 'CP') and gm_contour IS NOT NULL) AS sous_requete WHERE ST_GeometryType(geom) != 'ST_Polygon'
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

        CALL public.log_info('Indexation Territoire : Contour natif (COM_CP)');
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
            _message := ' traité';
            IF _nrows_affected > 1 THEN _message := _message || 's'; END IF;
            CALL public.log_info(CONCAT('ST_Snap autour de ', _municipality_with_many_zipcodes.codgeo, ' : #', _nrows_affected, _message));
        END LOOP;

        COMMIT;
         */

        CALL fr.set_municipality_subsection_geometry(
            subsection => 'ZA'
            , location_min => location_min
        );
    END IF;

    --
    -- PART/2 : initialize simplified geometry for based level (as COM_CP)
    --
    IF part_todo & 2 = 2 THEN
        DROP INDEX IF EXISTS ix_territory_gm_contour;

        CALL public.log_info(
            message => 'Commande SH de suivi : watch -d -c "cat ' || dir_tmp || '/SET_TERRITORY_GEOMETRY.notice.log | grep -o -P ''[0-9]+ traité'' | grep -o -P ''[0-9]+'' | awk ''{ SUM += \$1} END { print SUM }''"'
            , stamped => FALSE
        );

        CALL public.log_info('Reset des contours simplifiés');
        UPDATE fr.territory SET gm_contour = NULL;

        CALL public.log_info('Calcul des contours reprojetés (WGS84) simplifiés');
        CALL fr.ST_SimplifyTerritory(
            levels => ARRAY['COM_CP']
            , to_srid => 4326
            , bbox_split_over => 1000
            , tolerance => 100
        );

        COMMIT;
    END IF;

    --
    -- PART/3 : merge holes
    --
    IF part_todo & 4 = 4 THEN
        CALL public.log_info('Fusion des trous');
        CALL fr.set_territory_geometry_merge_hole();

        COMMIT;
    END IF;

    --
    -- PART/4 : eval area (COM_CP first), then SUPRA for (simplified geometry, area)
    --
    IF part_todo & 8 = 8 THEN
        -- unit= hm2 (1/100 km2)
        -- see: https://gis.stackexchange.com/questions/169422/how-does-st-area-in-postgis-work
        UPDATE fr.territory
        SET superficie = ROUND(ST_Area(ST_Transform(gm_contour_natif, 4326)::GEOGRAPHY)/10000)
        WHERE nivgeo = 'COM_CP';

        DROP INDEX IF EXISTS public.ix_territory_gm_contour;

        CALL public.log_info('remontée SUPRA pour superficie et contour simplifié');
        PERFORM fr.set_territory_supra(
            schema_name => 'fr'
            , table_name => 'territory'
            , base_level => 'COM_CP'
            , columns_agg => ARRAY['gm_contour', 'superficie']
            , update_mode => TRUE
        );

        CALL public.log_info('Indexation Territoire : Contour simplifié');
        CREATE INDEX IF NOT EXISTS ix_territory_gm_contour ON fr.territory USING GIST(nivgeo, gm_contour);

        COMMIT;
    END IF;

    CALL public.log_info('Fin du calcul des Contours');
END
$proc$ LANGUAGE plpgsql;

/*
SELECT ST_Collect(ST_Union(gm_contour), ST_Transform(ST_Union(gm_contour_natif), 4326))
FROM fr.territory WHERE nivgeo = 'COM_CP'
GROUP BY codgeo_dep_parent;

SELECT gm_contour, codgeo FROM fr.territory WHERE nivgeo = 'DEP';
SELECT gm_contour, codgeo FROM fr.territory WHERE nivgeo = 'ARR' AND codgeo_dep_parent = '86';
SELECT ST_Collect(
    gm_contour
    , ST_Transform(gm_contour_natif, 4326))
    , gm_contour
    , ST_Transform(gm_contour_natif, 4326)
    , codgeo
FROM fr.territory WHERE nivgeo = 'COM_CP' AND codgeo_arr_parent = '863';
SELECT gm_contour, codgeo_dep_parent FROM fr.territory WHERE nivgeo = 'ARR' LIMIT 10;

SELECT * FROM fr.territory WHERE nivgeo = 'COM_CP' AND gm_contour && (
	SELECT ST_Extent(gm_contour) FROM fr.territory WHERE nivgeo = 'COM_CP' AND libgeo ilike '%Mastribus%'
);
 */

-- update geometry with holes
SELECT drop_all_functions_if_exists('fr', 'set_territory_geometry_merge_hole');
CREATE OR REPLACE PROCEDURE fr.set_territory_geometry_merge_hole(
)
AS
$proc$
DECLARE
    _holes RECORD;
    _new_geom GEOMETRY;
BEGIN
    FOR _holes IN (
        WITH union_geom AS (
            SELECT ST_Union(gm_contour) AS geom
            FROM fr.territory
            WHERE nivgeo = 'COM_CP'
        )
        , polygons AS (
            SELECT (ST_Dump(geom)).* FROM union_geom
        )
        , rings AS (
            /*
            ST_DumpRings
            A set-returning function (SRF) that extracts the rings of a polygon. It returns a set of geometry_dump rows, each containing a geometry (geom field) and an array of integers (path field).
            The geom field contains each ring as a POLYGON. The path field is an integer array of length 1 containing the polygon ring index. The exterior ring (shell) has index 0. The interior rings (holes) have indices of 1 and higher.
             */
            SELECT
                polygons.path AS polygon_path
                , (ST_DumpRings(
                    polygons.geom
                )).*
            FROM polygons
        )
        , rings_analyze AS (
            SELECT
                ROW_NUMBER() OVER() AS polygon_id
                , (ROW_NUMBER() OVER (PARTITION BY polygon_path ORDER BY path)) = 1 AS is_exterior
                , polygon_path
                , ST_Area(ST_Transform(geom, 3857)) AS area
                , geom
            FROM rings
        )
        , hole_and_next_territory AS (
            SELECT
                rings_analyze.polygon_id
                , rings_analyze.area
                , territory.codgeo
                , ST_Length(ST_Intersection(territory.gm_contour, rings_analyze.geom)) AS common_length
                , rings_analyze.geom
                , territory.gm_contour
            FROM rings_analyze
                LEFT OUTER JOIN fr.territory
                    ON territory.nivgeo = 'COM_CP' AND ST_Intersects(territory.gm_contour, rings_analyze.geom)
            WHERE NOT is_exterior
        )
        , hole_and_best_next_territory AS (
            SELECT
                polygon_id
                , FIRST(area) AS area
                , ARRAY_AGG(codgeo ORDER BY common_length DESC) AS codgeos_voisin
                , FIRST(geom) AS geom
                , FIRST(gm_contour ORDER BY common_length DESC) AS gm_contour
            FROM hole_and_next_territory
            --WHERE common_length > 0
            GROUP BY polygon_id
        )
        -- ST_Collect not used!
        SELECT * --, ST_Collect(geom, gm_contour)
        FROM hole_and_best_next_territory
        ORDER BY area DESC
    )
    LOOP
        -- gt 1 km2, why ?
        IF _holes.area > 1000000 THEN
            RAISE NOTICE 'Trou d une surface anormale grande (%), voisin de %, ignoré', _holes.area, _holes.codgeos_voisin;
            CONTINUE;
        END IF;

        RAISE NOTICE 'Trou d une surface de %, voisin de %, unification avec le premier voisin', _holes.area, _holes.codgeos_voisin[1];
        UPDATE fr.territory
        SET gm_contour = ST_Multi(
            ST_Union(
                ST_Snap(
                    gm_contour
                    , _holes.geom
                    , 0.000001
                )
                , ST_Snap(
                    _holes.geom
                    , gm_contour
                    , 0.000001
                )
            )
        )
        WHERE territory.nivgeo = 'COM_CP'
        AND territory.codgeo = _holes.codgeos_voisin[1]
        RETURNING gm_contour INTO _new_geom;

        --RAISE NOTICE 'Difference et Snap avec les voisins %', _holes.codgeos_voisin;
        --Exemple de chevauchement et trou : {87006-87360, 87200-87360}
        UPDATE fr.territory
        SET gm_contour = ST_Multi(
            -- Pas nécessaire ? ST_Snap(
                ST_Difference(
                    gm_contour
                    , _new_geom
                )
            --	, _new_geom
            --	, 0.000001
            --)
        )
        WHERE territory.nivgeo = 'COM_CP'
        AND territory.codgeo = ANY(_holes.codgeos_voisin)
        AND territory.codgeo != _holes.codgeos_voisin[1]
        AND ST_Overlaps(gm_contour, _new_geom);
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

/* TEST

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_territory_backup AS (SELECT * FROM fr.territory WHERE nivgeo = 'COM_CP');

WITH liste_voisins_trous AS (
    SELECT UNNEST(
    ARRAY[
    '{27422-27430, 27691-27940, 27097-27700, 27016-27700, 27495-27700, 27249-27600, 27022-27600, 27635-27700, 27683-27700, 27332-27400}'
    , '{66072-66800, 66005-66760, 66181-66800, 66025-66760, 66202-66120, 66167-66800, 66218-66760}'
    , '{01453-01260, 01039-01350, 01415-01510, 01036-01260, 01138-01350}'
    , '{25325-25530, 25596-25530, 25625-25510}'
    , '{22166-22710}'
    , '{78217-78680, 78029-78410, 78267-78440}'
    , '{70323-70210, 70013-70210, 70419-70210, 88176-88240}'
    , '{17486-17840, 17337-17190}'
    , '{88153-88700, 88298-88700, 88527-88700}'
    , '{52124-52400, 52504-52400, 52135-52400}'
    , '{88153-88700, 88298-88700}'
    , '{29040-29217, 29201-29810, 29282-29217}'
    , '{88351-88370, 88048-88370}'
    , '{31573-31590, 81140-81500, 81025-81500}'
    , '{27014-27400, 27339-27400, 27003-27400}'
    , '{19146-19460, 19255-19700}'
    , '{59327-59167, 59375-59870, 59239-59148}'
    , '{87006-87360, 87200-87360}'
    , '{97414-97421, 97403-97414, 97416-97410, 97416-97432}'
    ]) AS liste
)
SELECT liste_voisins_trous.liste
    , ST_Collect(ARRAY[
        ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(
        ST_InternalBoundary((SELECT ST_Collect(gm_contour) FROM tmp_territoire_backup WHERE nivgeo = 'COM_CP' AND codgeo = ANY(liste_voisins_trous.liste::VARCHAR[])))
        , 4326), 3857), 20), 4326)
        , (SELECT ST_Collect(gm_contour) FROM tmp_territoire_backup WHERE nivgeo = 'COM_CP' AND codgeo = ANY(liste_voisins_trous.liste::VARCHAR[]))
    ]) AS avant
    , ST_Collect(ARRAY[
        ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(
        ST_InternalBoundary((SELECT ST_Collect(gm_contour) FROM territory WHERE nivgeo = 'COM_CP' AND codgeo = ANY(liste_voisins_trous.liste::VARCHAR[])))
        , 4326), 3857), 20), 4326)
        , (SELECT ST_Collect(gm_contour) FROM territory WHERE nivgeo = 'COM_CP' AND codgeo = ANY(liste_voisins_trous.liste::VARCHAR[]))
    ]) AS apres
FROM liste_voisins_trous
 */
