/***
 * FR-TERRITORY geometry management
 */

/* NOTE
 to read:
 http://postgis.net/workshops/postgis-intro/geometries.html
 https://postgis.net/docs/reference.html
 https://en.wikipedia.org/wiki/DE-9IM
 https://postgis.net/workshops/postgis-intro/de9im.html
 */

/* NOTE
 * type of location (LAPOSTE RAN/GEOPAD)
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

-- check validity of municipality_subsection
SELECT drop_all_functions_if_exists('fr', 'check_municipality_subsection');
CREATE OR REPLACE PROCEDURE fr.check_municipality_subsection(
    municipality_subsection VARCHAR                      -- ZA or COM_CP
    , check_level BOOLEAN DEFAULT TRUE
    , check_territory BOOLEAN DEFAULT TRUE
)
AS
$proc$
BEGIN
    IF NOT municipality_subsection = ANY(ARRAY['ZA', 'COM_CP']) THEN
        RAISE 'argument municipality_subsection % non valide, choix possibles {ZA, COM_CP}', municipality_subsection;
    END IF;
    IF check_level AND NOT EXISTS(
        SELECT 1 FROM public.territory_level WHERE level = municipality_subsection
    ) THEN
        RAISE 'manque niveau % dans public.territory_level', municipality_subsection;
    END IF;
    IF check_territory AND NOT EXISTS(
        SELECT 1 FROM fr.territory WHERE nivgeo = municipality_subsection
    ) THEN
        RAISE 'manque niveaux % dans fr.territory', municipality_subsection;
    END IF;
END
$proc$ LANGUAGE plpgsql;

/*
calculate native geometry of subsection(s) of municipalities
    PART/1
        reset
    PART/2
        w/ only 1 municipality_subsection
    PART/3
        w/ many subsections
    PART/4
        w/ many subsections : snap
 */
SELECT drop_all_functions_if_exists('fr', 'set_municipality_subsection_geometry');
CREATE OR REPLACE PROCEDURE fr.set_municipality_subsection_geometry(
    part_todo INT DEFAULT 1 | 2 | 4 | 8
    , municipality_subsection VARCHAR DEFAULT 'ZA'
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
    CALL fr.check_municipality_subsection(municipality_subsection => municipality_subsection);

    IF part_todo & 1 = 1 THEN
        -- reset
        CALL public.log_info('Reset des contours natifs ' || municipality_subsection);
        UPDATE fr.territory
        SET gm_contour_natif = NULL
        WHERE nivgeo = municipality_subsection
        -- TEST only evaluated for the department (others being reseted)
        AND (department_test IS NULL OR territory.codgeo_dep_parent = department_test);
    END IF;

    CALL public.log_info('Identification des Communes multi-' || municipality_subsection);
    DROP TABLE IF EXISTS tmp_municipality_with_many_subsections;
    CREATE TEMPORARY TABLE tmp_municipality_with_many_subsections AS (
        SELECT
            za.codgeo_com_parent co_insee_commune
            , COUNT(*) AS nb_subsections
            , CASE
                WHEN municipality_subsection = 'ZA' THEN FIRST(za.codgeo)
                WHEN municipality_subsection = 'COM_CP' THEN FIRST(za.codgeo_cp_parent)
            END first_subsection
        FROM fr.territory za
        WHERE
            nivgeo = municipality_subsection
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

    IF part_todo & 2 = 2 THEN
        -- only 1 municipality_subsection : same contour as municipality
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
            nivgeo = municipality_subsection
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
    END IF;

    IF part_todo & 4 = 4 THEN
        -- many subsections : divide contour between each municipality_subsection (VORONOI w/ delivery points)
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
                            WHEN municipality_subsection = 'ZA' THEN co_adr_za
                            WHEN municipality_subsection = 'COM_CP' THEN co_postal
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
                            , municipality_subsection
                            , _municipality_with_many_subsections.co_insee_commune;
                        -- set contour of municipality to first municipality_subsection
                        UPDATE fr.territory
                        SET gm_contour_natif = _municipality_with_many_subsections.gm_contour_natif
                        WHERE
                            nivgeo = municipality_subsection
                            AND
                            codgeo = CASE
                                WHEN municipality_subsection = 'ZA' THEN
                                    _municipality_with_many_subsections.first_subsection
                                WHEN municipality_subsection = 'COM_CP' THEN
                                    CONCAT_WS('-'
                                        , _municipality_with_many_subsections.co_insee_commune
                                        , _municipality_with_many_subsections.first_subsection
                                    )
                                END;

                        CONTINUE;
                    ELSIF _nof = 1 THEN
                        RAISE NOTICE 'INFO : 1 % (%) avec des points adresses sur la commune %, alors qu''on en attendait %'
                            , municipality_subsection
                            , _uniq
                            , _municipality_with_many_subsections.co_insee_commune
                            , _municipality_with_many_subsections.nb_subsections;
                        -- set contour of municipality to uniq municipality_subsection
                        UPDATE fr.territory
                        SET gm_contour_natif = _municipality_with_many_subsections.gm_contour_natif
                        WHERE
                            codgeo = CASE
                                WHEN municipality_subsection = 'ZA' THEN
                                    _uniq
                                WHEN municipality_subsection = 'COM_CP' THEN
                                    CONCAT_WS('-'
                                        , _municipality_with_many_subsections.co_insee_commune
                                        , _uniq
                                    )
                                END;

                        CONTINUE;
                    -- less or more : need to evaluate contour for each
                    ELSE
                        RAISE NOTICE 'INFO : % % avec des points adresses sur la commune %, alors qu''on en attendait %'
                            , _nof
                            , municipality_subsection
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

                -- remains to affect municipality_subsection to each polygon
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
                        tmp_geom_delivery_point_voronoi.voronoi_id
                        , tmp_geom_delivery_point.subsection_id
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
                            --est au moins supérieure à la moitié de celle de la plus grande géométrie du CP
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
                    nivgeo = municipality_subsection
                    AND
                    codgeo = CASE
                        WHEN municipality_subsection = 'ZA' THEN
                            set_of_contour_by_subsection.subsection_id
                        WHEN municipality_subsection = 'COM_CP' THEN
                            CONCAT_WS('-'
                                , _municipality_with_many_subsections.co_insee_commune
                                , set_of_contour_by_subsection.subsection_id
                            )
                        END;

            EXCEPTION WHEN OTHERS THEN
                GET STACKED DIAGNOSTICS _context = PG_EXCEPTION_CONTEXT;
                RAISE '% : Erreur sur traitement % pour la commune % : % (%)'
                    , TO_CHAR(clock_timestamp(), 'HH24:MI:SS')
                    , municipality_subsection
                    , _municipality_with_many_subsections.co_insee_commune
                    , SQLERRM
                    , _context;
            END;
        END LOOP;
    END IF;

    IF part_todo & 8 = 8 THEN
        -- snap
        /* NOTE
        be careful: this part needs all municipalities
        test w/ a department can be KO for municipalities near another missing department !
         */
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
                    nivgeo = municipality_subsection
                    AND
                    codgeo_com_parent = _municipality_with_many_subsections.co_insee_commune
            )
            , snap_territory_around AS (
                SELECT
                    territory_around.codgeo_com_parent co_insee_commune
                    , territory_around.codgeo
                    , ST_Snap(
                        territory_around.gm_contour_natif
                        , contour_of_municipality_with_many_subsections.gm_contour_natif
                        , 1.0
                    ) AS gm_contour_natif
                FROM contour_of_municipality_with_many_subsections
                INNER JOIN fr.territory AS territory_around
                    ON territory_around.nivgeo = municipality_subsection
                    AND
                    -- municipality_subsection with common border (+/-) from another municipality
                    /* NOTE
                    ST_Overlaps
                    https://gis.stackexchange.com/questions/422759/efficient-combination-of-st-intersects-and-not-st-touches
                    ST_Overlaps(
                        territory_around.gm_contour_natif
                        , contour_of_municipality_with_many_subsections.gm_contour_natif
                    )
                    or
                    DE9IM, faster!
                     */
                    territory_around.gm_contour_natif && contour_of_municipality_with_many_subsections.gm_contour_natif
                    /* NOTE
                    error w/o DE9IM filter: 17240, 17282 -> 2 empty zones
                    (due to 17240226IS in 2 parts)
                     */
                    -- for 2 overlapping polygonal geometries: dim[interior(a) ∩ interior(b)] = 2
                    AND ST_Relate(
                        territory_around.gm_contour_natif
                        , contour_of_municipality_with_many_subsections.gm_contour_natif
                        , '2********'
                    )
                    AND territory_around.codgeo_com_parent != _municipality_with_many_subsections.co_insee_commune
            )
            UPDATE fr.territory
            SET gm_contour_natif = snap_territory_around.gm_contour_natif
            FROM snap_territory_around
            WHERE
                nivgeo = municipality_subsection
                AND
                /* NOTE
                department = '17'
                w/ DE9IM filter, error empty zone for 172822223X (same contour as 172822223V, following this update!)
                --codgeo_com_parent = snap_territory_around.co_insee_commune
                need to be more precise : w/ codgeo itself
                 */
                territory.codgeo = snap_territory_around.codgeo
                -- If no snapping occurs then the input geometry is returned unchanged.
                AND NOT ST_Equals(territory.gm_contour_natif, snap_territory_around.gm_contour_natif);

            GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
            _message := ' traité';
            IF _nrows_affected > 1 THEN _message := _message || 's'; END IF;
            CALL public.log_info(CONCAT('ST_Snap autour de ', _municipality_with_many_subsections.co_insee_commune, ' : #', _nrows_affected, _message));
        END LOOP;
    END IF;

    COMMIT;
END
$proc$ LANGUAGE plpgsql;

/*
calculate geometry of territories
    PART/1
        based level (COM_CP|ZA) : native geometry
    PART/2
        based level (COM_CP|ZA) : simplified geometry, as (WGS-84 Long/Lat SRID 4326)
    PART/3
        deal w/ holes
    PART/4
        based level (COM_CP|ZA) : eval area
    PART/5
        apply SUPRA : simplified geometry and area

    TODO
    PART/6
        native geometry for other levels when available (administrative cuttings, w/ IGN)
 */
SELECT drop_all_functions_if_exists('fr', 'set_territory_geometry');
CREATE OR REPLACE PROCEDURE fr.set_territory_geometry(
    part_todo INT DEFAULT 1 | 2 | 4 | 8 | 16
    , municipality_subsection VARCHAR DEFAULT 'ZA'
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
    CALL fr.check_municipality_subsection(municipality_subsection => municipality_subsection);
    CALL public.log_info('Début du calcul des Contours');

    --
    -- PART/1 : initialize native geometry for based level (as COM_CP)
    --

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

    IF part_todo & 1 = 1 THEN
        CALL fr.drop_territory_index(drop_case => 'ONLY_GEOM_NATIVE');

        CALL public.log_info(
            message => 'Commande SH de suivi : watch -d -c "grep ''contours avec commune partielle'' $POW_DIR_TMP/FR_TERRITORY_GEOMETRY.notice.log | wc -l"'
            , stamped => FALSE
        );

        CALL fr.set_municipality_subsection_geometry(
            municipality_subsection => municipality_subsection
            , location_min => location_min
        );
    END IF;

    --
    -- PART/2 : initialize simplified geometry for based level (municipality_subsection)
    --
    IF part_todo & 2 = 2 THEN
        CALL fr.drop_territory_index(drop_case => 'ONLY_GEOM_WORLD');

        CALL public.log_info(
            message => 'Commande SH de suivi : watch -d -c "cat $POW_DIR_TMP/FR_TERRITORY_GEOMETRY.notice.log | grep -o -P ''[0-9]+ traité'' | grep -o -P ''[0-9]+'' | awk ''{ SUM += \$1} END { print SUM }''"'
            , stamped => FALSE
        );

        CALL public.log_info('Reset des contours simplifiés');
        UPDATE fr.territory SET gm_contour = NULL;

        CALL public.log_info('Calcul des contours reprojetés (WGS84) simplifiés');
        CALL fr.ST_SimplifyTerritory(
            levels => ARRAY[municipality_subsection]
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
        CALL fr.set_territory_geometry_merge_hole(
            municipality_subsection => municipality_subsection
        );

        COMMIT;
    END IF;

    --
    -- PART/4 : eval area (municipality_subsection first), then SUPRA for (simplified geometry, area)
    --
    IF part_todo & 8 = 8 THEN
        -- unit= hm2 (1/100 km2)
        -- see: https://gis.stackexchange.com/questions/169422/how-does-st-area-in-postgis-work

        CALL public.log_info('calcul : (superficie)');
        UPDATE fr.territory
        SET superficie = ROUND(ST_Area(ST_Transform(gm_contour_natif, 4326)::GEOGRAPHY)/10000)
        WHERE nivgeo = municipality_subsection;

        COMMIT;
    END IF;

    --
    -- PART/5 : eval SUPRA for (simplified geometry, area)
    IF part_todo & 16 = 16 THEN
        CALL fr.drop_territory_index(drop_case => 'ONLY_GEOM_WORLD');

        CALL public.log_info('remontée SUPRA : (superficie, contour simplifié)');
        PERFORM fr.set_territory_supra(
            schema_name => 'fr'
            , table_name => 'territory'
            , base_level => municipality_subsection
            , columns_agg => ARRAY['gm_contour', 'superficie']
            , update_mode => TRUE
        );

        CALL fr.set_territory_index(set_case => 'ONLY_GEOM_WORLD');

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
    municipality_subsection VARCHAR DEFAULT 'ZA'
    , simulation BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _holes RECORD;
    _old_geom GEOMETRY;
    _new_geom GEOMETRY;
BEGIN
    FOR _holes IN (
        WITH union_geom AS (
            SELECT ST_Union(gm_contour) AS geom
            FROM fr.territory
            WHERE nivgeo = municipality_subsection
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
                    ON territory.nivgeo = municipality_subsection
                    AND ST_Intersects(territory.gm_contour, rings_analyze.geom)
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

        IF NOT simulation THEN
            SELECT ST_Multi(
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
            INTO _new_geom
            FROM fr.territory
            WHERE territory.nivgeo = municipality_subsection
            AND territory.codgeo = _holes.codgeos_voisin[1];

            IF ST_GeometryType(_new_geom) != 'ST_MultiPolygon' THEN
                RAISE NOTICE 'ERREUR géométrie non Multi % : %', municipality_subsection, _holes.codgeos_voisin[1];
                CONTINUE;
            END IF;

            UPDATE fr.territory
            SET gm_contour = _new_geom
            WHERE territory.nivgeo = municipality_subsection
            AND territory.codgeo = _holes.codgeos_voisin[1];

            /*
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
            WHERE territory.nivgeo = municipality_subsection
            AND territory.codgeo = _holes.codgeos_voisin[1]
            RETURNING gm_contour INTO _new_geom;
             */

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
            WHERE territory.nivgeo = municipality_subsection
            AND territory.codgeo = ANY(_holes.codgeos_voisin)
            AND territory.codgeo != _holes.codgeos_voisin[1]
            AND ST_Overlaps(gm_contour, _new_geom);
        END IF;
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
