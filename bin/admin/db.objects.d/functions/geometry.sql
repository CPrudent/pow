/***
 * add GEOMETRY facilities
 */

-- convert '(x,y)' to POINT
SELECT public.drop_all_functions_if_exists('public', 'convert_xy_to_point');
CREATE OR REPLACE FUNCTION convert_xy_to_point(
    coord TEXT
    , srid INTEGER
    , inverse BOOLEAN DEFAULT FALSE
    )
RETURNS GEOMETRY(POINT) AS
$func$
DECLARE
    _x DOUBLE PRECISION;
    _y DOUBLE PRECISION;
BEGIN
    _x := REPLACE(SUBSTRING(coord FROM '^([0-9\-\, ]+|[0-9\-\.]+)'), ', ', '.')::DOUBLE PRECISION;
    _y := REPLACE(SUBSTRING(coord FROM '([0-9\-\, ]+|[0-9\-\.]+)$'), ', ', '.')::DOUBLE PRECISION;
    IF inverse THEN
        RETURN st_setsrid(st_makepoint(_y, _x), srid);
    ELSE
        RETURN st_setsrid(st_makepoint(_x, _y), srid);
    END IF;
END
$func$ LANGUAGE plpgsql;

-- convert POINT to '(y,x)'
SELECT public.drop_all_functions_if_exists('public', 'convert_point_to_lat_lng');
CREATE OR REPLACE FUNCTION public.convert_point_to_lat_lng(
    geom IN GEOMETRY(POINT)
    )
RETURNS VARCHAR AS
$$
DECLARE
BEGIN
    IF geom IS NULL THEN RETURN NULL; END IF;
    IF NULLIF(ST_Srid(geom),0) IS NULL THEN RAISE 'SRID indéfini'; END IF;
    IF ST_Srid(geom) != 4326 THEN
        geom := ST_Transform(geom, 4326);
    END IF;
    RETURN CONCAT_WS(',',ST_Y(geom),ST_X(geom));
END
$$ LANGUAGE plpgsql;

/* TEST
SELECT convert_point_to_lat_lng(ST_MakePoint(0,15))
SELECT convert_point_to_lat_lng(NULL::GEOMETRY)
SELECT convert_point_to_lat_lng(ST_SetSRID(ST_MakePoint(0,15),2154))
 */

-- eval distance between a point and a set of polygons
SELECT public.drop_all_functions_if_exists('public', 'ST_DistanceExterior');
CREATE OR REPLACE FUNCTION ST_DistanceExterior(
    point_in GEOMETRY(POINT)
    , geoms GEOMETRY(MULTIPOLYGON)
    )
RETURNS DOUBLE PRECISION AS
$func$
DECLARE
    _record RECORD;
BEGIN
    IF ST_Contains(geoms, point_in) THEN
        FOR _record IN SELECT (ST_Dump(geoms)).geom AS geom LOOP
            IF ST_Contains(_record.geom, point_in) THEN
                RETURN (ST_Distance(point_in, ST_ExteriorRing(_record.geom)));
            END IF;
        END LOOP;
    ELSE
        RETURN -ST_Distance(point_in, geoms);
    END IF;
END
$func$ LANGUAGE plpgsql;

/* NOTE
improve ST_RemoveRepeatedPoints()
see: https://postgis.net/docs/ST_RemoveRepeatedPoints.html
 */
SELECT public.drop_all_functions_if_exists('public','ST_RemoveRepeatedPoints');
CREATE OR REPLACE FUNCTION public.ST_RemoveRepeatedPoints(
    geom IN GEOMETRY
    , tolerance FLOAT8
    )
RETURNS GEOMETRY AS
$$
DECLARE
    _repeated_point_id INTEGER;
    _return GEOMETRY;
    _nrows INTEGER;
BEGIN
    --RAISE NOTICE 'Utilisation version BC2A de ST_RemoveRepeatedPoints';
    --DROP TABLE IF EXISTS tmp_remove_repeated_points_bc2a;
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_remove_repeated_points_bc2a (
        point_id SERIAL
        ,geom GEOMETRY
        ,repeated_points_id INTEGER[]
        ,nb_repeated_points INTEGER
    );
    TRUNCATE TABLE tmp_remove_repeated_points_bc2a;
    DROP INDEX IF EXISTS idx_tmp_remove_repeated_points_bc2a_point_id;
    DROP INDEX IF EXISTS idx_tmp_remove_repeated_points_bc2a_geom;
    DROP INDEX IF EXISTS idx_tmp_remove_repeated_points_bc2a_repeated_points_id;
    DROP INDEX IF EXISTS idx_tmp_remove_repeated_points_bc2a_nb_repeated_points;
    INSERT INTO tmp_remove_repeated_points_bc2a(geom)
        (SELECT DISTINCT (ST_Dump(geom)).geom);
    /*
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    RAISE NOTICE '% points à traiter', _nrows;
     */

    --Utile pour trouver l'élément à supprimer mais ralenti la suppression ?
    CREATE UNIQUE INDEX idx_tmp_remove_repeated_points_bc2a_point_id ON tmp_remove_repeated_points_bc2a (point_id);
    CREATE INDEX idx_tmp_remove_repeated_points_bc2a_geom ON tmp_remove_repeated_points_bc2a USING gist(geom);

    WITH point_has_repeated_points AS (
        SELECT 	point_a.point_id
                ,ARRAY_AGG(point_b.point_id) AS repeated_points_id
        FROM tmp_remove_repeated_points_bc2a AS point_a
        INNER JOIN tmp_remove_repeated_points_bc2a AS point_b
            ON point_a.point_id != point_b.point_id
            AND ST_DWithin(point_a.geom, point_b.geom, tolerance)
        GROUP BY point_a.point_id
    )
    UPDATE tmp_remove_repeated_points_bc2a
    SET repeated_points_id = point_has_repeated_points.repeated_points_id
        ,nb_repeated_points = ARRAY_LENGTH(point_has_repeated_points.repeated_points_id,1)
    FROM point_has_repeated_points
    WHERE tmp_remove_repeated_points_bc2a.point_id = point_has_repeated_points.point_id;

    /* NOTE
    Alternative: alone query, but slower
    WITH repeated_points AS
    (
        SELECT 	point_id
                --,repeated_points_id
                --,nb_repeated_points
                --,ARRAY_LENGTH(ARRAY_AGG(point_id) OVER (ORDER BY nb_repeated_points DESC, point_id),1)
                ,(ARRAY_REMOVE(repeated_points_id, ARRAY_AGG(point_id) OVER (ORDER BY nb_repeated_points DESC, point_id)) != '{}') AS doublon
        FROM tmp_remove_repeated_points_bc2a
        WHERE nb_repeated_points > 0
    )
    DELETE FROM tmp_remove_repeated_points_bc2a
    WHERE tmp_remove_repeated_points_bc2a.point_id = repeated_points.point_id
    AND repeated_points.doublon = TRUE;
     */

    /*
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    RAISE NOTICE '% points à traiter en doublon', _nrows;
     */

    -- no more useful
    --DROP INDEX idx_tmp_remove_repeated_points_bc2a_geom;
    CREATE INDEX idx_tmp_remove_repeated_points_bc2a_repeated_points_id ON tmp_remove_repeated_points_bc2a USING gin(repeated_points_id);
    CREATE INDEX idx_tmp_remove_repeated_points_bc2a_nb_repeated_points ON tmp_remove_repeated_points_bc2a (nb_repeated_points);

    _nrows := 0;
    LOOP
        SELECT point_id INTO _repeated_point_id
        FROM tmp_remove_repeated_points_bc2a
        WHERE nb_repeated_points > 0
        ORDER BY nb_repeated_points DESC
        LIMIT 1;

        EXIT WHEN _repeated_point_id IS NULL;

        UPDATE tmp_remove_repeated_points_bc2a
        SET nb_repeated_points = nb_repeated_points - 1
            --,repeated_points_id = ARRAY_REMOVE(repeated_points_id, _repeated_point_id)
        WHERE repeated_points_id @> ARRAY[_repeated_point_id];

        DELETE FROM tmp_remove_repeated_points_bc2a
        WHERE point_id = _repeated_point_id;

        /*
        _nrows := _nrows + 1;
        IF _nrows % 1000 = 0 THEN
            RAISE NOTICE '% points en doublon traités', _nrows;
        END IF;
         */
    END LOOP;

    SELECT ST_Collect(geom) INTO _return FROM tmp_remove_repeated_points_bc2a;
    RETURN _return;
END
$$ LANGUAGE plpgsql;

/* TEST
DROP TABLE IF EXISTS tmp_remove_repeated_points_bc2a_test;
CREATE TEMPORARY TABLE tmp_remove_repeated_points_bc2a_test AS (
    SELECT pdi_coord
    FROM public.pdi_view
    WHERE co_insee_commune = '33063'
    AND fl_active = true
    AND fl_diffusable = true
    AND pdi_etat = 1
    AND pdi_visible = true
    AND pdi_no_type_localisation_coord > 4
);
SELECT COUNT(*) FROM tmp_remove_repeated_points_bc2a_test;
--> 62560 points avant opération

SELECT COUNT(*) FROM
(
    SELECT ST_Dump(
        public.ST_RemoveRepeatedPoints(
            (SELECT ST_Collect(pdi_coord) FROM tmp_remove_repeated_points_bc2a_test)
            ,10
        )
    )
) AS sous_requete
--> 61924 points après opération public.ST_RemoveRepeatedPoints en 7 secs

SELECT COUNT(*) FROM
(
    SELECT ST_Dump(
        public.ST_RemoveRepeatedPoints(
            public.ST_RemoveRepeatedPoints(
                (SELECT ST_Collect(pdi_coord) FROM tmp_remove_repeated_points_bc2a_test)
                ,1
            )
            ,1
        )
    )
) AS sous_requete
--> toujours 61924 points après double opération public.ST_RemoveRepeatedPoints en 9 secs

SELECT COUNT(*) FROM
(
    SELECT ST_Dump(
        ext_postgis.ST_RemoveRepeatedPoints(
            public.ST_RemoveRepeatedPoints(
                (SELECT ST_Collect(pdi_coord) FROM tmp_remove_repeated_points_bc2a_test)
                ,1
            )
            ,1
        )
    )
) AS sous_requete
--> toujours 61924 points après opération public.ST_RemoveRepeatedPoints puis ext_postgis.ST_RemoveRepeatedPoints en 4 min et 13 secs

SELECT COUNT(*) FROM
(
    SELECT ST_Dump(
        ext_postgis.ST_RemoveRepeatedPoints(
            (SELECT ST_Collect(pdi_coord) FROM tmp_remove_repeated_points_bc2a_test)
            ,1
        )
    )
) AS sous_requete
--> 61923 points après opération ST_RemoveRepeatedPoints en 3 min

SELECT COUNT(*) FROM
(
    SELECT ST_Dump(
        public.ST_RemoveRepeatedPoints(
            ext_postgis.ST_RemoveRepeatedPoints(
                (SELECT ST_Collect(pdi_coord) FROM tmp_remove_repeated_points_bc2a_test)
                ,1
            )
            ,1
        )
    )
) AS sous_requete
--> toujours 61923 points après opération ext_postgis.ST_RemoveRepeatedPoints puis public.ST_RemoveRepeatedPoints en 3 min 56 secs
 */

/* NOTE
improve ST_VoronoiPolygons()
see: https://postgis.net/docs/ST_VoronoiPolygons.html
 */
SELECT public.drop_all_functions_if_exists('public','ST_VoronoiPolygons');
CREATE OR REPLACE FUNCTION public.ST_VoronoiPolygons(
    geom IN GEOMETRY
    , tolerance FLOAT8 DEFAULT 0.0
    , extent_to GEOMETRY DEFAULT NULL
    )
RETURNS GEOMETRY AS
$$
BEGIN
    -- want to assemble near points
    IF tolerance != 0.0 THEN
        -- RAISE NOTICE 'Utilisation version BC2A de ST_VoronoiPolygons';
        -- delete near points
        RETURN ext_postgis.ST_VoronoiPolygons(public.ST_RemoveRepeatedPoints(geom, tolerance), 0.0, extent_to);
    ELSE
        RETURN ext_postgis.ST_VoronoiPolygons(geom, 0.0, extent_to);
    END IF;
END
$$ LANGUAGE plpgsql;

/* TEST
SELECT pdi_id, ST_AsText(pdi_coord) FROM pdi_view WHERE pdi_id IN (10652325,24672957)
--> 10652325	"POINT(-63285.8936065736 5594957.98298803)"
--> 24672957	"POINT(-63285.8936065736 5594957.98298799)"

SELECT ST_AsText(ST_Collect(pdi_coord)) FROM pdi_view
WHERE co_insee_commune = '33063'
AND fl_active = true
AND fl_diffusable = true
AND pdi_etat = 1
AND pdi_visible = true
AND pdi_no_type_localisation_coord > 4
-- dans une étendue de 50 mètres autour de 2 PDIs très proches géographiquement :
and pdi_coord && (SELECT ST_Extent(ST_Buffer(pdi_coord,100)) AS etendue FROM pdi_view WHERE pdi_id = 10652325)

"MULTIPOINT(-63253.3450261578 5594962.51544104,-63271.5486856941 5594944.27784429,-63282.4652754374 5594913.00271138,-63247.8695433356 5594960.79310534,-63242.5402863549 5594958.98832436,-63236.3391333885 5594957.36927429,-63281.2316461463 5594917.89939258,-63329.9330894873 5594959.88201743,-63307.4392187397 5594960.54196022,-63322.9907332408 5594981.29019635,-63285.8936065736 5594957.98298803,-63297.4604537968 5594958.06215068,-63285.8936065736 5594957.98298799,-63316.468824146 5594963.81033297,-63318.8027404329 5594937.39166409,-63296.5634203015 5594936.42779418,-63313.1745760223 5594918.24209028,-63304.814323293 5594916.74916288,-63294.4394276426 5594914.48712498,-63282.870112603 5594914.91212849,-63312.3114263472 5594937.26184079,-63287.305623792 5594936.13309789,-63310.1932656081 5594937.30731209,-63326.8339777026 5594917.21968868,-63334.5994443262 5594939.38397809,-63305.178804347 5594972.95213598,-63321.8612034277 5594912.84117959)"

"MULTIPOINT(-63253.3450261578 5594962.51544104,-63271.5486856941 5594944.27784429,-63191.5734537447 5594986.17027418,-63297.4352173579 5594863.01255549,-63199.1264087256 5594959.79775379,-63282.4652754374 5594913.00271138,-63284.8539325727 5594905.23957568,-63193.9994300911 5594977.43074089,-63189.9845864334 5595010.97347389,-63247.8695433356 5594960.79310534,-63242.5402863549 5594958.98832436,-63211.9331526595 5594951.04092278,-63205.6629276841 5594949.54801208,-63236.3391333885 5594957.36927429,-63219.3894128038 5594952.88571149,-63281.2316461463 5594917.89939258,-63295.223126976 5594872.66379689,-63293.1330519852 5594881.02409689,-63291.341559133 5594885.20424689,-63196.6940807819 5594968.39609319,-63289.8051149571 5594893.32266988,-63230.0431787094 5594954.43404958,-63188.3923232157 5594996.92461198,-63287.7150399663 5594897.20423768,-63203.9775051001 5595041.63594279,-63192.4463657884 5595052.10939418,-63211.9226896531 5595046.77448668,-63218.5499548308 5595053.06924768,-63215.0058706188 5595050.64437503,-63375.6762362089 5594965.17663068,-63329.9330894873 5594959.88201743,-63307.4392187397 5594960.54196022,-63322.9907332408 5594981.29019635,-63341.2042439009 5594987.2618392,-63285.8936065736 5594957.98298803,-63297.4604537968 5594958.06215068,-63285.8936065736 5594957.98298799,-63338.3307661722 5594961.23932542,-63316.468824146 5594963.81033297,-63355.6402629755 5594963.43228987,-63318.8027404329 5594937.39166409,-63296.5634203015 5594936.42779418,-63313.1745760223 5594918.24209028,-63304.814323293 5594916.74916288,-63294.4394276426 5594914.48712498,-63369.5319381467 5594940.57433299,-63282.870112603 5594914.91212849,-63384.7280818929 5594941.83908529,-63377.2593591148 5594941.47096808,-63358.8542225811 5594917.34555519,-63312.3114263472 5594937.26184079,-63287.305623792 5594936.13309789,-63350.4433394722 5594921.11868718,-63310.1932656081 5594937.30731209,-63348.2965453199 5594941.01755039,-63362.3299059813 5594940.12180409,-63342.0263203445 5594939.52463978,-63355.4625167228 5594939.82322189,-63326.8339777026 5594917.21968868,-63334.5994443262 5594939.38397809,-63246.2058125544 5595016.28916134,-63305.178804347 5594972.95213598,-63221.7220769387 5595015.99057931,-63338.2518471012 5595045.65322709,-63253.8741515894 5595029.60353281,-63250.3625421301 5595045.07111436,-63187.1668418323 5595009.02581003,-63270.8586490836 5595050.75069549,-63239.069470903 5595042.6789575,-63291.8505058461 5595053.16374468,-63299.2495412629 5595054.98177668,-63200.3989985858 5595012.54818926,-63218.7362555261 5594991.20826149,-63260.4612062723 5595047.46316294,-63366.9280537885 5595045.85381899,-63306.3499948413 5595057.69539748,-63193.9868345441 5595010.73726616,-63208.3615355024 5595013.19730956,-63321.8612034277 5594912.84117959,-63341.6966298838 5594895.01419768,-63348.2944240921 5594869.07960118,-63344.8200421118 5594881.11195329,-63324.4665458438 5594902.65692398,-63339.0946457902 5594912.13863189,-63332.5568641996 5594858.60212498,-63347.7246007132 5594900.99657149,-63342.3501556252 5594902.48948729,-63324.6648989615 5594892.88734639,-63328.8450489461 5594882.13838928,-63339.9975142334 5594907.14635468,-63369.0899373486 5594908.01859688,-63373.1606237021 5594863.32637829,-63371.0032928117 5594893.56114748,-63368.3469175231 5594916.19898899,-63369.9309570505 5594880.29789899,-63384.6285080346 5594918.27708968,-63370.3513935265 5594873.30504269)"

SELECT ROUND(ST_Distance(
    ST_PointFromText('POINT(-63285.8936065736 5594957.98298803)',3857)
    ,ST_PointFromText('POINT(-63285.8936065736 5594957.98298799)',3857)
)::NUMERIC,10)
--> Ces deux points sont à 0.0000000400 mètres de distance

--> Dans ce cas ST_VoronoiPolygons avec une tolérance d'un mètre ne génère bien qu'un seul polygone pour ces 2 points qui sont proches
WITH points_test AS (
    SELECT 'point_a' AS id, ST_PointFromText('POINT(-63285.8936065736 5594957.98298803)',3857) AS geom
    UNION ALL
    SELECT 'point_b' AS id, ST_PointFromText('POINT(-63285.8936065736 5594957.98298799)',3857) AS geom
)
, multipoint_50m AS (
    SELECT ST_GeomFromText('MULTIPOINT(-63253.3450261578 5594962.51544104,-63271.5486856941 5594944.27784429,-63282.4652754374 5594913.00271138,-63247.8695433356 5594960.79310534,-63242.5402863549 5594958.98832436,-63236.3391333885 5594957.36927429,-63281.2316461463 5594917.89939258,-63329.9330894873 5594959.88201743,-63307.4392187397 5594960.54196022,-63322.9907332408 5594981.29019635,-63285.8936065736 5594957.98298803,-63297.4604537968 5594958.06215068,-63285.8936065736 5594957.98298799,-63316.468824146 5594963.81033297,-63318.8027404329 5594937.39166409,-63296.5634203015 5594936.42779418,-63313.1745760223 5594918.24209028,-63304.814323293 5594916.74916288,-63294.4394276426 5594914.48712498,-63282.870112603 5594914.91212849,-63312.3114263472 5594937.26184079,-63287.305623792 5594936.13309789,-63310.1932656081 5594937.30731209,-63326.8339777026 5594917.21968868,-63334.5994443262 5594939.38397809,-63305.178804347 5594972.95213598,-63321.8612034277 5594912.84117959)',3857) AS geom
)
, voronoi_polygons_50m AS (
    SELECT (ST_Dump(ST_VoronoiPolygons(multipoint_50m.geom,1))).*
    FROM multipoint_50m
)
, multipoint_100m AS (
SELECT ST_Collect(pdi_coord) AS geom FROM pdi_view
WHERE co_insee_commune = '33063'
AND fl_active = true
AND fl_diffusable = true
AND pdi_etat = 1
AND pdi_visible = true
AND pdi_no_type_localisation_coord > 4
-- dans une étendue de 50 mètres autour de 2 PDIs très proches géographiquement :
and pdi_coord && (SELECT ST_Extent(ST_Buffer(pdi_coord,200)) AS etendue FROM pdi_view WHERE pdi_id = 10652325)
    --SELECT ST_GeomFromText('MULTIPOINT(-63253.3450261578 5594962.51544104,-63271.5486856941 5594944.27784429,-63191.5734537447 5594986.17027418,-63297.4352173579 5594863.01255549,-63199.1264087256 5594959.79775379,-63282.4652754374 5594913.00271138,-63284.8539325727 5594905.23957568,-63193.9994300911 5594977.43074089,-63189.9845864334 5595010.97347389,-63247.8695433356 5594960.79310534,-63242.5402863549 5594958.98832436,-63211.9331526595 5594951.04092278,-63205.6629276841 5594949.54801208,-63236.3391333885 5594957.36927429,-63219.3894128038 5594952.88571149,-63281.2316461463 5594917.89939258,-63295.223126976 5594872.66379689,-63293.1330519852 5594881.02409689,-63291.341559133 5594885.20424689,-63196.6940807819 5594968.39609319,-63289.8051149571 5594893.32266988,-63230.0431787094 5594954.43404958,-63188.3923232157 5594996.92461198,-63287.7150399663 5594897.20423768,-63203.9775051001 5595041.63594279,-63192.4463657884 5595052.10939418,-63211.9226896531 5595046.77448668,-63218.5499548308 5595053.06924768,-63215.0058706188 5595050.64437503,-63375.6762362089 5594965.17663068,-63329.9330894873 5594959.88201743,-63307.4392187397 5594960.54196022,-63322.9907332408 5594981.29019635,-63341.2042439009 5594987.2618392,-63285.8936065736 5594957.98298803,-63297.4604537968 5594958.06215068,-63285.8936065736 5594957.98298799,-63338.3307661722 5594961.23932542,-63316.468824146 5594963.81033297,-63355.6402629755 5594963.43228987,-63318.8027404329 5594937.39166409,-63296.5634203015 5594936.42779418,-63313.1745760223 5594918.24209028,-63304.814323293 5594916.74916288,-63294.4394276426 5594914.48712498,-63369.5319381467 5594940.57433299,-63282.870112603 5594914.91212849,-63384.7280818929 5594941.83908529,-63377.2593591148 5594941.47096808,-63358.8542225811 5594917.34555519,-63312.3114263472 5594937.26184079,-63287.305623792 5594936.13309789,-63350.4433394722 5594921.11868718,-63310.1932656081 5594937.30731209,-63348.2965453199 5594941.01755039,-63362.3299059813 5594940.12180409,-63342.0263203445 5594939.52463978,-63355.4625167228 5594939.82322189,-63326.8339777026 5594917.21968868,-63334.5994443262 5594939.38397809,-63246.2058125544 5595016.28916134,-63305.178804347 5594972.95213598,-63221.7220769387 5595015.99057931,-63338.2518471012 5595045.65322709,-63253.8741515894 5595029.60353281,-63250.3625421301 5595045.07111436,-63187.1668418323 5595009.02581003,-63270.8586490836 5595050.75069549,-63239.069470903 5595042.6789575,-63291.8505058461 5595053.16374468,-63299.2495412629 5595054.98177668,-63200.3989985858 5595012.54818926,-63218.7362555261 5594991.20826149,-63260.4612062723 5595047.46316294,-63366.9280537885 5595045.85381899,-63306.3499948413 5595057.69539748,-63193.9868345441 5595010.73726616,-63208.3615355024 5595013.19730956,-63321.8612034277 5594912.84117959,-63341.6966298838 5594895.01419768,-63348.2944240921 5594869.07960118,-63344.8200421118 5594881.11195329,-63324.4665458438 5594902.65692398,-63339.0946457902 5594912.13863189,-63332.5568641996 5594858.60212498,-63347.7246007132 5594900.99657149,-63342.3501556252 5594902.48948729,-63324.6648989615 5594892.88734639,-63328.8450489461 5594882.13838928,-63339.9975142334 5594907.14635468,-63369.0899373486 5594908.01859688,-63373.1606237021 5594863.32637829,-63371.0032928117 5594893.56114748,-63368.3469175231 5594916.19898899,-63369.9309570505 5594880.29789899,-63384.6285080346 5594918.27708968,-63370.3513935265 5594873.30504269)',3857) AS geom
)
, voronoi_polygons_100m AS (
    SELECT (ST_Dump(ST_VoronoiPolygons(multipoint_100m.geom,1))).*
    FROM multipoint_100m
)
SELECT
    points_test.id
    ,(SELECT ARRAY_AGG(voronoi_polygons_50m.path) FROM voronoi_polygons_50m WHERE ST_Within(points_test.geom, voronoi_polygons_50m.geom))
    ,(SELECT ARRAY_AGG(voronoi_polygons_100m.path) FROM voronoi_polygons_100m WHERE ST_Within(points_test.geom, voronoi_polygons_100m.geom))
    ,(SELECT COUNT(*) FROM voronoi_polygons_50m WHERE ST_Within(points_test.geom, voronoi_polygons_50m.geom))
    ,(SELECT COUNT(*) FROM voronoi_polygons_100m WHERE ST_Within(points_test.geom, voronoi_polygons_100m.geom))
FROM points_test

--> Mais si on augmente l'étendue à 100m, ST_VoronoiPolygons génère deux polygones pour ces 2 points qui sont proches

SELECT pdi_id, pdi_coord AS geom, co_postal, pdi_no_type_localisation_coord AS no_type_localisation
FROM public.pdi_view
WHERE co_insee_commune = v_com_multi_cp.co_insee_commune
--WHERE co_insee_commune = '76476'
--WHERE co_insee_commune = '33063'
AND fl_active = true
and fl_diffusable = true
and pdi_etat = 1
and pdi_visible = true
and pdi_no_type_localisation_coord > 4
-- TEST PDI très proches géographiquement :
and pdi_coord && (SELECT ST_Extent(ST_Buffer(pdi_coord,100)) AS etendue FROM pdi_view WHERE pdi_id = 10652325)
 */

/* NOTE
improve ST_SplitFour()
 */
SELECT public.drop_all_functions_if_exists('public','ST_SplitFour');
CREATE OR REPLACE FUNCTION public.ST_SplitFour(
    box2d_in IN BOX2D
    )
RETURNS SETOF BOX2D AS
$$
DECLARE
	_rectangle GEOMETRY(POLYGON);
	_middle GEOMETRY(POINT);
	_bottom_left GEOMETRY(POINT);
	_top_left GEOMETRY(POINT);
	_top_right GEOMETRY(POINT);
	_bottom_right GEOMETRY(POINT);
BEGIN
	_rectangle := ST_MakePolygon(ST_ExteriorRing(box2d_in));
	_middle := ST_Centroid(_rectangle);
	SELECT (ST_DumpPoints(_rectangle)).geom INTO _bottom_left OFFSET 0 LIMIT 1;
	SELECT (ST_DumpPoints(_rectangle)).geom INTO _top_left OFFSET 1 LIMIT 1;
	SELECT (ST_DumpPoints(_rectangle)).geom INTO _top_right OFFSET 2 LIMIT 1;
	SELECT (ST_DumpPoints(_rectangle)).geom INTO _bottom_right OFFSET 3 LIMIT 1;

	RETURN NEXT Box2d(ST_Collect(_top_left,_middle));
	RETURN NEXT Box2d(ST_Collect(_top_right,_middle));
	RETURN NEXT Box2d(ST_Collect(_bottom_left,_middle));
	RETURN NEXT Box2d(ST_Collect(_bottom_right,_middle));
END
$$ LANGUAGE plpgsql;

/* TEST
--> Découpage en 4
SELECT  ST_SplitFour(
        -- extent COM_CP (France Métropolitaine) except Corse
        (SELECT ST_Extent(gm_contour) AS geom FROM territoire WHERE nivgeo = 'COM_CP' AND codgeo_metropole_dom_tom_parent = 'FRM' AND codgeo_reg_parent != '94')
        )

--> Découpage en 4 * 4 = 16
SELECT  ST_SplitFour(ST_SplitFour(
        (SELECT ST_Extent(gm_contour) AS geom FROM territoire WHERE nivgeo = 'COM_CP' AND codgeo_metropole_dom_tom_parent = 'FRM' AND codgeo_reg_parent != '94')
        ))

WITH split16 AS (
    SELECT 	ST_SplitFour(ST_SplitFour(
            (SELECT ST_Extent(gm_contour) AS geom FROM territoire WHERE nivgeo = 'COM_CP' AND codgeo_metropole_dom_tom_parent = 'FRM' AND codgeo_reg_parent != '94')
    )) AS bbox
)
SELECT
    split16.*
    ,(
        SELECT COUNT(*)
        FROM territoire WHERE nivgeo = 'COM_CP' AND codgeo_metropole_dom_tom_parent = 'FRM' AND codgeo_reg_parent != '94'
        AND gm_contour && split16.bbox
    )
FROM split16
 */

CREATE OR REPLACE FUNCTION public.ST_SplitFour(
    geom IN GEOMETRY
    )
RETURNS SETOF GEOMETRY AS
$$
BEGIN
	RETURN QUERY
        SELECT ST_SetSrid(ST_MakePolygon(ST_ExteriorRing(ST_SplitFour(Box2d(geom)))),ST_Srid(geom));
END
$$ LANGUAGE plpgsql;

/* TEST
SELECT  ST_SplitFour(
        -- _rectangle of extent COM_CP (France Métropolitaine) except Corse
        ST_SetSRID(
            ST_MakePolygon(
                ST_ExteriorRing(
                    (SELECT ST_Extent(gm_contour) AS geom FROM territoire WHERE nivgeo = 'COM_CP' AND codgeo_metropole_dom_tom_parent = 'FRM' AND codgeo_reg_parent != '94')
                )
            )
            ,3857
        )
    )
 */

-- BBOX for all parts of France (according to SRID)
SELECT public.drop_all_functions_if_exists('public', 'coordIsInSridBounds');
-- from (x, y, SRID)
CREATE OR REPLACE FUNCTION coordIsInSridBounds(
    in_x DOUBLE PRECISION
    , in_y DOUBLE PRECISION
    , in_srid INTEGER
    )
RETURNS BOOLEAN
IMMUTABLE
AS
$func$
DECLARE
BEGIN
    RETURN
        CASE in_srid
        -- France Métropolitaine, Monaco
        WHEN 2154 THEN
            --(in_x BETWEEN -357823.2365 AND 1313632.3628) AND (in_y BETWEEN 6037008.6939 AND 7230727.3772)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'France métropolitaine hors Corse', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep NOT LIKE '97%' AND insee_dep NOT IN ('2A','2B')
            --> BOX(97038 6135116,1084898 7112480)
            (in_x BETWEEN 97038 AND 1084898) AND (in_y BETWEEN 6135116 AND 7112480)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Corse', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep IN ('2A','2B')
            --> BOX(1154228 6044556,1244436 6237452)
            OR (in_x BETWEEN 1154228 AND 1244436) AND (in_y BETWEEN 6044556 AND 6237452)

        /* Guadeloupe Martinique (971XX et 972XX)
            * + Saint-Barthélemy (977XX), île francophone des Caraïbes
            * + Saint-Martin (978XX). Fait partie des îles Leeward dans la mer des Caraïbes. Elle est divisée entre 2 pays distincts : sa partie nord, appelée Saint-Martin, est française, et sa partie sud, Sint Maarten, est néerlandaise.
            */
        WHEN 4559 THEN (in_x BETWEEN 428749.41 AND 1079045.02) AND (in_y BETWEEN 1556673.78 AND 2058754.66)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Guadeloupe', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '971'
            --> BOX(625198 1748873,715444 1828464)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Martinique', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '972'
            --> BOX(688550 1589776,738127 1647746)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Saint-Barthélemy', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '977'
            --> NULL
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Saint-Martin', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '978'
            --> NULL

        --Guyane française (973XX), région d'outre-mer située sur la côte nord-est de l'Amérique du Sud
        WHEN 2972 THEN (in_x BETWEEN 99415.20 AND 669342.50) AND (in_y BETWEEN 233683.27 AND 981936.72)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Guyane', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '973'
            --> BOX(97207 231683,433296 638175)

        --Ile de la Réunion (974XX)
        WHEN 2975 THEN (in_x BETWEEN -23344.18 AND 631069.19) AND (in_y BETWEEN 7256163.66 AND 7978390.98)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Réunion', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '974'
            --> BOX(312668 7632101,381239 7693275)

        --Mayotte (976XX), archipel de l'océan Indien situé entre Madagascar et la côte du Mozambique
        WHEN 4471 THEN (in_x BETWEEN 357748.31 AND 685530.19) AND (in_y BETWEEN 8397670.97 AND 8746991.06)
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Mayotte', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '976'
            --> BOX(499991 8560261,534560 8605052)

        /* Saint-Pierre-et-Miquelon (975XX), archipel français au sud de l'île canadienne de Terre-Neuve
            * Pas de code projection RAN défini, ni d'adresse RAN existante
        WHEN 4467
            -- SELECT ST_Envelope(ST_Collect(ST_Transform(ST_Buffer(ST_Envelope(geom),2000),4326))), 'Saint-Pierre-et-Miquelon', ST_Extent(ST_Buffer(ST_Envelope(geom),2000)) FROM ign.admin_express_commune WHERE insee_dep = '975' OR insee_com LIKE '975%' OR nom ILIKE '%Miquelon%'
            --> NULL
        */
        END;
END
$func$ LANGUAGE plpgsql;

-- from POINT
CREATE OR REPLACE FUNCTION coordIsInSridBounds(
    point_in GEOMETRY(POINT)
    )
RETURNS BOOLEAN
IMMUTABLE
AS
$func$
DECLARE
BEGIN
	RETURN coordIsInSridBounds(ST_X(point_in), ST_Y(point_in), ST_SRID(point_in));
END
$func$ LANGUAGE plpgsql;

/* TESTS
DROP TABLE IF EXISTS tmp_pdi_srid_out_bounds;
CREATE TABLE tmp_pdi_srid_out_bounds AS
    SELECT * FROM geopad.pdi
    WHERE NOT coordIsInSridBounds(adresse_x, adresse_y, getSridCoordRanFromCodeInseeDepartement(getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id,agg_adresse_id))));

SELECT COALESCE(ST_SetSRID(ST_MakePoint(1,1), NULL), ST_MakePoint(2,2));

SELECT
    ST_Collect(ARRAY[
        ST_Transform(
            ST_SetSRID(
                ST_MakePoint(adresse_x, adresse_y)
                ,getSridCoordRanFromCodeInseeDepartement(getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id)))
            )
            ,4326
        )
        ,ST_MakeLine(
            ST_Transform(
                ST_SetSRID(
                    ST_MakePoint(adresse_x,adresse_y)
                    ,getSridCoordRanFromCodeInseeDepartement(getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id,agg_adresse_id)))
                )
                ,4326
            )
            ,ST_Transform(
                ST_SetSRID(
                    ST_MakePoint(adresse_x,adresse_y)
                    ,2154
                )
                ,4326
            )
        )
    ]) AS geom
    , pdi_id
    , adresse_id
    , adresse_geocode
    , agg_adresse_id
    , getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id, agg_adresse_id))
    , getSridCoordRanFromCodeInseeDepartement(
        getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id, agg_adresse_id))
    )
    , (
        SELECT CONCAT_WS('<br>',no_numero,lb_extension_numero,lb_voie,co_postal,lb_acheminement)
        FROM adresse_ran_view WHERE co_adr = COALESCE(adresse_id,agg_adresse_id)
    ) AS lb_adresse
FROM tmp_pdi_srid_out_bounds
LIMIT 1
--WHERE
WHERE NOT
    coordIsInSridBounds(
        ST_Transform(
            ST_SetSRID(
                ST_MakePoint(adresse_x, adresse_y)
                ,2154
            )
            , getSridCoordRanFromCodeInseeDepartement(
                getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id, agg_adresse_id))
            )
        )
    )
    AND adresse_id IS NOT NULL
LIMIT 1;

SELECT pdi_id
FROM geopad.pdi
WHERE
    ST_Astext(ST_SetSRID(
        ST_MakePoint(adresse_x,adresse_y)
        ,getSridCoordRanFromCodeInseeDepartement(getCodeInseeDepartementFromCodeInseeCommune(adresse_id))
    )) = 'POINT(inf inf)'
AND adresse_id LIKE '97%'
LIMIT 1;

SELECT pdi_id, adresse_x, adresse_y, getSridCoordRanFromCodeInseeDepartement(
    getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id,agg_adresse_id))) AS srid
FROM geopad.pdi
WHERE NOT coordIsInSridBounds(adresse_x, adresse_y, getSridCoordRanFromCodeInseeDepartement(
    getCodeInseeDepartementFromCodeInseeCommune(COALESCE(adresse_id, agg_adresse_id)))
)
LIMIT 1;

SELECT COUNT(*) FROM geopad.pdi WHERE adresse_id IS NULL AND agg_adresse_id IS NOT NULL LIMIT 1;
 */

-- extend line
SELECT public.drop_all_functions_if_exists('public','ST_ExtendLine');
CREATE OR REPLACE FUNCTION public.ST_ExtendLine(
    line_in IN GEOMETRY(LINESTRING)
    ,length_in FLOAT
    /*
     BOTH : extend 2 sides
     START : extend in direction of start of line
     END : extend in direction of end of line
     */
    ,direction VARCHAR DEFAULT 'BOTH'
    )
RETURNS GEOMETRY(LINESTRING)
AS $$
DECLARE
	_azimuth FLOAT;
	_start_point GEOMETRY(POINT);
	_end_point GEOMETRY(POINT);
BEGIN
    _start_point := ST_StartPoint(line_in);
    _end_point := ST_EndPoint(line_in);
    _azimuth := ST_Azimuth(_end_point,_start_point);

    -- get the length of the line StartPoint --> B
    -- length := ST_DISTANCE(A,B);
    -- newlength := length + (length * (1/3));   -- increase the line length by 1/3

    -- extend 2 sides or at start only
    IF direction != 'END' THEN -- equiv to: IN ('BOTH','START') THEN
        line_in := ST_AddPoint(
            line_in
            ,ST_Translate(_start_point, sin(_azimuth) * length_in, cos(_azimuth) * length_in)
            ,0
        );
    END IF;
    -- extend 2 sides or at end only
    IF direction != 'START' THEN -- equiv to IN ('BOTH','END') THEN
        line_in := ST_AddPoint(
            line_in
            ,ST_Translate(_end_point, sin(_azimuth) * length_in * -1, cos(_azimuth) * length_in * -1)
        );
    END IF;
    RETURN line_in;
END
$$ LANGUAGE plpgsql;

/* TEST
SELECT
    ST_Transform(
        ST_Collect(ARRAY[
            pdi.pdi_coord
            ,parcelle1.geom
            ,ST_ApproximateMedialAxis(parcelle1.geom)
            ,extend_line.geom
            --,ST_Intersection(extend_line.geom,parcelle1.geom)
            ,parcelle2.geom
        ])
    ,4326) AS geom
    ,pdi.no_numero
    ,parcelle1.id
    ,isParcelleBatie(parcelle1.geom)
    ,parcelle2.id
    ,isParcelleBatie(parcelle1.geom)
FROM pdi_view AS pdi
LEFT OUTER JOIN LATERAL (
    SELECT parcelle.*
    FROM divers.data_gouv_cadastre_parcelles AS parcelle
    WHERE ST_DWithin(parcelle.geom, pdi.pdi_coord, 15)
    ORDER BY ST_Distance(parcelle.geom, pdi.pdi_coord)
    LIMIT 1
) AS parcelle1 ON TRUE
LEFT OUTER JOIN LATERAL (
    SELECT ST_ExtendLine(
                ST_MakeLine(
                    ST_ClosestPoint(ST_Buffer(parcelle1.geom,100),
                    ST_ClosestPoint(getContourNonMitoyenParcelle(parcelle1.geom),pdi.pdi_coord))
                    ,ST_ClosestPoint(
                        ST_ApproximateMedialAxis(parcelle1.geom)
                        --,pdi.pdi_coord
                        ,ST_ClosestPoint(getContourNonMitoyenParcelle(parcelle1.geom),pdi.pdi_coord)
                    )
                )
                ,100
                ,'END'
            ) as geom
        where isParcelleEnclavee(parcelle1.geom) = false
        and isParcelleBatie(parcelle1.geom) = false
) as extend_line ON TRUE
LEFT OUTER JOIN LATERAL (
    SELECT parcelle.*
    FROM divers.data_gouv_cadastre_parcelles AS parcelle
    WHERE isParcelleBatie(parcelle1.geom) = FALSE
    AND ST_DWithin(parcelle.geom, parcelle1.geom, 15)
    AND ST_Intersects(
            parcelle.geom
            ,(
                ST_ExtendLine(
                    ST_MakeLine(
                        pdi.pdi_coord
                        ,ST_ClosestPoint(
                            ST_ApproximateMedialAxis(parcelle1.geom)
                            ,pdi.pdi_coord
                        )
                    )
                    ,100
                    ,'END'
                )
            )
        )
    AND parcelle.id != parcelle1.id
    ORDER BY ST_Distance(parcelle.geom, pdi.pdi_coord)
    LIMIT 1
) AS parcelle2 ON TRUE
WHERE pdi.co_insee_commune = '33032'
AND pdi.lb_voie IN ('RUE MICHEL DE MONTAIGNE','RUE ADRIEN PLANQUE')
 */

-- correct bad geometry
SELECT public.drop_all_functions_if_exists('public','ST_MakeValid2');
CREATE OR REPLACE FUNCTION public.ST_MakeValid2(
    geom IN GEOMETRY
    -- supply if known, to avoid new verification
    ,is_valid IN BOOLEAN DEFAULT NULL
)
RETURNS GEOMETRY
AS $$
DECLARE
BEGIN
    IF (is_valid IS NULL AND ST_IsValid(geom) = TRUE) OR is_valid = TRUE THEN RETURN geom; END IF;

    -- union ...
    SELECT ST_Union(valid_geom.geom)
    INTO geom
    FROM (
        -- ... of set of corrected geometries
        SELECT (ST_Dump(ext_postgis.ST_MakeValid(geom))).*
    ) AS valid_geom
    -- whose type is the same
    WHERE
        ST_GeometryType(valid_geom.geom) = REPLACE(ST_GeometryType(geom),'Multi','');

    RETURN geom;
END
$$ LANGUAGE plpgsql;

/* TEST
UPDATE divers.data_gouv_cadastre_parcelles
SET geom = ST_MakeValid2(geom, FALSE)
WHERE ST_IsValid(geom) = FALSE;

select * from divers.data_gouv_cadastre_parcelles where ST_GeometryType(geom) != 'ST_Polygon' LIMIT 1

select ST_GeometryType(ST_MakeValid2(geom)) from divers.data_gouv_cadastre_parcelles limit 1

select ST_Area(geom) = ST_Area(ST_MakeValid2(geom,false)), geom
, ST_MakeValid2(geom)
, ST_GeometryType(ST_MakeValid2(geom))
,(select st_collect(geom) from divers.data_gouv_cadastre_parcelles as p where p.geom && i.geom )
,(select st_collect(geom) from divers.data_gouv_cadastre_batiments as p where p.geom && i.geom )
,*
from divers.data_gouv_cadastre_parcelles_invalid as i

select geom
    from divers.data_gouv_cadastre_parcelles
    where id = '594670000A0183'
union all
select first(geom)
    from divers.data_gouv_cadastre_batiments
    where geom && (
        select geom
        from divers.data_gouv_cadastre_parcelles
        where id = '594670000A0183'
) group by st_area(geom)

select st_buffer(geom,-0.01),*
from divers.data_gouv_cadastre_parcelles
where geom && (
    select geom
    from divers.data_gouv_cadastre_parcelles
    where id = '59291000BA0352'
)

select st_intersection(
    st_buffer(st_snap(
        (
        select geom
        from divers.data_gouv_cadastre_parcelles
        where id = '59291000BA0352'
        )
        ,
        (
            select st_union(geom)
            from divers.data_gouv_cadastre_parcelles
            where geom && (
                select geom
                from divers.data_gouv_cadastre_parcelles
                where id = '59291000BA0352'
            )
                and id != '59291000BA0352'
        )
        ,1
    ),0)
    ,
    (
        select st_union(geom)
        from divers.data_gouv_cadastre_parcelles
        where geom && (
            select geom
            from divers.data_gouv_cadastre_parcelles
            where id = '59291000BA0352'
        )
            and id != '59291000BA0352'
    )
)

select
    p1.geom, p2.geom
    , ST_Collect(array[st_buffer(p1.geom,-0.01), st_buffer(p2.geom,-0.01)])
    , ST_MakeValid2(p1.geom, false)
    , st_snap(p1.geom,p2.geom,0.1)
    , ST_Collect(array[st_buffer(st_snap(p1.geom,p2.geom,1),0), p2.geom])
    ,st_buffer(p1.geom,0)
    , st_intersection(p1.geom, p2.geom)
    , st_intersection(st_buffer(st_snap(p1.geom,p2.geom,1),0), p2.geom)
from divers.data_gouv_cadastre_parcelles as p1
,divers.data_gouv_cadastre_parcelles as p2
--where p1.id = '594670000A0183' and p2.id = '594670000A0184'
where p1.id = '59291000BA0352'

select * from divers.data_gouv_cadastre_parcelles_invalid

CREATE TABLE divers.data_gouv_cadastre_parcelles_invalid AS (
    SELECT *
    FROM divers.data_gouv_cadastre_parcelles AS parcelle
    WHERE ST_IsValid(geom) = FALSE
);

UPDATE divers.data_gouv_cadastre_parcelles AS parcelle
SET geom = (
    SELECT ST_Union(valid_geom.geom)
    FROM (
        SELECT (ST_Dump(ST_MakeValid(parcelle.geom))).*
    ) AS valid_geom
    WHERE ST_GeometryType(valid_geom.geom) = 'ST_Polygon'
)
--WHERE ST_IsValid(geom) = FALSE
FROM divers.data_gouv_cadastre_parcelles_invalid AS invalid
WHERE invalid.id = parcelle.id
AND invalid.geom && parcelle.geom;

SELECT *, ST_Collect(ARRAY[geom, ST_Buffer(geom,5)]), ST_GeometryType(geom), ST_Area(geom), (
    SELECT st_collect(st_buffer(p.geom,0))
    from divers.data_gouv_cadastre_parcelles as p
    where p.geom && sr.geom
)

SELECT ST_IsValid(ST_Union(valid_geom.geom))
FROM (
    SELECT (ST_Dump(ST_MakeValid((SELECT geom FROM divers.data_gouv_cadastre_parcelles_invalid
WHERE id = '592760000A3993')))).*
) AS valid_geom
WHERE ST_GeometryType(valid_geom.geom) = 'ST_Polygon'

drop table if exists tmp_sr;
create temporary table tmp_sr as (
    SELECT (ST_Dump(ST_Buffer(geom,0))).*, id, ST_Buffer(geom,0.001)
    FROM divers.data_gouv_cadastre_parcelles_invalid
    WHERE id = '592760000A3993'
);
drop table if exists tmp_sr2;
create temporary table tmp_sr2 as (
    SELECT (ST_Dump(ST_MakeValid(geom))).*, id
    FROM divers.data_gouv_cadastre_parcelles_invalid
) ;
select *, st_astext(tmp_sr.geom), st_astext(tmp_sr2.geom)
from tmp_sr
full outer join tmp_sr2
on tmp_sr2.id = tmp_sr.id
 and st_equals(tmp_sr.geom,tmp_sr2.geom)
--and st_astext(tmp_sr2.geom) = st_astext(tmp_sr.geom)

SELECT *, ST_Collect(ARRAY[geom, ST_Buffer(geom,5)]), ST_GeometryType(geom), ST_Area(geom), (
    SELECT st_collect(st_buffer(p.geom,0))
    from divers.data_gouv_cadastre_parcelles as p
    where p.geom && sr.geom
)
FROM (
    SELECT (ST_Dump(ST_MakeValid(geom))).*
    FROM divers.data_gouv_cadastre_parcelles_invalid
) AS sr

SELECT ST_isvalid(ST_Buffer(geom,0))
FROM divers.data_gouv_cadastre_parcelles_invalid
LIMIT 1

UPDATE divers.data_gouv_cadastre_parcelles AS parcelle
SET geom = ST_MakeValid(parcelle.geom)
WHERE ST_IsValid(geom) = FALSE;

UPDATE divers.data_gouv_cadastre_batiments AS batiment
SET geom = (
    SELECT valid_geom.geom
    FROM (
        SELECT (ST_Dump(ST_MakeValid(batiment.geom))).*
    ) AS valid_geom
    WHERE ST_GeometryType(valid_geom.geom) = 'Polygon'
    ORDER BY ST_Area(valid_geom.geom) DESC
    LIMIT 1
)
WHERE ST_IsValid(geom) = FALSE;

SELECT ST_GeometryType(geom), REPLACE(REPLACE(ST_GeometryType(geom),'ST_',''),'Multi','') FROM divers.data_gouv_cadastre_parcelles LIMIT 1
SELECT COUNT(*) FROM divers.data_gouv_cadastre_parcelles AS parcelle WHERE ST_IsValid(geom) = FALSE LIMIT 1
SELECT * FROM divers.data_gouv_cadastre_batiments AS batiment WHERE ST_IsValid(geom) = FALSE LIMIT 1

select * from divers.data_gouv_cadastre_batiments AS batiment WHERE ST_IsValid(geom) = FALSE

SELECT ST_Transform((ST_Dump(ST_MakeValid(parcelle.geom))).geom
    ,4326)
from divers.data_gouv_cadastre_parcelles AS parcelle
WHERE ST_IsValid(geom) = FALSE
AND ST_GeometryType(ST_MakeValid(geom)) = 'ST_MultiPolygon'
 */

-- internal boundary
SELECT drop_all_functions_if_exists('public','ST_InternalBoundary');
CREATE OR REPLACE FUNCTION public.ST_InternalBoundary(
	geom IN GEOMETRY(MULTIPOLYGON)
    )
RETURNS GEOMETRY AS
$func$
DECLARE
	_return GEOMETRY;
BEGIN
	SELECT ST_Union(ST_Intersection(ST_Boundary(geom_a.geom),ST_Boundary(geom_b.geom)))
	INTO _return
	FROM ST_Dump(geom) AS geom_a
	CROSS JOIN ST_Dump(geom) AS geom_b
	WHERE geom_a.path != geom_b.path;

	RETURN _return;
END
$func$ LANGUAGE plpgsql;
