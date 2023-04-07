/***
 * add GEOMETRY facilities
 */

SELECT drop_all_functions_if_exists('fr', 'ST_SimplifyTerritory');
CREATE OR REPLACE PROCEDURE fr.ST_SimplifyTerritory(
    levels IN VARCHAR[]
    , from_srid IN INTEGER DEFAULT NULL
    , to_srid IN INTEGER DEFAULT 4326
    , bbox_in IN box2d DEFAULT NULL
    , tolerance INTEGER DEFAULT 100
    , bbox_split_over INTEGER DEFAULT 2000
    , subcall BOOLEAN DEFAULT FALSE
    , subcall_name VARCHAR DEFAULT NULL
)
AS $$
DECLARE
    _split_by_srid RECORD;
    _nrows_affected INTEGER;
    _split RECORD;
    _nterritories INTEGER;
    _bbox_territory BOX2D;
BEGIN
    IF NOT subcall THEN
        DROP TABLE IF EXISTS fr.tmp_polygon_to_simp;
        CREATE /*TEMPORARY*/ TABLE fr.tmp_polygon_to_simp AS (
            SELECT (ST_DumpRings((ST_Dump(territory.gm_contour_natif)).geom)).*, codgeo
            FROM fr.territory
            WHERE territory.gm_contour_natif IS NOT NULL
            AND nivgeo = ANY(levels)
        );
        CREATE INDEX ON fr.tmp_polygon_to_simp USING GIST(geom);
        ALTER TABLE fr.tmp_polygon_to_simp ADD COLUMN id SERIAL;
        CREATE UNIQUE INDEX ON fr.tmp_polygon_to_simp(id);
        ALTER TABLE fr.tmp_polygon_to_simp ADD COLUMN polygon_simp_id INTEGER;
        ALTER TABLE fr.tmp_polygon_to_simp ADD COLUMN polygon_simp_sim NUMERIC;
        ALTER TABLE fr.tmp_polygon_to_simp ADD COLUMN polygon_simp_geom GEOMETRY;
        ALTER TABLE fr.tmp_polygon_to_simp ADD COLUMN polygon_simp_subcallname VARCHAR;
        ALTER TABLE fr.tmp_polygon_to_simp ADD COLUMN polygon_simp_subcallbbox box2d;
    END IF;

    IF from_srid IS NULL THEN
        FOR _split_by_srid IN (
            SELECT DISTINCT ST_SRID(geom) AS srid
            FROM tmp_polygon_to_simp
            WHERE (bbox_in IS NULL OR geom && bbox_in)
        )
        LOOP
            RAISE NOTICE 'ST_SimplifyTerritory : traitement SRID %', _split_by_srid.srid;
            CALL fr.ST_SimplifyTerritory(
                levels => levels
                , from_srid => _split_by_srid.srid
                , to_srid => to_srid
                , bbox_in => bbox_in
                , tolerance => tolerance
                , bbox_split_over => bbox_split_over
                , subcall => TRUE
                , subcall_name => CONCAT_WS('.', subcall_name, _split_by_srid.srid)
            );
        END LOOP;
    ELSE
        SELECT COUNT(*), ST_Extent(geom)
        INTO _nterritories, _bbox_territory
        FROM fr.tmp_polygon_to_simp
        WHERE (bbox_in IS NULL OR geom && bbox_in)
        AND ST_SRID(geom) = from_srid;

        IF _nterritories = 0 THEN
            RAISE NOTICE 'ST_SimplifyTerritory % : 0 à traiter', subcall_name;
            RETURN;
        ELSIF _nterritories > bbox_split_over THEN
            RAISE NOTICE 'ST_SimplifyTerritory % : % à traiter : découpage en quatre de l''étendue', subcall_name, _nterritories;
            FOR _split IN (
                SELECT bbox_terr, ROW_NUMBER() OVER () AS bbox_number
                FROM (
                    SELECT ST_SplitFour(_bbox_territory) AS bbox_terr
                ) AS sous_requete
            )
            LOOP
                CALL fr.ST_SimplifyTerritory(
                    levels => levels
                    , from_srid => from_srid
                    , to_srid => to_srid
                    , bbox_in => _split.bbox_terr
                    , tolerance => tolerance
                    , bbox_split_over => bbox_split_over
                    , subcall => TRUE
                    , subcall_name => CONCAT_WS('.', subcall_name, _split.bbox_number)
                );
            END LOOP;
        ELSE
            DROP TABLE IF EXISTS fr.tmp_polygon_simp;
            CREATE /*TEMPORARY*/ TABLE fr.tmp_polygon_simp AS (
                WITH polygon_to_line_string AS (
                    --plus rapide mais nécessite d'avoir un ensemble de multilinestring, et donc de faire au préalable un dumprings
                    SELECT
                        ST_SimplifyPreserveTopology(
                            ST_Node(
                                ST_Collect(
                                    ST_Boundary(tmp_polygon_to_simp.geom)
                                )
                            )
                            , tolerance
                        ) AS geom
                    --équivalent en résultat, mais 10 fois plus lent :
                    --SELECT ST_SimplifyPreserveTopology(ST_LineMerge(ST_Union(ST_Boundary(polygon_to_simp.geom))), 500) AS geom
                    FROM fr.tmp_polygon_to_simp
                    --On simplifie aussi les territoires qui sont autour des territoires à simplifier pour assurer une cohérence globale
                    --WHERE (bbox_in IS NULL OR geom && bbox_in)
                    WHERE (bbox_in IS NULL OR geom && _bbox_territory)
                    AND ST_SRID(geom) = from_srid
                )
                SELECT (ST_Dump(ST_Polygonize(ST_Node(geom)))).* FROM polygon_to_line_string
            );
            CREATE INDEX ON fr.tmp_polygon_simp USING GIST(geom);
            ALTER TABLE fr.tmp_polygon_simp ADD COLUMN id SERIAL;
            CREATE UNIQUE INDEX ON fr.tmp_polygon_simp(id);

            WITH tmp1 AS (
                --Possibilités de similitudes entre polygones dans la même étendue
                SELECT
                    polygon_to_simp.id AS polygon_to_simp_id
                    , polygon_simp.id AS polygon_simp_id
                    , (
                        ST_Area(
                            ST_Intersection(
                                ST_Envelope(polygon_simp.geom)
                                , ST_Envelope(polygon_to_simp.geom)
                            )
                        ) * 2
                    ) /
                    (
                        ST_Area(ST_Envelope(polygon_to_simp.geom))
                        + ST_Area(ST_Envelope(polygon_simp.geom))
                    ) AS sim
                FROM fr.tmp_polygon_to_simp AS polygon_to_simp
                INNER JOIN fr.tmp_polygon_simp AS polygon_simp ON polygon_simp.geom && polygon_to_simp.geom
                --A VOIR : il y a peut être un risque en n'attribuant pas de polygone simplifié au polygones "trous" à simplifier
                --Le polygone simplifié correspondant pouvant alors
                WHERE polygon_to_simp.path[1] = 0
            )
            , tmp2 AS (
                --Meilleur polygone simplifié pour chaque polygone à simplifier
                SELECT
                    polygon_to_simp_id
                    , FIRST(polygon_simp_id ORDER BY sim DESC) AS polygon_simp_id
                    , FIRST(sim ORDER BY sim DESC) AS sim
                FROM tmp1
                GROUP BY polygon_to_simp_id
            )
            , tmp3 AS (
                --Meilleur polygone à simplifier pour chaque polygone simplifié
                SELECT
                    FIRST(polygon_to_simp_id ORDER BY sim DESC) AS polygon_to_simp_id
                    , polygon_simp_id
                    , FIRST(sim ORDER BY sim DESC) AS sim
                FROM tmp2
                GROUP BY polygon_simp_id
            )
            UPDATE fr.tmp_polygon_to_simp AS polygon_to_simp
            SET polygon_simp_id = tmp3.polygon_simp_id
                , polygon_simp_sim = tmp3.sim
                , polygon_simp_geom = (
                    SELECT geom FROM fr.tmp_polygon_simp WHERE id = tmp3.polygon_simp_id
                )
                , polygon_simp_subcallname = subcall_name
                , polygon_simp_subcallbbox = bbox_in
            FROM tmp3
            WHERE polygon_to_simp.id = tmp3.polygon_to_simp_id
            --Déjà simplifié sur une passe précédente
            AND polygon_to_simp.polygon_simp_geom IS NULL
            --On n'enregistre que les simplification sur l'étendue demandée (et pas sur les territoires autours de l'étendue demandée)
            AND (bbox_in IS NULL OR polygon_to_simp.geom && bbox_in)
            ;

            GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
            RAISE NOTICE 'ST_SimplifyTerritory % : % traités', subcall_name, _nrows_affected;
        END IF;
    END IF;

    IF NOT subcall THEN
        RAISE NOTICE 'ST_SimplifyTerritory : finalisation';
        WITH territoire_simp AS (
            SELECT codgeo, ST_Union(polygon_simp_geom) AS geom
            FROM fr.tmp_polygon_to_simp
            GROUP BY codgeo
        )
        UPDATE fr.territory
        SET gm_contour = ST_Multi(ST_Transform(territoire_simp.geom, to_srid))
        FROM territoire_simp
        WHERE territory.codgeo = territoire_simp.codgeo
        AND territory.nivgeo = ANY(levels)
        AND (bbox_in IS NULL OR gm_contour_natif && bbox_in)
        AND (from_srid IS NULL OR ST_SRID(geom) = from_srid)
        AND gm_contour IS NULL;
    END IF;

    COMMIT;
END
$$ LANGUAGE plpgsql;
