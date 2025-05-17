    /*
     * polygons_contain_point_t type
     */
DO $$
DECLARE
    _query VARCHAR;
BEGIN
    IF NOT type_exists('fr', 'polygons_contain_point_t') THEN
        _query := CONCAT(
            'CREATE TYPE ',
            'fr.polygons_contain_point_t',
            ' AS (',
            'polygon INTEGER,',
            'percent NUMERIC,',
            'percent_next NUMERIC,',
            'rank INTEGER,',
            'method VARCHAR,',
            'is_best BOOLEAN',
            ')'
        );
        EXECUTE _query;
    END IF;
END $$;

    /*
     * polygons_contain_point function
     */
SELECT public.drop_all_functions_if_exists('fr', 'polygons_contain_point');
CREATE OR REPLACE FUNCTION fr.polygons_contain_point(
    point IN GEOMETRY(POINT),
    n IN INTEGER,
    polygons IN GEOMETRY[],
    point_precision IN INTEGER DEFAULT 1,   -- worse precision (centroïd)
    best_only IN BOOLEAN DEFAULT TRUE,      -- only first (if successful match)
    best_multiplier IN NUMERIC DEFAULT 2    -- how many times greater has to be the first match percent (with followning one) to achieve best value
)
RETURNS SETOF fr.polygons_contain_point_t AS
$func$
DECLARE
    _polygons_contain_point fr.polygons_contain_point_t;
    _found BOOLEAN := FALSE;
BEGIN
    IF point IS NULL THEN point_precision := NULL; END IF;
    point_precision := COALESCE(point_precision, 1);

    IF COALESCE(n, 0) = 0 THEN
        RETURN NEXT ROW(
            NULL::INTEGER,          -- AS polygon
            NULL::NUMERIC,          -- AS percent
            NULL::NUMERIC,          -- AS percent_next
            NULL::INTEGER,          -- AS rank
            '0'::VARCHAR,           -- AS method
            FALSE                   -- AS is_best
        );
        RETURN;
    ELSIF n = 1 THEN
        RETURN NEXT ROW(
            1::INTEGER,             -- AS polygon
            1::NUMERIC,             -- AS percent
            NULL::NUMERIC,          -- AS percent_next
            NULL::INTEGER,          -- AS rank
            '1'::VARCHAR,           -- AS method
            TRUE                    -- AS is_best
        );
        RETURN;
    ELSIF (n > 1 AND point_precision < 4 AND best_only) THEN
        RETURN NEXT ROW(
            NULL::INTEGER,          -- AS polygon
            NULL::NUMERIC,          -- AS percent
            NULL::NUMERIC,          -- AS percent_next
            NULL::INTEGER,          -- AS rank
            '2'::VARCHAR,           -- AS method
            FALSE                   -- AS is_best
        );
        RETURN;
    ELSE
        FOR _polygons_contain_point IN (
            WITH
            point_buffer(geom_buffer) AS (
                VALUES(
                    CASE
                    WHEN point_precision >= 4 THEN
                        ST_Transform(
                            ST_Buffer(
                                point,
                                CASE point_precision
                                    WHEN 8 THEN 5
                                    WHEN 7 THEN 10
                                    WHEN 6 THEN 25
                                    WHEN 5 THEN 100
                                    WHEN 4 THEN 500
                                END
                            ),
                            ST_Srid(polygons[1])
                        )
                    ELSE
                        NULL::GEOMETRY(POINT)
                    END
                )
            ),
            distribution AS (
                SELECT
                    polygon_index,
                    CASE
                        -- equi probability (for all polygon), no precision enough!
                        WHEN point_precision < 4 THEN 0.5
                        ELSE
                            ST_Area(ST_Intersection(geom_buffer, polygon_geom)) / (SUM(ST_Area(ST_Intersection(geom_buffer, polygon_geom))) OVER ())
                    END polygon_percent
                FROM
                    UNNEST(polygons) WITH ORDINALITY AS polygon(polygon_geom, polygon_index)
                        CROSS JOIN point_buffer
                WHERE
                    (point_precision < 4 OR ST_Intersects(geom_buffer, polygon_geom))
            )

            SELECT
                polygon_index,
                polygon_percent,
                LEAD(polygon_percent) OVER(distribution_rank) polygon_percent_next,
                (RANK() OVER (distribution_rank))::INT polygon_rank,
                '3'::VARCHAR polygon_method,
                FALSE
            FROM
                distribution
            WINDOW
                distribution_rank AS (ORDER BY polygon_percent DESC)
            ORDER BY
                polygon_rank
        ) LOOP
            _found := TRUE;
            -- evaluate best solution (w/o indecision)
            IF _polygons_contain_point.rank = 1
            AND (
                _polygons_contain_point.percent_next IS NULL
                OR _polygons_contain_point.percent > (_polygons_contain_point.percent_next * best_multiplier)
            )
            THEN
                _polygons_contain_point.is_best := TRUE;
            END IF;

            IF best_only THEN
                IF NOT _polygons_contain_point.is_best THEN
                    -- too many near polygons
                    _polygons_contain_point.method := '4'::VARCHAR;
                END IF;
                RETURN NEXT _polygons_contain_point;
                EXIT;
            ELSE
                RETURN NEXT _polygons_contain_point;
            END IF;
        END LOOP;
        IF NOT _found THEN
            RETURN NEXT ROW(
                NULL::INTEGER,          -- AS polygon
                NULL::NUMERIC,          -- AS percent
                NULL::NUMERIC,          -- AS percent_next
                NULL::INTEGER,          -- AS rank
                '5'::VARCHAR,           -- AS method
                FALSE                   -- AS is_best
            );
        END IF;
        RETURN;
    END IF;
END
$func$ LANGUAGE plpgsql;
