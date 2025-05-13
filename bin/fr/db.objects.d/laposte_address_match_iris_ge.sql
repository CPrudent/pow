/***
 * match LAPOSTE addresses w/ IRIS-GE
 */

CREATE TABLE IF NOT EXISTS fr.laposte_address_match_iris_ge (
    code_address CHAR(10) NOT NULL,
    code_iris VARCHAR,
    match_polygon INTEGER,
    match_percent NUMERIC(3, 2),
    match_percent_next NUMERIC(3, 2),
    match_rank INTEGER,
    match_method VARCHAR,
    match_is_best BOOLEAN
);

ALTER TABLE fr.laposte_address_match_iris_ge SET (
	autovacuum_enabled = FALSE
);

CREATE UNIQUE INDEX iux_laposte_address_match_iris_ge_code_address ON fr.laposte_address_match_iris_ge (code_address);

-- version
SELECT drop_all_functions_if_exists('fr', 'get_match_iris_ge_version');
CREATE OR REPLACE FUNCTION fr.get_match_iris_ge_version()
RETURNS VARCHAR AS
$func$
DECLARE
    _match_version VARCHAR := '1.0';
BEGIN
    RETURN _match_version;
END
$func$ LANGUAGE plpgsql;

-- match IRIS for each address (of municipality)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_match_iris_ge');
CREATE OR REPLACE FUNCTION fr.set_laposte_address_match_iris_ge(
    municipality IN VARCHAR,
    force_init IN BOOLEAN DEFAULT FALSE,        -- force INIT mode
    version IN VARCHAR DEFAULT NULL,            -- match version
    iris_id IN INTEGER DEFAULT NULL,            -- last IRIS-GE id (io_history)
    nrows OUT INTEGER
)
AS $$
DECLARE
    _context RECORD;
    _version VARCHAR;
    _iris_id INTEGER;
BEGIN
    _version := COALESCE(version, fr.get_match_iris_ge_version());
    _iris_id := COALESCE(iris_id, (get_last_io('FR-TERRITORY-IGN-IRIS-GE')).id;

    SELECT
        -- INIT already done, mode DELTA ...
        CASE WHEN NOT force_init AND SUM(CASE WHEN init THEN 1 ELSE 0 END) > 0 THEN 'DELTA'
        -- ... else, mode INIT
        ELSE 'INIT' END mode,
        MAX(date_data_end::DATE) date_last
    INTO
        _context
    FROM (
        SELECT
            date_data_end,
            COALESCE(
                NULLIF(attributes::json->>'VERSION', '') = _version
                AND
                NULLIF(attributes::json->>'IRIS_ID', '') = _iris_id,
                FALSE
            ) init
        FROM
            public.io_history
        WHERE
            name = CONCAT('LAPOSTE-', municipality, '-IRIS-GE')
            AND
            status = 'SUCCES'
    ) t
    ;

    IF _context.mode = 'INIT' THEN
        INSERT INTO fr.laposte_address_match_iris_ge (
            code_address,
            code_iris,
            match_polygon,
            match_percent,
            match_percent_next,
            match_rank,
            match_method,
            match_is_best
        )
        WITH
        municipality_iris_ge_agg AS (
            SELECT
                lvi.laposte,
                lvi.laposte_previous,
                CASE
                    WHEN MIN(lvi.iris) IS NULL THEN 0
                    ELSE (COUNT(*))::INT
                END count_iris,
                ARRAY_AGG(code_iris ORDER BY code_iris) set_iris,
                ARRAY_AGG(geom ORDER BY code_iris) set_geom_iris
            FROM
                fr.laposte_municipality_vs_iris_ge lvi
                    LEFT OUTER JOIN fr.ign_iris_ge i ON lvi.iris = i.insee_com
            WHERE
                lvi.laposte = municipality
            GROUP BY
                lvi.laposte,
                lvi.laposte_previous
        )
        SELECT
            a.co_adr,
            CASE
                WHEN mi.polygon IS NOT NULL THEN
                    iagg.set_iris[mi.polygon]
                ELSE
                    NULL::VARCHAR
            END code_iris,
            mi.percent,
            mi.percent_next,
            mi.rank,
            mi.method,
            mi.is_best
        FROM
            fr.address_view a
                JOIN municipality_iris_ge_agg iagg ON
                    a.co_insee_commune = iagg.laposte
                    AND
                    a.co_insee_commune_precedente IS NOT DISTINCT FROM iagg.laposte_previous
                -- match IRIS
                CROSS JOIN fr.polygons_contain_point(
                    point => a.gm_coord,
                    point_precision => a.no_type_localisation_coord,
                    n => iagg.count_iris,
                    polygons => iagg.set_geom_iris,
                    best_only => TRUE,
                    best_multiplier => 2
                ) mi
        WHERE
            a.co_insee_commune = municipality
            AND
            a.co_niveau != 'ZA'
        ON CONFLICT(code_address) DO UPDATE
            SET
                code_iris = EXCLUDED.code_iris,
                match_polygon = EXCLUDED.match_polygon,
                match_percent = EXCLUDED.match_percent,
                match_percent_next = EXCLUDED.match_percent_next,
                match_rank = EXCLUDED.match_rank,
                match_method = EXCLUDED.match_method,
                match_is_best = EXCLUDED.match_is_best
        ;
        -- number of addresses
        GET DIAGNOSTICS nrows = ROW_COUNT;
    -- DELTA mode (after INIT) for address w/ newer XY update (for last)
    ELSIF _context.mode = 'DELTA' THEN
        IF _date_last IS NOT NULL THEN
            INSERT INTO fr.laposte_address_match_iris_ge (
                code_address,
                code_iris,
                match_polygon,
                match_percent,
                match_percent_next,
                match_rank,
                match_method,
                match_is_best
            )
            WITH
            municipality_iris_ge_agg AS (
                SELECT
                    lvi.laposte,
                    lvi.laposte_previous,
                    CASE
                        WHEN MIN(lvi.iris) IS NULL THEN 0
                        ELSE (COUNT(*))::INT
                    END count_iris,
                    ARRAY_AGG(code_iris ORDER BY code_iris) set_iris,
                    ARRAY_AGG(geom ORDER BY code_iris) set_geom_iris
                FROM
                    fr.laposte_municipality_vs_iris_ge lvi
                        LEFT OUTER JOIN fr.ign_iris_ge i ON lvi.iris = i.insee_com
                WHERE
                    lvi.laposte = municipality
                GROUP BY
                    lvi.laposte,
                    lvi.laposte_previous
            )
            SELECT
                a.co_adr,
                CASE
                    WHEN mi.polygon IS NOT NULL THEN
                        iagg.set_iris[mi.polygon]
                    ELSE
                        NULL::VARCHAR
                END code_iris,
                mi.percent,
                mi.percent_next,
                mi.rank,
                mi.method,
                mi.is_best
            FROM
                fr.address_view a
                    JOIN municipality_iris_ge_agg iagg ON
                        a.co_insee_commune = iagg.laposte
                        AND
                        a.co_insee_commune_precedente IS NOT DISTINCT FROM iagg.laposte_previous
                    -- match IRIS
                    CROSS JOIN fr.polygons_contain_point(
                        point => a.gm_coord,
                        point_precision => a.no_type_localisation_coord,
                        n => iagg.count_iris,
                        polygons => iagg.set_geom_iris,
                        best_only => TRUE,
                        best_multiplier => 2
                    ) mi
            WHERE
                a.co_insee_commune = municipality
                AND
                a.co_niveau != 'ZA'
                AND
                -- address w/ XY update (for last date)
                a.dt_reference_coord > _context.date_last
            ON CONFLICT(code_address) DO UPDATE
                SET
                    code_iris = EXCLUDED.code_iris,
                    match_polygon = EXCLUDED.match_polygon,
                    match_percent = EXCLUDED.match_percent,
                    match_percent_next = EXCLUDED.match_percent_next,
                    match_rank = EXCLUDED.match_rank,
                    match_method = EXCLUDED.match_method,
                    match_is_best = EXCLUDED.match_is_best
            ;
            -- number of addresses
            GET DIAGNOSTICS nrows = ROW_COUNT;
        END IF;
    END IF;
END $$ LANGUAGE plpgsql;
