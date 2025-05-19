/***
 * match LAPOSTE addresses w/ IRIS_GE
 */

CREATE TABLE IF NOT EXISTS fr.laposte_address_match_iris_ge (
    code_address CHAR(10) NOT NULL,
    code_iris VARCHAR,
    code_match VARCHAR,
    match_polygon INTEGER,
    match_percent NUMERIC(3, 2),
    match_percent_next NUMERIC(3, 2),
    match_rank INTEGER,
    match_is_best BOOLEAN
);

ALTER TABLE fr.laposte_address_match_iris_ge SET (
	autovacuum_enabled = FALSE
);

DO $$
BEGIN
    IF column_exists('fr', 'laposte_address_match_iris_ge', 'match_method') THEN
        ALTER TABLE fr.laposte_address_match_iris_ge RENAME COLUMN match_method TO code_match;
    END IF;
END $$;

DROP INDEX IF EXISTS ix_laposte_address_match_iris_ge_code_address;
CREATE INDEX IF NOT EXISTS ix_laposte_address_match_iris_ge_code_address ON fr.laposte_address_match_iris_ge (code_address);

/* version
 CHANGELOG
 v1.1
    . not overload percent if best_only (w/ 100%)
    . laposte_address_match_iris_ge.code_match
      link w/ fr.constant FR_MATCH_IRIS
 */
SELECT drop_all_functions_if_exists('fr', 'get_match_iris_ge_version');
CREATE OR REPLACE FUNCTION fr.get_match_iris_ge_version(
    version OUT VARCHAR
)
AS $func$
DECLARE
    _match_version VARCHAR := '1.1';
BEGIN
    version := _match_version;
END
$func$ LANGUAGE plpgsql;

-- mode
SELECT drop_all_functions_if_exists('fr', 'get_match_iris_ge_mode');
CREATE OR REPLACE FUNCTION fr.get_match_iris_ge_mode(
    municipality IN VARCHAR,
    force_init IN BOOLEAN DEFAULT FALSE,        -- force INIT mode
    version IN VARCHAR DEFAULT NULL,            -- match version
    iris_id IN INTEGER DEFAULT NULL,            -- last IRIS_GE id (io_history)
    mode OUT VARCHAR
)
AS $func$
DECLARE
    _attributes VARCHAR;
    _attributes_json JSON;
    _date_data_end DATE;
    _io_name VARCHAR;
BEGIN
    mode := 'INIT';
    IF NOT force_init THEN
        version := COALESCE(version, fr.get_match_iris_ge_version());
        -- can't use function inside COALESCE
        IF iris_id IS NULL THEN
            iris_id := (get_last_io(name => 'FR-TERRITORY-IGN-IRIS_GE')).id;
        END IF;

        _io_name := CONCAT('FR-LAPOSTE-', municipality, '-IRIS_GE');
        IF EXISTS(
            SELECT 1
            FROM io_history
            WHERE
                name = _io_name
                AND
                status = 'SUCCES'
        ) THEN
            SELECT
                attributes,
                date_data_end
            INTO
                _attributes,
                _date_data_end
            FROM
                get_last_io(_io_name)
            ;
            -- match already done ?
            IF _attributes IS NOT NULL AND _date_data_end IS NOT NULL THEN
                _attributes_json := _attributes::JSON;
                -- w/ current (version, data IRIS_GE), not oldest ?
                IF COALESCE(
                    (NULLIF(_attributes_json->>'version', '') = version)
                    AND
                    (NULLIF((_attributes_json->>'iris_id')::INT, 0) = iris_id),
                    FALSE
                ) THEN
                    -- w/ newer address to match ?
                    IF EXISTS(
                        SELECT 1
                        FROM fr.laposte_address_xy
                        WHERE
                            co_insee = municipality
                            AND
                            dt_reference > _date_data_end
                    ) THEN
                        mode := 'DELTA';
                    ELSE
                        -- up to date !
                        mode := NULL::VARCHAR;
                    END IF;
                END IF;
            END IF;
        END IF;
    END IF;

    /*
    -- needed condition, but time-consumer !
    IF mode = 'INIT' THEN
        IF NOT EXISTS(
            SELECT 1
            FROM fr.address_view
            WHERE
                co_insee_insee = municipality
                AND
                gm_coord IS NOT NULL
        ) THEN
            -- no address to match !
            mode := NULL::VARCHAR;
        END IF;
    END IF;
     */
END
$func$ LANGUAGE plpgsql;

-- match IRIS for each address (of municipality)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_match_iris_ge');
CREATE OR REPLACE FUNCTION fr.set_laposte_address_match_iris_ge(
    municipality IN VARCHAR,
    mode IN VARCHAR DEFAULT NULL,               -- INIT | DELTA
    force_init IN BOOLEAN DEFAULT FALSE,        -- force INIT mode
    version IN VARCHAR DEFAULT NULL,            -- match version
    iris_id IN INTEGER DEFAULT NULL,            -- last IRIS_GE id (io_history)
    nrows OUT INTEGER
)
AS $$
DECLARE
    _date_data_end DATE;
BEGIN
    IF mode IS NULL THEN
        mode := fr.get_match_iris_ge_mode(
            municipality => municipality,
            force_init => force_init,
            version => version,
            iris_id => iris_id
        );
    END IF;

    IF mode = 'INIT' THEN
        INSERT INTO fr.laposte_address_match_iris_ge (
            code_address,
            code_iris,
            code_match,
            match_percent,
            match_percent_next,
            match_rank,
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
            mi.method,
            mi.percent,
            mi.percent_next,
            mi.rank,
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
                code_match = EXCLUDED.code_match,
                match_polygon = EXCLUDED.match_polygon,
                match_percent = EXCLUDED.match_percent,
                match_percent_next = EXCLUDED.match_percent_next,
                match_rank = EXCLUDED.match_rank,
                match_is_best = EXCLUDED.match_is_best
        ;
        -- number of addresses
        GET DIAGNOSTICS nrows = ROW_COUNT;

    -- DELTA mode (after INIT) for address w/ newer XY update (for last)
    ELSIF mode = 'DELTA' THEN
        _date_data_end := (get_last_io(CONCAT('FR-LAPOSTE-', municipality, '-IRIS_GE'))).date_data_end;

        INSERT INTO fr.laposte_address_match_iris_ge (
            code_address,
            code_iris,
            code_match,
            match_percent,
            match_percent_next,
            match_rank,
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
            mi.method,
            mi.percent,
            mi.percent_next,
            mi.rank,
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
            a.dt_reference_coord > _date_data_end
        ON CONFLICT(code_address) DO UPDATE
            SET
                code_iris = EXCLUDED.code_iris,
                code_match = EXCLUDED.code_match,
                match_polygon = EXCLUDED.match_polygon,
                match_percent = EXCLUDED.match_percent,
                match_percent_next = EXCLUDED.match_percent_next,
                match_rank = EXCLUDED.match_rank,
                match_is_best = EXCLUDED.match_is_best
        ;
        -- number of addresses
        GET DIAGNOSTICS nrows = ROW_COUNT;
    END IF;
END $$ LANGUAGE plpgsql;
