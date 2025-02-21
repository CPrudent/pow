/***
 * FR: add BAL aggregate
 */

CREATE TABLE IF NOT EXISTS fr.bal_agg (
    code CHAR(5) NOT NULL,
    areas INTEGER NOT NULL DEFAULT 0,
    streets INTEGER,
    housenumbers INTEGER,
    last_io INTEGER
)
;

DO $$
BEGIN
    IF NOT column_exists('fr', 'bal_agg', 'last_io') THEN
        ALTER TABLE fr.bal_agg ADD COLUMN last_io INTEGER;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_bal_agg_index');
CREATE OR REPLACE PROCEDURE fr.set_bal_agg_index()
AS
$proc$
BEGIN
    -- uniq code
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_agg_code ON fr.bal_agg (code);
END
$proc$ LANGUAGE plpgsql;

-- delete obsolete addresses, dealing w/ dependences
SELECT public.drop_all_functions_if_exists('fr', 'bal_set_agg');
CREATE OR REPLACE FUNCTION fr.bal_set_agg(
    list IN VARCHAR[],
    nrows OUT INT
)
AS
$func$
BEGIN
    INSERT INTO fr.bal_agg (
            code,
            areas,
            streets,
            housenumbers,
            last_io
        )
        WITH
        laposte_areas_with_old AS (
            SELECT
                co_insee_commune code,
                SUM(CASE WHEN lb_l5_nn IS NOT NULL THEN 1 ELSE 0 END) areas
            FROM
                fr.laposte_address_area a
            WHERE
                co_insee_commune = ANY(list)
                AND
                fl_active
                AND
                EXISTS(
                    SELECT 1
                    FROM
                        fr.laposte_address r
                    WHERE
                        r.co_cea_za = a.co_cea
                        AND
                        r.fl_active
                        AND
                        r.co_cea_voie IS NOT NULL
                )
            GROUP BY
                co_insee_commune
        )
        SELECT
            m.code,
            COALESCE(la.areas, 0),
            m.areas + m.streets,
            m.housenumbers_auth,
            io.id
        FROM
            bal_municipality m
                LEFT OUTER JOIN get_last_io('BAL_' || m.code) io ON SUBSTR(io.name, 5) = m.code
                LEFT OUTER JOIN laposte_areas_with_old la ON m.code = la.code
        WHERE
            m.code = ANY(list)
    ON CONFLICT(code) DO UPDATE
        SET
            areas = EXCLUDED.areas,
            streets = EXCLUDED.streets,
            housenumbers = EXCLUDED.housenumbers,
            last_io = EXCLUDED.last_io
    ;
    GET DIAGNOSTICS nrows = ROW_COUNT;
END
$func$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_agg_index();
END
$$;
