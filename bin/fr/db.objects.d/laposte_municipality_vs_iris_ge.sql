/***
 * FR-LAPOSTE-MUNICIPALITY-VS-IRIS-GE management
 */

CREATE TABLE IF NOT EXISTS fr.laposte_municipality_vs_iris_ge (
    laposte CHARACTER VARYING NOT NULL,
    laposte_previous CHARACTER VARYING,
    iris CHARACTER VARYING
);

ALTER TABLE fr.laposte_municipality_vs_iris_ge SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'set_laposte_municipality_vs_iris_ge');
CREATE OR REPLACE PROCEDURE fr.set_laposte_municipality_vs_iris_ge(
)
AS $proc$
BEGIN
    TRUNCATE TABLE fr.laposte_municipality_vs_iris_ge;
    PERFORM public.drop_table_indexes('fr', 'laposte_municipality_vs_iris_ge');

    INSERT INTO fr.laposte_municipality_vs_iris_ge (
        laposte,
        laposte_previous,
        iris
    )
    WITH
    last_iris (date) AS (
        SELECT (get_last_io('FR-TERRITORY-IGN-IRIS-GE')).date_data_end
    ),

    laposte_municipalities AS (
        SELECT DISTINCT
            co_insee_commune municipality_code_laposte,
            co_insee_commune_precedente municipality_code_laposte_previous
        FROM fr.area_view
        WHERE fl_active
    ),

    iris_municipalities_to_now AS (
        SELECT
            iris.insee_com municipality_code_iris,
            iris_to_now.code municipality_code_iris_now,
            NULLIF(iris_to_now.code_previous, iris_to_now.code) municipality_code_iris_previous,
            iris_to_now.information
        FROM
            (
                -- #34981
                SELECT DISTINCT
                    insee_com
                FROM
                    fr.ign_iris_ge
            ) iris
                CROSS JOIN last_iris ref
                CROSS JOIN fr.get_municipality_to_date(
                    code => iris.insee_com,
                    code_previous => iris.insee_com,
                    date_geography_from => ref.date::DATE,
                    check_exists => FALSE
                ) iris_to_now
    )

    /*
     Successfully run. Total query runtime: 15 secs 125 msec.
     37991 rows affected.
     */
    SELECT
        laposte_municipalities.municipality_code_laposte,
        laposte_municipalities.municipality_code_laposte_previous,
        COALESCE(iris_to_now_1.municipality_code_iris, iris_to_now_2.municipality_code_iris) municipality_code_iris
    FROM laposte_municipalities
        -- search for (as priority) a municipality (IRIS data) which after to_now update is correlated to part of the new municipality
        LEFT OUTER JOIN iris_municipalities_to_now AS iris_to_now_1
            ON iris_to_now_1.municipality_code_iris_now = laposte_municipalities.municipality_code_laposte
            -- previous equal (either NULL nor NOT NULL)
            AND iris_to_now_1.municipality_code_iris_previous IS NOT DISTINCT FROM laposte_municipalities.municipality_code_laposte_previous
        LEFT OUTER JOIN iris_municipalities_to_now AS iris_to_now_2
            ON iris_to_now_2.municipality_code_iris_now = laposte_municipalities.municipality_code_laposte
            -- previous not equal AND previous#2 NULL (so previous#1 NOT NULL)
            AND iris_to_now_2.municipality_code_iris_previous IS NULL
            AND iris_to_now_1.municipality_code_iris IS NULL
    ;

    CREATE UNIQUE INDEX iux_laposte_municipality_vs_iris_ge_laposte ON fr.laposte_municipality_vs_iris_ge (laposte, laposte_previous);
END $proc$ LANGUAGE plpgsql;
