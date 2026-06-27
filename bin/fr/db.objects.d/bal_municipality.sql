/***
 * FR: add BAL municipality
 */

DO $$
BEGIN
    -- old structure inherited from BCAA
    IF column_exists('fr', 'bal_municipality', 'composed_at') THEN
        DROP TABLE fr.bal_municipality;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.bal_municipality (
    id SERIAL NOT NULL,
    code CHAR(5) NOT NULL,
    name VARCHAR NOT NULL,
    population INTEGER,
    areas INTEGER,
    streets INTEGER,
    housenumbers INTEGER,
    housenumbers_auth INTEGER,
    last_update TIMESTAMP WITHOUT TIME ZONE
)
;

-- manual VACUUM
ALTER TABLE fr.bal_municipality SET (
    AUTOVACUUM_ENABLED = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'set_bal_municipality_index');
CREATE OR REPLACE PROCEDURE fr.set_bal_municipality_index()
AS
$proc$
BEGIN
    -- uniq ID, code
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_municipality_id ON fr.bal_municipality (id);
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_municipality_code ON fr.bal_municipality (code);
END
$proc$ LANGUAGE plpgsql;

-- oldies: delete obsolete addresses, dealing w/ dependencies
SELECT public.drop_all_functions_if_exists('fr', 'bal_delete_obsolete_addresses');

-- get query to select addresses of a municipality (option to limit only street w/ certified housenumbers)
SELECT public.drop_all_functions_if_exists('fr', 'bal_municipality_addresses');
CREATE OR REPLACE FUNCTION fr.bal_municipality_addresses(
    code IN VARCHAR,
    only_certified_housenumbers IN BOOLEAN DEFAULT TRUE,
    q OUT TEXT
)
AS
$func$
DECLARE
    _query_hn TEXT;
BEGIN
    IF only_certified_housenumbers THEN
        _query_hn := '
            AND
            s.housenumbers_auth > 0
        ';
    END IF;

    /*
     * remember:
     * only authed housenumber are stored, so no condition (only for street)
     */
    q := CONCAT(
        '
        WITH
        last_match(last_match) AS (
            VALUES(fr.bal_get_last_match(''', bal_municipality_addresses.code, '''))
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY t.code) rowid,
            t.*
        FROM (
            SELECT
                n.code,
                n.number,
                n.extension,
                s.name street,
                n.area,
                n.postcode,
                m.name municipality,
                m.code insee,
                n.location,
                CASE WHEN n.geom IS NOT NULL THEN
                    ST_SetSRID(ST_MakePoint(n.geom[1], n.geom[2]), 3857)
                END geom
            FROM
                fr.bal_housenumber n
                    JOIN fr.bal_street s ON s.id = n.id_street
                    JOIN fr.bal_municipality m ON m.id = s.id_municipality
                    CROSS JOIN last_match lm
            WHERE
                m.code = ''', bal_municipality_addresses.code, '''
                AND
                n.last_update > lm.last_match
            UNION
            SELECT
                s.code,
                NULL,
                NULL,
                s.name street,
                NULL,
                NULL,
                m.name municipality,
                m.code insee,
                NULL,
                CASE WHEN s.geom IS NOT NULL THEN
                    ST_SetSRID(ST_MakePoint(s.geom[1], s.geom[2]), 3857)
                END geom
            FROM
                fr.bal_street s
                    JOIN fr.bal_municipality m ON m.id = s.id_municipality
                    CROSS JOIN last_match lm
            WHERE
                m.code = ''', bal_municipality_addresses.code, '''
                AND
                s.last_update > lm.last_match
            ', _query_hn,
            '
        ) t
        '
    );
END
$func$ LANGUAGE plpgsql;

-- get last update (from BAL import)
SELECT public.drop_all_functions_if_exists('fr', 'bal_get_last_update');
CREATE OR REPLACE FUNCTION fr.bal_get_last_update(
    code IN VARCHAR,
    last_update OUT TIMESTAMP WITHOUT TIME ZONE
)
AS
$func$
BEGIN
    SELECT
        m.last_update
    INTO
        bal_get_last_update.last_update
    FROM
        fr.bal_municipality m
    WHERE
        m.code = bal_get_last_update.code
    ;
    IF NOT FOUND THEN
        RAISE 'code Commune % non trouvé!', code;
    END IF;
END
$func$ LANGUAGE plpgsql;

/*
 * tests
 *

SELECT * FROM fr.bal_get_last_update(code => '01024');  -- OK
SELECT * FROM fr.bal_get_last_update(code => '00024');  -- KO

 */

SELECT public.drop_all_functions_if_exists('fr', 'bal_get_last_match');
CREATE OR REPLACE FUNCTION fr.bal_get_last_match(
    code IN VARCHAR,
    last_match OUT TIMESTAMP WITHOUT TIME ZONE
)
AS
$func$
DECLARE
    _last_match TIMESTAMP WITHOUT TIME ZONE;
BEGIN
    WITH
    all_matched AS (
        SELECT
            ((((JSONB_PATH_QUERY(
                io.attributes::JSONB,
                '$ ? (@.usecases[*].name == "match")'
            ))->'usecases'->> 0)::JSON)->>'id')::INT id_request
        FROM
            io_history io
        WHERE
            io.name ~ CONCAT('^FR-BAL-', code)
            AND
            io.attributes IS JSON OBJECT
            AND
            'match' IN (
                SELECT (JSON_ARRAY_ELEMENTS((io.attributes::JSON)->'usecases'))->>'name'
            )
    )

    SELECT
        MAX(mrq.date_create)
    INTO
        _last_match
    FROM
        all_matched mm
            JOIN fr.address_match_request mrq ON mm.id_request = mrq.id
    ;

    IF _last_match IS NULL THEN
        _last_match := TO_TIMESTAMP('1970-01-01', 'YYYY-MM-DD');
    END IF;
    last_match := _last_match;
END
$func$ LANGUAGE plpgsql;

/*
 * tests
 *

SELECT * FROM fr.bal_get_last_match(code => '01024');  -- date
SELECT * FROM fr.bal_get_last_match(code => '75001');  -- default (not matched yet!)

 */

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_municipality_index();
END
$$;

