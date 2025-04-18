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

-- delete obsolete addresses, dealing w/ dependencies
SELECT public.drop_all_functions_if_exists('fr', 'bal_delete_obsolete_addresses');
CREATE OR REPLACE FUNCTION fr.bal_delete_obsolete_addresses(
    municipality IN VARCHAR,
    list IN VARCHAR,
    simulation IN BOOLEAN DEFAULT FALSE,
    counters OUT INT[]
)
AS
$func$
DECLARE
    _queries        TEXT[];
    _code           VARCHAR;
    _level          VARCHAR;
    _i              INT;
    _nrows          INT;
BEGIN
    IF list IS NULL OR list = '{}' THEN
        RAISE 'liste des codes vide!';
    END IF;
    _code := (list::VARCHAR[])[1];
    _level := CASE
        WHEN _code ~ '^[^_]{5}_[^_]*$' THEN 'STREET'
        WHEN _code ~ '^[^_]{5}_[^_]*_' THEN 'HOUSENUMBER'
        WHEN LENGTH(_code) = 5 THEN 'MUNICIPALITY'
        ELSE 'UNKNOWN'
        END
        ;
    IF _level = 'UNKNOWN' THEN
        RAISE 'typologie des codes non reconnue (%)', _code;
    END IF;
    IF simulation THEN
        RAISE NOTICE 'level=% municipality=%', _level, municipality;
    END IF;

    _queries := ARRAY_FILL(NULL::TEXT, ARRAY[3]);
    IF _level = 'HOUSENUMBER' THEN
        _queries[3] := '
            DELETE FROM fr.bal_housenumber n
            USING fr.bal_municipality m, fr.bal_street s
            WHERE
                s.id = n.id_street
                AND
                m.id = s.id_municipality
                AND
                m.code = $2
                AND
                n.code = ANY($1)
        ';
    ELSIF _level = 'STREET' THEN
        _queries[3] := '
            DELETE FROM fr.bal_housenumber n
            USING fr.bal_municipality m, fr.bal_street s
            WHERE
                s.id = n.id_street
                AND
                m.id = s.id_municipality
                AND
                m.code = $2
                AND
                s.code = ANY($1)
        ';
        _queries[2] := '
            DELETE FROM fr.bal_street s
            USING fr.bal_municipality m
            WHERE
                m.id = s.id_municipality
                AND
                m.code = $2
                AND
                s.code = ANY($1)
        ';
    ELSE
        _queries[3] := '
            DELETE FROM fr.bal_housenumber n
            USING fr.bal_municipality m, fr.bal_street s
            WHERE
                s.id = n.id_street
                AND
                m.id = s.id_municipality
                AND
                m.code = ANY($1)
                AND
                $2 IS NOT DISTINCT FROM $2
        ';
        _queries[2] := '
            DELETE FROM fr.bal_street s
            USING fr.bal_municipality m
            WHERE
                m.id = s.id_municipality
                AND
                m.code = ANY($1)
                AND
                $2 IS NOT DISTINCT FROM $2
        ';
        _queries[1] := '
            DELETE FROM fr.bal_municipality m
            WHERE
                m.code = ANY($1)
                AND
                $2 IS NOT DISTINCT FROM $2
        ';
    END IF;

    counters := ARRAY_FILL(0, ARRAY[3]);
    FOR _i IN REVERSE 3 .. 1 LOOP
        CONTINUE WHEN _queries[_i] IS NULL;

        IF simulation THEN
            RAISE NOTICE '%: query=%', _i, _queries[_i];
        ELSE
            EXECUTE _queries[_i]
                USING list::VARCHAR[], municipality
                ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            counters[_i] := _nrows;
        END IF;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

-- get query to select addresses of a municipality (option to limit for street only w/ certified housenumbers)
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

    q := CONCAT(
        'SELECT
            ROW_NUMBER() OVER () rowid,
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
            WHERE
                m.code = ''', bal_municipality_addresses.code, '''
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
            WHERE
                m.code = ''', bal_municipality_addresses.code, '''
            ', _query_hn,
            '
        ) t'
    );
END
$func$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_municipality_index();
END
$$;
