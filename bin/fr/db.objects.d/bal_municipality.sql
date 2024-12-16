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

-- delete obsolete addresses, dealing w/ dependences
SELECT public.drop_all_functions_if_exists('fr', 'bal_delete_obsolete_addresses');
CREATE OR REPLACE FUNCTION fr.bal_delete_obsolete_addresses(
    list IN VARCHAR,
    level IN VARCHAR DEFAULT 'MUNICIPALITY',
    municipality IN VARCHAR DEFAULT NULL,
    simulation IN BOOLEAN DEFAULT FALSE,
    counters OUT INT[]
)
AS
$func$
DECLARE
    _queries    TEXT[];
    _i          INT;
    _nrows      INT;
BEGIN
    IF NOT level = ANY('{MUNICIPALITY,STREET,HOUSENUMBER}') THEN
        RAISE 'niveau Adresse non géré! (%)', level;
    END IF;
    IF level != 'MUNICIPALITY' AND municipality IS NULL THEN
        RAISE 'code Commune obligatoire pour niveau(%)', level;
    END IF;

    _queries := ARRAY_FILL(NULL::TEXT, ARRAY[3]);
    IF level = 'HOUSENUMBER' THEN
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
    ELSIF level = 'STREET' THEN
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
                USING list, municipality
                ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            counters[_i] := _nrows;
        END IF;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_municipality_index();
END
$$;
