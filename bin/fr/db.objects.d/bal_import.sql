/***
 * FR: BAL import
 */


-- apply change(s) on municipalities (from downloaded CSV summary)
SELECT public.drop_all_functions_if_exists('fr', 'bal_upgrade_municipalities');
CREATE OR REPLACE FUNCTION fr.bal_upgrade_municipalities(
    table_name IN VARCHAR,          -- name of table loaded by external process
    simulation IN BOOLEAN DEFAULT FALSE,
    counters OUT INT[]              -- result counters {+,-,!}
)
AS
$func$
DECLARE
    _level          VARCHAR := 'commune';
    _items          CHAR(5)[];
    _q_select       TEXT := 'SELECT ARRAY_AGG(#COL1#) FROM fr.bal_municipality m FULL OUTER JOIN #TAB# s ON m.code = s.code_commune WHERE ';
    _q_where1       TEXT := '#COL2# IS NULL';
    _q_where2       TEXT := 's.composed_at::TIMESTAMP WITHOUT TIME ZONE > m.last_update';
    _q              TEXT;
    _nrows          INT;
BEGIN
    -- new ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 's.code_commune'), '#TAB#', table_name),
        REGEXP_REPLACE(_q_where1, '#COL2#', 'm.code')
    );
    EXECUTE _q INTO _items;
    counters[1] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (+) : #% %', _level, counters[1], _items;
    ELSE
        IF counters[1] > 0 THEN
            _q := CONCAT(
                '
                INSERT INTO fr.bal_municipality (
                    code,
                    name,
                    population,
                    areas,
                    streets,
                    housenumbers,
                    housenumbers_auth,
                    last_update
                )
                SELECT
                    code_commune,
                    nom_commune,
                    population::INT,
                    nb_lieux_dits::INT,
                    nb_voies::INT,
                    nb_numeros::INT,
                    nb_numeros_certifies::INT,
                    composed_at::TIMESTAMP WITHOUT TIME ZONE
                FROM
                ',
                table_name,
                '
                WHERE code_commune = ANY($1)
                '
            );
            EXECUTE _q USING _items;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[1] THEN
                RAISE '% (+) erreur: liste#%, ajout#%', _level, counters[1], _nrows;
            END IF;
        END IF;
    END IF;

    -- depreciated ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'm.code'), '#TAB#', table_name),
        REGEXP_REPLACE(_q_where1, '#COL2#', 's.code_commune')
    );
    EXECUTE _q INTO _items;
    counters[2] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (-) : #% %', _level, counters[2], _items;
    ELSE
        IF counters[2] > 0 THEN
            -- need to delete dependencies BEFORE!
            DELETE FROM fr.bal_housenumber n
            USING fr.bal_municipality m, fr.bal_street s
            WHERE
                s.id = n.id_street
                AND
                m.id = s.id_municipality
                AND
                m.code = ANY(_items)
            ;
            DELETE FROM fr.bal_street s
            USING fr.bal_municipality m
            WHERE
                m.id = s.id_municipality
                AND
                m.code = ANY(_items)
            ;
            _q := 'DELETE FROM fr.bal_municipality WHERE m.code = ANY($1)';
            EXECUTE _q USING _items;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[2] THEN
                RAISE '% (-) erreur: liste#%, suppression#%', _level, counters[2], _nrows;
            END IF;
        END IF;
    END IF;

    -- update ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'm.code'), '#TAB#', table_name),
        _q_where2
    );
    EXECUTE _q INTO _items;
    counters[3] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (!) : #% %', _level, counters[3], _items;
    ELSE
        IF counters[3] > 0 THEN
            _q := CONCAT(
                '
                UPDATE fr.bal_municipality m SET
                    name = s.nom_commune,
                    population = s.population::INT,
                    areas = s.nb_lieux_dits::INT,
                    streets = s.nb_voies::INT,
                    housenumbers = s.nb_numeros::INT,
                    housenumbers_auth = s.nb_numeros_certifies::INT,
                    last_update = s.composed_at::TIMESTAMP WITHOUT TIME ZONE
                FROM
                ',
                table_name,
                '
                s WHERE m.code = s.code_commune AND m.code = ANY($1)
                '
            );
            EXECUTE _q USING _items;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[3] THEN
                RAISE '% (!) erreur: liste#%, suppression#%', _level, counters[3], _nrows;
            END IF;
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

/*
 * tests
 *
SELECT counters FROM fr.bal_upgrade_municipalities(table_name => 'fr.tmp_bal_summary', simulation => TRUE);
 */

-- apply change(s) on street(s) (from downloaded JSON) for a given municipality
SELECT public.drop_all_functions_if_exists('fr', 'bal_upgrade_streets');
CREATE OR REPLACE FUNCTION fr.bal_upgrade_streets(
    code IN VARCHAR,                -- municipality
    table_name IN VARCHAR,          -- name of table loaded by external process
    simulation IN BOOLEAN DEFAULT FALSE,
    counters OUT INT[]              -- result counters {+,-,!}
)
AS
$func$
DECLARE
    _level          VARCHAR := 'voie';
    _check_data     BOOLEAN;
    _last           TIMESTAMP WITHOUT TIME ZONE;
    _items          VARCHAR[];
    _q_select       TEXT := '
        SELECT
            ARRAY_AGG(#COL1#)
        FROM
            (
                SELECT
                    v->>''idVoie'' code,
                    v->>''nomVoie'' name,
                    v->>''type'' kind,
                    CASE
                        WHEN v->''sources'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(v->''sources''))::VARCHAR[]
                        ELSE ARRAY[v->>''sources'']::VARCHAR[]
                    END sources,
                    (v->>''nbNumeros'')::INT housenumbers,
                    (v->>''nbNumerosCertifies'')::INT housenumbers_auth
                FROM
                    #TAB#
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->''voies'') v
                        JOIN fr.bal_municipality m ON m.code = $1
                WHERE
                    v->>''nomVoie'' IS NOT NULL
                    AND
                    data->>''codeCommune'' = $1
            ) n
            FULL OUTER JOIN
            (
                SELECT
                    s.code,
                    s.name,
                    s.kind,
                    s.sources,
                    s.housenumbers,
                    s.housenumbers_auth
                FROM
                    fr.bal_street s
                        JOIN fr.bal_municipality m ON s.id_municipality = m.id
                WHERE
                    m.code = $1
            ) o ON n.code = o.code
        WHERE
    ';
    _q_where1       TEXT := '#COL2# IS NULL';
    _q_where2       TEXT := '
        o.code = n.code
        AND
        (
            (n.name IS DISTINCT FROM o.name)
            OR
            (n.kind IS DISTINCT FROM o.kind)
            OR
            (n.sources IS DISTINCT FROM o.sources)
            OR
            (n.housenumbers IS DISTINCT FROM o.housenumbers)
            OR
            (n.housenumbers_auth IS DISTINCT FROM o.housenumbers_auth)
        )
    ';
    _q              TEXT;
    _nrows          INT;
BEGIN
    -- check data for this municipality ?
    _q := CONCAT(
        'SELECT (SELECT data->>''codeCommune'' FROM ',
        table_name,
        ') = $1'
    );
    EXECUTE _q INTO _check_data USING code;
    IF NOT COALESCE(_check_data, FALSE) THEN
        RAISE 'données Voie non valides pour commune %!', code;
    END IF;

    -- get last update of municipality (checking its code)
    _last = fr.bal_get_last_update(code => code);

    -- new ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'n.code'), '#TAB#', table_name),
        REGEXP_REPLACE(_q_where1, '#COL2#', 'o.code')
    );
    EXECUTE _q INTO _items USING code;
    counters[1] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (+) : #% %', _level, counters[1], _items;
    ELSE
        IF counters[1] > 0 THEN
            _q := CONCAT(
                '
                INSERT INTO fr.bal_street (
                    id_municipality,
                    code,
                    name,
                    kind,
                    sources,
                    housenumbers,
                    housenumbers_auth,
                    last_update
                )
                SELECT
                    m.id,
                    v->>''idVoie'',
                    v->>''nomVoie'',
                    v->>''type'',
                    CASE
                        WHEN v->''sources'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(v->''sources''))::TEXT[]
                        ELSE ARRAY[v->>''sources'']::TEXT[]
                    END,
                    (v->>''nbNumeros'')::INT,
                    (v->>''nbNumerosCertifies'')::INT,
                    $3
                FROM
                ',
                table_name,
                '
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->''voies'') v
                        JOIN fr.bal_municipality m ON m.code = $1
                WHERE v->>''idVoie'' = ANY($2)
                '
            );
            EXECUTE _q USING code, _items, _last;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[1] THEN
                RAISE '% (+) erreur: liste#%, ajout#%', _level, counters[1], _nrows;
            END IF;
        END IF;
    END IF;

    -- depreciated ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'o.code'), '#TAB#', table_name),
        REGEXP_REPLACE(_q_where1, '#COL2#', 'n.code')
    );
    EXECUTE _q INTO _items USING code;
    counters[2] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (-) : #% %', _level, counters[2], _items;
    ELSE
        IF counters[2] > 0 THEN
            -- need to delete dependencies BEFORE! here housenumbers
            DELETE FROM fr.bal_housenumber n
            USING fr.bal_municipality m, fr.bal_street s
            WHERE
                s.id = n.id_street
                AND
                m.id = s.id_municipality
                AND
                m.code = bal_upgrade_streets.code
                AND
                s.code = ANY(_items)
            ;
            _q := 'DELETE FROM fr.bal_street s USING fr.bal_municipality m WHERE m.id = s.id_municipality AND m.code = $1 AND s.code = ANY($2)';
            EXECUTE _q USING code, _items;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[2] THEN
                RAISE '% (-) erreur: liste#%, suppression#%', _level, counters[2], _nrows;
            END IF;
        END IF;
    END IF;

    -- update ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'o.code'), '#TAB#', table_name),
        _q_where2
    );
    EXECUTE _q INTO _items USING code;
    counters[3] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (!) : #% %', _level, counters[3], _items;
    ELSE
        IF counters[3] > 0 THEN
            _q := CONCAT(
                '
                UPDATE fr.bal_street s SET
                    name = v->>''nomVoie'',
                    kind = v->>''type'',
                    sources = CASE
                        WHEN v->''sources'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(v->''sources''))::TEXT[]
                        ELSE ARRAY[v->>''sources'']::TEXT[]
                    END,
                    housenumbers = (v->>''nbNumeros'')::INT,
                    housenumbers_auth = (v->>''nbNumerosCertifies'')::INT,
                    last_update = $2
                FROM
                ',
                table_name,
                '
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->''voies'') v
                WHERE v->>''idVoie'' = s.code AND s.code = ANY($1)
                '
            );
            EXECUTE _q USING _items, _last;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[3] THEN
                RAISE '% (!) erreur: liste#%, suppression#%', _level, counters[3], _nrows;
            END IF;
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

/*
 * tests
 *
SELECT counters FROM fr.bal_upgrade_streets(code => '01024', table_name => 'fr.tmp_bal_street', simulation => TRUE);    -- OK
SELECT counters FROM fr.bal_upgrade_streets(code => '01025', table_name => 'fr.tmp_bal_street', simulation => TRUE);    -- KO
 */

-- apply change(s) on housenumber(s) (from downloaded JSON) for a given municipality
SELECT public.drop_all_functions_if_exists('fr', 'bal_upgrade_housenumbers');
CREATE OR REPLACE FUNCTION fr.bal_upgrade_housenumbers(
    code IN VARCHAR,                -- municipality
    table_name IN VARCHAR,          -- name of table loaded by external process
    simulation IN BOOLEAN DEFAULT FALSE,
    counters OUT INT[]              -- result counters {+,-,!}
)
AS
$func$
DECLARE
    _level          VARCHAR := 'numéro';
    _check_data     BOOLEAN;
    _last           TIMESTAMP WITHOUT TIME ZONE;
    _items          VARCHAR[];
    _q_select       TEXT := '
        SELECT
            ARRAY_AGG(#COL1#)
        FROM
            (
                SELECT
                    n->>''id'' code,
                    (n->>''numero'')::INT number,
                    n->>''suffixe'' extension,
                    CASE
                        WHEN n->''sources'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''sources''))::VARCHAR[]
                        ELSE ARRAY[n->>''sources'']::VARCHAR[]
                    END sources,
                    n->>''postcode'' postcode,
                    n->>''lieuDitComplementNom'' area,
                    CASE
                        WHEN n->''parcelles'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''parcelles''))::VARCHAR[]
                        ELSE ARRAY[n->>''parcelles'']::VARCHAR[]
                    END parcels,
                    CASE
                        WHEN n->''position''->''coordinates'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''position''->''coordinates''))::TEXT[]::FLOAT[]
                        ELSE NULL::FLOAT[]
                    END geom,
                    n->>''positionType'' location
                FROM
                    #TAB#
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->''numeros'') n
                        JOIN fr.bal_street s ON s.code = data->>''idVoie''
                WHERE
                    UPPER(n->>''certifie'') = ''TRUE''
            ) n
            FULL OUTER JOIN
            (
                SELECT
                    n.code,
                    n.number,
                    n.extension,
                    n.sources,
                    n.postcode,
                    n.area,
                    n.parcels,
                    n.geom,
                    n.location
                FROM
                    fr.bal_housenumber n
                        JOIN fr.bal_street s ON n.id_street = s.id
                        JOIN fr.bal_municipality m ON s.id_municipality = m.id
                WHERE
                    m.code = $1
            ) o ON n.code = o.code
        WHERE
    ';
    _q_where1       TEXT := '#COL2# IS NULL';
    _q_where2       TEXT := '
        o.code = n.code
        AND
        (
            (n.number != o.number)
            OR
            (n.extension IS DISTINCT FROM o.extension)
            OR
            (n.sources IS DISTINCT FROM o.sources)
            OR
            (n.postcode IS DISTINCT FROM o.postcode)
            OR
            (n.area IS DISTINCT FROM o.area)
            OR
            (
                (CARDINALITY(n.parcels) IS DISTINCT FROM CARDINALITY(o.parcels))
                OR
                NOT ((n.parcels @> o.parcels) AND (n.parcels <@ o.parcels))
            )
            OR
            (
                (CARDINALITY(n.geom) IS DISTINCT FROM CARDINALITY(o.geom))
                OR
                (
                    (CARDINALITY(n.geom) = CARDINALITY(o.geom))
                    AND
                    (
                        (n.geom[1] != o.geom[1])
                        OR
                        (n.geom[2] != o.geom[2])
                    )
                )
            )
            OR
            (n.location IS DISTINCT FROM o.location)
        )
    ';
    _q              TEXT;
    _nrows          INT;
BEGIN
    -- check data for this municipality ?
    _q := CONCAT(
        'SELECT $1 = ALL(SELECT data->''commune''->>''id'' FROM ',
        table_name,
        ')'
    );
    EXECUTE _q INTO _check_data USING code;
    IF NOT COALESCE(_check_data, FALSE) THEN
        RAISE 'données Numéro non valides pour commune %!', code;
    END IF;

    -- get last update of municipality (checking its code)
    _last = fr.bal_get_last_update(code => code);

    -- new ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'n.code'), '#TAB#', table_name),
        REGEXP_REPLACE(_q_where1, '#COL2#', 'o.code')
    );
    EXECUTE _q INTO _items USING code;
    counters[1] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (+) : #% %', _level, counters[1], _items;
    ELSE
        IF counters[1] > 0 THEN
            _q := CONCAT(
                '
                INSERT INTO fr.bal_housenumber (
                    id_street,
                    code,
                    number,
                    extension,
                    sources,
                    postcode,
                    parcels,
                    geom,
                    location,
                    last_update
                )
                SELECT
                    s.id,
                    n->>''id'',
                    (n->>''numero'')::INT,
                    n->>''suffixe'',
                    CASE
                        WHEN n->''sources'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''sources''))::VARCHAR[]
                        ELSE ARRAY[n->>''sources'']::VARCHAR[]
                    END,
                    n->>''postcode'',
                    CASE
                        WHEN n->''parcelles'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''parcelles''))::VARCHAR[]
                        ELSE ARRAY[n->>''parcelles'']::VARCHAR[]
                    END,
                    CASE
                        WHEN n->''position''->''coordinates'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''position''->''coordinates''))::TEXT[]::FLOAT[]
                        ELSE NULL::FLOAT[]
                    END,
                    n->>''positionType'',
                    $2
                FROM
                ',
                table_name,
                '
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->''numeros'') n
                        JOIN fr.bal_street s ON s.code = data->>''idVoie''
                WHERE n->>''id'' = ANY($1)
                '
            );
            EXECUTE _q USING _items, _last;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[1] THEN
                RAISE '% (+) erreur: liste#%, ajout#%', _level, counters[1], _nrows;
            END IF;
        END IF;
    END IF;

    -- depreciated ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'o.code'), '#TAB#', table_name),
        REGEXP_REPLACE(_q_where1, '#COL2#', 'n.code')
    );
    EXECUTE _q INTO _items USING code;
    counters[2] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (-) : #% %', _level, counters[2], _items;
    ELSE
        IF counters[2] > 0 THEN
            _q := '
                DELETE FROM fr.bal_housenumber n
                USING fr.bal_municipality m, fr.bal_street s
                WHERE
                    s.id = n.id_street AND m.id = s.id_municipality
                    AND
                    m.code = $1
                    AND
                    n.code = ANY($2)
            ';
            EXECUTE _q USING code, _items;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[2] THEN
                RAISE '% (-) erreur: liste#%, suppression#%', _level, counters[2], _nrows;
            END IF;
        END IF;
    END IF;

    -- update ones ?
    _q := CONCAT(
        REGEXP_REPLACE(REGEXP_REPLACE(_q_select, '#COL1#', 'o.code'), '#TAB#', table_name),
        _q_where2
    );
    EXECUTE _q INTO _items USING code;
    counters[3] := COALESCE(CARDINALITY(_items), 0);
    IF simulation THEN
        RAISE NOTICE '% (!) : #% %', _level, counters[3], _items;
    ELSE
        IF counters[3] > 0 THEN
            _q := CONCAT(
                '
                UPDATE fr.bal_housenumber hn SET
                    number = (n->>''numero'')::INT,
                    extension = n->>''suffixe'',
                    sources = CASE
                        WHEN n->''sources'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''sources''))::VARCHAR[]
                        ELSE ARRAY[n->>''sources'']::VARCHAR[]
                    END,
                    postcode = n->>''postcode'',
                    parcels = CASE
                        WHEN n->''parcelles'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''parcelles''))::VARCHAR[]
                        ELSE ARRAY[n->>''parcelles'']::VARCHAR[]
                    END,
                    geom = CASE
                        WHEN n->''position''->''coordinates'' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->''position''->''coordinates''))::TEXT[]::FLOAT[]
                        ELSE NULL::FLOAT[]
                    END,
                    location = n->>''positionType'',
                    last_update = $2
                FROM
                ',
                table_name,
                '
                    CROSS JOIN JSON_ARRAY_ELEMENTS(data->''numeros'') n
                WHERE hn.code = n->>''id'' AND hn.code = ANY($1)
                '
            );
            EXECUTE _q USING _items, _last;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows IS DISTINCT FROM counters[3] THEN
                RAISE '% (!) erreur: liste#%, suppression#%', _level, counters[3], _nrows;
            END IF;
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

/*
 * tests
 *
SELECT counters FROM fr.bal_upgrade_housenumbers(code => '01024', table_name => 'fr.tmp_bal_housenumber', simulation => TRUE);  -- OK
SELECT counters FROM fr.bal_upgrade_housenumbers(code => '01025', table_name => 'fr.tmp_bal_housenumber', simulation => TRUE);  -- KO
 */
