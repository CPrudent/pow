/***
 * FR: add LAPOSTE/RAN street faults
 */

/* NOTE
initialization will be done w/ constant
 */

DO $$
BEGIN
    IF table_exists(
            schema_name => 'fr'
            , table_name => 'laposte_address_fault_street'
        )
        AND (
            NOT column_exists(
                schema_name => 'fr'
                , table_name => 'laposte_address_fault_street'
                , column_name => 'help_to_fix'
            )
            OR
            NOT column_exists(
                schema_name => 'fr'
                , table_name => 'laposte_address_fault_street'
                , column_name => 'name_before'
            )
        ) THEN
        DROP TABLE fr.laposte_address_fault_street;
    END IF;
END $$;

-- to store fault
CREATE TABLE IF NOT EXISTS fr.laposte_address_fault_street (
    name_id INT NOT NULL
    , fault_id INT NOT NULL
    , help_to_fix VARCHAR
    , name_before VARCHAR
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_fault_street_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_fault_street_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_fault_street_id ON fr.laposte_address_fault_street (name_id, fault_id);
END
$proc$ LANGUAGE plpgsql;

-- identify street faults
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_fault_street');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_fault_street(
    fault IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _faults VARCHAR[];
    _keys VARCHAR[];
    _values VARCHAR[];
    _delete BOOLEAN := FALSE;
    _i INT;
    _fault_i INT;
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq')
        AND NOT table_exists('fr', 'laposte_address_street_membership') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Identification des anomalies dans les libellés de voie');

     _keys := ARRAY(
        SELECT key FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_STREET' ORDER BY value
    );
     _values := ARRAY(
        SELECT value FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_STREET' ORDER BY value
    );
    _faults := CASE
        WHEN fault = 'ALL' THEN _keys
        ELSE STRING_TO_ARRAY(fault, ',')
        END;

    IF fault = 'ALL' AND NOT simulation THEN
        CALL public.log_info(' Purge');
        TRUNCATE TABLE fr.laposte_address_fault_street;
        _delete := TRUE;
    END IF;
    IF NOT simulation THEN
        PERFORM public.drop_table_indexes('fr', 'laposte_address_fault_street');
    END IF;

    CALL public.log_info(' Identification');
    FOR _i IN 1 .. ARRAY_LENGTH(_faults, 1)
    LOOP
        _fault_i := CASE
            WHEN _faults[_i] ~ '^[0-9]+$' THEN ARRAY_POSITION(_values, _faults[_i])
            ELSE ARRAY_POSITION(_keys, _faults[_i])
            END
            ;

        IF (_fault_i > 0) THEN
            IF simulation THEN
                RAISE NOTICE ' Anomalie (%)', _keys[_fault_i];
                CONTINUE;
            END IF;

            IF NOT _delete THEN
                DELETE FROM fr.laposte_address_fault_street
                WHERE
                    fault_id = _values[_fault_i]::INT
                ;
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                CALL public.log_info(CONCAT(' Purge anomalies (', _keys[_fault_i], '): ', _nrows));
            END IF;

            IF _keys[_fault_i] = 'BAD_SPACE' THEN
                INSERT INTO fr.laposte_address_fault_street
                    WITH
                    bad_space AS (
                        SELECT
                            u.id
                        FROM
                            fr.laposte_address_street_uniq u
                            , bad_space_in_name(
                                name => u.name
                                , test_only => TRUE
                            ) bs
                        WHERE
                            bs.to_fix
                    )
                    SELECT
                        u.id
                        , _values[_fault_i]::INT
                        , fix.name
                        , u.name
                    FROM
                        fr.laposte_address_street_uniq u
                            JOIN bad_space bs ON u.id = bs.id
                        , bad_space_in_name(
                            name => u.name
                        ) fix
                    ;
            ELSIF _keys[_fault_i] = 'DUPLICATE_WORD' THEN
                INSERT INTO fr.laposte_address_fault_street
                    WITH
                    -- real double words!
                    except_dup_words(word) AS (
                        VALUES
                            ('AH')
                            , ('BIN')
                            , ('BLIN')
                            , ('BORA')
                            , ('CRI')
                            , ('CUIS')
                            , ('FOU')
                            , ('FROUS')
                            , ('HA')
                            , ('HOURA')
                            , ('MOUCOU')
                            , ('MOUKOUS')
                            , ('PILI')
                            , ('POUSSE')
                            , ('PRIS')
                            , ('QUIN')
                            , ('TCHA')
                            , ('TECS')
                            , ('TUIT')
                            , ('TUITS')
                            , ('VALA')
                            , ('YLANG')
                            , ('YLANGS')
                    )
                    , dup_words AS (
                        SELECT
                            id
                            , REGEXP_MATCHES(name, '\m([ A-Z]+)\s+\1\M') dup
                        FROM
                            fr.laposte_address_street_uniq
                    )
                    SELECT
                        u.id
                        , _values[_fault_i]::INT
                        , d.dup[1]
                        , u.name
                    FROM
                        fr.laposte_address_street_uniq u
                            JOIN dup_words d ON u.id = d.id
                    WHERE
                        d.dup IS NOT NULL
                        AND
                        NOT EXISTS(
                            SELECT 1 FROM except_dup_words x
                            WHERE d.dup[1] = x.word
                        )
                    ;
            ELSIF _keys[_fault_i] = 'WITH_ABBREVIATION' THEN
                INSERT INTO fr.laposte_address_fault_street
                    -- others than ST|STE have too counter examples
                    --  DU|EN not classified as article, ALL (ALL blacks), COR (chasse)
                    WITH
                    word_abbreviation(abbr) AS (
                        VALUES
                            ('ST')
                            , ('STE')
                    )
                    SELECT
                        u.id
                        , _values[_fault_i]::INT
                        , wa.abbr
                        , u.name
                    FROM
                        fr.laposte_address_street_uniq u
                        , word_abbreviation wa
                    WHERE
                        u.words @> ARRAY[wa.abbr]::TEXT[]
                    ;
            ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
                -- word containing digit, but not classified as number
                INSERT INTO fr.laposte_address_fault_street
                    SELECT DISTINCT
                        m.name_id
                        , _values[_fault_i]::INT
                        , m.word
                        , u.name
                    FROM
                        fr.laposte_address_street_membership m
                            JOIN fr.laposte_address_street_uniq u ON m.name_id = u.id
                    WHERE
                        m.word ~ '[0-9]+'
                        -- even if not necessary (membership exclude is_number)
                        AND
                        NOT fr.is_normalized_number(m.word)

                    ;
            ELSIF _keys[_fault_i] = 'DESCRIPTORS' THEN
                INSERT INTO fr.laposte_address_fault_street
                    WITH
                    descriptors AS (
                        SELECT
                            id
                            , fr.get_descriptor_of_street(name) descriptors_pow
                            , descriptors descriptors_laposte
                        FROM
                            fr.laposte_address_street_uniq
                    )
                    SELECT
                        id
                        , _values[_fault_i]::INT
                        , descriptors_pow
                        , NULL::VARCHAR
                    FROM
                        descriptors
                    WHERE
                        descriptors_pow IS DISTINCT FROM descriptors_laposte
                    ;
            ELSIF _keys[_fault_i] = 'TYPE' THEN
                INSERT INTO fr.laposte_address_fault_street
                    WITH
                    type_of_street AS (
                        SELECT
                            u.id
                            , ts.kw type_pow
                            , CASE
                                WHEN d.v IS NOT NULL THEN
                                    items_of_array_to_string(
                                        elements => u.words
                                        , from_ => 1
                                        , to_ => LENGTH(d.v[1])
                                    )
                                ELSE
                                    NULL
                                END type_laposte
                            FROM
                            fr.laposte_address_street_uniq u
                                LEFT OUTER JOIN LATERAL REGEXP_MATCHES(u.descriptors, '^(V+)') d(v) ON TRUE
                                CROSS JOIN fr.get_type_of_street(
                                    name => u.name
                                    , words => u.words
                                ) ts
                    )
                    SELECT
                        id
                        , _values[_fault_i]::INT
                        , type_pow
                        , NULL::VARCHAR
                    FROM
                        type_of_street
                    WHERE
                        type_pow IS DISTINCT FROM type_laposte
                    ;
            END IF;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            CALL public.log_info(CONCAT(' Ajout anomalies (', _keys[_fault_i], '): ', _nrows));
        ELSE
            RAISE NOTICE ' Anomalie % non valide!', _faults[_i];
        END IF;
    END LOOP;

    IF NOT simulation THEN
        CALL fr.set_laposte_address_fault_street_index();
        CALL public.log_info(' Indexation');
    END IF;
END
$proc$ LANGUAGE plpgsql;

/* TEST
DO $$
BEGIN
    CALL fr.set_laposte_address_fault_street(
        fault => 'BAD_SPACE,DUPLICATE_WORD,WITH_ABBREVIATION,TYPO_ERROR'
    );
END $$;

16:53:42.448 Identification des anomalies dans les libellés de voie
16:53:42.449  Identification
16:53:42.449  Purge anomalies (BAD_SPACE): 0
16:53:46.236  Ajout anomalies (BAD_SPACE): 6
16:53:46.236  Purge anomalies (DUPLICATE_WORD): 0
16:53:58.570  Ajout anomalies (DUPLICATE_WORD): 134
16:53:58.570  Purge anomalies (WITH_ABBREVIATION): 0
16:53:59.182  Ajout anomalies (WITH_ABBREVIATION): 46
16:53:59.182  Purge anomalies (TYPO_ERROR): 0
16:54:00.022  Ajout anomalies (TYPO_ERROR): 11
16:54:00.057  Indexation

Query returned successfully in 17 secs 919 msec.
 */

-- fix street faults
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault_street');
CREATE OR REPLACE PROCEDURE fr.fix_laposte_address_fault_street(
    fault IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _faults VARCHAR[];
    _keys VARCHAR[];
    _values VARCHAR[];
    _fix BOOLEAN;
    _i INT;
    _fault_i INT;
    _nrows INT;
    _nrows_history INT;
    _nrows_referential INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Correction des anomalies dans les libellés de voie');

     _keys := ARRAY(
        SELECT key FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_STREET' ORDER BY value
    );
     _values := ARRAY(
        SELECT value FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_STREET' ORDER BY value
    );
    _faults := CASE
        WHEN fault = 'ALL' THEN _keys
        ELSE STRING_TO_ARRAY(fault, ',')
        END;

    CALL public.log_info(' Correction');
    FOR _i IN 1 .. ARRAY_LENGTH(_faults, 1)
    LOOP
        _fault_i := CASE
            WHEN _faults[_i] ~ '^[0-9]+$' THEN ARRAY_POSITION(_values, _faults[_i])
            ELSE ARRAY_POSITION(_keys, _faults[_i])
            END
            ;

        IF (_fault_i > 0) THEN
            IF simulation THEN
                RAISE NOTICE ' Anomalie (%)', _keys[_fault_i];
                CONTINUE;
            END IF;

            _fix := FALSE;
            IF _keys[_fault_i] = 'BAD_SPACE' THEN
                _fix := TRUE;

                UPDATE fr.laposte_address_street_uniq u SET
                    name = fs.help_to_fix
                    FROM fr.laposte_address_fault_street fs
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        u.id = fs.name_id
                        AND
                        u.name = fs.name_before
                ;
            ELSIF _keys[_fault_i] = 'DUPLICATE_WORD' THEN
                _fix := TRUE;

                WITH
                fix_bad_space AS (
                    SELECT
                        u.id
                        , fix.name
                    FROM
                        fr.laposte_address_street_uniq u
                            JOIN fr.laposte_address_fault_street fs ON u.id = fs.name_id
                        , bad_space_in_name(
                            name => REGEXP_REPLACE(
                                u.name
                                , CONCAT('\m', fs.help_to_fix, '\M')
                                , ''
                            )
                        ) fix
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        u.name = fs.name_before
                )
                UPDATE fr.laposte_address_street_uniq u
                    SET name = fbs.name
                    FROM fix_bad_space fbs
                    WHERE u.id = fbs.id
                ;
            ELSIF _keys[_fault_i] = 'WITH_ABBREVIATION' THEN
                _fix := TRUE;

                WITH
                word_abbreviation(word) AS (
                    SELECT DISTINCT
                        help_to_fix
                    FROM
                        fr.laposte_address_fault_street
                    WHERE
                        fault_id = _values[_fault_i]::INT
                )
                , not_abbreviated AS (
                    SELECT
                        MIN(wa.word) word
                        , MIN(k.name) name
                    FROM
                        fr.laposte_address_street_keyword k
                            JOIN word_abbreviation wa ON k.name_abbreviated = wa.word
                    WHERE
                        k.group = ANY('{TITLE,TYPE}')
                    GROUP BY
                        k.name_abbreviated
                    HAVING COUNT(*) = 1
                    /*
                    UNION
                    VALUES
                        ('STE', 'SAINTE')
                     */
                )
                UPDATE fr.laposte_address_street_uniq u SET
                    name = REGEXP_REPLACE(
                        u.name
                        , CONCAT('\m', fs.help_to_fix, '\M')
                        , na.name
                        , 'g'
                    )
                    FROM
                        fr.laposte_address_fault_street fs
                        , not_abbreviated na
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        u.id = fs.name_id
                        AND
                        u.name = fs.name_before
                        AND
                        fs.help_to_fix = na.word
                    ;
            --ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
            END IF;

            IF _fix THEN
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                CALL public.log_info(CONCAT(' Mise à jour anomalies (', _keys[_fault_i], '): ', _nrows));

                -- history
                INSERT INTO fr.laposte_address_history (
                        code_address
                        , date_change
                        , change
                        , kind
                        , values
                    )
                    SELECT
                        s.co_cea
                        , TIMEOFDAY()::DATE
                        , _keys[_fault_i]
                        , 'STREET'
                        , ROW_TO_JSON(s.*)::JSONB
                    FROM
                        fr.laposte_address_street s
                            JOIN fr.laposte_address_fault_street fs ON s.lb_voie = fs.name_before
                            JOIN fr.laposte_address_street_uniq u ON fs.name_id = u.id
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        s.lb_voie IS DISTINCT FROM u.name
                        AND
                        NOT EXISTS(
                            SELECT 1 FROM fr.laposte_address_history h
                            WHERE
                                h.code_address = s.co_cea
                                AND
                                h.change = _keys[_fault_i]
                                AND
                                h.values->>'lb_voie' = fs.name_before
                        )
                ;
                GET DIAGNOSTICS _nrows_history = ROW_COUNT;
                CALL public.log_info(CONCAT(' Insertion Historique (', _keys[_fault_i], '): ', _nrows_history));

                -- referential
                UPDATE fr.laposte_address_street s SET
                    lb_voie = u.name
                    FROM
                        fr.laposte_address_fault_street fs
                            JOIN fr.laposte_address_street_uniq u ON u.id = fs.name_id
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        s.lb_voie = fs.name_before
                        AND
                        s.lb_voie IS DISTINCT FROM u.name
                ;
                GET DIAGNOSTICS _nrows_referential = ROW_COUNT;
                CALL public.log_info(CONCAT(' Mise à jour Référentiel (', _keys[_fault_i], '): ', _nrows_referential));

                IF _nrows_history IS DISTINCT FROM _nrows_referential THEN
                    RAISE ' Ecart (%): hist=%, ref=%', _keys[_fault_i], _nrows_history, _nrows_referential;
                END IF;
                COMMIT;
            ELSE
                RAISE NOTICE ' Anomalie % non corrigée en automatique', _faults[_i];
            END IF;
        ELSE
            RAISE NOTICE ' Anomalie % non valide!', _faults[_i];
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

/* TEST

-- BAD_SPACE
DO $$
BEGIN
    CALL fr.fix_laposte_address_fault_street(
        fault => 'BAD_SPACE,TYPO_ERROR'
        --, simulation => TRUE
    );
END $$;

-- first call
17:39:56.592 Correction des anomalies dans les libellés de voie
17:39:56.592  Correction
17:39:56.593  Mise à jour anomalies (BAD_SPACE): 6
17:39:56.769  Insertion Historique (BAD_SPACE): 6
17:39:57.075  Mise à jour Référentiel (BAD_SPACE): 6
Anomalie TYPO_ERROR non corrigée en automatique

Query returned successfully in 510 msec.

-- other call (already done, so 0)
19:16:38.734 Correction des anomalies dans les libellés de voie
19:16:38.734  Correction
19:16:38.735  Mise à jour anomalies (BAD_SPACE): 0
19:16:40.250  Insertion Historique (BAD_SPACE): 0
19:16:40.383  Mise à jour Référentiel (BAD_SPACE): 0
Anomalie TYPO_ERROR non corrigée en automatique

Query returned successfully in 1 secs 668 msec.

-- DUPLICATE_WORD
DO $$
BEGIN
    CALL fr.fix_laposte_address_fault_street(
        fault => 'DUPLICATE_WORD'
    );
END $$;

19:22:04.803 Correction des anomalies dans les libellés de voie
19:22:04.804  Correction
19:22:05.739  Mise à jour anomalies (DUPLICATE_WORD): 134
19:22:38.921  Insertion Historique (DUPLICATE_WORD): 155
19:22:42.466  Mise à jour Référentiel (DUPLICATE_WORD): 155

Query returned successfully in 38 secs 523 msec.

-- WITH_ABBREVIATION
DO $$
BEGIN
    CALL fr.fix_laposte_address_fault_street(
        fault => 'WITH_ABBREVIATION'
    );
END $$;

19:24:18.525 Correction des anomalies dans les libellés de voie
19:24:18.525  Correction
19:24:18.837  Mise à jour anomalies (WITH_ABBREVIATION): 46
19:24:21.216  Insertion Historique (WITH_ABBREVIATION): 50
19:24:22.980  Mise à jour Référentiel (WITH_ABBREVIATION): 50

Query returned successfully in 4 secs 475 msec.

-- DESCRIPTORS
execute_query --name FAULT_DESCRIPTORS --query 'CALL fr.set_laposte_address_fault_street(
fault => '"'"'DESCRIPTORS'"'"')'

2024-01-24T19:18:17Z|info|6562|christophe|/usr/bin/bash|Lancement de l'exécution de FAULT_DESCRIPTORS (requête)
2024-01-24T19:29:36Z|info|6562|christophe|/usr/bin/bash|Exécution avec succès de FAULT_DESCRIPTORS en 0h:11m:19s

-- TYPE
execute_query --name FAULT_TYPE --query 'CALL fr.set_laposte_address_fault_street(
fault => '"'"'TYPE'"'"')'
2024-01-25T14:31:45Z|info|2718|christophe|/usr/bin/bash|Lancement de l'exécution de FAULT_TYPE (requête)
2024-01-25T14:34:17Z|info|2718|christophe|/usr/bin/bash|Exécution avec succès de FAULT_TYPE en 0h:2m:32s

-- COUNTS
FAULT   COUNT
    1       6
    2     134
    3      46
    4      11
    5    8747
    6    1495

 */
