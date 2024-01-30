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
                            , fr.get_descriptors_of_street(name) descriptors_pow
                            , descriptors descriptors_laposte
                            , name
                        FROM
                            fr.laposte_address_street_uniq
                    )
                    SELECT
                        id
                        , _values[_fault_i]::INT
                        , descriptors_pow
                        , name
                    FROM
                        descriptors
                    WHERE
                        descriptors_pow IS DISTINCT FROM descriptors_laposte
                    ;
            ELSIF _keys[_fault_i] = 'TYPE' THEN
                /* NOTE
                need DESCRIPTORS-fault before!
                 */
                INSERT INTO fr.laposte_address_fault_street
                    WITH
                    type_of_street AS (
                        SELECT
                            u.id
                            , u.type_of_street type_laposte
                            , CASE
                                WHEN p.v IS NOT NULL THEN
                                    items_of_array_to_string(
                                        elements => u.words
                                        , from_ => 1
                                        , to_ => LENGTH(p.v[1])
                                    )
                                ELSE
                                    NULL
                                END type_pow
                            , u.name
                        FROM
                            fr.laposte_address_street_uniq u
                                JOIN fr.laposte_address_fault_street fs ON u.id = fs.name_id
                                LEFT OUTER JOIN LATERAL REGEXP_MATCHES(fs.help_to_fix, '^(V+)') p(v) ON TRUE
                        WHERE
                            fs.fault_id = (
                                SELECT
                                    value::INT
                                FROM
                                    fr.constant
                                WHERE
                                    usecase = 'LAPOSTE_ADDRESS_FAULT_STREET'
                                    AND
                                    key = 'DESCRIPTORS'
                            )
                    )
                    SELECT
                        id
                        , _values[_fault_i]::INT
                        , type_pow
                        , name
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

-- DESCRIPTORS
execute_query --name FAULT_DESCRIPTORS --query 'CALL fr.set_laposte_address_fault_street(
fault => '"'"'DESCRIPTORS'"'"')'

2024-01-29T09:01:41Z|info|5438|christophe|/usr/bin/bash|Lancement de l'exécution de FAULT_DESCRIPTORS (requête)
2024-01-29T09:13:46Z|info|5438|christophe|/usr/bin/bash|Exécution avec succès de FAULT_DESCRIPTORS en 0h:12m:5s

-- TYPE
execute_query --name FAULT_TYPE --query 'CALL fr.set_laposte_address_fault_street(
fault => '"'"'TYPE'"'"')'
2024-01-29T09:19:18Z|info|5438|christophe|/usr/bin/bash|Lancement de l'exécution de FAULT_TYPE (requête)
2024-01-29T09:19:19Z|info|5438|christophe|/usr/bin/bash|Exécution avec succès de FAULT_TYPE en 0h:0m:1s

-- COUNTS
FAULT   COUNT
    1       6
    2     134
    3      46
    4      11
    5    8507
    6     604

 */

-- fix fault of address (referential)
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault');
CREATE OR REPLACE FUNCTION fr.fix_laposte_address_fault(
    address_element IN VARCHAR                -- AREA|STREET|HOUSENUMBER|COMPLEMENT
    , address_join_column IN VARCHAR
    , address_update_column IN VARCHAR
    , fault_id IN INT
    , nrows OUT INT
    , column_with_new_value IN VARCHAR DEFAULT 'name'
    , column_with_old_value IN VARCHAR DEFAULT 'name_before'
    , address_alias IN VARCHAR DEFAULT 'a'
    , fault_alias IN VARCHAR DEFAULT 'f'
    , uniq_alias IN VARCHAR DEFAULT 'u'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$func$
DECLARE
    _query TEXT;
    _address_table VARCHAR;
    _fault_table VARCHAR;
    _uniq_table VARCHAR;
    _fault_key VARCHAR := CONCAT(fault_alias, '.name_id');
    _uniq_key VARCHAR := CONCAT(uniq_alias, '.id');
    _join_uniq_fault VARCHAR := CONCAT(_fault_key, ' = ', _uniq_key);
    _address_join_column VARCHAR := CONCAT(address_alias, '.', address_join_column);
    _address_update_column VARCHAR := CONCAT(address_alias, '.', address_update_column);
    _column_with_old_value VARCHAR := CONCAT(fault_alias, '.', column_with_old_value);
    _column_with_new_value VARCHAR := CONCAT(uniq_alias, '.', column_with_new_value);
BEGIN
    IF NOT address_element = ANY('{AREA,STREET,HOUSENUMBER,COMPLEMENT}') THEN
        RAISE 'élément adresse (%) non valide!', address_element;
    END IF;

    _address_table := CONCAT('fr.laposte_address_', LOWER(address_element));
    _fault_table := CONCAT('fr.laposte_address_fault_', LOWER(address_element));
    _uniq_table := CONCAT('fr.laposte_address_', LOWER(address_element), '_uniq');

    /*
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
     */

    _query := CONCAT('UPDATE ', _address_table, ' ', address_alias, ' SET
        ', address_update_column, ' = ', _column_with_new_value, '
        , dt_reference = TIMEOFDAY()::DATE
        FROM
        ', _fault_table, ' ', fault_alias, '
            JOIN ', _uniq_table, ' ', uniq_alias, ' ON ', _join_uniq_fault, '
        WHERE
            ', fault_alias, '.fault_id = ', fault_id, '
            AND
            ', _address_join_column, ' = ', _column_with_old_value, '
            AND
            ', _address_update_column, ' IS DISTINCT FROM ', _column_with_new_value
    );

    IF NOT simulation THEN
        EXECUTE _query;
        GET DIAGNOSTICS nrows = ROW_COUNT;
    ELSE
        RAISE NOTICE 'requête=%', _query;
        nrows := 0;
    END IF;
END
$func$ LANGUAGE plpgsql;

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
    _address_element VARCHAR := 'STREET';
    _address_join_column VARCHAR;
    _address_update_column VARCHAR;
    _column_with_old_value VARCHAR;
    _column_with_new_value VARCHAR;
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
    CALL public.log_info(' Chargement des anomalies de niveau Voie');

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

            _fix := TRUE;
            _address_join_column := 'lb_voie';
            _address_update_column := _address_join_column;
            _column_with_new_value := 'name';
            IF _keys[_fault_i] = 'BAD_SPACE' THEN
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
                UPDATE fr.laposte_address_street_uniq u SET
                    name = fbs.name
                    FROM fix_bad_space fbs
                    WHERE u.id = fbs.id
                ;
            ELSIF _keys[_fault_i] = 'WITH_ABBREVIATION' THEN
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
            ELSIF _keys[_fault_i] = 'DESCRIPTORS' THEN
                _address_update_column := 'lb_desc';
                _column_with_new_value := 'descriptors';
                UPDATE fr.laposte_address_street_uniq u SET
                    descriptors = fs.help_to_fix
                    FROM
                        fr.laposte_address_fault_street fs
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        u.id = fs.name_id
                ;
            ELSIF _keys[_fault_i] = 'TYPE' THEN
                _address_update_column := 'lb_type';
                _column_with_new_value := 'type_of_street';
                UPDATE fr.laposte_address_street_uniq u SET
                    type_of_street = fs.help_to_fix
                    FROM
                        fr.laposte_address_fault_street fs
                    WHERE
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        u.id = fs.name_id
                ;
            ELSE
                _fix := FALSE;
            END IF;

            IF _fix THEN
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                CALL public.log_info(CONCAT(' Mise à jour anomalies (', _keys[_fault_i], '): ', _nrows));

                -- history (before fix)
                SELECT nrows
                INTO _nrows_history
                FROM fr.add_history_address_fault(
                    address_change => _keys[_fault_i]
                    , address_element => _address_element
                    , address_join_column => _address_join_column
                    , address_update_column => _address_update_column
                    , fault_id => _values[_fault_i]::INT
                    , column_with_new_value => _column_with_new_value
                );
                CALL public.log_info(CONCAT(' Insertion Historique (', _keys[_fault_i], '): ', _nrows_history));

                -- referential
                SELECT nrows
                INTO _nrows_referential
                FROM fr.fix_laposte_address_fault(
                    address_element => _address_element
                    , address_join_column => _address_join_column
                    , address_update_column => _address_update_column
                    , fault_id => _values[_fault_i]::INT
                    , column_with_new_value => _column_with_new_value
                );
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
19:16:38.734  Chargement des anomalies de niveau Voie
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
19:22:04.804  Chargement des anomalies de niveau Voie
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
19:24:18.525  Chargement des anomalies de niveau Voie
19:24:18.837  Mise à jour anomalies (WITH_ABBREVIATION): 46
19:24:21.216  Insertion Historique (WITH_ABBREVIATION): 50
19:24:22.980  Mise à jour Référentiel (WITH_ABBREVIATION): 50

Query returned successfully in 4 secs 475 msec.

-- DESCRIPTORS
16:08:12.071 Correction des anomalies dans les libellés de voie
16:08:12.150  Chargement des anomalies de niveau Voie
16:08:14.182  Mise à jour anomalies (DESCRIPTORS): 8507
16:08:23.802  Insertion Historique (DESCRIPTORS): 20814
16:09:53.166  Mise à jour Référentiel (DESCRIPTORS): 20814

Query returned successfully in 1 min 41 secs.

-- TYPE
16:15:37.907 Correction des anomalies dans les libellés de voie
16:15:37.907  Chargement des anomalies de niveau Voie
16:15:37.925  Mise à jour anomalies (TYPE): 604
16:16:00.591  Insertion Historique (TYPE): 660
16:16:15.056  Mise à jour Référentiel (TYPE): 660

Query returned successfully in 37 secs 436 msec.
 */

-- undo fix street faults
SELECT drop_all_functions_if_exists('fr', 'undo_laposte_address_fault_street');
CREATE OR REPLACE PROCEDURE fr.undo_laposte_address_fault_street(
    fault IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
    , raise_notice IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _faults VARCHAR[];
    _keys VARCHAR[];
    _values VARCHAR[];
    _i INT;
    _fault_i INT;
    _nrows INT;
    _nrows_history INT;
    _nrows_referential INT;
    _set RECORD;
    _total_uniq INT;
    _total_referential INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Annulation des corrections des anomalies dans les libellés de voie');

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

    CALL public.log_info(' Annulation');
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

            _total_uniq := 0;
            _total_referential := 0;
            FOR _set IN (
                SELECT
                    name_before name
                FROM
                    fr.laposte_address_fault_street
                WHERE
                    fault_id = _values[_fault_i]::INT
            )
            LOOP
                IF raise_notice THEN RAISE NOTICE ' nom (%)', _set.name; END IF;

                -- referential
                UPDATE fr.laposte_address_street s SET
                    lb_voie = fs.name_before
                    FROM
                        fr.laposte_address_fault_street fs
                            JOIN fr.laposte_address_street_uniq u ON u.id = fs.name_id
                            JOIN fr.laposte_address_history h ON h.values->>'lb_voie' = _set.name
                    WHERE
                        s.lb_voie = u.name
                        AND
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        fs.name_before = _set.name
                        AND
                        h.change = _keys[_fault_i]
                        AND
                        h.kind = 'STREET'
                        AND
                        h.code_address = s.co_cea
                ;
                GET DIAGNOSTICS _nrows_referential = ROW_COUNT;
                IF raise_notice THEN
                    RAISE NOTICE ' mise à jour Référentiel (%): %', _keys[_fault_i], _nrows_referential;
                END IF;

                -- uniq
                UPDATE fr.laposte_address_street_uniq u SET
                    name = fs.name_before
                    FROM
                        fr.laposte_address_fault_street fs
                    WHERE
                        u.id = fs.name_id
                        AND
                        fs.fault_id = _values[_fault_i]::INT
                        AND
                        fs.name_before = _set.name
                ;
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                IF raise_notice THEN
                    RAISE NOTICE ' mise à jour Uniq (%): %', _keys[_fault_i], _nrows;
                END IF;

                -- history
                DELETE FROM fr.laposte_address_history
                WHERE
                    change = _keys[_fault_i]
                    AND
                    kind = 'STREET'
                    AND
                    values->>'lb_voie' = _set.name
                ;
                GET DIAGNOSTICS _nrows_history = ROW_COUNT;
                IF raise_notice THEN
                    RAISE NOTICE ' effacement Historique (%): %', _keys[_fault_i],
                    _nrows_history;
                END IF;

                IF _nrows_history IS DISTINCT FROM _nrows_referential THEN
                    RAISE ' Ecart (%): hist=%, ref=%', _keys[_fault_i], _nrows_history, _nrows_referential;
                END IF;

                _total_uniq := _total_uniq + _nrows;
                _total_referential := _total_referential + _nrows_referential;
            END LOOP;

            IF raise_notice THEN
                RAISE NOTICE ' Annulation (%): uniq=%, ref=%', _keys[_fault_i], _total_uniq, _total_referential;
            END IF;
        ELSE
            RAISE NOTICE ' Anomalie % non valide!', _faults[_i];
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;
