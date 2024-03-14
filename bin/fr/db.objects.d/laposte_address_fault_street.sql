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
            column_exists(
                schema_name => 'fr'
                , table_name => 'laposte_address_fault_street'
                , column_name => 'name_before'
            )
        ) THEN
        ALTER TABLE fr.laposte_address_fault_street DROP COLUMN name_before;
    END IF;
END $$;

-- to store fault
CREATE TABLE IF NOT EXISTS fr.laposte_address_fault_street (
    name_id INT NOT NULL
    , fault_id INT NOT NULL
    , help_to_fix VARCHAR
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
    _set_dictionary BOOLEAN;
    _query TEXT;
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
        PERFORM public.drop_table_indexes('fr', 'laposte_address_fault_street');
        _delete := TRUE;
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
            IF NOT _delete AND NOT simulation THEN
                DELETE FROM fr.laposte_address_fault_street
                WHERE
                    fault_id = _values[_fault_i]::INT
                ;
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                CALL public.log_info(CONCAT(' Purge anomalies (', _keys[_fault_i], '): ', _nrows));
            END IF;

            _set_dictionary := TRUE;
            IF _keys[_fault_i] = 'BAD_SPACE' THEN
                _query := CONCAT(
                    '
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
                            , ', _values[_fault_i]::INT, '
                            , fix.name
                        FROM
                            fr.laposte_address_street_uniq u
                                JOIN bad_space bs ON u.id = bs.id
                            , bad_space_in_name(
                                name => u.name
                            ) fix
                    '
                );
            ELSIF _keys[_fault_i] = 'DUPLICATE_WORD' THEN
                _query := CONCAT(
                    '
                        WITH
                        -- true double words!
                        except_dup_words(word) AS (
                            VALUES
                                  (''AH'')
                                , (''BADEN'')
                                , (''BIN'')
                                , (''BLIN'')
                                , (''BORA'')
                                , (''BOUTSI'')
                                , (''CASSE'')
                                , (''COLLES'')
                                , (''COTTE'')
                                , (''CRI'')
                                , (''CUIS'')
                                , (''FOU'')
                                , (''FROUS'')
                                , (''GABA'')
                                , (''HA'')
                                , (''JEAN'')
                                , (''HOURA'')
                                , (''MOUCOU'')
                                , (''MOUKOUS'')
                                , (''NOEL'')
                                , (''PAUL'')
                                , (''PEUT'')
                                , (''PILI'')
                                , (''PITE'')
                                , (''PIOU'')
                                , (''POC'')
                                , (''POUSSE'')
                                , (''PRIS'')
                                , (''RENE'')
                                , (''SOEURS'')
                                , (''QUIN'')
                                , (''TCHA'')
                                , (''TCHAT'')
                                , (''TECS'')
                                , (''TRIN'')
                                , (''TUIT'')
                                , (''TUITS'')
                                , (''VALA'')
                                , (''YLANG'')
                                , (''YLANGS'')
                        )
                        , dup_words AS (
                            SELECT
                                id
                                , REGEXP_MATCHES(name, ''\m([ A-Z]+)\s+\1\M'') dup
                            FROM
                                fr.laposte_address_street_uniq
                        )
                        SELECT
                            u.id
                            , ', _values[_fault_i]::INT, '
                            , d.dup[1]
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
                    '
                );
            ELSIF _keys[_fault_i] = 'WITH_ABBREVIATION' THEN
                -- others than ST|STE have too counter examples
                --  DU|EN not classified as article, ALL (ALL blacks), COR (chasse)
                _query := CONCAT(
                    '
                        WITH
                        word_abbreviation(abbr) AS (
                            VALUES
                                (''ST'')
                                , (''STE'')
                        )
                        SELECT
                            u.id
                            , ', _values[_fault_i]::INT, '
                            , wa.abbr
                        FROM
                            fr.laposte_address_street_uniq u
                            , word_abbreviation wa
                        WHERE
                            u.words @> ARRAY[wa.abbr]::TEXT[]
                    '
                );
            ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
                -- word containing digit, but not classified as number
                _query := CONCAT(
                    '
                        SELECT DISTINCT
                            m.name_id
                            , ', _values[_fault_i]::INT, '
                            , m.word
                        FROM
                            fr.laposte_address_street_membership m
                                JOIN fr.laposte_address_street_uniq u ON m.name_id = u.id
                        WHERE
                            m.word ~ ''[0-9]+''
                            -- even if not necessary (membership exclude is_number)
                            AND
                            NOT fr.is_normalized_number(m.word)
                    '
                );
            ELSE
                _set_dictionary := FALSE;
            END IF;

            IF _set_dictionary THEN
                _query := CONCAT(
                    '
                    INSERT INTO fr.laposte_address_fault_street
                    '
                    , _query
                );
                IF simulation THEN
                    RAISE NOTICE ' requête=%', _query;
                ELSE
                    EXECUTE _query;
                    GET DIAGNOSTICS _nrows = ROW_COUNT;
                    CALL public.log_info(CONCAT(' Ajout anomalies (', _keys[_fault_i], '): ', _nrows));
                END IF;
            END IF;
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
CALL fr.set_laposte_address_fault_street();

13:09:59.573 Identification des anomalies dans les libellés de voie
13:09:59.574  Purge
DROP INDEX IF EXISTS fr.iux_laposte_address_fault_street_id
13:09:59.665  Identification
13:10:02.911  Ajout anomalies (BAD_SPACE): 33
13:10:14.620  Ajout anomalies (DUPLICATE_WORD): 134
13:10:15.236  Ajout anomalies (WITH_ABBREVIATION): 46
13:10:16.021  Ajout anomalies (TYPO_ERROR): 10
13:10:16.066  Indexation
 */

-- fix street-faults from list (manual corrections)
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault_street_from_list');
CREATE OR REPLACE FUNCTION fr.fix_laposte_address_fault_street_from_list(
    fault IN VARCHAR
    , query_fix OUT TEXT
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$func$
DECLARE
    _exists BOOLEAN;
    _nrows INT;
BEGIN
    _exists := table_exists(
        schema_name => 'fr'
        , table_name => 'laposte_address_fault_street_correction'
    );
    IF _exists THEN
        _nrows := (
            SELECT COUNT(*) FROM fr.laposte_address_fault_street_correction
            WHERE fault_key = fault
        );
    END IF;
    IF NOT _exists OR _nrows = 0 THEN
        RAISE 'Données de corrections manquantes (%)', fault;
    END IF;

    query_fix := CONCAT('
        UPDATE fr.laposte_address_street_uniq u SET
            name = mc.name_fixed
            FROM fr.laposte_address_fault_street_correction mc
            WHERE u.name = mc.name
            AND mc.fault_key = ', quote_literal(fault)
    );
END
$func$ LANGUAGE plpgsql;

-- fix street faults
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault_street');
CREATE OR REPLACE PROCEDURE fr.fix_laposte_address_fault_street(
    fault IN VARCHAR DEFAULT 'ALL'
    , fix IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _faults VARCHAR[];
    _keys VARCHAR[];
    _values VARCHAR[];
    _fix_dictionary BOOLEAN;
    _exists BOOLEAN;
    _query TEXT;
    _i INT;
    _fault_i INT;
    _fault_id INT;
    _nrows INT;
    _nrows_history INT;
    _nrows_referential INT;
    _address_element VARCHAR := 'STREET';
    _address_join_column VARCHAR;
    _address_update_column VARCHAR;
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
            _fix_dictionary := TRUE;
            _address_join_column := 'co_cea';
            _address_update_column := 'lb_voie';
            _column_with_new_value := 'name';
            _fault_id := _values[_fault_i]::INT;
            IF _keys[_fault_i] = 'BAD_SPACE' THEN
                _query := CONCAT('
                    UPDATE fr.laposte_address_street_uniq u SET
                        name = fs.help_to_fix
                        FROM fr.laposte_address_fault_street fs
                        WHERE
                            fs.fault_id = ', _fault_id, '
                            AND
                            u.id = fs.name_id
                    '
                );
            ELSIF _keys[_fault_i] = 'DUPLICATE_WORD' THEN
                _query := fr.fix_laposte_address_fault_street_from_list(
                    fault => _keys[_fault_i]
                );
            ELSIF _keys[_fault_i] = 'WITH_ABBREVIATION' THEN
                _query := CONCAT('
                    WITH
                    word_abbreviation(word) AS (
                        SELECT DISTINCT
                            help_to_fix
                        FROM
                            fr.laposte_address_fault_street
                        WHERE
                            fault_id = ', _fault_id, '
                    )
                    , not_abbreviated(abbr, name) AS (
                        SELECT
                            MIN(wa.word)
                            , MIN(k.name)
                        FROM
                            fr.laposte_address_keyword k
                                JOIN word_abbreviation wa ON k.name_abbreviated = wa.word
                        WHERE
                            k.group = ANY(''{TITLE,TYPE}'')
                        GROUP BY
                            k.name_abbreviated
                        HAVING COUNT(*) = 1
                        /*
                        UNION
                        VALUES
                            (''STE'', ''SAINTE'')
                        */
                    )
                    UPDATE fr.laposte_address_street_uniq u SET
                        name = REGEXP_REPLACE(
                            u.name
                            , CONCAT(''\m'', fs.help_to_fix, ''\M'')
                            , na.name
                            , ''g''
                        )
                        FROM
                            fr.laposte_address_fault_street fs
                            , not_abbreviated na
                        WHERE
                            fs.fault_id = ', _values[_fault_i]::INT, '
                            AND
                            u.id = fs.name_id
                            AND
                            fs.help_to_fix = na.abbr
                    '
                );
            ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
                _query := fr.fix_laposte_address_fault_street_from_list(
                    fault => _keys[_fault_i]
                );
            ELSE
                _fix_dictionary := FALSE;
                _fault_id := -1;
                IF _keys[_fault_i] = 'DESCRIPTORS' THEN
                    _address_update_column := 'lb_desc';
                    _column_with_new_value := 'descriptors';
                ELSIF _keys[_fault_i] = 'TYPE' THEN
                    _address_update_column := 'lb_type';
                    _column_with_new_value := 'CASE
                        WHEN u.descriptors ~ ''^V'' THEN
                            fr.get_property_ordinal_item(
                                property_key => ''NAME''
                                , property_value => u.name
                                , as_words => u.as_words
                                , ordinal => 1
                            )
                        END';
                END IF;
            END IF;

            IF fix = ANY('{ALL,DICTIONARY}') THEN
                IF _fix_dictionary THEN
                    IF simulation THEN
                        RAISE NOTICE ' requête=%', _query;
                    ELSE
                        EXECUTE _query;
                        GET DIAGNOSTICS _nrows = ROW_COUNT;
                        CALL public.log_info(CONCAT(' Mise à jour anomalies (', _keys[_fault_i], '): ', _nrows));
                    END IF;
                END IF;
            END IF;

            IF fix = ANY('{ALL,REFERENTIAL}') THEN
                -- history (before updating referential)
                SELECT nrows
                INTO _nrows_history
                FROM fr.add_history_address_fault(
                    address_change => _keys[_fault_i]
                    , address_element => _address_element
                    , address_update_column => _address_update_column
                    , fault_id => _fault_id
                    , column_with_new_value => _column_with_new_value
                    , simulation => simulation
                );
                CALL public.log_info(CONCAT(' Insertion Historique (', _keys[_fault_i], '): ', _nrows_history));

                -- referential
                SELECT nrows
                INTO _nrows_referential
                FROM fr.fix_laposte_address_fault(
                    address_element => _address_element
                    , address_join_column => _address_join_column
                    , address_update_column => _address_update_column
                    , fault_id => _fault_id
                    , column_with_new_value => _column_with_new_value
                    , simulation => simulation
                );
                CALL public.log_info(CONCAT(' Mise à jour Référentiel (', _keys[_fault_i], '): ', _nrows_referential));

                IF NOT simulation THEN
                    IF _nrows_history IS DISTINCT FROM _nrows_referential THEN
                        RAISE ' Ecart (%): hist=%, ref=%', _keys[_fault_i], _nrows_history, _nrows_referential;
                    END IF;
                    COMMIT;
                END IF;
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
