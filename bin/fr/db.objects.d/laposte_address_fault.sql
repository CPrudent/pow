/***
 * FR: add LAPOSTE/RAN faults
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store fault
CREATE TABLE IF NOT EXISTS fr.laposte_address_fault (
    element VARCHAR NOT NULL
    , name_id INT NOT NULL
    , fault_id INT NOT NULL
    , help_to_fix VARCHAR
);

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_fault_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_fault_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_fault_id ON fr.laposte_address_fault (element, name_id, fault_id);
END
$proc$ LANGUAGE plpgsql;

-- identify element-faults
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_fault');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_fault(
    element IN VARCHAR
    , fault IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
    , raise_notice IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _table_uniq VARCHAR := fr.get_table_name(element, 'UNIQ');
    _table_membership VARCHAR := fr.get_table_name(element, 'MEMBERSHIP');
    _usecase_fault VARCHAR :=
        CONCAT('LAPOSTE_ADDRESS_FAULT_', UPPER(element));
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
    IF (
        NOT table_exists('fr', _table_uniq)
        AND
        NOT table_exists('fr', _table_membership)
    ) THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info(
        CONCAT('Identification des anomalies dans les libellés de '
            , CASE element
                WHEN 'STREET' THEN 'voie'
                ELSE 'complément (L3)'
                END
        )
    );

     _keys := ARRAY(
        SELECT key FROM fr.constant WHERE usecase = _usecase_fault ORDER BY value
    );
     _values := ARRAY(
        SELECT value FROM fr.constant WHERE usecase = _usecase_fault ORDER BY value
    );
    _faults := CASE
        WHEN fault = 'ALL' THEN _keys
        ELSE STRING_TO_ARRAY(fault, ',')
        END;
    IF raise_notice THEN
        RAISE NOTICE ' Anomalies k=% v=%', _keys, _values;
    END IF;

    IF fault = 'ALL' AND NOT simulation THEN
        CALL public.log_info(' Purge');
        _query := '
            DELETE FROM fr.laposte_address_fault
            WHERE element = $1
        ';
        EXECUTE _query USING element;
        PERFORM public.drop_table_indexes('fr', 'laposte_address_fault');
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
                _query := CONCAT(
                    '
                    DELETE FROM fr.laposte_address_fault
                    WHERE
                        element = $1
                        AND
                        fault_id = $2::INT
                    '
                );
                EXECUTE _query USING element, _values[_fault_i];
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
                            fr.', _table_uniq, ' u
                            , bad_space_in_name(
                                name => u.name
                                , test_only => TRUE
                            ) bs
                        WHERE
                            bs.to_fix
                    )
                    SELECT
                        $1
                        , u.id
                        , $2::INT
                        , fix.name
                    FROM
                        fr.', _table_uniq, ' u
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
                    dup_words AS (
                        SELECT
                            id
                            , REGEXP_MATCHES(name, ''\m([ A-Z]+)\s+\1\M'') dup
                        FROM
                            fr.', _table_uniq, '
                    )
                    SELECT
                        $1
                        , u.id
                        , $2::INT
                        , d.dup[1]
                    FROM
                        fr.', _table_uniq, ' u
                            JOIN dup_words d ON u.id = d.id
                    WHERE
                        d.dup IS NOT NULL
                        AND
                        -- avoid spaced abbreviation (i.e. A A P H)
                        LENGTH(d.dup[1]) > 1
                        AND
                        -- true double words!
                        NOT EXISTS(
                            SELECT 1 FROM fr.constant c
                            WHERE
                                c.usecase = ''LAPOSTE_ADDRESS_FAULT_EXCEPTION''
                                AND
                                c.key = ''DUPLICATE_WORD''
                                AND
                                d.dup[1] = c.value
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
                        $1
                        , u.id
                        , $2::INT
                        , wa.abbr
                    FROM
                        fr.', _table_uniq, ' u
                        , word_abbreviation wa
                    WHERE
                        u.words @> ARRAY[wa.abbr]::TEXT[]
                    '
                );
            ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
                -- word containing digit, but not classified as number
                /* NOTE
                have to unify row of element (w/ 2 faults: 1A3, 18A21)
                VILLA 1A3 18A21 LOTISSEMENT HAMEAU DE LA CROIX
                 */
                _query := CONCAT(
                    '
                    SELECT
                        $1
                        , m.name_id
                        , $2::INT
                        , FIRST(m.word) word
                    FROM
                        fr.', _table_membership, ' m
                    WHERE
                        m.word ~ ''[0-9]+''
                        AND
                        NOT fr.is_normalized_number(m.word)
                        AND
                        -- exception! true names
                        NOT m.word = ANY(''{5LYS,PARC2CE,H2HOME}'')
                    GROUP BY
                        m.name_id
                    '
                );
            ELSE
                _set_dictionary := FALSE;
            END IF;

            IF _set_dictionary THEN
                _query := CONCAT(
                    '
                    INSERT INTO fr.laposte_address_fault
                    '
                    , _query
                );
                IF simulation THEN
                    RAISE NOTICE ' requête=%', _query;
                ELSE
                    EXECUTE _query USING element, _values[_fault_i];
                    GET DIAGNOSTICS _nrows = ROW_COUNT;
                    CALL public.log_info(CONCAT(' Ajout anomalies (', _keys[_fault_i], '): ', _nrows));
                END IF;
            END IF;
        ELSE
            RAISE NOTICE ' Anomalie % non valide!', _faults[_i];
        END IF;
    END LOOP;

    IF NOT simulation THEN
        CALL fr.set_laposte_address_fault_index();
        CALL public.log_info(' Indexation');
    END IF;
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_fault(element => 'STREET');

CALL fr.set_laposte_address_fault(element => 'COMPLEMENT');

11:40:49.420 Identification des anomalies dans les libellés de complément (L3)
Anomalies k={BAD_SPACE,DUPLICATE_WORD,WITH_ABBREVIATION,TYPO_ERROR,DESCRIPTORS} v={400,401,402,403,404}
11:40:49.421  Purge
11:40:49.422  Identification
11:40:50.675  Ajout anomalies (BAD_SPACE): 0
11:40:57.494  Ajout anomalies (DUPLICATE_WORD): 180
11:40:57.747  Ajout anomalies (WITH_ABBREVIATION): 749
11:40:58.553  Ajout anomalies (TYPO_ERROR): 174
11:40:58.959  Indexation

Query returned successfully in 9 secs 574 msec.
 */

-- fix element-faults (in referential)
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault_referential');
CREATE OR REPLACE FUNCTION fr.fix_laposte_address_fault_referential(
    element IN VARCHAR                          -- AREA|STREET|HOUSENUMBER|COMPLEMENT
    , column_join IN VARCHAR                    -- join ADDRESS to REFERENCE
    , column_update IN VARCHAR                  -- column to change
    , fault_id IN INT                           -- fault ID
    , column_with_new_value IN VARCHAR DEFAULT 'name'
    , alias_address IN VARCHAR DEFAULT 'a'
    , alias_fault IN VARCHAR DEFAULT 'f'
    , alias_uniq IN VARCHAR DEFAULT 'u'
    , alias_reference IN VARCHAR DEFAULT 'r'
    , simulation IN BOOLEAN DEFAULT FALSE
    , nrows OUT INT
)
AS
$func$
DECLARE
    _query TEXT;
    _table_address VARCHAR;
    _table_uniq VARCHAR;
    _table_reference VARCHAR;
    _fault_key VARCHAR := CONCAT(alias_fault, '.name_id');
    _uniq_key VARCHAR := CONCAT(alias_uniq, '.id');
    _reference_key VARCHAR := CONCAT(alias_reference, '.name_id');
    _join_uniq_fault VARCHAR := CONCAT(_fault_key, ' = ', _uniq_key);
    _join_uniq_reference VARCHAR := CONCAT(_reference_key, ' = ', _uniq_key);
    _column_join VARCHAR := CONCAT(alias_address, '.', column_join);
    _column_update VARCHAR := CONCAT(alias_address, '.', column_update);
    _column_with_new_value VARCHAR := CASE
        WHEN count_words(column_with_new_value) = 1 THEN
            CONCAT(alias_uniq, '.', column_with_new_value)
        ELSE
            column_with_new_value
        END
        ;
BEGIN
    IF NOT element = ANY('{AREA,STREET,HOUSENUMBER,COMPLEMENT}') THEN
        RAISE 'élément adresse (%) non valide!', element;
    END IF;

    _table_address := CONCAT('fr.', fr.get_table_name(element, 'ADDRESS'));
    _table_uniq := CONCAT('fr.', fr.get_table_name(element, 'UNIQ'));
    _table_reference := CONCAT('fr.', fr.get_table_name(element, 'REFERENCE'));

    _query := CONCAT('UPDATE ', _table_address, ' ', alias_address, ' SET
        ', column_update, ' = ', _column_with_new_value, '
        , dt_reference = TIMEOFDAY()::DATE
        FROM
        '
    );
    IF fault_id >= 0 THEN
        -- correction from element-fault (only address w/ fault)
        _query := CONCAT(_query
            , _table_fault, ' ', alias_fault, '
                JOIN ', _table_uniq, ' ', alias_uniq, ' ON ', _join_uniq_fault, '
                JOIN ', _table_reference, ' ', alias_reference, ' ON ', _join_uniq_reference, '
            WHERE
            ', alias_fault, '.fault_id = ', fault_id, '
            AND
            '
        );
    ELSE
        -- correction from element-dictionary (all address w/ difference)
        _query := CONCAT(_query
            , _table_uniq, ' ', alias_uniq, '
                JOIN ', _table_reference, ' ', alias_reference, ' ON ', _join_uniq_reference, '
            WHERE
            '
        );
    END IF;
    _query := CONCAT(_query
        , _column_join, ' = ', CONCAT(alias_reference, '.address_id'), '
        AND
        ', _column_update, ' IS DISTINCT FROM ', _column_with_new_value
    );

    IF NOT simulation THEN
        EXECUTE _query;
        GET DIAGNOSTICS nrows = ROW_COUNT;
    ELSE
        RAISE NOTICE ' requête=%', _query;
        nrows := 0;
    END IF;
END
$func$ LANGUAGE plpgsql;

-- fix element-faults (dictionary and/or referential)
/* NOTE
fix dictionary (uniq)
fix referential (address) : no!
    . POW's updates not uploaded in RAN, so will be detected as changes w/
      a more recent RAN archive!
    . hard to do w/ COMPLEMENT (divided in 3 groups, many sub-values)
    . moreover, no rule to choice group (w/o group keyword)

=> at last dictionary becomes new referential, except AREA
 */
SELECT drop_all_functions_if_exists('fr', 'fix_laposte_address_fault');
CREATE OR REPLACE PROCEDURE fr.fix_laposte_address_fault(
    element IN VARCHAR
    , fault IN VARCHAR DEFAULT 'ALL'
    , fix IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _table_uniq VARCHAR := fr.get_table_name(element, 'UNIQ');
    _usecase_fault VARCHAR :=
        CONCAT('LAPOSTE_ADDRESS_FAULT_', UPPER(element));
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
    _column_join VARCHAR;
    _column_update VARCHAR;
    _column_with_new_value VARCHAR;
    _manual_correction BOOLEAN;
BEGIN
    IF (
        NOT table_exists('fr', _table_uniq)
    ) THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info(
        CONCAT('Correction des anomalies dans les libellés de '
            , CASE element
                WHEN 'STREET' THEN 'voie'
                ELSE 'complément (L3)'
                END
        )
    );

     _keys := ARRAY(
        SELECT key FROM fr.constant WHERE usecase = _usecase_fault ORDER BY value
    );
     _values := ARRAY(
        SELECT value FROM fr.constant WHERE usecase = _usecase_fault ORDER BY value
    );
    _faults := CASE
        WHEN fault = 'ALL' THEN _keys
        ELSE STRING_TO_ARRAY(fault, ',')
        END;
    CALL public.log_info(
        CONCAT(' Chargement des anomalies de niveau '
            , CASE element
                WHEN 'STREET' THEN 'Voie'
                ELSE 'Complément (L3)'
                END
        )
    );

    FOR _i IN 1 .. ARRAY_LENGTH(_faults, 1)
    LOOP
        _fault_i := CASE
            WHEN _faults[_i] ~ '^[0-9]+$' THEN ARRAY_POSITION(_values, _faults[_i])
            ELSE ARRAY_POSITION(_keys, _faults[_i])
            END
            ;

        IF (_fault_i > 0) THEN
            _fix_dictionary := TRUE;
            _manual_correction := FALSE;
            _column_join := 'co_cea';
            _column_update := CASE element
                WHEN 'STREET' THEN 'lb_voie'
                WHEN 'COMPLEMENT' THEN
                    '
                    CONCAT_WS('' ''
                        , lb_type_groupe1_l3
                        , lb_groupe1
                        , lb_type_groupe2_l3
                        , lb_groupe2
                        , lb_type_groupe3_l3
                        , lb_groupe3
                    )
                    '
                END
            ;
            _column_with_new_value := 'name';
            _fault_id := _values[_fault_i]::INT;
            IF _keys[_fault_i] = 'BAD_SPACE' THEN
                _query := CONCAT('
                    UPDATE fr.', _table_uniq, ' u SET
                        name = f.help_to_fix
                        FROM fr.laposte_address_fault f
                        WHERE
                            f.element = $1
                            AND
                            f.fault_id = $2
                            AND
                            u.id = f.name_id
                    '
                );
            ELSIF _keys[_fault_i] = 'DUPLICATE_WORD' THEN
                _manual_correction := TRUE;
                _query := fr.get_query_to_fix_from_manual_correction(
                    element => element
                    , fault => _keys[_fault_i]
                );
            ELSIF _keys[_fault_i] = 'WITH_ABBREVIATION' THEN
                _query := CONCAT('
                    WITH
                    word_abbreviation(word) AS (
                        SELECT DISTINCT
                            help_to_fix
                        FROM
                            fr.laposte_address_fault
                        WHERE
                            element = $1
                            AND
                            fault_id = $2
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
                    )
                    UPDATE fr.', _table_uniq, ' u SET
                        name = REGEXP_REPLACE(
                            u.name
                            , CONCAT(''\m'', f.help_to_fix, ''\M'')
                            , na.name
                            , ''g''
                        )
                        FROM
                            fr.laposte_address_fault f
                            , not_abbreviated na
                        WHERE
                            f.element = $1
                            AND
                            f.fault_id = $2
                            AND
                            u.id = f.name_id
                            AND
                            f.help_to_fix = na.abbr
                    '
                );
            ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
                _manual_correction := TRUE;
                _query := fr.get_query_to_fix_from_manual_correction(
                    element => element
                    , fault => _keys[_fault_i]
                );
            ELSE
                _fix_dictionary := FALSE;
                _fault_id := -1;
                IF _keys[_fault_i] = 'DESCRIPTORS' THEN
                    _column_update := CASE element
                        WHEN 'STREET' THEN 'lb_desc'
                        WHEN 'COMPLEMENT' THEN
                            '
                            CONCAT(
                                lb_descr_nn_groupe1
                                , lb_descr_nn_groupe2
                                , lb_descr_nn_groupe3
                            )
                            '
                        END
                    ;
                    _column_with_new_value := 'descriptors';
                ELSIF _keys[_fault_i] = 'TYPE' THEN
                    _column_update := 'lb_type';
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
                        IF _manual_correction THEN
                            EXECUTE _query;
                        ELSE
                            EXECUTE _query
                                USING element, _fault_id;
                        END IF;
                        GET DIAGNOSTICS _nrows = ROW_COUNT;
                        CALL public.log_info(CONCAT(' Mise à jour DICTIONNAIRE (', _keys[_fault_i], '): ', _nrows));
                    END IF;
                END IF;
            END IF;

            IF fix = ANY('{ALL,HISTORY,REFERENTIAL}') THEN
                -- history
                SELECT nrows
                INTO _nrows_history
                FROM fr.add_history_address_fault(
                    element => element
                    , column_update => _column_update
                    , fault_name => _keys[_fault_i]
                    , fault_id => _fault_id
                    , column_with_new_value => _column_with_new_value
                    , simulation => simulation
                );
                CALL public.log_info(CONCAT(' Insertion HISTORIQUE (', _keys[_fault_i], '): ', _nrows_history));

                IF 1=0 THEN
                    -- referential
                    SELECT nrows
                    INTO _nrows_referential
                    FROM fr.fix_laposte_address_fault_referential(
                        element => element
                        , column_join => _column_join
                        , column_update => _column_update
                        , fault_id => _fault_id
                        , column_with_new_value => _column_with_new_value
                        , simulation => simulation
                    );
                    CALL public.log_info(CONCAT(' Mise à jour REFERENTIEL (', _keys[_fault_i], '): ', _nrows_referential));

                    IF NOT simulation THEN
                        IF _nrows_history IS DISTINCT FROM _nrows_referential THEN
                            RAISE ' Ecart (%): hist=%, ref=%', _keys[_fault_i], _nrows_history, _nrows_referential;
                        END IF;
                        COMMIT;
                    END IF;
                ELSE
                    CALL public.log_info(CONCAT(' Mise à jour REFERENTIEL dévalidée (élément=', element, ')'));
                END IF;
            END IF;
        ELSE
            RAISE NOTICE ' Anomalie % non valide!', _faults[_i];
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.fix_laposte_address_fault(
    element => 'COMPLEMENT'
    , fault => 'DUPLICATE_WORD'
);

10:44:53.244 Correction des anomalies dans les libellés de complément (L3)
10:44:53.245  Chargement des anomalies de niveau Complément (L3)
10:44:56.986  Mise à jour DICTIONNAIRE (DUPLICATE_WORD): 56
10:44:58.986  Insertion HISTORIQUE (DUPLICATE_WORD): 81
              Mise à jour REFERENTIEL dévalidée (élément=COMPLEMENT)

Query returned successfully in 5 secs 772 msec.

CALL fr.fix_laposte_address_fault(
    element => 'COMPLEMENT'
    , fault => 'WITH_ABBREVIATION'
);

10:54:05.439 Correction des anomalies dans les libellés de complément (L3)
10:54:05.440  Chargement des anomalies de niveau Complément (L3)
10:54:06.902  Mise à jour DICTIONNAIRE (WITH_ABBREVIATION): 749
10:54:14.042  Insertion HISTORIQUE (WITH_ABBREVIATION): 1126
10:54:14.042  Mise à jour REFERENTIEL dévalidée (élément=COMPLEMENT)

Query returned successfully in 8 secs 622 msec.

CALL fr.fix_laposte_address_fault(
    element => 'COMPLEMENT'
    , fault => 'TYPO_ERROR'
);

10:55:15.333 Correction des anomalies dans les libellés de complément (L3)
10:55:15.333  Chargement des anomalies de niveau Complément (L3)
10:55:20.008  Mise à jour DICTIONNAIRE (TYPO_ERROR): 110
10:55:21.536  Insertion HISTORIQUE (TYPO_ERROR): 142
10:55:21.537  Mise à jour REFERENTIEL dévalidée (élément=COMPLEMENT)

Query returned successfully in 6 secs 219 msec.
 */

-- undo fix element-faults
SELECT drop_all_functions_if_exists('fr', 'undo_laposte_address_fault_street');
SELECT drop_all_functions_if_exists('fr', 'undo_laposte_address_fault');
CREATE OR REPLACE PROCEDURE fr.undo_laposte_address_fault(
    element IN VARCHAR
    , fault IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
    , raise_notice IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _table_uniq VARCHAR;
    _table_reference VARCHAR;
    _usecase_fault VARCHAR :=
        CONCAT('LAPOSTE_ADDRESS_FAULT_', UPPER(element));
    _faults VARCHAR[];
    _keys VARCHAR[];
    _values VARCHAR[];
    _query TEXT;
    _i INT;
    _fault_i INT;
    _column_value VARCHAR;
    _column_update VARCHAR;
    _nrows INT;
    _nrows_history INT;
    _nrows_uniq INT;
BEGIN
    CALL public.log_info(
        CONCAT('Annulation des corrections des anomalies dans les libellés de '
            , CASE element
                WHEN 'STREET' THEN 'voie'
                ELSE 'complément (L3)'
                END
        )
    );

    _table_uniq := CONCAT('fr.', fr.get_table_name(element, 'UNIQ'));
    _table_reference := CONCAT('fr.', fr.get_table_name(element, 'REFERENCE'));

     _keys := ARRAY(
        SELECT key FROM fr.constant WHERE usecase = _usecase_fault ORDER BY value
    );
     _values := ARRAY(
        SELECT value FROM fr.constant WHERE usecase = _usecase_fault ORDER BY value
    );
    _faults := CASE
        WHEN fault = 'ALL' THEN _keys
        ELSE STRING_TO_ARRAY(fault, ',')
        END;

    IF NOT simulation THEN
        DROP TABLE IF EXISTS fr.tmp_address_fault_undo;
        CREATE UNLOGGED TABLE fr.tmp_address_fault_undo (
            code_address CHAR(10) NOT NULL
            , id INT
            , date_change DATE
            , value_before VARCHAR
        );
    END IF;

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

            _column_value := CASE element
                WHEN 'STREET' THEN
                    CASE _keys[_fault_i]
                        WHEN 'DESCRIPTORS' THEN
                            CONCAT('h.values->>', quote_literal('lb_desc'))
                        WHEN 'TYPE' THEN
                            CONCAT('h.values->>', quote_literal('lb_type'))
                        ELSE
                            CONCAT('h.values->>', quote_literal('lb_voie'))
                        END
                WHEN 'COMPLEMENT' THEN
                    CASE _keys[_fault_i]
                        WHEN 'DESCRIPTORS' THEN
                            CONCAT(
                                CONCAT(
                                    'h.values->>', quote_literal('lb_descr_nn_groupe1')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_descr_nn_groupe2')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_descr_nn_groupe3')
                                )
                            )
                        WHEN 'TYPE' THEN
                            NULL
                        ELSE
                            CONCAT_WS(' '
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_type_groupe1_l3')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_groupe1')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_type_groupe2_l3')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_groupe2')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_type_groupe3_l3')
                                )
                                , CONCAT(
                                    'h.values->>', quote_literal('lb_groupe3')
                                )
                            )
                        END
                END
            ;
            _column_update := CASE element
                WHEN 'STREET' THEN
                    CASE _keys[_fault_i]
                        WHEN 'DESCRIPTORS' THEN
                            'descriptors'
                        WHEN 'TYPE' THEN
                            NULL
                        ELSE
                            'name'
                        END
                WHEN 'COMPLEMENT' THEN
                    CASE _keys[_fault_i]
                        WHEN 'DESCRIPTORS' THEN
                            'descriptors'
                        WHEN 'TYPE' THEN
                            NULL
                        ELSE
                            'name'
                        END
                END
            ;

            IF _column_value IS NULL THEN
                CONTINUE;
            END IF;

            IF NOT simulation THEN
                TRUNCATE TABLE fr.tmp_address_fault_undo;
            END IF;

            _query := CONCAT(
                '
                INSERT INTO fr.tmp_address_fault_undo
                WITH
                last_change AS (
                    SELECT
                        h.code_address
                        , MAX(h.date_change) date_change
                        , FIRST(r.name_id) id
                    FROM
                        fr.laposte_address_history h
                            JOIN ', _table_reference, ' r ON h.code_address = r.address_id
                    WHERE
                        h.kind = $1
                        AND
                        h.change = $2
                    GROUP BY
                        h.code_address
                )
                , last_change_with_value AS (
                    SELECT
                        h.code_address
                        , lc.id
                        , lc.date_change
                        , ', _column_value, ' value_before
                    FROM
                        fr.laposte_address_history h
                            JOIN last_change lc ON h.code_address = lc.code_address
                    WHERE
                        h.date_change = lc.date_change
                        AND
                        h.kind = $1
                        AND
                        h.change = $2
                )
                SELECT * FROM last_change_with_value
                '
            );

            IF simulation THEN
                RAISE NOTICE ' requête=%', _query;
            ELSE
                EXECUTE _query USING element, _keys[_fault_i];
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                CALL public.log_info(CONCAT(' Préparation (', _keys[_fault_i], '): ', _nrows));
            END IF;

            -- uniq
            IF _column_update IS NOT NULL THEN
                _query := CONCAT(
                    '
                    UPDATE ', _table_uniq, ' u SET
                        ', _column_update, ' = fu.value_before
                        FROM
                            fr.tmp_address_fault_undo fu
                        WHERE
                            u.id = fu.id
                    '
                );
                IF simulation THEN
                    RAISE NOTICE ' requête=%', _query;
                ELSE
                    EXECUTE _query;
                    GET DIAGNOSTICS _nrows_uniq = ROW_COUNT;
                    IF raise_notice THEN
                        RAISE NOTICE ' Mise à jour DICTIONNAIRE (%): %', _keys[_fault_i], _nrows_uniq;
                    END IF;
                END IF;
            END IF;

            -- history
            _query :=
                '
                DELETE FROM fr.laposte_address_history h
                USING fr.tmp_address_fault_undo fu
                WHERE
                    h.code_address = fu.code_address
                    AND
                    h.change = $2
                    AND
                    h.kind = $1
                '
            ;
            IF simulation THEN
                RAISE NOTICE ' requête=%', _query;
            ELSE
                EXECUTE _query USING element, _keys[_fault_i];
                GET DIAGNOSTICS _nrows_history = ROW_COUNT;
                IF raise_notice THEN
                    RAISE NOTICE ' Effacement HISTORIQUE (%): %', _keys[_fault_i],
                    _nrows_history;
                END IF;
            END IF;
        ELSE
            RAISE NOTICE ' Anomalie % non valide!', fault_id;
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

-- fix link-faults
SELECT public.drop_all_functions_if_exists('fr', 'fix_laposte_address_fault_links');
CREATE OR REPLACE PROCEDURE fr.fix_laposte_address_fault_links(
    simulation BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _set RECORD;
    _query TEXT;
    _nrows_found INT;
    _nrows_history INT;
    _nrows_fixed INT;
    _table_from VARCHAR;
    _column_from VARCHAR;
    _columns_to VARCHAR;
    _kind VARCHAR := 'LINK';
BEGIN
    FOR _set IN (
        SELECT key FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_LINK'
    )
    LOOP
        DROP TABLE IF EXISTS fr.tmp_address_fault_links;
        _query := 'CREATE UNLOGGED TABLE fr.tmp_address_fault_links AS';
        IF _set.key = 'COMPLEMENT_WITH_STREET_ERROR' THEN
            _table_from := 'fr.laposte_address';
            _column_from := 'co_cea_l3';
            _columns_to := 'co_cea_voie = ac.co_cea_voie';
            _query := CONCAT(_query
                , '
                WITH
                housenumber_with_multiple_streets AS (
                    SELECT co_cea_numero
                    FROM fr.laposte_address
                    WHERE fl_active AND co_cea_numero IS NOT NULL
                    GROUP BY co_cea_numero
                    HAVING COUNT(DISTINCT co_cea_voie) > 1
                )
                , good_street_of_housenumber AS (
                    SELECT DISTINCT a.co_cea_numero, a.co_cea_voie
                    FROM fr.laposte_address a
                        JOIN housenumber_with_multiple_streets e ON a.co_cea_numero = e.co_cea_numero
                    WHERE
                        a.fl_active
                        AND
                        a.co_niveau = ''NUMERO''
                )
                , good_street_of_complement AS (
                    SELECT DISTINCT a.co_cea_l3 code_address, s.co_cea_voie
                    FROM fr.laposte_address a
                        JOIN housenumber_with_multiple_streets e ON a.co_cea_numero = e.co_cea_numero
                        JOIN good_street_of_housenumber s ON a.co_cea_numero = s.co_cea_numero
                    WHERE
                        a.fl_active
                        AND
                        a.co_niveau = ''L3''
                        AND
                        a.co_cea_voie != s.co_cea_voie
                )
                SELECT * FROM good_street_of_complement
                '
            );
        END IF;
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_found = ROW_COUNT;
        ELSE
            RAISE NOTICE ' requête=%', _query;
        END IF;

        _query := CONCAT('INSERT INTO fr.laposte_address_history (
                code_address
                , date_change
                , change
                , kind
                , values
            )
            SELECT
                a.', _column_from, '
                , TIMEOFDAY()::DATE
                , ', quote_literal(_set.key)
                , ', ', quote_literal(_kind), '
                , ROW_TO_JSON(a.*)::JSONB
            FROM ', _table_from, ' a
                JOIN fr.tmp_address_fault_links ac ON a.', _column_from, ' = ac.code_address
            '
        );
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_history = ROW_COUNT;
        ELSE
            RAISE NOTICE ' requête=%', _query;
        END IF;

        _query := CONCAT('UPDATE ', _table_from, ' a SET
            ', _columns_to, '
            FROM fr.tmp_address_fault_links ac
            WHERE
                a.', _column_from, ' = ac.code_address
            '
        );
        IF NOT simulation THEN
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_fixed = ROW_COUNT;

            IF _nrows_fixed = _nrows_history AND _nrows_fixed = _nrows_found THEN
                COMMIT;
            ELSE
                ROLLBACK;
                CALL public.log_info(CONCAT('%: error (found,history,fixed)=(%,%,%)', _set.key, _nrows_found, _nrows_history, _nrows_fixed));
            END IF;
        ELSE
            RAISE NOTICE ' requête=%', _query;
        END IF;
    END LOOP;
END;
$proc$ LANGUAGE plpgsql;
