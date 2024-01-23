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
        AND
        NOT column_exists(
            schema_name => 'fr'
            , table_name => 'laposte_address_fault_street'
            , column_name => 'help_to_fix'
        ) THEN
        DROP TABLE fr.laposte_address_fault_street;
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

    CALL public.log_info(' Initialisation');
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
                    SELECT
                        id
                        , _values[_fault_i]::INT
                        , bad_space_in_name(
                            name => u.name
                        )
                    FROM
                        fr.laposte_address_street_uniq u
                        , bad_space_in_name(
                            name => u.name
                            , test_only => TRUE
                        ) fn
                    WHERE
                        fn.to_fix
                    ;
            ELSIF _keys[_fault_i] = 'DUPLICATE_WORD' THEN
                INSERT INTO fr.laposte_address_fault_street
                    WITH
                    dup_words AS (
                        SELECT
                            id
                            , REGEXP_MATCHES(name, '\m([ A-Z]+)\s+\1\M') dup
                        FROM fr.laposte_address_street_uniq
                    )
                    SELECT
                        u.id
                        , _values[_fault_i]::INT
                        , d.dup[1]
                    FROM
                        fr.laposte_address_street_uniq u
                            JOIN dup_words d ON u.id = d.id
                    WHERE
                        d.dup IS NOT NULL
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
                        id
                        , _values[_fault_i]::INT
                        , wa.abbr
                    FROM
                        fr.laposte_address_street_uniq
                        , word_abbreviation wa
                    WHERE
                        words @> ARRAY[wa.abbr]::TEXT[]
                    ;
            ELSIF _keys[_fault_i] = 'TYPO_ERROR' THEN
                -- word containing digit, but not classified as number
                INSERT INTO fr.laposte_address_fault_street
                    SELECT DISTINCT
                        name_id
                        , _values[_fault_i]::INT
                        , word
                    FROM
                        fr.laposte_address_street_membership
                    WHERE
                        word ~ '[0-9]+'
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
                            id
                            , ts.kw type_pow
                            , items_of_array_to_string(
                                elements => u.words
                                , from_ => 1
                                , to_ => LENGTH((REGEXP_MATCHES(u.descriptors, '^(V+)'))[1])
                            ) type_laposte
                        FROM
                            fr.laposte_address_street_uniq u
                                CROSS JOIN fr.get_type_of_street(
                                    name => u.name
                                    , words => u.words
                                ) ts
                    )
                    SELECT
                        id
                        , _values[_fault_i]::INT
                        , type_pow
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
