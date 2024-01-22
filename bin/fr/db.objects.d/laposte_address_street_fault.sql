/***
 * FR: add LAPOSTE/RAN street fault
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store fault
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_fault (
    name_id INT NOT NULL
    , fault_id INT NOT NULL
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_fault_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_fault_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_street_fault_id ON fr.laposte_address_street_fault (name_id, fault_id);
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_fault');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_fault(
    fault IN VARCHAR DEFAULT 'ALL'
    , simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Identification des anomalies dans les libellés de voie');

    CALL public.log_info(' Purge');
    IF fault = 'ALL' THEN
        TRUNCATE TABLE fr.laposte_address_street_fault;
    ELSE
        WITH
        street_fault AS (
            SELECT key, value::INT
            FROM fr.constant
            WHERE usecase = 'LAPOSTE_ADDRESS_FAULT'
        )
        DELETE FROM fr.laposte_address_street_fault f
        USING street_fault sf
        WHERE f.fault_id = sf.value
        ;
    END IF;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_fault');

    CALL public.log_info(' Initialisation');
    FOR _set IN (
        SELECT key, value FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_FAULT'
    )
    LOOP
        -- requested fault?
        IF ((fault = 'ALL')
            OR
            (fault ~ '^[0-9]+$' AND _set.value = fault)
            OR
            (fault !~ '^[0-9]+$' AND _set.key = fault)
        ) THEN
            IF simulation THEN
                RAISE NOTICE ' Anomalie (%)', _set.key;
                CONTINUE;
            END IF;

            IF _set.key = 'BAD_SPACE' THEN
                INSERT INTO fr.laposte_address_street_fault
                    SELECT
                        id
                        , _set.value::INT
                    FROM
                        fr.laposte_address_street_uniq u
                        , fr.normalize_space_in_name(
                            name => u.name
                            , test_only => TRUE
                        ) fn
                        , fr.normalize_space_in_name(
                            name => u.name_normalized
                            , test_only => TRUE
                        ) fnn
                    WHERE
                        fn.to_fix
                        OR
                        fnn.to_fix
                    ;
            ELSIF _set.key = 'DUPLICATE_WORD' THEN
                INSERT INTO fr.laposte_address_street_fault
                    WITH
                    dup_words AS (
                        SELECT
                            id
                            , REGEXP_MATCHES(name, '\m([ A-Z]+)\s+\1\M') dup
                        FROM fr.laposte_address_street_uniq
                    )
                    SELECT
                        id
                        , _set.value::INT
                    FROM
                        fr.laposte_address_street_uniq u
                            JOIN dup_words d ON u.id = d.id
                    WHERE
                        d.dup IS NOT NULL
                    ;
            ELSIF _set.key = 'WITH_ABBREVIATION' THEN
                INSERT INTO fr.laposte_address_street_fault

            END IF;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            CALL public.log_info(CONCAT(' Anomalies (', _set.key, '): ', _nrows));
        END IF;
    END LOOP;

    CALL fr.set_laposte_address_street_fault_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;
