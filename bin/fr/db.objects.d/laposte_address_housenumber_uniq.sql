/***
 * FR: add LAPOSTE/RAN housenumber uniq (dictionary)
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store uniq number (as dictionary of housenumber)
CREATE TABLE IF NOT EXISTS fr.laposte_address_housenumber_uniq (
    id SERIAL NOT NULL
    , number INT NOT NULL
    , extension VARCHAR
    , occurs INT
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_housenumber_uniq_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_housenumber_uniq_index(
)
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS ix_laposte_address_housenumber_uniq_id ON fr.laposte_address_housenumber_uniq (id);

    CREATE INDEX IF NOT EXISTS ix_laposte_address_housenumber_uniq_number ON fr.laposte_address_housenumber_uniq (number, extension);
END
$proc$ LANGUAGE plpgsql;

-- build housenumber dictionnary w/ (normalized name, descriptors, words array, nof words)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_housenumber_uniq');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_housenumber_uniq(
)
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_housenumber') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Dictionnaire des numéros');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_housenumber_uniq;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_housenumber_uniq');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_housenumber_uniq(
          number
        , extension
        , occurs
    )
    WITH
    number_uniq AS (
        SELECT
              no_voie
            , lb_ext
            , COUNT(*) occurs
        FROM
            fr.laposte_address_housenumber
        WHERE
            fl_active
        GROUP BY
              no_voie
            , lb_ext
    )
    SELECT * FROM number_uniq
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Création: ', _nrows));

    CALL fr.set_laposte_address_housenumber_uniq_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_housenumber_uniq();

17:00:22.122 Dictionnaire des voies
17:00:22.122  Purge
17:00:22.146  Initialisation
17:01:01.256  Création: 1120726
17:01:06.927  Indexation

Query returned successfully in 45 secs 662 msec.
 */
