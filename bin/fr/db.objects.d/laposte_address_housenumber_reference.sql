/***
 * FR: add LAPOSTE/RAN housenumber reference (dictionary / referential)
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store references
CREATE TABLE IF NOT EXISTS fr.laposte_address_housenumber_reference (
    number_id INT NOT NULL
    , address_id CHAR(10) NOT NULL
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_housenumber_reference_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_housenumber_reference_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_housenumber_reference_name_id ON fr.laposte_address_housenumber_reference (number_id);

    CREATE INDEX IF NOT EXISTS ix_laposte_address_housenumber_reference_address_id ON fr.laposte_address_housenumber_reference (address_id);
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_housenumber_reference');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_housenumber_reference()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_housenumber_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Référence des numéros (Dictionnaire/Référentiel)');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_housenumber_reference;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_housenumber_reference');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_housenumber_reference(
        number_id
        , address_id
    )
    SELECT
          u.id
        , h.co_cea
    FROM
        fr.laposte_address_housenumber_uniq u
            JOIN fr.laposte_address_housenumber h ON u.number = h.no_voie AND COALESCE(u.extension, '') = COALESCE(h.lb_ext, '')
    WHERE
        h.fl_active
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Création: ', _nrows));

    CALL fr.set_laposte_address_housenumber_reference_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_housenumber_reference();

16:30:21.039 Référence des numéros (Dictionnaire/Référentiel)
16:30:21.039  Purge
16:30:21.040  Initialisation
16:31:28.064  Création: 23866995
16:33:23.120  Indexation

Query returned successfully in 3 min 3 secs.
 */
