/***
 * FR: add LAPOSTE/RAN street reference (dictionary / referential)
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store references
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_reference (
    name_id INT NOT NULL
    , address_id CHAR(10) NOT NULL
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_reference_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_reference_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_reference_name_id ON fr.laposte_address_street_reference (name_id);

    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_reference_address_id ON fr.laposte_address_street_reference (address_id);
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_reference');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_reference()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Référence des voies (Dictionnaire/Référentiel)');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_reference;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_reference');

    CALL public.log_info(' Initialisation');
    -- reminder: words, nwords are initiated by trigger
    INSERT INTO fr.laposte_address_street_reference(
        name_id
        , address_id
    )
    SELECT
        u.id
        , s.co_cea
    FROM
        fr.laposte_address_street_uniq u
            JOIN fr.laposte_address_street s ON u.name = s.lb_voie
    WHERE
        s.fl_active
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Création: ', _nrows));

    CALL fr.set_laposte_address_street_reference_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;
