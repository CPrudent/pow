/***
 * FR: add LAPOSTE/RAN complement reference (dictionary / referential)
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store references
CREATE TABLE IF NOT EXISTS fr.laposte_address_complement_reference (
    name_id INT NOT NULL
    , address_id CHAR(10) NOT NULL
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_reference_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_reference_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_complement_reference_name_id ON fr.laposte_address_complement_reference (name_id);

    CREATE INDEX IF NOT EXISTS ix_laposte_address_complement_reference_address_id ON fr.laposte_address_complement_reference (address_id);
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_reference');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_reference()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_complement_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Référence des compléments (Dictionnaire/Référentiel)');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_complement_reference;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_complement_reference');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_complement_reference(
        name_id
        , address_id
    )
    SELECT
        u.id
        , c.co_cea
    FROM
        fr.laposte_address_complement_uniq u
            JOIN fr.laposte_address_complement c ON u.name = CONCAT_WS(' '
                , lb_type_groupe1_l3
                , lb_groupe1
                , lb_type_groupe2_l3
                , lb_groupe2
                , lb_type_groupe3_l3
                , lb_groupe3
            )
    WHERE
        c.fl_active
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Création: ', _nrows));

    CALL fr.set_laposte_address_complement_reference_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_complement_reference();

19:11:45.611 Référence des compléments (Dictionnaire/Référentiel)
19:11:45.611  Purge
19:11:45.612  Initialisation
19:11:47.698  Création: 1065696
19:11:51.180  Indexation

Query returned successfully in 5 secs 607 msec.
 */
