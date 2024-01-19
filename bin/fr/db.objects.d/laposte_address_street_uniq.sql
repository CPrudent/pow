/***
 * FR: add LAPOSTE/RAN street uniq
 */

/* NOTE
initialization will be done w/ constant
 */

DO $$
BEGIN
    IF table_exists(
            schema_name => 'fr'
            , table_name => 'laposte_address_street_uniq'
        )
        AND
        NOT column_exists(
            schema_name => 'fr'
            , table_name => 'laposte_address_street_uniq'
            , column_name => 'id'
        ) THEN
        DROP TABLE fr.laposte_address_street_uniq;
    END IF;
END $$;

-- to store uniq name
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_uniq (
    id INT NOT NULL
    , name VARCHAR NOT NULL
    , name_normalized VARCHAR
    , descriptors VARCHAR
    , occurs INT
    , words TEXT[]
    , nwords INT
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_uniq_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_uniq_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS ix_laposte_address_street_uniq_id ON fr.laposte_address_street_uniq (id);

    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_uniq_name ON fr.laposte_address_street_uniq USING GIN(name GIN_TRGM_OPS);
END
$proc$ LANGUAGE plpgsql;

-- build street dictionnary w/ (normalized name, descriptors, words array, nof words)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_uniq');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_uniq()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Dictionnaire des voies');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_uniq;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_uniq');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_uniq(
        id
        , name
        , name_normalized
        , descriptors
        , occurs
    )
    WITH
    uniq_street AS (
        SELECT
            lb_voie name
            , CASE WHEN lb_voie != lb_voie_normalise THEN lb_voie_normalise
                ELSE NULL
                END name_normalized
            , MIN(lb_desc) descriptors
            , COUNT(*)
        FROM
            fr.laposte_address_street
        WHERE
            fl_active
        GROUP BY
            lb_voie
            , CASE WHEN lb_voie != lb_voie_normalise THEN lb_voie_normalise
                ELSE NULL
                END
    )
    -- #1120726
    SELECT
        ROW_NUMBER() OVER (ORDER BY name) id
        , *
    FROM
        uniq_street
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Création: ', _nrows));

    WITH
    street_infos AS (
        SELECT
            name
            , REGEXP_SPLIT_TO_ARRAY(name, '\s+') as_words
            , count_words(name) n_words
        FROM
            fr.laposte_address_street_uniq
    )
    UPDATE fr.laposte_address_street_uniq u SET
        words = s.as_words
        , nwords = s.n_words
        FROM street_infos s
        WHERE u.name = s.name
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mise à jour: ', _nrows));

    CALL fr.set_laposte_address_street_uniq_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;
