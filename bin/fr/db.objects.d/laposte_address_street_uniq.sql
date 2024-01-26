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
    , type_of_street VARCHAR
)
;

DO $UNIQ$
BEGIN
    IF NOT column_exists('fr', 'laposte_address_street_uniq', 'type_of_street') THEN
        ALTER TABLE fr.laposte_address_street_uniq ADD COLUMN type_of_street VARCHAR;
    END IF;
END $UNIQ$;

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
    -- reminder: words, nwords are initiated by trigger
    INSERT INTO fr.laposte_address_street_uniq(
        id
        , name
        , name_normalized
        , descriptors
        , occurs
        , type_of_street
    )
    WITH
    -- columns would be uniq, but not! group by is OK because they are 1..1
    columns_with_double AS (
        SELECT
            lb_voie name
            , lb_desc descriptors
            , lb_type type_of_street
            , COUNT(*) n
        FROM
            fr.laposte_address_street
        WHERE
            fl_active
        GROUP BY
            lb_voie
            , lb_desc
            , lb_type
    )
    , larger_values AS (
        SELECT
            name
            , FIRST(descriptors ORDER BY n DESC) AS descriptors
            , FIRST(type_of_street ORDER BY n DESC) AS type_of_street
        FROM
            columns_with_double
        GROUP BY
            name
    )
    , uniq_street AS (
        SELECT
            lb_voie name
            , CASE WHEN lb_voie != lb_voie_normalise THEN lb_voie_normalise
                ELSE NULL
                END name_normalized
            , MIN(lv.descriptors) descriptors
            , COUNT(*) occurs
            , MIN(lv.type_of_street) type_of_street
        FROM
            fr.laposte_address_street s
                JOIN larger_values lv ON s.lb_voie = lv.name
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

    CALL fr.set_laposte_address_street_uniq_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'laposte_address_street_uniq_fill_columns');
CREATE OR REPLACE FUNCTION fr.laposte_address_street_uniq_fill_columns(
)
RETURNS TRIGGER AS
$func$
DECLARE
    _words TEXT[];
    --_v TEXT[];
BEGIN
    -- words, nwords
    IF (((TG_OP = 'UPDATE') AND (OLD.name != NEW.name))
        OR
        ((TG_OP = 'INSERT') AND (NEW.words IS NULL))) THEN
        --RAISE NOTICE 'begin % : OLD=%, NEW=%', TG_OP, OLD, NEW;
        _words := REGEXP_SPLIT_TO_ARRAY(NEW.name, '\s+');
        --RAISE NOTICE 'words=%', _words;
        NEW.words := _words;
        NEW.nwords := ARRAY_LENGTH(_words, 1);
        --RAISE NOTICE 'end % : OLD=%, NEW=%', TG_OP, OLD, NEW;
    END IF;

    /*
    -- type_of_street
    IF ((TG_OP = 'INSERT') AND (NEW.type_of_street IS NULL)) THEN
        _v := REGEXP_MATCHES(NEW.descriptors, '^(V+)');
        NEW.type_of_street := CASE
            WHEN _v IS NOT NULL THEN
                items_of_array_to_string(
                    elements => NEW.words
                    , from_ => 1
                    , to_ => LENGTH(_v[1])
                )
            ELSE
                NULL
            END
        ;
    END IF;
     */

    RETURN NEW;
END
$func$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS before_laposte_address_street_uniq ON fr.laposte_address_street_uniq;
CREATE TRIGGER before_laposte_address_street_uniq
    BEFORE INSERT OR UPDATE OF name
        ON fr.laposte_address_street_uniq
    FOR EACH ROW
    EXECUTE PROCEDURE fr.laposte_address_street_uniq_fill_columns();
