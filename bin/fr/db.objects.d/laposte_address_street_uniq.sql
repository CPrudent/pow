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
    id SERIAL NOT NULL
    , name VARCHAR NOT NULL
    , name_normalized VARCHAR
    , descriptors VARCHAR
    , type_of_street VARCHAR
    , occurs INT
    , words TEXT[]
    , nwords INT
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
        name
        , name_normalized
        , descriptors
        , type_of_street
        , occurs
    )
    WITH
    /* NOTE
    columns would be uniq, but not! #367 same names own 2 different descriptors (so type)
    group by is OK because they are 1..1 (descriptors involve type)
     */
    name_with_counters AS (
        SELECT
            lb_voie name
            , COUNT(DISTINCT lb_desc) n_descriptors
            , COUNT(DISTINCT lb_type) n_types
            , COUNT(*) occurs
            , MIN(lb_desc) descriptors
            , MIN(lb_type) type_of_street
            , MIN(lb_voie_normalise) name_normalized
        FROM
            fr.laposte_address_street s
        WHERE
            fl_active
        GROUP BY
            lb_voie
    )
    , name_with_many_descriptors AS (
        SELECT
            s.lb_voie name
            , s.lb_desc descriptors
            , COUNT(*) n
        FROM
            fr.laposte_address_street s
                JOIN name_with_counters wc ON s.lb_voie = wc.name
        WHERE
            s.fl_active
            AND
            wc.n_descriptors > 1
        GROUP BY
            s.lb_voie
            , s.lb_desc
    )
    , name_with_larger_value AS (
        SELECT
            name
            , FIRST(descriptors ORDER BY n DESC) AS descriptors
        FROM
            name_with_many_descriptors
        GROUP BY
            name
    )
    , name_other_attributs_for_larger_value AS (
        SELECT
            lv.name
            , lv.descriptors
            , MIN(s.lb_type) type_of_street
            , MIN(s.lb_voie_normalise) name_normalized
        FROM
            fr.laposte_address_street s
                JOIN name_with_larger_value lv ON (s.lb_voie, s.lb_desc) = (lv.name, lv.descriptors)
        WHERE
            s.fl_active
        GROUP BY
            lv.name
            , lv.descriptors
    )
    , uniq_street AS (
        SELECT
            oalv.name
            , CASE WHEN oalv.name != oalv.name_normalized THEN oalv.name_normalized
                ELSE NULL
                END name_normalized
            , oalv.descriptors
            , oalv.type_of_street
            , wc.occurs
        FROM
            name_other_attributs_for_larger_value oalv
                JOIN name_with_counters wc ON oalv.name = wc.name
        UNION
        SELECT
            wc.name
            , CASE WHEN wc.name != wc.name_normalized THEN wc.name_normalized
                ELSE NULL
                END name_normalized
            , wc.descriptors
            , wc.type_of_street
            , wc.occurs
        FROM
            name_with_counters wc
        WHERE
            wc.n_descriptors = 1
    )
    -- #1120726
    SELECT * FROM uniq_street
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Création: ', _nrows));

    CALL fr.set_laposte_address_street_uniq_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_street_uniq();

17:00:22.122 Dictionnaire des voies
17:00:22.122  Purge
17:00:22.146  Initialisation
17:01:01.256  Création: 1120726
17:01:06.927  Indexation

Query returned successfully in 45 secs 662 msec.
 */

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
