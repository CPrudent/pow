/***
 * FR: add LAPOSTE/RAN street keyword exceptions
 */

/* NOTE
initialization will be done w/ constant
 */

-- to store keyword exceptions
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_kw_exception (
    keyword VARCHAR NOT NULL
    , as_default VARCHAR
    , as_except VARCHAR
    , if_followed_by VARCHAR
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_kw_exception_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_kw_exception_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_kw_exception_keyword ON fr.laposte_address_street_kw_exception (keyword);
END
$proc$ LANGUAGE plpgsql;

-- build keyword exceptions (for firstname)
-- Query returned successfully in 4 min 1 secs.
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_kw_exception');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_kw_exception()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Définition des exceptions de mots clé des voies');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_kw_exception;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_kw_exception');

    CALL public.log_info(' Préparation');
    DROP TABLE IF EXISTS fr.tmp_address_street_fname_occur;
    CREATE TABLE fr.tmp_address_street_fname_occur AS
    -- 147018
    WITH
    split_as_word AS (
        SELECT
            s.name
            , w.word
            , w.i::INT
            , s.words
            , s.descriptors
            , s.nwords
        FROM
            fr.laposte_address_street_uniq s
                INNER JOIN LATERAL UNNEST(s.words) WITH ORDINALITY AS w(word, i) ON TRUE
    )
    , word_with_descriptor AS (
        SELECT
            name
            , word
            , i
            , SUBSTR(descriptors, i, 1) descriptor
        FROM
            split_as_word sw
        WHERE
            EXISTS(
                SELECT 1 FROM fr.constant
                WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME'
                AND key = sw.word
            )
            AND
            -- not last word (name!)
            i < nwords
            AND
            -- not followed by a number
            NOT fr.is_normalized_number(words[i +1])
    )
    SELECT * FROM word_with_descriptor WHERE descriptor != 'V'
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Occurences: ', _nrows));

    DROP TABLE IF EXISTS fr.tmp_address_street_fname_as_kw;
    CREATE TABLE fr.tmp_address_street_fname_as_kw AS
    -- #3296
        SELECT
            word
            , descriptor
            , COUNT(*) occurs
        FROM
            fr.tmp_address_street_fname_occur
        GROUP BY
            word
            , descriptor
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Groupements par (mot, descripteur): ', _nrows));

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_kw_exception(
        keyword
        , as_default
        , as_except
        , if_followed_by
    )
    WITH
    -- not only one occurence, so exception
    word_with_except AS (
        SELECT
            word
        FROM
            fr.tmp_address_street_fname_as_kw
        GROUP BY
            word
        HAVING COUNT(*) > 1
    )
    -- #1020
    --SELECT * FROM word_with_except ORDER BY 1
    , word_count_by_descriptor AS (
        SELECT
            o.word
            , SUM(CASE WHEN descriptor = 'N' THEN 1 ELSE 0 END) as_name
            , SUM(CASE WHEN descriptor = 'P' THEN 1 ELSE 0 END) as_fname
            , SUM(CASE WHEN descriptor = 'T' THEN 1 ELSE 0 END) as_title
        FROM
            fr.tmp_address_street_fname_occur o
                JOIN word_with_except x ON o.word = x.word
        GROUP BY
            o.word
    )
    --SELECT * FROM word_count_by_descriptor ORDER BY 1
    , word_as_default AS (
        SELECT
            word
            , CASE
                WHEN as_name >= (as_fname + as_title) THEN 'N'
                WHEN as_fname >= (as_name + as_title) THEN 'P'
                WHEN as_title >= (as_name + as_fname) THEN 'T'
                END as_default
        FROM
            word_count_by_descriptor
    )
    -- #2322
    --SELECT * FROM word_as_default ORDER BY 1
    , word_as_except AS (
        SELECT DISTINCT
            o.word kw
            , d.as_default
            , o.descriptor as_except
            , CASE
                WHEN s.nwords >= (i+3) AND fr.is_normalized_article(s.words[i+1]) AND fr.is_normalized_article(s.words[i+2]) THEN
                    items_of_array_to_string(
                        elements => s.words
                        , from_ => (i+1)
                        , to_ => (i+3)
                    )
                WHEN s.nwords >= (i+2) AND fr.is_normalized_article(s.words[i+1]) THEN
                    items_of_array_to_string(
                        elements => s.words
                        , from_ => (i+1)
                        , to_ => (i+2)
                    )
                ELSE s.words[i+1]
                END if_followed_by
        FROM
            fr.tmp_address_street_fname_occur o
                JOIN fr.laposte_address_street_uniq s ON o.name = s.name
                JOIN word_with_except x ON o.word = x.word
                JOIN word_as_default d ON o.word = d.word
                JOIN word_count_by_descriptor c ON o.word = c.word
        WHERE
            -- exception
            o.descriptor != d.as_default
            -- which exists?
            AND
            (
                CASE
                WHEN o.descriptor = 'N' THEN c.as_name
                WHEN o.descriptor = 'P' THEN c.as_fname
                WHEN o.descriptor = 'T' THEN c.as_title
                END > 0
            )
            -- exclusion
            AND NOT
            (
                (
                    fr.is_normalized_number(s.words[i +1])
                    AND
                    (o.descriptor = ANY('{N,P}'))
                )
            )
            -- not last word
            AND
            (i+1 < nwords)
    )
    -- #3382
    SELECT * FROM word_as_except
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Exceptions: ', _nrows));

    CALL fr.set_laposte_address_street_kw_exception_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;
