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
    , followed_by VARCHAR
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

-- build keyword exceptions (for firstname, article, title)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_kw_exception');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_kw_exception()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq')
        AND NOT table_exists('fr', 'laposte_address_street_word') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion des exceptions de mots clé des voies');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_kw_exception;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_kw_exception');

    /* NOTE
    for some words, you can find two-possibilities (as descriptor) for following word!
    keep only those in case where occurs (as exception w/ default) are greater
     */
    CALL public.log_info(' Initialisation');
    TRUNCATE TABLE fr.laposte_address_street_kw_exception;
    INSERT INTO fr.laposte_address_street_kw_exception(
        keyword
        , as_default
        , as_except
        , followed_by
    )
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
    , word_firstname AS (
        SELECT
            name
            , word
            , i
            , SUBSTR(descriptors, i, 1) descriptor
        FROM
            split_as_word sw
        WHERE
            fr.is_normalized_firstname(sw.word)
            AND
            -- not last word (name!)
            i < nwords
            AND
            -- not followed by a number
            NOT fr.is_normalized_number(words[i +1])
    )
    -- #123016
    --SELECT * FROM word_firstname ORDER BY 2
    , word_exception AS (
        SELECT
            o.word
            , w.as_default
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
                END followed_by
            , o.i
            , o.name
            , s.descriptors
        FROM
            word_firstname o
                JOIN fr.laposte_address_street_uniq s ON o.name = s.name
                JOIN fr.laposte_address_street_word w ON o.word = w.word
        WHERE
            -- exception
            o.descriptor != w.as_default
            -- exclusion
            AND NOT
            (
                (
                    fr.is_normalized_number(s.words[i +1])
                    AND
                    (o.descriptor = ANY('{N,P}'))
                )
            )
    )
    --SELECT * FROM word_exception ORDER BY 1, 4
    , word_usecase AS (
        SELECT
            x.word
            , x.followed_by
            , SUBSTR(s.descriptors, ARRAY_POSITION(s.words, x.word), 1) as_usecase
            , x.as_except
        FROM
            word_exception x
                JOIN fr.laposte_address_street_membership m ON x.word = m.word
                JOIN fr.laposte_address_street_uniq s ON m.name_id = s.id
        WHERE
            (ARRAY_POSITION(s.words, x.word) + count_words(x.followed_by)) <= s.nwords
            AND
            -- and followed too
            s.words[(ARRAY_POSITION(s.words, x.word) + count_words(x.followed_by))]= REGEXP_REPLACE(x.followed_by, '^.* ', '')
    )
    -- #37448
    --SELECT * FROM word_usecase ORDER BY 1, 2
    , count_usecase AS (
        SELECT
            word
            , followed_by
            , SUM(CASE WHEN as_usecase = as_except THEN 1 ELSE 0 END) ok_except
            , SUM(CASE WHEN as_usecase != as_except THEN 1 ELSE 0 END) ko_except
        FROM
            word_usecase
        GROUP BY
            word
            , followed_by
    )
    --SELECT * FROM count_usecase ORDER BY 1, 2
    , with_exception AS (
        SELECT
            x.*
        FROM
            (
                SELECT DISTINCT
                    word
                    , as_default
                    , as_except
                    , followed_by
                FROM
                    word_exception
            ) x
                JOIN count_usecase cu ON (x.word, x.followed_by) = (cu.word, cu.followed_by)
        WHERE
            COALESCE(cu.ok_except, 0) > COALESCE(cu.ko_except, 0)
    )
    -- #2591
    SELECT * FROM with_exception
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Exceptions (prénom): ', _nrows));

    INSERT INTO fr.laposte_address_street_kw_exception(
        keyword
        , as_default
        , as_except
        , followed_by
    )
    VALUES
    ('SOUS', 'A', 'T', 'LIEUTENANT')
    , ('SOUS', 'A', 'N', 'MARIN')
    , ('SOUS', 'A', 'N', 'PREFECTURE')
    , ('SOUS', 'A', 'N', 'STATION')
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Exceptions (article): ', _nrows));

    INSERT INTO fr.laposte_address_street_kw_exception(
        keyword
        , as_default
        , as_except
        , followed_by
    )
    WITH
    name_as_abbr_kw AS (
        SELECT
            w.word
        FROM
            fr.laposte_address_street_word w
        WHERE
            as_default = 'N'
            AND
            LENGTH(w.word) > 1
            AND
            EXISTS(
                SELECT 1
                FROM fr.laposte_address_street_keyword k
                WHERE k.name_abbreviated = w.word
            )
    )
    , split_as_word AS (
        SELECT
            u.name
            , w.word
            , w.i::INT
            , u.nwords
        FROM
            fr.laposte_address_street_uniq u
                INNER JOIN LATERAL UNNEST(u.words) WITH ORDINALITY AS w(word, i) ON TRUE
    )
    , word_as_abbr_kw AS (
        SELECT
            sw.name
            , nakw.word
            , sw.i
        FROM
            split_as_word sw
                JOIN name_as_abbr_kw nakw ON sw.word = nakw.word
        WHERE
            -- not last word (name!)
            sw.i < sw.nwords
    )
    , word_exception AS (
        SELECT
            o.word
            , CASE
                WHEN u.nwords >= (i+3) AND fr.is_normalized_article(u.words[i+1]) AND fr.is_normalized_article(u.words[i+2]) THEN
                    items_of_array_to_string(
                        elements => u.words
                        , from_ => (i+1)
                        , to_ => (i+3)
                    )
                WHEN u.nwords >= (i+2) AND fr.is_normalized_article(u.words[i+1]) THEN
                    items_of_array_to_string(
                        elements => u.words
                        , from_ => (i+1)
                        , to_ => (i+2)
                    )
                ELSE u.words[i+1]
                END followed_by
        FROM
            word_as_abbr_kw o
                JOIN fr.laposte_address_street_uniq u ON o.name = u.name
                --JOIN fr.laposte_address_street_word w ON o.word = w.word
    )
    -- #4347
    SELECT DISTINCT word, 'T', 'N', followed_by FROM word_exception
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Exceptions (titre): ', _nrows));

    CALL fr.set_laposte_address_street_kw_exception_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
17:33:36.261 Gestion des exceptions de mots clé des voies
17:33:36.261  Purge
DROP INDEX IF EXISTS fr.ix_laposte_address_street_kw_exception_keyword
17:33:36.319  Initialisation
17:34:51.332  Exceptions (prénom): 3258
17:34:51.332  Exceptions (article): 3
17:38:41.298  Exceptions (titre): 4347
17:38:41.362  Indexation

Query returned successfully in 5 min 7 secs.
 */
