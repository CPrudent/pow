/***
 * FR: add LAPOSTE/RAN street words
 */

-- to store words, and counters by descriptor
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_word (
    word VARCHAR NOT NULL
    , as_article INT            -- A
    , as_number INT             -- C
    , as_reserved INT           -- E
    , as_name INT               -- N
    , as_fname INT              -- P
    , as_title INT              -- T
    , as_type INT               -- V
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS ix_laposte_address_street_word_word ON fr.laposte_address_street_word (word);
END
$proc$ LANGUAGE plpgsql;

-- build counters (by descriptor) for each word
-- Query returned successfully in 4 min 5 secs.
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion des mots dans les noms de voies');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_word;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_word');

    CALL public.log_info(' Préparation');
    DROP TABLE IF EXISTS fr.tmp_address_street_fname_occur;
    CREATE TABLE fr.tmp_address_street_fname_occur AS
    --
    WITH
    split_as_word AS (
        SELECT
            w.word
            , SUBSTR(s.descriptors, i, 1) descriptor
        FROM
            fr.laposte_address_street_uniq s
                INNER JOIN LATERAL UNNEST(s.words) WITH ORDINALITY AS w(word, i) ON TRUE
    )
    , word_with_descriptor AS (
        SELECT
            word
            /* NOTE
             in case of a tie, order as above (asc)
             */
            SUM(CASE WHEN descriptor = 'N' THEN 1 ELSE 0 END) as_name
            , SUM(CASE WHEN descriptor = 'E' THEN 1 ELSE 0 END) as_reserved
            , SUM(CASE WHEN descriptor = 'A' THEN 1 ELSE 0 END) as_article
            , SUM(CASE WHEN descriptor = 'C' THEN 1 ELSE 0 END) as_number
            , SUM(CASE WHEN descriptor = 'P' THEN 1 ELSE 0 END) as_fname
            , SUM(CASE WHEN descriptor = 'T' THEN 1 ELSE 0 END) as_title
            , SUM(CASE WHEN descriptor = 'V' THEN 1 ELSE 0 END) as_type
        FROM
            split_as_word
        /*
        WHERE
            -- not last word (name!)
            i < nwords
         */
        GROUP BY
            word
    )
    SELECT * FROM word_with_descriptor
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Comptages descripteur par (mot): ', _nrows));

    CALL fr.set_laposte_address_street_word_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;
