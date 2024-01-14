/***
 * FR: add LAPOSTE/RAN street words
 */

-- to store words, counters by descriptor, default descriptor, ranks
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_word (
    word VARCHAR NOT NULL
    , as_default CHAR(1)
    , as_article INT            -- A
    , as_number INT             -- C
    , as_reserved INT           -- E
    , as_name INT               -- N
    , as_fname INT              -- P
    , as_title INT              -- T
    , as_type INT               -- V
    , rank_0 INT                -- for all
    , rank_1 INT                -- partition by descriptor
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS ix_laposte_address_street_word_word_default ON fr.laposte_address_street_word (word, as_default);
END
$proc$ LANGUAGE plpgsql;

-- build counters (by descriptor), ranks and default for each word
-- Query returned successfully in 13 secs.
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
    INSERT INTO fr.laposte_address_street_word(
        word
        , as_article
        , as_number
        , as_reserved
        , as_name
        , as_fname
        , as_title
        , as_type
    )
    --
    WITH
    split_as_word AS (
        SELECT
            w.word
            , SUBSTR(s.descriptors, i::INT, 1) descriptor
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
            , SUM(CASE WHEN descriptor = 'A' THEN 1 ELSE 0 END) as_article
            , SUM(CASE WHEN descriptor = 'C' THEN 1 ELSE 0 END) as_number
            , SUM(CASE WHEN descriptor = 'E' THEN 1 ELSE 0 END) as_reserved
            , SUM(CASE WHEN descriptor = 'N' THEN 1 ELSE 0 END) as_name
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
    CALL public.log_info(CONCAT(' Comptage descripteurs (mot): ', _nrows));

    UPDATE fr.laposte_address_street_word SET
        as_default = CASE
            WHEN as_name > GREATEST(as_reserved, as_article, as_number, as_fname, as_title, as_type) THEN 'N'
            WHEN as_reserved >= GREATEST(as_name, as_article, as_number, as_fname, as_title, as_type) THEN 'E'
            WHEN as_article >= GREATEST(as_name, as_reserved, as_number, as_fname, as_title, as_type) THEN 'A'
            WHEN as_number >= GREATEST(as_name, as_reserved, as_article, as_fname, as_title, as_type) THEN 'C'
            WHEN as_fname >= GREATEST(as_name, as_reserved, as_article, as_number, as_title, as_type) THEN 'P'
            WHEN as_title >= GREATEST(as_name, as_reserved, as_article, as_number, as_fname, as_type) THEN 'T'
            WHEN as_type > GREATEST(as_name, as_reserved, as_article, as_number, as_fname, as_title) THEN 'V'
            END
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Défaut (mot): ', _nrows));

    WITH
    word_rank AS (
        SELECT
            word
            , row_number() OVER (ORDER BY (
                COALESCE(as_name, 0)
                + COALESCE(as_reserved, 0)
                + COALESCE(as_article, 0)
                + COALESCE(as_number, 0)
                + COALESCE(as_fname, 0)
                + COALESCE(as_title, 0)
                + COALESCE(as_type, 0)
            ) DESC) rank_0
            , row_number() OVER (PARTITION BY as_default ORDER BY (
                CASE
                WHEN as_default = 'A' THEN as_article
                WHEN as_default = 'C' THEN as_number
                WHEN as_default = 'E' THEN as_reserved
                WHEN as_default = 'N' THEN as_name
                WHEN as_default = 'P' THEN as_fname
                WHEN as_default = 'T' THEN as_title
                WHEN as_default = 'V' THEN as_type
                END
            ) DESC) rank_1
        FROM
           fr.laposte_address_street_word
    )
    UPDATE fr.laposte_address_street_word w SET
        rank_0 = r.rank_0
        , rank_1 = r.rank_1
        FROM word_rank r
        WHERE
            w.word = r.word
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Rangs (mot): ', _nrows));

    CALL fr.set_laposte_address_street_word_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;
