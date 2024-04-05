/***
 * FR: add LAPOSTE/RAN street words (w/ default descriptor)
 */

-- old name
DROP TABLE IF EXISTS fr.laposte_address_street_word;

DO $$
BEGIN
    IF table_exists(
            schema_name => 'fr'
            , table_name => 'laposte_address_street_word_descriptor'
        )
        AND
        NOT column_exists(
            schema_name => 'fr'
            , table_name => 'laposte_address_street_word_descriptor'
            , column_name => 'as_last'
        ) THEN
        DROP TABLE fr.laposte_address_street_word_descriptor;
    END IF;
END $$;

-- to store words, counters by descriptor, default descriptor, ranks
-- Query returned successfully in 11 secs 584 msec.
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_word_descriptor (
    word VARCHAR NOT NULL
    , as_default CHAR(1)
    , as_article INT            -- A
    , as_number INT             -- C
    , as_reserved INT           -- E
    , as_name INT               -- N
    , as_last INT               -- N (at end of name)
    , as_fname INT              -- P
    , as_title INT              -- T
    , as_type INT               -- V
    , rank_0 INT                -- for all
    , rank_1 INT                -- partition by descriptor
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word_descriptor_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word_descriptor_index()
AS
$proc$
BEGIN
    -- https://stackoverflow.com/questions/28975517/difference-between-gist-and-gin-index
    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_word_descriptor_word ON fr.laposte_address_street_word_descriptor USING GIN(word GIN_TRGM_OPS);

    /* TEST
    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_word_descriptor_name ON fr.laposte_address_street_word_descriptor (as_default, as_name, as_last)
    WHERE as_default = 'N';
     */
END
$proc$ LANGUAGE plpgsql;

-- build counters (by descriptor), ranks and default for each word
-- Query returned successfully in 13 secs.
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word_descriptor');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word_descriptor()
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
    TRUNCATE TABLE fr.laposte_address_street_word_descriptor;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_word_descriptor');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_word_descriptor(
        word
        , as_article
        , as_number
        , as_reserved
        , as_name
        , as_last
        , as_fname
        , as_title
        , as_type
    )
    -- #371536
    WITH
    split_as_word AS (
        SELECT
            w.word
            , SUBSTR(u.descriptors, w.i::INT, 1) descriptor
            , w.i::INT
            , u.nwords
        FROM
            fr.laposte_address_street_uniq u
                INNER JOIN LATERAL UNNEST(u.words) WITH ORDINALITY AS w(word, i) ON TRUE
    )
    , word_with_descriptor AS (
        SELECT
            word
            , SUM(CASE WHEN descriptor = 'A' THEN 1 ELSE 0 END) as_article
            , SUM(CASE WHEN descriptor = 'C' THEN 1 ELSE 0 END) as_number
            , SUM(CASE WHEN descriptor = 'E' THEN 1 ELSE 0 END) as_reserved
            , SUM(CASE WHEN descriptor = 'N' AND i < nwords THEN 1 ELSE 0 END) as_name
            , SUM(CASE WHEN descriptor = 'N' AND i = nwords THEN 1 ELSE 0 END) as_last
            , SUM(CASE WHEN descriptor = 'P' THEN 1 ELSE 0 END) as_fname
            , SUM(CASE WHEN descriptor = 'T' THEN 1 ELSE 0 END) as_title
            , SUM(CASE WHEN descriptor = 'V' THEN 1 ELSE 0 END) as_type
        FROM
            split_as_word

        WHERE
            -- to exclude row created w/ empty word
            LENGTH(word) > 0
        GROUP BY
            word
    )
    SELECT * FROM word_with_descriptor
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Comptage descripteurs (mot): ', _nrows));

    UPDATE fr.laposte_address_street_word_descriptor SET
        as_default = CASE
            WHEN as_reserved > GREATEST(as_name + as_last, as_article, as_number, as_fname, as_title, as_type) THEN 'E'
            WHEN as_article > GREATEST(as_name + as_last, as_reserved, as_number, as_fname, as_title, as_type) THEN 'A'
            WHEN as_number > GREATEST(as_name + as_last, as_reserved, as_article, as_fname, as_title, as_type) THEN 'C'
            WHEN as_fname > GREATEST(as_name + as_last, as_reserved, as_article, as_number, as_title, as_type) THEN 'P'
            WHEN as_title > GREATEST(as_name + as_last, as_reserved, as_article, as_number, as_fname, as_type) THEN 'T'
            WHEN as_type > GREATEST(as_name + as_last, as_reserved, as_article, as_number, as_fname, as_title) THEN 'V'
            ELSE 'N'
            END
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Défaut (mot): ', _nrows));

    WITH
    word_rank AS (
        SELECT
            word
            , ROW_NUMBER() OVER (ORDER BY (
                as_name
                + as_last
                + as_reserved
                + as_article
                + as_number
                + as_fname
                + as_title
                + as_type
            ) DESC) rank_0
            , ROW_NUMBER() OVER (PARTITION BY as_default ORDER BY (
                CASE
                WHEN as_default = 'A' THEN as_article
                WHEN as_default = 'C' THEN as_number
                WHEN as_default = 'E' THEN as_reserved
                WHEN as_default = 'N' THEN as_name + as_last
                WHEN as_default = 'P' THEN as_fname
                WHEN as_default = 'T' THEN as_title
                WHEN as_default = 'V' THEN as_type
                END
            ) DESC) rank_1
        FROM
           fr.laposte_address_street_word_descriptor
    )
    UPDATE fr.laposte_address_street_word_descriptor w SET
        rank_0 = r.rank_0
        , rank_1 = r.rank_1
        FROM word_rank r
        WHERE
            w.word = r.word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Rangs (mot): ', _nrows));

    CALL fr.set_laposte_address_street_word_descriptor_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
17:29:22.819 Gestion des mots dans les noms de voies
17:29:22.819  Purge
17:29:22.848  Initialisation
17:29:29.873  Comptage descripteurs (mot): 371531
17:29:30.678  Défaut (mot): 371531
17:29:34.587  Rangs (mot): 371531
17:29:37.155  Indexation

Query returned successfully in 14 secs 632 msec.

-- always name 'N' as default
SELECT
    *
FROM
    fr.laposte_address_street_word_descriptor
WHERE
    GREATEST(as_reserved, as_name, as_article, as_number, as_fname, as_title, as_type) > 0
    AND (
        as_reserved  = GREATEST(as_name, as_article, as_number, as_fname, as_title, as_type)
        OR
        as_article = GREATEST(as_name, as_reserved, as_number, as_fname, as_title, as_type)
        OR
        as_number = GREATEST(as_name, as_reserved, as_article, as_fname, as_title, as_type)
        OR
        as_fname = GREATEST(as_name, as_reserved, as_article, as_number, as_title, as_type)
        OR
        as_title = GREATEST(as_name, as_reserved, as_article, as_number, as_fname, as_type)
        OR
        as_type = GREATEST(as_name, as_reserved, as_article, as_number, as_fname, as_title)
    )
ORDER BY
    1
 */

/*
-- get default of word
SELECT drop_all_functions_if_exists('fr', 'fr.get_default_of_word');
SELECT drop_all_functions_if_exists('fr', 'fr.get_default_of_street_word');
CREATE OR REPLACE FUNCTION fr.get_default_of_street_word(
    word IN VARCHAR
    , as_default OUT VARCHAR
)
AS
$func$
BEGIN
    SELECT w.as_default
    INTO
        get_default_of_street_word.as_default
    FROM fr.laposte_address_street_word_descriptor w
    WHERE
        w.word = get_default_of_street_word.word
    ;
END
$func$ LANGUAGE plpgsql;
 */
