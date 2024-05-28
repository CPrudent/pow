/***
 * FR: add LAPOSTE/RAN complement words (w/ default descriptor)
 */

-- to store words, counters by descriptor, default descriptor, ranks
CREATE TABLE IF NOT EXISTS fr.laposte_address_complement_word_descriptor (
    word VARCHAR NOT NULL,
    as_default CHAR(1),
    as_article INT,            -- A
    as_number INT,             -- C
    as_reserved INT,           -- E
    as_group_1 INT,            -- G
    as_group_2 INT,            -- H
    as_group_3 INT,            -- I
    as_name INT,               -- N
    as_fname INT,              -- P
    as_title INT,              -- T
    as_type INT,               -- V
    rank_0 INT,                -- for all
    rank_1 INT                 -- partition by descriptor
)
;

DO $$
BEGIN
    IF column_exists('fr', 'laposte_address_complement_word_descriptor', 'as_group1') THEN
        ALTER TABLE fr.laposte_address_complement_word_descriptor RENAME COLUMN as_group1 TO as_group_1;
    END IF;
    IF column_exists('fr', 'laposte_address_complement_word_descriptor', 'as_group2') THEN
        ALTER TABLE fr.laposte_address_complement_word_descriptor RENAME COLUMN as_group2 TO as_group_2;
    END IF;
    IF column_exists('fr', 'laposte_address_complement_word_descriptor', 'as_group3') THEN
        ALTER TABLE fr.laposte_address_complement_word_descriptor RENAME COLUMN as_group3 TO as_group_3;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_word_descriptor_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_word_descriptor_index()
AS
$proc$
BEGIN
    -- https://stackoverflow.com/questions/28975517/difference-between-gist-and-gin-index
    CREATE INDEX IF NOT EXISTS ix_laposte_address_complement_word_descriptor_word ON fr.laposte_address_complement_word_descriptor USING GIN(word GIN_TRGM_OPS);
END
$proc$ LANGUAGE plpgsql;

-- build counters (by descriptor), ranks and default for each word
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_word_descriptor');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_word_descriptor()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_complement_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion des mots dans les noms de compléments (L3)');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_complement_word_descriptor;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_complement_word_descriptor');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_complement_word_descriptor(
        word,
        as_article,
        as_number,
        as_reserved,
        as_group_1,
        as_group_2,
        as_group_3,
        as_name,
        as_fname,
        as_title,
        as_type
    )
    WITH
    split_as_word AS (
        SELECT
            w.word,
            SUBSTR(u.descriptors, w.i::INT, 1) descriptor,
            w.i::INT,
            u.nwords
        FROM
            fr.laposte_address_complement_uniq u
                INNER JOIN LATERAL UNNEST(u.words) WITH ORDINALITY AS w(word, i) ON TRUE
    ),
    word_with_descriptor AS (
        SELECT
            word,
            SUM(CASE WHEN descriptor = 'A' THEN 1 ELSE 0 END) as_article,
            SUM(CASE WHEN descriptor = 'C' THEN 1 ELSE 0 END) as_number,
            SUM(CASE WHEN descriptor = 'E' THEN 1 ELSE 0 END) as_reserved,
            SUM(CASE WHEN descriptor = 'G' THEN 1 ELSE 0 END) as_group_1,
            SUM(CASE WHEN descriptor = 'H' THEN 1 ELSE 0 END) as_group_2,
            SUM(CASE WHEN descriptor = 'I' THEN 1 ELSE 0 END) as_group_3,
            SUM(CASE WHEN descriptor = 'N' THEN 1 ELSE 0 END) as_name,
            SUM(CASE WHEN descriptor = 'P' THEN 1 ELSE 0 END) as_fname,
            SUM(CASE WHEN descriptor = 'T' THEN 1 ELSE 0 END) as_title,
            SUM(CASE WHEN descriptor = 'V' THEN 1 ELSE 0 END) as_type
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

    UPDATE fr.laposte_address_complement_word_descriptor SET
        as_default = CASE
            WHEN as_article > GREATEST(as_number, as_reserved, as_group_1, as_group_2, as_group_3, as_name, as_fname, as_title, as_type) THEN 'A'
            WHEN as_number > GREATEST(as_article, as_reserved, as_group_1, as_group_2, as_group_3, as_name, as_fname, as_title, as_type) THEN 'C'
            WHEN as_reserved > GREATEST(as_article, as_number, as_group_1, as_group_2, as_group_3, as_name, as_fname, as_title, as_type) THEN 'E'
            WHEN as_group_1 > GREATEST(as_article, as_number, as_reserved, as_group_2, as_group_3, as_name, as_fname, as_title, as_type) THEN 'G'
            WHEN as_group_2 > GREATEST(as_article, as_number, as_reserved, as_group_1, as_group_3, as_name, as_fname, as_title, as_type) THEN 'H'
            WHEN as_group_3 > GREATEST(as_article, as_number, as_reserved, as_group_1, as_group_2, as_name, as_fname, as_title, as_type) THEN 'I'
            WHEN as_fname > GREATEST(as_article, as_number, as_reserved, as_group_1, as_group_2, as_group_3, as_name, as_title, as_type) THEN 'P'
            WHEN as_title > GREATEST(as_article, as_number, as_reserved, as_group_1, as_group_2, as_group_3, as_name, as_fname, as_type) THEN 'T'
            WHEN as_type > GREATEST(as_article, as_number, as_reserved, as_group_1, as_group_2, as_group_3, as_name, as_fname, as_title) THEN 'V'
            ELSE 'N'
            END
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Défaut (mot): ', _nrows));

    WITH
    word_rank AS (
        SELECT
            word,
            ROW_NUMBER() OVER (ORDER BY (
                  as_article
                + as_number
                + as_reserved
                + as_group_1
                + as_group_2
                + as_group_3
                + as_name
                + as_fname
                + as_title
                + as_type
            ) DESC) rank_0,
            ROW_NUMBER() OVER (PARTITION BY as_default ORDER BY (
                CASE
                WHEN as_default = 'A' THEN as_article
                WHEN as_default = 'C' THEN as_number
                WHEN as_default = 'E' THEN as_reserved
                WHEN as_default = 'G' THEN as_group_1
                WHEN as_default = 'H' THEN as_group_2
                WHEN as_default = 'I' THEN as_group_3
                WHEN as_default = 'N' THEN as_name
                WHEN as_default = 'P' THEN as_fname
                WHEN as_default = 'T' THEN as_title
                WHEN as_default = 'V' THEN as_type
                END
            ) DESC) rank_1
        FROM
           fr.laposte_address_complement_word_descriptor
    )
    UPDATE fr.laposte_address_complement_word_descriptor w SET
        rank_0 = r.rank_0,
        rank_1 = r.rank_1
        FROM word_rank r
        WHERE
            w.word = r.word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Rangs (mot): ', _nrows));

    CALL fr.set_laposte_address_complement_word_descriptor_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
11:07:37.136 Gestion des mots dans les noms de compléments (L3)
11:07:37.136  Purge
11:07:37.169  Initialisation
11:07:39.968  Comptage descripteurs (mot): 83997
11:07:40.168  Défaut (mot): 83997
11:07:41.136  Rangs (mot): 83997
11:07:41.676  Indexation

Query returned successfully in 4 secs 595 msec.
 */

/*
-- get default of complement word
SELECT drop_all_functions_if_exists('fr', 'get_default_of_complement_word');
CREATE OR REPLACE FUNCTION fr.get_default_of_complement_word(
    word IN VARCHAR
    as_default OUT VARCHAR
)
AS
$func$
BEGIN
    SELECT w.as_default
    INTO
        get_default_of_complement_word.as_default
    FROM fr.laposte_address_complement_word_descriptor w
    WHERE
        w.word = get_default_of_complement_word.word
    ;
END
$func$ LANGUAGE plpgsql;
 */
