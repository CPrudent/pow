/***
 * FR: add LAPOSTE/RAN municipality/area words
 */

DO $$
BEGIN
    IF column_exists('fr', 'laposte_address_municipality_word', 'code_area') THEN
        DROP TABLE IF EXISTS fr.laposte_address_municipality_word;
    END IF;
END $$;

-- to store words by municipality
CREATE TABLE IF NOT EXISTS fr.laposte_address_municipality_word (
    municipality_code VARCHAR NOT NULL
    , word VARCHAR NOT NULL
    , count INT NOT NULL
    , rank INT
)
;

-- to store words by area
DROP TABLE IF EXISTS fr.laposte_address_area_word;
/*
CREATE TABLE IF NOT EXISTS fr.laposte_address_area_word (
    code_area CHAR(10) NOT NULL
    , word VARCHAR NOT NULL
    , count INT NOT NULL
    , rank INT
)
;
 */

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_municipality_word_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_municipality_word_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_municipality_word_municipality ON fr.laposte_address_municipality_word (municipality_code);
END
$proc$ LANGUAGE plpgsql;

/*
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_area_word_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_area_word_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_area_word_area ON fr.laposte_address_area_word (code_area);
END
$proc$ LANGUAGE plpgsql;
 */

-- build counters, ranks for each word (by municipality)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_municipality_word');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_municipality_word()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_word') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion des mots dans les noms de voies par communes');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_municipality_word;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_municipality_word');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_municipality_word(
        municipality_code
        , word
        , count
    )
    SELECT
        s.co_insee_commune
        , sw.word
        , COUNT(*)
    FROM fr.street_view s
        JOIN fr.laposte_address_street_reference sr ON sr.address_id = s.co_adr
        JOIN fr.laposte_address_street_membership sm ON sm.name_id = sr.name_id
        JOIN fr.laposte_address_street_word sw ON sw.word = sm.word
    GROUP BY
        s.co_insee_commune
        , sw.word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Comptage (mot): ', _nrows));

    WITH
    word_rank AS (
        SELECT
            municipality_code
            , word
            , ROW_NUMBER() OVER (PARTITION BY municipality_code ORDER BY count DESC) "rank"
        FROM
            fr.laposte_address_municipality_word
    )
    UPDATE fr.laposte_address_municipality_word w SET
        rank = r.rank
        FROM word_rank r
        WHERE
            (w.municipality_code, w.word) = (r.municipality_code, r.word)
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Rangs (mot) : ', _nrows));

    /*
    CALL public.log_info('Gestion des mots dans les noms de voies par zones');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_area_word;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_area_word');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_area_word(
        code_area
        , word
        , count
    )
    SELECT
        s.co_adr_za
        , sw.word
        , COUNT(*)
    FROM fr.street_view s
        JOIN fr.laposte_address_street_reference sr ON sr.address_id = s.co_adr
        JOIN fr.laposte_address_street_membership sm ON sm.name_id = sr.name_id
        JOIN fr.laposte_address_street_word sw ON sw.word = sm.word
    GROUP BY
        s.co_adr_za
        , sw.word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Comptage (mot): ', _nrows));

    WITH
    word_rank AS (
        SELECT
            code_area
            , word
            , ROW_NUMBER() OVER (PARTITION BY code_area ORDER BY count DESC) "rank"
        FROM
            fr.laposte_address_area_word
    )
    UPDATE fr.laposte_address_area_word w SET
        rank = r.rank
        FROM word_rank r
        WHERE
            (w.code_area, w.word) = (r.code_area, r.word)
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Rangs (mot) : ', _nrows));
     */

    CALL fr.set_laposte_address_municipality_word_index();
    --CALL fr.set_laposte_address_area_word_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
19:09:49.655 Gestion des mots dans les noms de voies par communes
19:09:49.655  Purge
19:09:49.682  Initialisation
19:10:47.342  Comptage (mot): 2835436
19:11:10.751  Rangs (mot) : 2835436
19:11:14.344  Indexation

Query returned successfully in 1 min 26 secs.
 */
