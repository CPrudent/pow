/***
 * FR: add LAPOSTE/RAN muncipality words
 */

-- TODO add rank_0, but rank_1 more difficult

-- to store words by muncipality
CREATE TABLE IF NOT EXISTS fr.laposte_address_municipality_word (
    municipality_code VARCHAR NOT NULL
    , word VARCHAR NOT NULL
    , count INT NOT NULL
    , rank_0 INT
)
;

DO $$
BEGIN
    IF NOT column_exists('fr', 'laposte_address_municipality_word', 'rank_0') THEN
        ALTER TABLE fr.laposte_address_municipality_word ADD COLUMN rank_0 INT;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_municipality_word_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_municipality_word_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_municipality_word_code ON fr.laposte_address_municipality_word (municipality_code);
END
$proc$ LANGUAGE plpgsql;

-- build counters (by descriptor), ranks and default for each word
-- Query returned successfully in 13 secs.
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

    CALL public.log_info('Gestion des mots dans les noms de voies des communes');

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
        , COUNT(*) nb
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
            , row_number() OVER (PARTITION BY municipality_code ORDER BY count DESC) rank_0
        FROM
            fr.laposte_address_municipality_word
    )
    UPDATE fr.laposte_address_municipality_word w SET
        rank_0 = r.rank_0
        FROM word_rank r
        WHERE
            (w.municipality_code, w.word) = (r.municipality_code, r.word)
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Rangs (mot): ', _nrows));

    CALL fr.set_laposte_address_municipality_word_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
11:11:33.663 Gestion des mots dans les noms de voies des communes
11:11:33.663  Purge
11:11:33.693  Initialisation
11:12:34.414  Comptage (mot): 2835436
11:12:35.426  Indexation

Query returned successfully in 1 min 2 secs.
 */
