/***
 * FR: add LAPOSTE/RAN street words membership
 */

-- to store words, counters by descriptor, default descriptor, ranks
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_membership (
    word VARCHAR NOT NULL
    , name_id INT NOT NULL
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_membership_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_membership_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_membership_word ON fr.laposte_address_street_membership (word);
END
$proc$ LANGUAGE plpgsql;

-- build membership of all words
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_membership');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_membership()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion de l''appartenance des mots dans les noms de voies');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_membership;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_membership');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_membership(
        word
        , name_id
    )
    -- #2726516
    WITH
    split_as_word AS (
        SELECT
            w.word
            , u.id
        FROM
            fr.laposte_address_street_uniq u
                INNER JOIN LATERAL UNNEST(u.words) WITH ORDINALITY AS w(word, i) ON TRUE
        WHERE
            -- except: number, article
            NOT fr.is_normalized_number(w.word)
            AND
            NOT fr.is_normalized_article(w.word)
    )
    SELECT * FROM split_as_word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Appartenance (mots): ', _nrows));

    CALL fr.set_laposte_address_street_membership_index();
    CALL public.log_info(' Indexation');
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_street_membership();

17:05:34.849 Gestion de l'appartenance des mots dans les noms de voies
17:05:34.849  Purge
DROP INDEX IF EXISTS fr.ix_laposte_address_street_membership_word
17:05:34.896  Initialisation
17:10:21.333  Appartenance (mots): 2726040
17:10:26.813  Indexation

Query returned successfully in 4 min 56 secs.
 */
