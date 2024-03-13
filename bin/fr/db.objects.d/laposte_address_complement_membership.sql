/***
 * FR: add LAPOSTE/RAN complement words membership
 */

-- to store words, counters by descriptor, default descriptor, ranks
CREATE TABLE IF NOT EXISTS fr.laposte_address_complement_membership (
    word VARCHAR NOT NULL
    , name_id INT NOT NULL
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_membership_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_membership_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_complement_membership_word ON fr.laposte_address_complement_membership (word);
END
$proc$ LANGUAGE plpgsql;

-- build membership of all words
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_membership');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_membership(
    set_case IN VARCHAR DEFAULT 'CREATION'                  -- CREATION | CORRECTION
    , listof IN INT[] DEFAULT NULL
)
AS
$proc$
DECLARE
    _nrows INT;
    _query TEXT;
    _info VARCHAR := CASE
        WHEN set_case = 'CREATION' THEN 'Initialisation'
        WHEN set_case = 'CORRECTION' THEN 'Correction'
        END
    ;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_complement_uniq') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion de l''appartenance des mots dans les noms de compléments');

    CALL public.log_info(' Purge');
    IF set_case = 'CREATION' THEN
        TRUNCATE TABLE fr.laposte_address_complement_membership;
        PERFORM public.drop_table_indexes('fr', 'laposte_address_complement_membership');
    ELSIF set_case = 'CORRECTION' THEN
        DELETE FROM fr.laposte_address_complement_membership
            WHERE name_id = ANY(listof)
        ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(' Effacement: ', _nrows));
    END IF;

    CALL public.log_info(CONCAT(' ', _info));
    _query := '
        INSERT INTO fr.laposte_address_complement_membership(
            word
            , name_id
        )
        WITH
        split_as_word AS (
            SELECT
                w.word
                , u.id
            FROM
                fr.laposte_address_complement_uniq u
                    INNER JOIN LATERAL UNNEST(u.words) AS w(word) ON TRUE
            WHERE
        '
        ;
    IF set_case = 'CORRECTION' THEN
        _query := CONCAT(_query
            , '
            u.id = ANY($1)
            AND
            '
        );
    END IF;
    _query := CONCAT(_query
        , '
            -- except: article
            NOT fr.is_normalized_article(w.word)
        )
        SELECT * FROM split_as_word
        '
    );
    IF set_case = 'CREATION' THEN
        EXECUTE _query;
    ELSIF set_case = 'CORRECTION' THEN
        EXECUTE _query USING listof;
    END IF;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Appartenance (mots): ', _nrows));

    IF set_case = 'CREATION' THEN
        CALL fr.set_laposte_address_complement_membership_index();
        CALL public.log_info(' Indexation');
    END IF;
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_complement_membership(set_case => 'CREATION');

19:16:51.510 Gestion de l'appartenance des mots dans les noms de compléments
19:16:51.510  Purge
19:16:51.534  Initialisation
19:16:54.878  Appartenance (mots): 1327471
19:16:56.265  Indexation

Query returned successfully in 4 secs 781 msec.
 */
