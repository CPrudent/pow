/***
 * add FR-ADDRESS facilities (similarity)
 */

SELECT drop_all_functions_if_exists('fr', 'get_descriptor_factor');
CREATE OR REPLACE FUNCTION fr.get_descriptor_factor(
    descriptor IN VARCHAR
    , descriptor_factor OUT REAL
)
AS
$func$
BEGIN
    descriptor_factor := CASE descriptor
        WHEN 'A' THEN   0.25
        WHEN 'V' THEN   0.5
        WHEN 'T' THEN   0.75
        ELSE            1.25
        END
    ;
END
$func$ LANGUAGE plpgsql;

-- order words according to (similarity, rarity and descriptor)
SELECT drop_all_functions_if_exists('fr', 'get_words_ordered_by_rank');
SELECT drop_all_functions_if_exists('fr', 'get_ordered_words_with_similarity_criteria');
CREATE OR REPLACE FUNCTION fr.get_ordered_words_with_similarity_criteria(
    words IN TEXT[]
    , municipality_code IN VARCHAR
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , similarity_threshold IN REAL DEFAULT 0.5
    , ordered_words OUT TEXT[]
)
AS
$func$
DECLARE
    _orders INT[];
    _i INT;
    _nof INT := (SELECT (current_setting('fr.address.n_uniq_streets', TRUE))::INT);
BEGIN
    IF municipality_code IS NULL THEN
        CALL public.log_info('manque paramètre (code INSEE)');
        RETURN;
    END IF;

    -- TODO take rank_0 from municipality

    IF _nof IS NULL THEN
        CALL public.log_info('manque paramètre global (fr.address.n_uniq_streets)');
        _nof := (
            SELECT MAX(rank_0) FROM fr.laposte_address_street_word
        );
    END IF;

    _orders := ARRAY(
        WITH
        similarity_word(i, similarity, rank, descriptor_factor) AS (
            SELECT
                w.i
                , get_similarity(mw.word, w.word)
                , sw.rank_0
                , fr.get_descriptor_factor(sw.as_default)
            FROM
                fr.laposte_address_municipality_word mw
                    JOIN fr.laposte_address_street_word sw ON mw.word = sw.word
                    JOIN LATERAL UNNEST(words) WITH ORDINALITY AS w(word, i) ON TRUE
            WHERE
                mw.municipality_code = get_words_ordered_by_rank.municipality_code
        )
        SELECT i
        FROM similarity_word
        WHERE similarity >= similarity_threshold
        ORDER BY
            -- get word w/ better similarity and rareness
            similarity * EXP((1 - (((_nof - "rank") +1)::NUMERIC / _nof))) * descriptor_factor DESC
    );

    /*
    FOR _i IN 1..ARRAY_LENGTH(_words, 1)
    LOOP
        IF fr.is_normalized_article(_words[_i]) THEN
            _orders[_i] := NULL;
        ELSE
            _orders[_i] := (
                WITH
                similarity_word(word, similarity) AS (
                    SELECT word, get_similarity(word, _words[_i])
                    FROM fr.laposte_address_municipality_word mw
                    WHERE mw.municipality_code = get_words_ordered_by_rank.municipality_code
                )
                SELECT similarity FROM similarity_word
                WHERE similarity >= similarity_threshold
                ORDER BY similarity DESC
                LIMIT 1
            );
        END IF;
    END LOOP;

    ordered_words := ARRAY(
        SELECT
            w.word
        FROM
            UNNEST(_orders) WITH ORDINALITY AS r(rank, i)
            , UNNEST(_words) WITH ORDINALITY AS w(word, i)
        WHERE
            r.i = w.i
        ORDER BY
            r.rank
    );
     */

    FOR _i IN 1..ARRAY_LENGTH(words, 1)
    LOOP
        IF raise_notice THEN
            RAISE NOTICE ' word #% : % (order=%)', _i, words[_i], _orders[_i];
        END IF;
        ordered_words[_i] := words[_orders[_i]];
    END LOOP;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_similarity_street_with_rarity');
CREATE OR REPLACE FUNCTION fr.get_similarity_street_with_rarity(
    name IN VARCHAR
    , code_address_compare_to IN CHAR(10)
    , municipality_code IN CHAR(5) DEFAULT NULL
    , similarity OUT NUMERIC
)
SET client_min_messages = error
AS
$func$
DECLARE
	_words TEXT[];
BEGIN
    --Si code insee commune de la voie comparée pas fourni, on le charge
    IF municipality_code IS NULL THEN
        SELECT street.co_insee_commune
        INTO municipality_code
        FROM fr.street_view AS street
        WHERE street.co_adr = code_address_compare_to
        ;
    END IF;

    --DROP TABLE IF EXISTS tmp_municipality_words;
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_municipality_words (
        municipality_code CHAR(5) NOT NULL
        , words TEXT[]
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ix_tmp_municipality_words_code ON tmp_municipality_words(municipality_code);
    BEGIN
        SELECT words
        INTO STRICT _words
        FROM tmp_municipality_words mw
        WHERE
            mw.municipality_code = get_similarity_street_with_rarity.municipality_code
        ;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'contruction de la liste des mots voie de la commune %', municipality_code;

            SELECT ARRAY_AGG(CONCAT_WS(';', word, nb))
            INTO _words
            FROM
            (
                SELECT
                    sw.word
                    , COUNT(*) nb
                FROM fr.street_view s
                    JOIN fr.laposte_address_street_reference sr ON sr.address_id = s.co_adr
                    -- TODO: add number!
                    JOIN fr.laposte_address_street_membership sm ON sm.name_id = sr.name_id
                    JOIN fr.laposte_address_street_word sw ON sw.word = sm.word
                WHERE s.co_insee_commune = municipality_code
                GROUP BY
                    sw.word
            ) t
            ;

            INSERT INTO tmp_municipality_words (municipality_code, words) VALUES (municipality_code, _words);
    END;

    SELECT
        GREATEST(
            MAX(similitude * rarete)
            ,SUM(similitude * proportion_rarete)
        ) AS similitude
        /* FIXME : il n'est pas forcément logique d'additionner les similitude pondérée. exemple (100% similitude * 50% rareté + 100% similitude * 50% rareté = 100% certitude, or on a deux fois un chance sur deux, on devrait rester à 50% de certitude = la plus grande)
        SUM(
            similitude
            *
            GREATEST(proportion_rarete,rarete)
        ) AS similitude
        */
        /*
        a creuser :
        mot rare pas ou mal trouvé -> grave
        mot courant pas ou mal trouvé -> pas important
        plusieurs mots rares trouvés -> bonus exponentiels, tout en restant à 0.5 si 2 * 0.5
        */
    INTO similarity
    FROM(
        WITH rarete_mots_voie AS
        (
            SELECT
                mot
                ,ordre
                ,rarete AS rarete
                ,COALESCE(proportion_rarete
                    , (rarete / (SUM(rarete) OVER ())) - (SUM(proportion_rarete) OVER ())
                ) AS proportion_rarete
            FROM
            (
                SELECT	mots.mot
                    ,mots.ordre
                    ,descripteurs.descripteur
                    ,CASE WHEN descripteurs.descripteur NOT IN ('C','N') THEN NULL
                    ELSE
                        (
                            1
                            /
                            get_similarity(
                                mots.mot
                                ,_words
                                --,(SELECT mots FROM tmp_municipality_words WHERE co_insee_commune = '33051')
                                ,0.5
                                ,'SUM'
                            )
                        )
                    END AS rarete
                    --les mots de type de voie se partagent 20% ?
                    --,CASE WHEN descripteurs.descripteur = 'V' THEN 0.2 / (SUM(CASE WHEN descripteurs.descripteur = 'V' THEN 1 ELSE 0 END) OVER ())
                    --les mots qui ne sont pas de type Chiffre ou Nom se partagent 30% ?
                    --,CASE WHEN descripteurs.descripteur NOT IN ('C','N') THEN 0.3 / (SUM(CASE WHEN descripteurs.descripteur NOT IN ('C','N') THEN 1 ELSE 0 END) OVER ())
                    --les mots qui ne sont pas de type Chiffre ou Nom on chacun 10% ?
                    --,CASE WHEN descripteurs.descripteur NOT IN ('C','N') THEN 0.1

                    ,CASE
                        WHEN descripteurs.descripteur = 'V'
                            THEN 0.12 / (SUM(CASE WHEN descripteurs.descripteur = 'V' THEN 1 ELSE 0 END) OVER ())
                        WHEN descripteurs.descripteur NOT IN ('C','N','V')
                            THEN 0.12
                        ELSE NULL
                    END AS proportion_rarete
                FROM fr.street_view AS street
                INNER JOIN LATERAL UNNEST(STRING_TO_ARRAY(street.lb_voie, ' '))
                        WITH ORDINALITY AS mots(mot, ordre) ON TRUE
                INNER JOIN LATERAL UNNEST(STRING_TO_ARRAY(street.lb_voie_desc, null))
                        WITH ORDINALITY AS descripteurs(descripteur, ordre) ON mots.ordre = descripteurs.ordre
                        AND descripteurs.descripteur != 'A'
                WHERE co_adr = code_address_compare_to
            ) t
        )
        SELECT
            recherche.mot AS mot_recherche
            ,recherche.ordre AS ordre_recherche
            ,rarete_mots_voie.mot AS mot_trouve
            --,rarete_mots_voie.ordre AS ordre_trouve
            ,rarete_mots_voie.rarete
            ,rarete_mots_voie.proportion_rarete
            ,getSimilitude(rarete_mots_voie.mot,recherche.mot) AS similitude
            ,RANK() OVER (PARTITION BY recherche.ordre ORDER BY getSimilitude(rarete_mots_voie.mot,recherche.mot) DESC) AS ordre_meilleure_similitude
            ,RANK() OVER (PARTITION BY rarete_mots_voie.ordre ORDER BY getSimilitude(rarete_mots_voie.mot,recherche.mot) DESC) AS ordre_meilleure_similitude2
        FROM
            rarete_mots_voie
        LEFT OUTER JOIN UNNEST(STRING_TO_ARRAY(public.removeMotsOutils(name), ' ')) WITH ORDINALITY AS recherche(mot, ordre) ON TRUE
        --LEFT OUTER JOIN UNNEST(STRING_TO_ARRAY(public.removeMotsOutils('CHEMIN DES FONTANELLES'), ' ')) WITH ORDINALITY AS recherche(mot, ordre) ON TRUE

    ) AS sous_requete
    WHERE ordre_meilleure_similitude = 1 AND ordre_meilleure_similitude2 = 1
    ;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_similarity_street');
CREATE OR REPLACE FUNCTION fr.get_similarity_street(
    name IN VARCHAR
    , code_address_compare_to IN CHAR(10)
    , name_compare_to IN VARCHAR
    , municipality_code IN CHAR(5) DEFAULT NULL
    , similarity OUT NUMERIC
)
AS
$func$
BEGIN
    similarity := public.get_similarity(name, name_compare_to);
    IF similarity < 1 OR similarity IS NULL THEN
        similarity := fr.get_similarity_street_with_rarity(
            name, code_address_compare_to, municipality_code
        );
    END IF;
END
$func$ LANGUAGE plpgsql;
