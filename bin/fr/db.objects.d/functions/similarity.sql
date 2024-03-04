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
SELECT drop_all_functions_if_exists('fr', 'get_ordered_words_with_similarity_criteria');
SELECT drop_all_functions_if_exists('fr', 'get_better_word_with_similarity_criteria');
CREATE OR REPLACE FUNCTION fr.get_better_word_with_similarity_criteria(
    words IN TEXT[]
    , municipality_code IN VARCHAR
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , order_word OUT INT
    , better_word OUT TEXT
)
AS
$func$
DECLARE
    _criteria RECORD;
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

    WITH
    similarity_word(i, word, similarity, rank, descriptor_factor) AS (
        SELECT
            w.i
            , mw.word
            , get_similarity(mw.word, w.word)
            , sw.rank_0
            , fr.get_descriptor_factor(sw.as_default)
        FROM
            fr.laposte_address_municipality_word mw
                -- remember: w/o article
                JOIN fr.laposte_address_street_word sw ON mw.word = sw.word
                JOIN LATERAL UNNEST(words) WITH ORDINALITY AS w(word, i) ON TRUE
        WHERE
            mw.municipality_code = get_better_word_with_similarity_criteria.municipality_code
    )
    SELECT
        i
        , word
    INTO
        order_word
        , better_word
    FROM
        similarity_word
    ORDER BY
        -- get word w/ better similarity and rarity
        (similarity * EXP((1 - (((_nof - "rank") +1)::NUMERIC / _nof))) * descriptor_factor) DESC
    LIMIT
        1
    ;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_similarity_street');
CREATE OR REPLACE FUNCTION fr.get_similarity_street(
    words_a IN TEXT[]
    , words_b IN TEXT[]
    , descriptors_a IN VARCHAR
    , descriptors_b IN VARCHAR
    , similarity OUT NUMERIC
)
AS
$func$
BEGIN
    similarity := (
    SELECT
        SUM(sim)
    FROM (
        SELECT
            get_similarity(word1, word2) sim
            , RANK() OVER (PARTITION BY i1 ORDER BY get_similarity(word1, word2) DESC) best_order_similarity_1
            , RANK() OVER (PARTITION BY i2 ORDER BY get_similarity(word1, word2) DESC) best_order_similarity_2
        FROM (
            SELECT
                a.word word1
                , a.i i1
                , b.word word2
                , b.i i2
            FROM (
                    UNNEST(words_a) WITH ORDINALITY AS w1(word, i)
                        JOIN LATERAL UNNEST(STRING_TO_ARRAY(descriptors_a, NULL))
                        WITH ORDINALITY AS d1(descriptor, j) ON w1.i = d1.j AND d1.descriptor != 'A'
                ) a
                    LEFT OUTER JOIN (
                    UNNEST(words_b) WITH ORDINALITY AS w2(word, i)
                        JOIN LATERAL UNNEST(STRING_TO_ARRAY(descriptors_b, NULL))
                        WITH ORDINALITY AS d2(descriptor, j) ON w2.i = d2.j AND d2.descriptor != 'A'
                ) b ON TRUE
        ) t
    ) tt
    WHERE
        -- better list of similarities between searched name (a) and compared one (b)
        best_order_similarity_1 = 1
        AND
        best_order_similarity_2 = 1
    )
    ;

    /*
    similarity := (
        WITH
        similarity_streets AS (
            SELECT
                /*
                word1
                , i1
                , word2
                , i2

                , */
                get_similarity(word1, word2) similarity
                , RANK() OVER (PARTITION BY i1 ORDER BY get_similarity(word1, word2) DESC) best_order_similarity_1
                , RANK() OVER (PARTITION BY i2 ORDER BY get_similarity(word1, word2) DESC) best_order_similarity_2
            FROM (
                SELECT
                    a.word word1
                    , a.i i1
                    , b.word word2
                    , b.i i2
                FROM
                (
                    UNNEST(words_a) WITH ORDINALITY AS w1(word, i)
                        JOIN LATERAL UNNEST(STRING_TO_ARRAY(descriptors_a, NULL))
                        WITH ORDINALITY AS d1(descriptor, j) ON w1.i = d1.j AND d1.descriptor != 'A'
                ) a
                    LEFT OUTER JOIN (
                    UNNEST(words_b) WITH ORDINALITY AS w2(word, i)
                        JOIN LATERAL UNNEST(STRING_TO_ARRAY(descriptors_b, NULL))
                        WITH ORDINALITY AS d2(descriptor, j) ON w2.i = d2.j AND d2.descriptor != 'A'
                ) b ON TRUE
            ) t
        )
        SELECT
            SUM(similarity)
        FROM
            similarity_streets
        WHERE
            best_order_similarity_1 = 1 AND best_order_similarity_2 = 1
    )
    ;
     */
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_similarity_street_with_rarity');
/*
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
 */
