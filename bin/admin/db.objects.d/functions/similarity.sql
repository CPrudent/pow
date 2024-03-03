/***
 * add SIMILARITY facilities
 */

SELECT drop_all_functions_if_exists('public', 'get_metaphone');
CREATE OR REPLACE FUNCTION public.get_metaphone(
    str IN VARCHAR
    , metaphone OUT VARCHAR
)
AS
$func$
BEGIN
    --str := getChaineAvecNombresEnLettresClassiques(str);

    -- word by word, keeping digits (better if transformed in letters?)
    SELECT
        STRING_AGG(
            COALESCE(
                NULLIF(
                    METAPHONE(word, 10)
                    , ''
                )
                , word
            )
            /* FIXME
            w/ space ? will allow to fully match 'RUE O QUIN' w/ 'RUE OQUIN'
             */
            , ''
        )
    INTO metaphone
    FROM (
        SELECT UNNEST(
            STRING_TO_ARRAY(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(str, '([0-9])([^0-9 ])', '\1 \2', 'g')
                    , '([^0-9 ])([0-9])', '\1 \2'
                    , 'g'
                )
                , ' '
            )
        ) AS word
    ) t
    ;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT get_metaphone('RUE DE BELLE FLEUR');
SELECT get_metaphone('RUE DE BELLES FLEURS');

SELECT get_metaphone('AVENUE DU 18EME REGIMENT INFANTERIE');
SELECT get_metaphone('AVENUE DU 18EME R I');
SELECT get_similarity('AVENUE DU 18EME R I', 'AVENUE DU 18EME REGIMENT INFANTERIE');
 */

SELECT drop_all_functions_if_exists('public', 'get_similarity_semantics');
CREATE OR REPLACE FUNCTION public.get_similarity_semantics(
    str_a IN VARCHAR
    , str_b IN VARCHAR
    , similarity OUT NUMERIC
)
AS
$func$
DECLARE
    _lenvenshtein INTEGER;
    _length_avg INTEGER;
    _similarity_lenvenshtein NUMERIC;
BEGIN
    similarity := SIMILARITY(str_a, str_b);
    IF similarity < 1 OR similarity IS NULL THEN
        -- Indice puissance 2, pour diminuer le score de façon exponentielle au fur et à mesure que la distance grandi
        _lenvenshtein := LEVENSHTEIN(str_a, str_b);
        _length_avg := (LENGTH(str_a) + LENGTH(str_b))/2;
        IF _lenvenshtein < _length_avg THEN
            _similarity_lenvenshtein := POWER(1 - ((_lenvenshtein::NUMERIC) / (_length_avg)), 2);
        END IF;
        similarity := GREATEST(similarity, _similarity_lenvenshtein);
    END IF;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'get_similarity_phonetics');
CREATE OR REPLACE FUNCTION public.get_similarity_phonetics(
    str_a IN VARCHAR
    , str_b IN VARCHAR
    , similarity OUT NUMERIC
)
RETURNS NUMERIC AS
$func$
DECLARE
BEGIN
    similarity := get_similarity_semantics(get_metaphone(str_a), get_metaphone(str_b));
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'get_similarity');
CREATE OR REPLACE FUNCTION public.get_similarity(
    str_a IN VARCHAR
    , str_b IN VARCHAR
    , similarity OUT NUMERIC
)
AS
$func$
DECLARE
    _similarity_semantics NUMERIC;
    _similarity_phonetics NUMERIC;
BEGIN
    --On applique le traitement autant pour tous les types de tests de similtude, et plus seulement pour la phonétique, est-ce vraiment bien ?
    --str_a := public.getChaineAvecNombresEnLettresClassiques(str_a);
    --str_b := public.getChaineAvecNombresEnLettresClassiques(str_b);
    _similarity_semantics := get_similarity_semantics(str_a, str_b);
    IF _similarity_semantics < 1 OR _similarity_semantics IS NULL THEN
        _similarity_phonetics := get_similarity_phonetics(str_a, str_b);
        IF _similarity_phonetics > _similarity_semantics  THEN
            similarity := (_similarity_phonetics + _similarity_semantics) / 2;
            RETURN;
        END IF;
    END IF;
    similarity := _similarity_semantics;
END
$func$ LANGUAGE plpgsql;

/*
 * eval (sum ou avg) similarity string A w/ each word of string B
 */
CREATE OR REPLACE FUNCTION public.get_similarity(
    str_a IN VARCHAR
    , strs_b IN VARCHAR[]
    , similarity_min IN REAL DEFAULT NULL
    , method IN VARCHAR DEFAULT 'SUM'
    , similarity OUT NUMERIC
)
AS
$func$
BEGIN
    /* NOTE
    convert array if not formatted as number for each word
    we can then eval similarity once for each distinct word
     */
    IF strs_b[1] NOT LIKE '%;%' THEN
        --RAISE NOTICE 'regroupement des mots et comptage';
        SELECT ARRAY_AGG(CONCAT_WS(';', str, nb))
        INTO strs_b
        FROM (
            SELECT UNNEST(strs_b) AS str, COUNT(*) AS nb GROUP BY str
        ) t;
    END IF;

    IF method = 'SUM' THEN
        SELECT
            SUM(
                get_similarity(
                    str_a
                    , (STRING_TO_ARRAY(_strs_b.str_with_nb, ';'))[1]
                )
                *
                (STRING_TO_ARRAY(_strs_b.str_with_nb, ';'))[2]::INTEGER
            )
        INTO similarity
        FROM (SELECT UNNEST(strs_b) AS str_with_nb) AS _strs_b
        WHERE
            similarity_min IS NULL
            OR
            get_similarity(
                str_a
                , (STRING_TO_ARRAY(_strs_b.str_with_nb, ';'))[1]
            ) >= similarity_min
        ;
    ELSIF method = 'AVG' THEN
        SELECT
            AVG(
                get_similarity(
                    str_a
                    , (STRING_TO_ARRAY(_strs_b.str_with_nb, ';'))[1]
                )
                *
                (STRING_TO_ARRAY(_strs_b.str_with_nb, ';'))[2]::INTEGER
            )
        INTO similarity
        FROM (SELECT UNNEST(strs_b) AS str_with_nb) AS _strs_b
        WHERE
            similarity_min IS NULL
            OR
            get_similarity(
                str_a
                , (STRING_TO_ARRAY(_strs_b.str_with_nb, ';'))[1]
            ) >= similarity_min
        ;
    END IF;
END
$func$ LANGUAGE plpgsql;

