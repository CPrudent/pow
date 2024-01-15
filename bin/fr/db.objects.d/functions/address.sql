/***
 * add FR-ADDRESS facilities
 */

-- deduce department code from municipality code
SELECT public.drop_all_functions_if_exists('fr', 'get_department_code_from_municipality_code');
CREATE OR REPLACE FUNCTION fr.get_department_code_from_municipality_code(
    municipality_code CHARACTER(5)
)
RETURNS CHARACTER VARYING(3)
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE
        -- DOM + (98) = POLYNESIE
        WHEN LEFT(municipality_code, 2) IN ('97', '98') THEN LEFT(municipality_code, 3)
        -- FRANCE métropolitaine + (99) = MONACO
        ELSE LEFT(municipality_code, 2)
        END;
END
$func$ LANGUAGE plpgsql;

-- get project code from department code
SELECT public.drop_all_functions_if_exists('FR','get_project_code_from_department_code');
CREATE OR REPLACE FUNCTION FR.get_project_code_from_department_code(
    department_code CHARACTER(5)
)
RETURNS CHARACTER(1)
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE
        /* Guadeloupe Martinique (971XX et 972XX)
            * + Saint-Barthélemy (977XX), île francophone des Caraïbes
            * + Saint-Martin (978XX). Fait partie des îles Leeward dans la mer des Caraïbes. Elle est divisée entre 2 pays distincts : sa partie nord, appelée Saint-Martin, est française, et sa partie sud, Sint Maarten, est néerlandaise.
         */
        WHEN department_code IN ('971','972','977','978') THEN '2'

        /* Obsolète (LAMBERT II ETENDU)
        WHEN XXXX THEN '3'
         */

        --Guyane française (973XX), région d'outre-mer située sur la côte nord-est de l'Amérique du Sud
        WHEN department_code = '973' THEN '4'

        --Ile de la Réunion (974XX)
        WHEN department_code = '974' THEN '5'

        --Mayotte (976XX et 985XX sur co_adr), archipel de l'océan Indien situé entre Madagascar et la côte du Mozambique
        WHEN department_code IN ('976','985'/*Ancien code ? les codes adresses RAN commencent par 985*/) THEN '6'

        /* Saint-Pierre-et-Miquelon (975XX), archipel français au sud de l'île canadienne de Terre-Neuve
         * Pas de code projection RAN défini, ni d'adresse RAN existante
         */
        WHEN department_code = '975' THEN NULL --Saint-Pierre-et-Miquelon

        -- France Métropolitaine, Monaco
        ELSE '1'

        END;
END
$func$ LANGUAGE plpgsql;

-- get SRID from project code
SELECT public.drop_all_functions_if_exists('fr','get_srid_from_project_code');
CREATE OR REPLACE FUNCTION fr.get_srid_from_project_code(
    project_code CHARACTER
)
RETURNS SMALLINT
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE project_code
        -- France Métropolitaine, Monaco
        WHEN '1' THEN 2154

        /* Guadeloupe Martinique (971XX et 972XX)
            * + Saint-Barthélemy (977XX), île francophone des Caraïbes
            * + Saint-Martin (978XX). Fait partie des îles Leeward dans la mer des Caraïbes. Elle est divisée entre 2 pays distincts : sa partie nord, appelée Saint-Martin, est française, et sa partie sud, Sint Maarten, est néerlandaise.
         */
        WHEN '2' THEN 4559

        /* Obsolète (LAMBERT II ETENDU)
        WHEN '3' THEN XXXX
         */

        --Guyane française (973XX), région d'outre-mer située sur la côte nord-est de l'Amérique du Sud
        WHEN '4' THEN 2972

        --Ile de la Réunion (974XX)
        WHEN '5' THEN 2975

        --Mayotte (976XX), archipel de l'océan Indien situé entre Madagascar et la côte du Mozambique
        WHEN '6' THEN 4471

        /* Saint-Pierre-et-Miquelon (975XX), archipel français au sud de l'île canadienne de Terre-Neuve
            * Pas de code projection RAN défini, ni d'adresse RAN existante
        'X' THEN 4467
         */
        END;
END
$func$ LANGUAGE plpgsql;

-- get SRID from department code
SELECT public.drop_all_functions_if_exists('fr', 'get_srid_from_department_code');
CREATE OR REPLACE FUNCTION fr.get_srid_from_department_code(
    department_code CHARACTER(5)
)
RETURNS SMALLINT
IMMUTABLE
AS
$func$
BEGIN
	RETURN fr.get_srid_from_project_code(fr.get_project_code_from_department_code(department_code));
END
$func$ LANGUAGE plpgsql;

-- get title(s) from name of street
SELECT drop_all_functions_if_exists('fr', 'get_titles_from_name');
CREATE OR REPLACE FUNCTION fr.get_titles_from_name(
    name IN VARCHAR
    , descriptor IN VARCHAR
    , titles OUT TEXT[]
)
AS
$func$
DECLARE
    _words TEXT[] := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _title TEXT;
    _descriptor VARCHAR;
    _descriptor_len INT := LENGTH(descriptor);
    _descriptor_prev VARCHAR := 'Z';
    _i INT;
BEGIN
    FOR _i IN 1 .. _descriptor_len
    LOOP
        _descriptor := SUBSTR(descriptor, _i, 1);
        IF _descriptor = 'T' THEN
            _title :=
                CASE WHEN _descriptor_prev = 'T' THEN CONCAT(_title, ' ', _words[_i])
                ELSE _words[_i]
                END;
        ELSE
            IF _title IS NOT NULL THEN
                titles := ARRAY_APPEND(titles, _title);
                _title := NULL;
            END IF;
        END IF;
        _descriptor_prev := _descriptor;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

/*
-- get keyword(s) from name of street
SELECT drop_all_functions_if_exists('fr', 'get_keywords_from_name');
CREATE OR REPLACE FUNCTION fr.get_keywords_from_name(
    descriptor IN VARCHAR
    , descriptors IN VARCHAR
    , name IN VARCHAR DEFAULT NULL
    , words IN TEXT[] DEFAULT NULL
    , keywords OUT TEXT[]
)
AS
$func$
DECLARE
    _words TEXT[];
    _kw TEXT;
    _descriptor VARCHAR;
    _descriptor_len INT := LENGTH(descriptors);
    _descriptor_prev VARCHAR := 'Z';
    _i INT;
BEGIN
    IF name IS NULL AND words IS NULL THEN
        RAISE 'indiquer le nom de la voie (par son libellé OU par ses mots)';
    ELSIF name IS NOT NULL AND words IS NULL THEN
        _words := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    ELSIF name IS NULL AND words IS NOT NULL THEN
        _words := ARRAY_CAT(_words, words);
    END IF;
    IF _descriptor_len != ARRAY_UPPER(_words, 1) THEN
        RAISE 'traiter le nom de la voie avec un descripteur de même taille';
    END IF;

    FOR _i IN 1 .. _descriptor_len
    LOOP
        _descriptor := SUBSTR(descriptors, _i, 1);
        IF _descriptor = descriptor THEN
            _kw :=
                CASE WHEN _descriptor_prev = descriptor THEN CONCAT(_kw, ' ', _words[_i])
                ELSE _words[_i]
                END;
        ELSE
            IF _kw IS NOT NULL THEN
                keywords := ARRAY_APPEND(keywords, _kw);
                _kw := NULL;
            END IF;
        END IF;
        _descriptor_prev := _descriptor;
    END LOOP;
END
$func$ LANGUAGE plpgsql;
 */

-- count potential number of words (w/ _descriptor)
SELECT drop_all_functions_if_exists('fr', 'count_potential_nof_words');
CREATE OR REPLACE FUNCTION fr.count_potential_nof_words(
    descriptor IN VARCHAR
    , nof OUT INT
)
AS
$func$
DECLARE
    _descriptor VARCHAR;
    _descriptor_len INT := LENGTH(descriptor);
    _descriptor_prev VARCHAR := 'Z';
    _i INT;
BEGIN
    nof := 0;
    FOR _i IN 1 .. _descriptor_len
    LOOP
        _descriptor := SUBSTR(descriptor, _i, 1);
        IF (_descriptor != _descriptor_prev) THEN
            nof := nof +1;
        END IF;
        _descriptor_prev := _descriptor;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

-- split name of street as words, descriptors (w/ same descriptor)
SELECT drop_all_functions_if_exists('fr', 'split_name_of_street_as_descriptor');
CREATE OR REPLACE FUNCTION fr.split_name_of_street_as_descriptor(
    name IN VARCHAR
    , descriptor IN VARCHAR
    , is_normalized IN BOOLEAN DEFAULT FALSE
    , split_only IN VARCHAR DEFAULT NULL        -- specific descriptor: A,C,E,N,P,T,V
    , words OUT TEXT[]
    , descriptors OUT TEXT[]
)
AS
$func$
DECLARE
    _i INT;
    _j INT;
    _k INT;
    _n INT;
    _offset INT := 1;
    _usecase_article INT;
    _usecase_title INT;
    _words TEXT[] := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _descriptor VARCHAR;
    _descriptor_len INT := LENGTH(descriptor);
    _descriptor_prev VARCHAR := 'Z';
    _descriptor_word VARCHAR := NULL;
    _descriptor_same VARCHAR := NULL;
    _descriptor_before  VARCHAR;
    _descriptor_with_a  VARCHAR;
    _descriptor_remainder VARCHAR;
    _descriptor_from INT;
    _descriptor_type VARCHAR;
    _descriptor_title VARCHAR;
    _descriptor_others VARCHAR;
    --_descriptor_next VARCHAR;
    _descriptor_wo_a VARCHAR;
    _descriptor_only_a VARCHAR;
    --_descriptor_only_t VARCHAR;
    _descriptor_start_a VARCHAR;
    _kw VARCHAR;
    _kw_more VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _descriptor_tmp VARCHAR;
BEGIN
    /* NOTE
    when is_normalized, normalized name (eventually w/ deleted article, or abbreviated _words)
    is_normalized => TRUE

    -- deleted article (one)
    AVENUE DE LA 9E DIVISION INFANTERIE DE CAVALERIE
        name => 'AV LA 9E DIV INFANT DE CAVALERIE'
        , descriptor => 'VAACTTAN'
    -- deleted article (all)
    CHEMIN D EXPLOITATION DU MAS SAINT PAUL
        name => 'CHEMIN EXPLOITATION MAS ST PAUL'
        , descriptor => 'VANATTN'
    CHEMIN DE NOTRE DAME DES CHAMPS ET DES VIGNES
        name => 'CHEMIN ND CHAMPS ET DES VIGNES'
        , descriptor => 'VATTANAAN'
    -- not an article!
    PARC D ACTIVITES NURIEUX CROIX CHALON'
        name => 'PARC A NURIEUX CRX CHALON'
        , descriptor => 'VANNTN'
    AVENUE DES ANCIENS COMBATTANTS FRANCAIS D INDOCHINE
        name => 'AV A COMBATTANTS FR INDOCHINE'
        , descriptor => 'VANNTAN'

    -- deleted (word of) title
    CHEMIN RURAL DIT ANCIEN CHEMIN DE BRISON A THUET
        name => 'CHEM R DIT ANCIEN BRISON THUET'
        , descriptor => 'VNNTTANAN'

    -- abbreviated title
    ZONE ARTISANALE CENTRE COMMERCIAL BEAUGE
        name => 'ZONE ARTISANALE CCIAL BEAUGE'
        , descriptor => 'VVTTN'
    PLACE NOTRE DAME DE LA LEGION D HONNEUR
        name => 'PL ND DE LA LEGION D HONNEUR'
        , descriptor => 'VTTAANAN'
    -- w/ deleted article (prev='T')
    CHEMIN DE NOTRE DAME DES CHAMPS ET DES VIGNES
        name => 'CHEMIN ND CHAMPS ET DES VIGNES'
        , descriptor => 'VATTANAAN'

    -- abbreviated type
    LIEU DIT LE GRAND BOIS DE LA DURANDIERE
        name => 'LD LE GD BOIS DE LA DURANDIERE'
        , descriptor => 'VVATTAAN'
     */

    _n := ARRAY_LENGTH(_words, 1);
    RAISE NOTICE '#d=%, #w=%', _descriptor_len, _n;
    FOR _i IN 1 .. _n
    LOOP
        _k := (_i + _offset -1);
        IF _k > _descriptor_len THEN
            EXIT;
        END IF;
        IF (_i = _n) AND (_k < _descriptor_len) THEN
            RAISE NOTICE ' ajustement k=% len=%', _k, _descriptor_len;
            _k := _descriptor_len;
        END IF;
        _descriptor := SUBSTR(descriptor, _k, 1);
        RAISE NOTICE '(i=% ofs=% k=%): d=% (pd=%), w=%', _i, _offset, _k, _descriptor, _descriptor_prev, _words[_i];

        -- article
        IF (is_normalized
            AND _n < _descriptor_len
            AND _descriptor = 'A'
        ) THEN
            IF (_descriptor_len - _k) != (_n - _i) THEN
                --_descriptor_from := CASE WHEN _descriptor_prev = 'A' THEN _k ELSE _k +1 END;
                _descriptor_from := _k +1;
                _descriptor_remainder := SUBSTR(descriptor, _descriptor_from);
                _descriptor_only_a := REGEXP_REPLACE(_descriptor_remainder, '[^A]', '', 'gi');
                _descriptor_tmp := SUBSTR(_descriptor_remainder, 1, 1);

                _usecase_article := CASE
                    -- remains only words except article, this is not an article
                    /* TODO
                    not fully OK !

                    add-on 1
                        RUE ADJ BESNAULT GENDARME LEFORT (VAATNAANN)
                    add-on 2
                        RUE DU LTDV LE BRIS (VATTTAN), but add-on 1 TRUE !
                     */
                    WHEN (((LENGTH(_descriptor_remainder) - LENGTH(_descriptor_only_a)) = (_n - _i +1))
                        AND
                        (
                            -- add-on 1
                            (
                                (_descriptor_tmp != 'A')
                                OR
                                (
                                    (_descriptor_tmp = 'A')
                                    AND
                                    fr.is_normalized_article(_words[_i])
                                )
                            )
                    /*
                            OR
                            -- add-on 2
                            (
                                (_descriptor_tmp != 'A')
                                AND
                                (fr.count_potential_nof_words(_descriptor_remainder) != (_n - _i))
                            )
                     */
                        )
                    ) THEN 1
                    -- current word not an article, so deleted article
                    WHEN (NOT fr.is_normalized_article(_words[_i])) THEN 2
                    ELSE 0
                    END;
                RAISE NOTICE ' usecase article=%', _usecase_article;

                -- as such number of words
                IF _usecase_article = ANY('{1}') THEN
                    IF _descriptor_word IS NOT NULL AND (
                        (split_only IS NULL)
                        OR
                        (_descriptor_same ~ split_only)
                    ) THEN
                        words := ARRAY_APPEND(words, _descriptor_word);
                        descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                    END IF;
                    _descriptor_wo_a := REGEXP_REPLACE(_descriptor_remainder, '[A]', '', 'gi');
                    _descriptor_word := _words[_i];
                    _descriptor_same := SUBSTR(_descriptor_wo_a, 1, 1);
                    _descriptor_prev := _descriptor_same;
                    _descriptor_start_a := (REGEXP_MATCHES(_descriptor_remainder, '^([A]+)'))[1];
                    IF _descriptor_start_a IS NOT NULL THEN
                        _offset := _offset + LENGTH(_descriptor_start_a);
                    ELSE
                        _offset := _offset + 1;
                    END IF;

                    RAISE NOTICE ' remainder=%', _descriptor_remainder;
                    RAISE NOTICE ' only_a=%', _descriptor_only_a;
                    RAISE NOTICE ' wo_a=%', _descriptor_wo_a;
                    RAISE NOTICE ' start_a=%', _descriptor_start_a;
                    RAISE NOTICE ' offset=%', _offset;
                -- deleted article
                ELSIF _usecase_article = ANY('{2}') THEN
                    FOR _j IN (_k + 1) .. _descriptor_len
                    LOOP
                        _descriptor := SUBSTR(descriptor, _j, 1);
                        _offset := _offset +1;
                        IF _descriptor != 'A' THEN
                            IF _descriptor_word IS NOT NULL AND (
                                (split_only IS NULL)
                                OR
                                (_descriptor_same ~ split_only)
                            ) THEN
                                words := ARRAY_APPEND(words, _descriptor_word);
                                descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                            END IF;
                            _descriptor_word := _words[_i];
                            _descriptor_same := _descriptor;
                            _descriptor_prev := _descriptor;

                            RAISE NOTICE ' descriptor=%', _descriptor;
                            RAISE NOTICE ' offset=%', _offset;
                            EXIT;
                        END IF;
                    END LOOP;
                -- remain at least one article
                ELSE
                    IF (_descriptor != _descriptor_prev) THEN
                        IF _descriptor_word IS NOT NULL AND (
                            (split_only IS NULL)
                            OR
                            (_descriptor_same ~ split_only)
                        ) THEN
                            words := ARRAY_APPEND(words, _descriptor_word);
                            descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                        END IF;
                        _descriptor_word := _words[_i];
                        _descriptor_same := _descriptor;
                    ELSE
                        _descriptor_word := CONCAT(_descriptor_word, ' ', _words[_i]);
                        _descriptor_same := CONCAT(_descriptor_same, _descriptor);
                    END IF;
                    _descriptor_prev := _descriptor;
                END IF;
                CONTINUE;
            END IF;
        -- type
        ELSIF (is_normalized
            AND _n < _descriptor_len
            AND _descriptor = 'V'
            AND _i = 1
        ) THEN
            SELECT kw, kw_is_abbreviated, kw_nwords
            INTO _kw, _kw_is_abbreviated, _kw_nwords
            FROM fr.get_type_of_street(
                name => name
            )
            ;
            -- abbreviated ?
            IF _kw_is_abbreviated THEN
                _descriptor_type := (REGEXP_MATCHES(descriptor, '^(V+)([^V])'))[1];
                -- be careful w/ multiple abbreviated type (as ZA, PCH) !
                IF count_words(_kw) != LENGTH(_descriptor_type) THEN
                    SELECT k.name
                    INTO _kw_more
                    FROM fr.laposte_address_street_keyword k
                    WHERE k.group = 'TYPE'
                    AND k.name_abbreviated = _words[_i]
                    AND count_words(k.name) = LENGTH(_descriptor_type)
                    ORDER BY occurs DESC
                    LIMIT 1;
                    IF FOUND THEN
                        _kw := _kw_more;
                    ELSE
                        RAISE NOTICE 'indécision libellé normalisé (lib=%, abr=%)', name, _kw_is_abbreviated;
                    END IF;
                END IF;
                _offset := count_words(_kw);
                _descriptor_word := _words[_i];
                _descriptor_same := _descriptor;
                CONTINUE;
            END IF;
        -- title
        ELSIF (is_normalized
            AND _n < _descriptor_len
            AND _descriptor = 'T'
        ) THEN
            _descriptor_from := _k;

            -- previous deleted article (one descriptor T already consumed)
            _descriptor_before := SUBSTR(descriptor, 1, _descriptor_from);
            _descriptor_with_a := (REGEXP_MATCHES(_descriptor_before, '(A+)(T+)$'))[1];
            IF (_descriptor_with_a IS NOT NULL
                AND
                (descriptors[ARRAY_UPPER(descriptors, 1)] !~ 'A')
            ) THEN
                _descriptor_from := _descriptor_from -1;
            END IF;

            _descriptor_remainder := SUBSTR(descriptor, _descriptor_from);
            _descriptor_title := (REGEXP_MATCHES(_descriptor_remainder, '(T+)([^T])'))[1];
            _descriptor_others := (REGEXP_MATCHES(_descriptor_remainder, '(T+)(.*)$'))[2];
            _descriptor_wo_a := REGEXP_REPLACE(_descriptor_others, '[A]', '', 'gi');
            _descriptor_only_a := REGEXP_REPLACE(_descriptor_others, '[^A]', '', 'gi');
            --_descriptor_only_t := REGEXP_REPLACE(_descriptor_others, '[^T]', '', 'gi');
            --_descriptor_next := SUBSTR(descriptor, (_descriptor_from + LENGTH(_descriptor_title)), 1);

            /*
            -- remains others than article less or equal to articles
            (((_n - _i) - LENGTH(_descriptor_wo_a)) < LENGTH(_descriptor_only_a))
             */

            _usecase_title := CASE
                -- same number of remaining words
                WHEN (_n - _i +1) = LENGTH(_descriptor_remainder) THEN 1
                -- same number of remaining words (w/o article), except current one
                WHEN (_n - _i) = LENGTH(_descriptor_wo_a) THEN 2
                -- same number of remaining words, except current one
                WHEN (_n - _i) = LENGTH(_descriptor_others) THEN 3
                -- same number of remaining words, except current one (potentially abbreviated)
                WHEN (_n - _i) = (LENGTH(_descriptor_only_a) + fr.count_potential_nof_words(_descriptor_wo_a)) THEN 5
                WHEN LENGTH(_descriptor_title) = 1 THEN 6
                -- remains others (as words), eventually w/ deleted article(s)
                WHEN ((_n - _i +1) <= LENGTH(_descriptor_others))
                    AND
                    (LENGTH(_descriptor_only_a) >= (LENGTH(_descriptor_others) - (_n - _i +1))) THEN 4
                /*
                WHEN
                (
                    -- useful ?
                    ((_descriptor_next = 'A' AND fr.is_normalized_article(_words[_i +1]))
                    OR
                    (_descriptor_next = 'C' AND fr.is_normalized_number(_words[_i +1]))
                    OR
                    (_descriptor_next = 'N' AND ((_i +1) = _n))
                    OR
                    (_descriptor_next = 'P' AND fr.is_normalized_firstname(_words[_i +1])))
                ) THEN x
                    */
                ELSE 0
                END;
            RAISE NOTICE ' usecase title=%', _usecase_title;

            -- as such number of words
            IF _usecase_title = ANY('{1,2,3,5,6}') THEN
                IF (_descriptor != _descriptor_prev) THEN
                    IF _descriptor_word IS NOT NULL AND (
                        (split_only IS NULL)
                        OR
                        (_descriptor_same ~ split_only)
                    ) THEN
                        words := ARRAY_APPEND(words, _descriptor_word);
                        descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                    END IF;
                    _descriptor_word := _words[_i];
                    _descriptor_same := _descriptor;
                ELSE
                    _descriptor_word := CONCAT(_descriptor_word, ' ', _words[_i]);
                    _descriptor_same := CONCAT(_descriptor_same, _descriptor);
                END IF;
                _descriptor_prev := _descriptor;

                -- abbreviated or deleted title ?
                -- RUE DU LTDV D ESTIENNE D ORVES
                IF ((_usecase_title = ANY('{2,3}'))
                    AND
                    (LENGTH(_descriptor_title) > 1)
                    AND
                    -- not many words
                    (count_words(_descriptor_word) = 1)
                ) THEN
                    _offset := _offset + LENGTH(_descriptor_title) -1;
                END IF;
                CONTINUE;
            -- more descriptors, so deleted word(s)
            ELSIF _usecase_title = ANY('{4}') THEN
                IF _descriptor_word IS NOT NULL AND (
                    (split_only IS NULL)
                    OR
                    (_descriptor_same ~ split_only)
                ) THEN
                    words := ARRAY_APPEND(words, _descriptor_word);
                    descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                END IF;

                _descriptor_word := _words[_i];
                -- search for next descriptor by adjusting offset
                FOR _j IN (_descriptor_from + LENGTH(_descriptor_title)) .. _descriptor_len
                LOOP
                    _descriptor_tmp := SUBSTR(descriptor, _j, 1);
                    IF (_descriptor_tmp != 'A')
                        OR
                        (
                            (_descriptor_tmp = 'A')
                            AND
                            fr.is_normalized_article(_words[_i])
                        )
                    THEN
                        EXIT;
                    END IF;
                    _offset := _offset +1;
                END LOOP;
                -- shift title (one word here)
                _offset := _offset +1;
                _descriptor_same := _descriptor_tmp;
                _descriptor_prev := _descriptor_same;

                RAISE NOTICE ' remainder=%', _descriptor_remainder;
                RAISE NOTICE ' others=%', _descriptor_others;
                RAISE NOTICE ' only_a=%', _descriptor_only_a;
                RAISE NOTICE ' wo_a=%', _descriptor_wo_a;
                RAISE NOTICE ' offset=%', _offset;
                CONTINUE;
            END IF;
        END IF;

        IF (_descriptor != _descriptor_prev) THEN
            IF _descriptor_word IS NOT NULL AND (
                (split_only IS NULL)
                OR
                (_descriptor_same ~ split_only)
            ) THEN
                -- last item already w/ same descriptor
                IF (descriptors[ARRAY_UPPER(descriptors, 1)] ~ SUBSTR(_descriptor_same, 1, 1)) THEN
                    words[ARRAY_UPPER(words, 1)] := CONCAT(
                        words[ARRAY_UPPER(words, 1)]
                        , ' '
                        , _descriptor_word
                    );
                    descriptors[ARRAY_UPPER(descriptors, 1)] := CONCAT(
                        descriptors[ARRAY_UPPER(descriptors, 1)]
                        , _descriptor_same
                    );
                ELSE
                    words := ARRAY_APPEND(words, _descriptor_word);
                    descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                END IF;
            END IF;
            _descriptor_word := _words[_i];
            _descriptor_same := _descriptor;
        ELSE
            _descriptor_word := CONCAT(_descriptor_word, ' ', _words[_i]);
            _descriptor_same := CONCAT(_descriptor_same, _descriptor);
        END IF;
        _descriptor_prev := _descriptor;
    END LOOP;
    IF ((split_only IS NULL)
        OR
        (_descriptor_same ~ split_only)
    ) THEN
        -- last item already w/ same descriptor
        IF (descriptors[ARRAY_UPPER(descriptors, 1)] ~ SUBSTR(_descriptor_same, 1, 1)) THEN
            words[ARRAY_UPPER(words, 1)] := CONCAT(
                words[ARRAY_UPPER(words, 1)]
                , ' '
                , _descriptor_word
            );
            descriptors[ARRAY_UPPER(descriptors, 1)] := CONCAT(
                descriptors[ARRAY_UPPER(descriptors, 1)]
                , _descriptor_same
            );
        ELSE
            words := ARRAY_APPEND(words, _descriptor_word);
            descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

-- get type of street (from full name)
SELECT drop_all_functions_if_exists('fr', 'get_type_of_street');
CREATE OR REPLACE FUNCTION fr.get_type_of_street(
    name IN VARCHAR                   -- name of street
    , with_abbreviation IN BOOLEAN DEFAULT FALSE
    , kw_group OUT VARCHAR
    , kw OUT VARCHAR
    , kw_abbreviated OUT VARCHAR
    , kw_is_abbreviated OUT BOOLEAN
    , kw_nwords OUT INT
)
AS
$func$
BEGIN
    SELECT ks.kw_group, ks.kw, ks.kw_abbreviated, ks.kw_is_abbreviated, ks.kw_nwords
    INTO
        get_type_of_street.kw_group
        , get_type_of_street.kw
        , get_type_of_street.kw_abbreviated
        , get_type_of_street.kw_is_abbreviated
        , get_type_of_street.kw_nwords
    FROM fr.get_keyword_of_street(
        name => name
        , group_ => 'TYPE'
        , with_abbreviation => with_abbreviation
    ) ks
    ;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM fr.get_type_of_street('CHEMIN DES SANSONNIERES LE VIEIL BAUGE');
SELECT * FROM fr.get_type_of_street('LD KER FRANCOIS BONEN');
SELECT * FROM fr.get_type_of_street('LE PONT D OIR MONTGOTHIER');
SELECT * FROM fr.get_type_of_street('ZONE D AMENAGEMENT CONCERTE DES GRANDS CHAMPS');
SELECT * FROM fr.get_type_of_street('ZA DES GRANDS CHAMPS');
 */

-- get descriptor of word taking into account exceptions
SELECT drop_all_functions_if_exists('fr', 'get_descriptor_from_exception');
CREATE OR REPLACE FUNCTION fr.get_descriptor_from_exception(
    words IN TEXT[]
    , nwords IN INT
    , at_ IN INT
    , as_descriptor IN VARCHAR
    , is_exception OUT BOOLEAN
    , descriptor OUT VARCHAR
)
AS
$func$
DECLARE
    -- NOTE be careful w/ this usage! when table not yet exists
    --_kw_except fr.laposte_address_street_kw_exception[];
    _i INT;
    _descriptor VARCHAR;
BEGIN
    /* RULE

    apply taking account position!

    search for keyword
    if found
        if as_descriptor is default, apply exceptions to eventually alter descriptor
            if one exception is valid (following word(s)) then return as_except
        else
            if as_descriptor is one of as_except, verify exception
                if not return as_default
    else
        no exception
     */

    is_exception := FALSE;

    SELECT
        x.as_except
    INTO
        _descriptor
    FROM
        fr.laposte_address_street_kw_exception x
    WHERE
        x.keyword = words[at_]
        AND
        nwords >= (at_ + count_words(x.followed_by))
        AND
        x.followed_by = items_of_array_to_string(
            elements => words
            , from_ => (at_ +1)
            , to_ => (at_ + count_words(x.followed_by))
        )
        ;

    IF FOUND THEN
        IF _descriptor != as_descriptor THEN
            is_exception := TRUE;
            descriptor := _descriptor;
        END IF;
    ELSE
        SELECT
            as_default
        INTO
            _descriptor
        FROM
            fr.laposte_address_street_kw_exception x
        WHERE
            x.keyword = words[at_]
        LIMIT
            1
        ;
        IF FOUND AND _descriptor != as_descriptor THEN
            is_exception := TRUE;
            descriptor := _descriptor;
        END IF;
    END IF;

     /*
    _kw_except := ARRAY(
        SELECT
            laposte_address_street_kw_exception
        FROM
            fr.laposte_address_street_kw_exception
        WHERE
            keyword = words[at_]
    );
    IF ARRAY_UPPER(_kw_except, 1) IS NULL THEN
        is_exception := FALSE;
        RETURN;
    END IF;

    IF _kw_except[1].as_default = as_descriptor THEN
    ELSE
    END IF;

    FOR _i IN 1 .. ARRAY_UPPER(_kw_except, 1) LOOP
        IF _kw_except[1].as_default = as_descriptor THEN
        END IF;
    END LOOP;
     */
END
$func$ LANGUAGE plpgsql;

/* TEST
-- one word after
SELECT * FROM fr.get_descriptor_from_exception(
    words => '{JETEE,ALBERT,EDOUARD}'::TEXT[]
    , nwords => 3
    , at_ => 2
    , as_descriptor => 'P'
);
-- many words after
SELECT * FROM fr.get_descriptor_from_exception(
    words => '{QUAI,AGENOR,DE,GASPARIN}'::TEXT[]
    , nwords => 4
    , at_ => 2
    , as_descriptor => 'N'
);
 */

 -- get descriptor of street (from full name)
SELECT drop_all_functions_if_exists('fr', 'get_descriptor_of_street');
CREATE OR REPLACE FUNCTION fr.get_descriptor_of_street(
    name IN VARCHAR                   -- name of street
)
RETURNS VARCHAR AS
$func$
DECLARE
    _kw_group VARCHAR;
    _kw VARCHAR;
    _kw_abbreviated VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _descriptors VARCHAR := '';
    _words TEXT[];
    _words_len INT;
    _words_i INT := 0;
    _words_d VARCHAR;
    _words_skip INT := 0;
    _i INT;
    _with_exception BOOLEAN;
    _is_exception BOOLEAN;
    _exception VARCHAR;
BEGIN
    RAISE NOTICE 'name= %', name;

    _words := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _words_len := ARRAY_LENGTH(_words, 1);
    FOR _i IN 1 .. _words_len
    LOOP
        _kw_nwords := 1;
        IF _i < _words_skip THEN
            CONTINUE;
        END IF;

        RAISE NOTICE ' word= %, i=%', _words[_i], _i;

        /* NOTE
        name of street (so descriptor) is ended by: number, reserved or name (CEN)
         */
        IF fr.is_normalized_number(_words[_i]) THEN
            _words_d := CASE
                WHEN _words[_i] = ANY('{D,L}') THEN 'A'
                WHEN _words[_i] = ANY('{C,M}') THEN 'N'
                WHEN _words[_i] ~ '^[DLM]I$' THEN 'N'
                ELSE 'C'
                END
                ;
        ELSE
            _words_d := 'N';
            IF _i < _words_len THEN
                _with_exception := FALSE;
                IF fr.is_normalized_article(_words[_i]) THEN
                    /* NOTE
                    see WIKIPEDIA, not a name!
                    https://fr.wikipedia.org/wiki/Particule_(onomastique)#:~:text=La%20particule%20est%20une%20pr%C3%A9position,du%20%C2%BB%20ou%20%C2%AB%20des%20%C2%BB.

                    DE GAULLE
                    if preceded by title then N         VATNN   PLACE DU GENERAL DE GAULLE
                    if preceded by firstname then A     VPAN    PLACE CHARLES DE GAULLE

                    counter examples
                        IMPASSE DU GENERAL DE GAULLE            VATNN
                        IMPASSE GENERAL DE GAULLE               VTAN
                        QUAI DU GENERAL CHARLES DE GAULLE       VATPNN
                        ALLEE GENERAL CHARLES DE GAULLE         VTPAN

                    counter examples
                        IMPASSE HONORE DE BALZAC
                        RUE ANGELIQUE DU COUDRAY
                        RUE HECTOR DE CORLAY
                    not article (D, L), but lastname
                        RUE ARSENE D ARSONVAL

                    always article!
                     */

                    -- exception for A# (as highway)
                    IF _words[_i] = 'A' AND fr.is_normalized_number(
                        word => _words[_i +1]
                        , only_digit => 'ARABIC'
                    ) THEN
                        _words_d := 'N';
                    ELSE
                        _words_d := 'A';
                    END IF;
                ELSE
                    SELECT kw_group, kw, kw_is_abbreviated, kw_nwords
                    INTO _kw_group, _kw, _kw_is_abbreviated, _kw_nwords
                    FROM fr.get_keyword_of_street(
                        name => name
                        , at_ => _i
                        , words => _words
                        , groups => CASE WHEN _i = 1 THEN ARRAY['TYPE','TITLE','EXT']::VARCHAR[]
                                    ELSE ARRAY['TITLE','EXT','TYPE']::VARCHAR[]
                                    END
                        , with_abbreviation => FALSE
                    );

                    IF _i = 1 AND _kw_group = 'TYPE' AND _kw IS NOT NULL THEN
                        _words_d := REPEAT('V', _kw_nwords);
                    ELSIF _kw IS NOT NULL THEN
                        _words_d := REPEAT('T', _kw_nwords);
                    ELSE
                        IF fr.is_normalized_firstname(_words[_i]) THEN
                            _words_d := 'P';
                        END IF;
                        --END IF;
                    END IF;
                END IF;

                /* RULE
                (firstname|title) followed by a number only (at the end) is a name
                 */
                IF _words_d ~ 'P|T' THEN
                    IF fr.is_normalized_number(_words[_i +1]) AND _words_len = (_i +1) THEN
                        _words_d := REPEAT('N', LENGTH(_words_d));
                    ELSE
                        _with_exception := TRUE;
                    END IF;
                END IF;

                IF _with_exception THEN
                    SELECT is_exception, descriptor
                    INTO _is_exception, _exception
                    FROM fr.get_descriptor_from_exception(
                        words => _words
                        , nwords => _words_len
                        , at_ => _i
                        , as_descriptor => SUBSTR(_words_d, 1, 1)
                    );
                    IF _is_exception THEN
                        _words_d := REPEAT(_exception, LENGTH(_words_d));
                    END IF;
                END IF;
            ELSIF fr.is_normalized_reserved_word(_words[_i]) THEN
                _words_d := 'E';
            END IF;
        END IF;

        _descriptors := CONCAT(_descriptors, _words_d);
        _words_skip := _i;
        IF _kw_nwords > 1 THEN
            _words_skip := _words_skip + _kw_nwords;
        END IF;
    END LOOP;

    -- fix others
    -- not type only (eventually followed by number), but name
    IF _descriptors ~ '^V+C*$' THEN
        _descriptors := REPLACE(_descriptors, 'V', 'N');
    -- not type, but name
    ELSIF _descriptors ~ '^V+N*$' THEN
        IF EXISTS(
            SELECT 1 FROM fr.laposte_address_street_keyword k
            WHERE k.group = 'TYPE'
            AND k.name = get_descriptor_of_street.name
        ) THEN
            _descriptors := REPEAT('N', LENGTH(_descriptors));
        END IF;
    --
    ELSIF _descriptors ~ '^V[PT]C$' THEN
        _descriptors := REGEXP_REPLACE(_descriptors, '^V[PT]C$', 'VNC');
    END IF;

    RAISE NOTICE 'descriptor= %', _descriptors;
    RETURN _descriptors;
END
$func$ LANGUAGE plpgsql;

/* TEST

-- difference from RAN !
SELECT
    descriptor_pow
    , descriptor_laposte
    , code
    , name
FROM (
    SELECT
        fr.get_descriptor_of_street(lb_voie) AS descriptor_pow
        , lb_desc AS descriptor_laposte
        , co_cea code
        , lb_voie name
    FROM
        fr.laposte_address_street
    WHERE
        fl_active
    LIMIT
        1000
    ) t
WHERE
    descriptor_pow IS DISTINCT FROM descriptor_laposte
    ;
 */
