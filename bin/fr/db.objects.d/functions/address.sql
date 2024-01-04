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
    _words TEXT[] := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _kw VARCHAR;
    _kw_more VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _descriptor VARCHAR;
    _descriptor_len INT := LENGTH(descriptor);
    _descriptor_tmp VARCHAR;
    _descriptor_prev VARCHAR := 'Z';
    _descriptor_word VARCHAR := NULL;
    _descriptor_same VARCHAR := NULL;
    _descriptor_remainder VARCHAR;
    _descriptor_from INT;
    _descriptor_type VARCHAR;
    _descriptor_title VARCHAR;
    _descriptor_others VARCHAR;
    _descriptor_next VARCHAR;
    _descriptor_wo_a VARCHAR;
    _descriptor_only_a VARCHAR;
    _i INT;
    _j INT;
    _k INT;
    _n INT;
    _offset INT := 1;
    _last BOOLEAN := FALSE;
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
        _descriptor := SUBSTR(descriptor, _k, 1);
        RAISE NOTICE '(i=% ofs=%): d=% (pd=%), w=%', _i, _offset, _descriptor, _descriptor_prev, _words[_i];

        -- deleted article (or not an article!)
        IF (is_normalized
                AND _n < _descriptor_len
                AND _descriptor = 'A') THEN
            -- this is not an article
            _descriptor_remainder := SUBSTR(descriptor, _k +1);
            _descriptor_only_a := REGEXP_REPLACE(_descriptor_remainder, '[^A]', '', 'gi');
            IF (LENGTH(_descriptor_remainder) - LENGTH(_descriptor_only_a)) = (_n - _i +1) THEN
                IF _descriptor_word IS NOT NULL AND (
                    (split_only IS NULL)
                    OR
                    (_descriptor_same ~ split_only)
                ) THEN
                    words := ARRAY_APPEND(words, _descriptor_word);
                    descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                END IF;
                _descriptor_word := _words[_i];
                _descriptor_same := SUBSTR(descriptor, _k +1, 1);
                _descriptor_prev := _descriptor_same;
                _offset := _offset +1;
            -- current word not an article
            ELSIF (NOT fr.is_normalized_article(_words[_i])) THEN
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
                        EXIT;
                    END IF;
                END LOOP;
            END IF;
            CONTINUE;
        -- abbreviated type
        ELSIF (is_normalized
                AND _n < _descriptor_len
                AND _descriptor = 'V'
                AND _i = 1) THEN
            SELECT kw, kw_is_abbreviated, kw_nwords
            INTO _kw, _kw_is_abbreviated, _kw_nwords
            FROM fr.get_type_of_street(
                name => name
            )
            AS (kw_group VARCHAR, kw VARCHAR, kw_abbreviated VARCHAR, kw_is_abbreviated BOOLEAN, kw_nwords INT);
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
                _descriptor_same := REPEAT('V', _offset);
                CONTINUE;
            END IF;
        -- abbreviated or deleted title
        ELSIF (is_normalized
                AND _n < _descriptor_len
                AND _descriptor = 'T') THEN
            _descriptor_from := CASE WHEN _descriptor_prev = 'T' THEN _k -1 ELSE _k END;
            _descriptor_remainder := SUBSTR(descriptor, _descriptor_from);
            _descriptor_title := (REGEXP_MATCHES(_descriptor_remainder, '(T+)([^T])'))[1];
            -- title w/ many words, but remains one only
            IF LENGTH(_descriptor_title) > 1 THEN
                _descriptor_others := (REGEXP_MATCHES(_descriptor_remainder, '(T+)(.*)$'))[2];
                _descriptor_wo_a := REGEXP_REPLACE(_descriptor_others, '[A]', '', 'gi');
                _descriptor_only_a := REGEXP_REPLACE(_descriptor_others, '[^A]', '', 'gi');
                _descriptor_next := SUBSTR(descriptor, (_descriptor_from + LENGTH(_descriptor_title)), 1);

                IF  (
                        -- remains others than article less or equal to articles
                        (((_n - _i) - LENGTH(_descriptor_wo_a)) <= LENGTH(_descriptor_only_a))
                    )
                    OR
                    (
                        ((_descriptor_next = 'A' AND fr.is_normalized_article(_words[_i +1]))
                        OR
                        (_descriptor_next = 'C' AND fr.is_normalized_number(_words[_i +1]))
                        OR
                        (_descriptor_next = 'N' AND ((_i +1) = _n))
                        OR
                        (_descriptor_next = 'P' AND fr.is_normalized_firstname(_words[_i +1])))
                    )
                    THEN
                    IF _descriptor_word IS NOT NULL AND (
                        (split_only IS NULL)
                        OR
                        (_descriptor_same ~ split_only)
                    ) THEN
                        words := ARRAY_APPEND(words, _descriptor_word);
                        IF _descriptor_prev = 'T' THEN
                            descriptors := ARRAY_APPEND(descriptors, REPEAT('T', LENGTH(_descriptor_title)));
                        ELSE
                            descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                        END IF;
                    END IF;
                    _descriptor_word := _words[_i];
                    IF _descriptor_prev = 'T' THEN
                        FOR _j IN (_k + 1) .. _descriptor_len
                        LOOP
                            _descriptor_tmp := SUBSTR(descriptor, _j, 1);
                            IF NOT (_descriptor_tmp = 'A' AND NOT fr.is_normalized_article(_words[_i ])) THEN
                                EXIT;
                            END IF;
                        END LOOP;
                        _descriptor_same := _descriptor_tmp;
                    ELSE
                        _descriptor_same := REPEAT('T', LENGTH(_descriptor_title));
                    END IF;

                    _offset := _offset + LENGTH(_descriptor_title);
                    IF _descriptor_prev != 'T' THEN
                        _offset := _offset -1;
                    END IF;
                    _descriptor_prev := _descriptor;
                    CONTINUE;
                END IF;
            END IF;
        END IF;

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
            IF _i = _n AND (
                (split_only IS NULL)
                OR
                (_descriptor_same ~ split_only)
            ) THEN
                words := ARRAY_APPEND(words, _descriptor_word);
                descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
                _last := TRUE;
            END IF;
        END IF;
        _descriptor_prev := _descriptor;
    END LOOP;
    IF NOT _last AND (
        (split_only IS NULL)
        OR
        (_descriptor_same ~ split_only)
    ) THEN
        words := ARRAY_APPEND(words, _descriptor_word);
        descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
    END IF;
END
$func$ LANGUAGE plpgsql;

-- get type of street (from full name)
SELECT drop_all_functions_if_exists('fr', 'get_type_of_street');
CREATE OR REPLACE FUNCTION fr.get_type_of_street(
    name IN VARCHAR                   -- name of street
)
RETURNS RECORD AS
$func$
BEGIN
    RETURN fr.get_keyword_of_street(
        name => name
        , group_ => 'TYPE'
    );
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM fr.get_type_of_street('CHEMIN DES SANSONNIERES LE VIEIL BAUGE')
    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
SELECT * FROM fr.get_type_of_street('LD KER FRANCOIS BONEN')
    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
SELECT * FROM fr.get_type_of_street('LE PONT D OIR MONTGOTHIER')
    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
SELECT * FROM fr.get_type_of_street('ZONE D AMENAGEMENT CONCERTE DES GRANDS CHAMPS')
    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
SELECT * FROM fr.get_type_of_street('ZA DES GRANDS CHAMPS')
    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
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
    _descriptor VARCHAR := '';
    _words TEXT[];
    _words_len INT;
    _words_i INT := 0;
    _words_d VARCHAR;
    _words_skip INT := 0;
    _i INT;
    _not_a_if_n TEXT[] := '{AU,AUX,EN,LA,LE,LES,SUR}'::TEXT[];
    _found BOOLEAN;
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

        IF fr.is_normalized_number(_words[_i]) THEN
            IF _i > 1 AND RIGHT(_descriptor, 1) = ANY('{A,V}') AND _words[_i] = ANY('{D,L}') THEN
                _words_d := 'A';
            ELSE
                _words_d := 'C';
            END IF;
        ELSIF fr.is_normalized_article(_words[_i]) THEN
            _words_d := 'A';
        ELSE
            _words_d := 'N';
            IF _i < _words_len THEN
                SELECT kw_group, kw, kw_is_abbreviated, kw_nwords
                INTO _kw_group, _kw, _kw_is_abbreviated, _kw_nwords
                FROM fr.get_keyword_of_street(
                    name => name
                    , at_ => _i
                    , words => _words
                    , groups => CASE WHEN _i = 1 THEN ARRAY['TYPE','TITLE','EXT']::VARCHAR[]
                                ELSE ARRAY['TITLE','EXT','TYPE']::VARCHAR[]
                                END
                )
                AS (kw_group VARCHAR, kw VARCHAR, kw_abbreviated VARCHAR, kw_is_abbreviated BOOLEAN, kw_nwords INT);

                IF _i = 1 AND _kw_group = 'TYPE' AND _kw IS NOT NULL THEN
                    _words_d := REPEAT('V', _kw_nwords);
                ELSIF _kw IS NOT NULL THEN
                    _words_d := REPEAT('T', _kw_nwords);
                ELSE
                    -- not if previous is (article|number)
                    RAISE NOTICE ' last= %', RIGHT(_descriptor, 1);
                    IF _i > 1 AND RIGHT(_descriptor, 1) = ANY('{A,C}') THEN
                        _words_d := 'N';
                    ELSE
                        IF fr.is_normalized_firstname(_words[_i]) THEN
                            _words_d := 'P';
                        END IF;
                    END IF;
                END IF;
            ELSIF fr.is_normalized_reserved_word(_words[_i]) THEN
                _words_d := 'E';
            END IF;
        END IF;

        -- fix bad uses
        -- 'LA METAIRIE D EN HAUT' not roman number D, but article
        IF _i > 1
            AND _words_d = ANY('{A,N}')
            AND LEFT(_words[_i], 1) = ANY('{A,E,I,O,U,Y}')
            AND RIGHT(_descriptor, 1) = 'C'
            AND _words[_i -1] = ANY('{D,L}') THEN
            _descriptor := CONCAT(
                SUBSTR(_descriptor, 1, LENGTH(_descriptor) - 1)
                , 'A'
            );
        END IF;

        _descriptor := CONCAT(_descriptor, _words_d);
        _words_skip := _i;
        IF _kw_nwords > 1 THEN
            _words_skip := _words_skip + _kw_nwords;
        END IF;
    END LOOP;

    -- not type only (eventually followed by number)
    IF _descriptor ~ '^V+C*$' THEN
        _descriptor := REPLACE(_descriptor, 'V', 'N');
    -- not article, but lastname
    ELSIF _descriptor ~ 'PAN' THEN
        _descriptor := REPLACE(_descriptor, 'PAN', 'PNN');
    /*
    -- not article, but name
    ELSIF _descriptor ~ '^AN$' THEN
        IF _words[1] = ANY(_not_a_if_n) THEN
            _descriptor := REPLACE(_descriptor, 'AN', 'NN');
        END IF;
     */

    ELSIF _descriptor ~ '^V[PT]C$' THEN
        _descriptor := REGEXP_REPLACE(_descriptor, '^V[PT]C$', 'VNC');
    END IF;

    RAISE NOTICE 'descriptor= %', _descriptor;
    RETURN _descriptor;
END
$func$ LANGUAGE plpgsql;
