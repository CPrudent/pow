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
SELECT public.drop_all_functions_if_exists('fr', 'get_project_code_from_department_code');
CREATE OR REPLACE FUNCTION fr.get_project_code_from_department_code(
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
        WHEN department_code IN ('971', '972', '977', '978') THEN '2'

        /* Obsolète (LAMBERT II ETENDU)
        WHEN XXXX THEN '3'
         */

        --Guyane française (973XX), région d'outre-mer située sur la côte nord-est de l'Amérique du Sud
        WHEN department_code = '973' THEN '4'

        --Ile de la Réunion (974XX)
        WHEN department_code = '974' THEN '5'

        --Mayotte (976XX et 985XX sur co_adr), archipel de l'océan Indien situé entre Madagascar et la côte du Mozambique
        WHEN department_code IN ('976', '985'/*Ancien code ? les codes adresses RAN commencent par 985*/) THEN '6'

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
SELECT public.drop_all_functions_if_exists('fr', 'get_srid_from_project_code');
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

-- remove article(s) from name
SELECT drop_all_functions_if_exists('fr', 'get_street_name_without_article');
CREATE OR REPLACE FUNCTION fr.get_street_name_without_article(
    words IN TEXT[],
    nwords IN INT,
    descriptors IN VARCHAR DEFAULT NULL,
    without_article OUT TEXT[]
)
AS
$func$
DECLARE
    _i INT;
BEGIN
    FOR _i IN 1 .. nwords
    LOOP
        IF ((
                descriptors IS NOT NULL
                AND
                SUBSTR(descriptors, _i, 1) = 'A'
            )
            OR
            (
                fr.is_normalized_article(words[_i])
            )
        ) THEN
            CONTINUE;
        ELSE
            without_article := ARRAY_APPEND(without_article, words[_i]);
        END IF;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

-- split descriptors as array
/* NOTE
words here must be the result of fr.get_descriptors_of_street(), splited w/ descriptors
 */
SELECT drop_all_functions_if_exists('fr', 'split_descriptors_as_array');
CREATE OR REPLACE FUNCTION fr.split_descriptors_as_array(
    descriptors IN VARCHAR,
    words IN TEXT[],
    nwords IN INT,
    as_array OUT TEXT[]
)
AS
$func$
DECLARE
    _descriptors VARCHAR;
    _nwords INT;
    _i INT;
    _j INT := 1;
BEGIN
    FOR _i IN 1 .. nwords
    LOOP
        _nwords := count_words(words[_i]);
        _descriptors := SUBSTR(descriptors, _j, _nwords);
        as_array := ARRAY_APPEND(as_array, _descriptors);
        _j := _j + _nwords;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT
    -- VVATPAN
    ds.descriptors,
    -- {"ANCIENNE ROUTE", DE, SAINT, LAURENT, DES, ARBRES}
    ds.words_by_descriptor,
    -- {"ANCI ROUTE", NULL, ST}
    ds.words_abbreviated_by_descriptor,
    -- {-, NULL, -}
    ds.words_todo_by_descriptor
FROM
    fr.get_descriptors_of_street(
        name => 'ANCIENNE ROUTE DE SAINT LAURENT DES ARBRES',
        with_abbreviation => TRUE
    ) ds;

SELECT fr.split_descriptors_as_array(
    descriptors => 'VVATPAN',
    words => '{"ANCIENNE ROUTE", DE, SAINT, LAURENT, DES, ARBRES}',
    nwords => 6
) => {VV, A, T, P, A, N}
 */

-- get property item (from as_words array)
SELECT drop_all_functions_if_exists('fr', 'get_property_ordinal_item');
CREATE OR REPLACE FUNCTION fr.get_property_ordinal_item(
    property_key IN VARCHAR,
    property_value IN VARCHAR,
    as_words IN INT[],
    ordinal IN INT,
    ordinal_as_words IN INT DEFAULT 1,    -- useful for type of street (1st item)
    property_ordinal_item OUT VARCHAR
)
AS
$func$
BEGIN
    property_ordinal_item := CASE
        WHEN property_key = 'DESCRIPTORS' THEN
            SUBSTR(property_value, ordinal_as_words, as_words[ordinal])
        WHEN property_key = 'NAME' THEN
            extract_words(
                str => property_value,
                n => as_words[ordinal],
                from_ => ordinal_as_words
            )
        END
    ;
END
$func$ LANGUAGE plpgsql;

-- split property as words
SELECT drop_all_functions_if_exists('fr', 'split_property_as_words');
CREATE OR REPLACE FUNCTION fr.split_property_as_words(
    property_key IN VARCHAR,
    property_value IN VARCHAR,
    as_words IN INT[],
    property_as_words OUT TEXT[]
)
AS
$func$
DECLARE
    _nwords INT := ARRAY_LENGTH(as_words, 1);
    _item VARCHAR;
    _i INT;
    _j INT := 1;
BEGIN
    FOR _i IN 1 .. _nwords
    LOOP
        _item := fr.get_property_ordinal_item(
            property_key => property_key,
            property_value => property_value,
            ordinal => _i,
            ordinal_as_words => _j
        );
        property_as_words := ARRAY_APPEND(property_as_words, _item);
        _j := _j + as_words[_i];
    END LOOP;
END
$func$ LANGUAGE plpgsql;

-- number of word(s) from name (STREET or COMPLEMENT) w/o article
SELECT drop_all_functions_if_exists('fr', 'get_nwords_wo_article');
CREATE OR REPLACE FUNCTION fr.get_nwords_wo_article(
    nwords IN INT,
    descriptors_as_words IN TEXT[],
    nwords_xa OUT INT
)
AS
$func$
BEGIN
    nwords_xa := nwords - COALESCE(ARRAY_LENGTH(ARRAY_POSITIONS(descriptors_as_words, 'A'), 1), 0);
END
$func$ LANGUAGE plpgsql;

-- define as_words array from splitted value (name or descriptors)
/* TODO neither splitted nor splited, but split!
 */
SELECT drop_all_functions_if_exists('fr', 'get_as_words_from_splited_value');
CREATE OR REPLACE FUNCTION fr.get_as_words_from_splited_value(
    property_as_words IN TEXT[],
    as_words OUT INT[]
)
AS
$func$
DECLARE
    _nwords INT := ARRAY_LENGTH(property_as_words, 1);
    _i INT;
BEGIN
    FOR _i IN 1 .. _nwords
    LOOP
        as_words := ARRAY_APPEND(as_words, count_words(property_as_words[_i]));
    END LOOP;
END
$func$ LANGUAGE plpgsql;

-- get type of street (from full name)
SELECT drop_all_functions_if_exists('fr', 'get_type_of_street');
CREATE OR REPLACE FUNCTION fr.get_type_of_street(
    name IN VARCHAR,                  -- name of street
    words IN TEXT[] DEFAULT NULL,
    with_abbreviation IN BOOLEAN DEFAULT FALSE,
    raise_notice IN BOOLEAN DEFAULT FALSE,
    kw_group OUT VARCHAR,
    kw OUT VARCHAR,
    kw_abbreviated OUT VARCHAR,
    kw_is_abbreviated OUT BOOLEAN,
    kw_nwords OUT INT
)
AS
$func$
BEGIN
    SELECT ks.kw_group, ks.kw, ks.kw_abbreviated, ks.kw_is_abbreviated, ks.kw_nwords
    INTO
        get_type_of_street.kw_group,
        get_type_of_street.kw,
        get_type_of_street.kw_abbreviated,
        get_type_of_street.kw_is_abbreviated,
        get_type_of_street.kw_nwords
    FROM fr.get_keyword_from_name(
        name => name,
        words => words,
        groups => 'TYPE',
        with_abbreviation => with_abbreviation,
        raise_notice => raise_notice
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
    words IN TEXT[],
    nwords IN INT,
    at_ IN INT,
    as_descriptor IN VARCHAR,
    is_exception OUT BOOLEAN,
    descriptor OUT VARCHAR
)
AS
$func$
DECLARE
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
            elements => words,
            from_ => (at_ +1),
            to_ => (at_ + count_words(x.followed_by))
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
END
$func$ LANGUAGE plpgsql;

/* TEST
-- one word after
-- VNN
SELECT * FROM fr.get_descriptor_from_exception(
    words => '{JETEE, ALBERT, EDOUARD}'::TEXT[],
    nwords => 3,
    at_ => 2,
    as_descriptor => 'P'
) => N
-- many words after
-- VNAN
SELECT * FROM fr.get_descriptor_from_exception(
    words => '{QUAI, AGENOR, DE, GASPARIN}'::TEXT[],
    nwords => 4,
    at_ => 2,
    as_descriptor => 'N'
) => N
 */

 -- get descriptors of street (from full name)
/* NOTE
see WIKIPEDIA, not a name!
https://fr.wikipedia.org/wiki/Particule_(onomastique)#:~:text=La%20particule%20est%20une%20pr%C3%A9position, du%20%C2%BB%20ou%20%C2%AB%20des%20%C2%BB.

DE GAULLE:
if preceded by title then N         VATNN   PLACE DU GENERAL DE GAULLE
if preceded by firstname then A     VPAN    PLACE CHARLES DE GAULLE

counter examples
    IMPASSE DU GENERAL DE GAULLE            VATNN
    IMPASSE GENERAL DE GAULLE               VTAN
    QUAI DU GENERAL CHARLES DE GAULLE       VATPNN
    ALLEE GENERAL CHARLES DE GAULLE         VTPAN

and other cases:
counter examples
    IMPASSE HONORE DE BALZAC
    RUE ANGELIQUE DU COUDRAY
    RUE HECTOR DE CORLAY

not article (D, L), but lastname
    RUE ARSENE D ARSONVAL

=> always article
 */

/* NOTE
article as 1st word

A: 660 A, 1 N
AU: 545 A, 2 N
AUX: 239 A, 2 N
D: 12 A, 5 N
DE: 35 A
DES: 8 A
DU: 15 A
EN: 947 A
ET: NULL
L: 4059 A, 1 P
LA: 40814 A, 2 P, 2 N
LE: 34358 A, 2 N
LES: 20044 A, 8 P, 1 N
SOUS: 242 A, 1 N
SUR: 115 A, 64 N
UN: NULL
UNE: 1 A

=> always article
 */

/* NOTE
too bad! 11N (name) / 10E (reserved) when nwords=2
and sometimes N & E for same
    VN: CORNICHE INFERIEURE
    NE: CORNICHE SUPERIEURE
=> always E
 */

/*
SELECT drop_all_functions_if_exists('fr', 'get_descriptors_of_street');
CREATE OR REPLACE FUNCTION fr.get_descriptors_of_street(
    name IN VARCHAR,                  -- name of street
    with_abbreviation IN BOOLEAN DEFAULT FALSE,
    raise_notice IN BOOLEAN DEFAULT FALSE,
    descriptors OUT VARCHAR,
    words_by_descriptor OUT TEXT[],
    words_abbreviated_by_descriptor OUT TEXT[],
    words_todo_by_descriptor OUT TEXT[],
    as_words OUT INT[]
)
AS
$func$
DECLARE
    _kw_group VARCHAR;
    _kw VARCHAR;
    _kw_abbreviated VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _descriptors_tmp VARCHAR;
    _descriptors_c VARCHAR;
    _descriptors_t VARCHAR;
    _descriptors_v VARCHAR;
    _words TEXT[];
    _words_len INT;
    _words_d VARCHAR;
    _words_skip INT := 0;
    _i INT;
    _len_c INT;
    _last_t INT;
    _with_exception BOOLEAN;
    _is_exception BOOLEAN;
    _exception VARCHAR;
    _init_by_descriptor BOOLEAN;
    _abbr_e VARCHAR;
BEGIN
    IF raise_notice THEN RAISE NOTICE 'name="%"', name; END IF;

    IF name ~ '^ +' OR name ~ ' +$' THEN
        RAISE NOTICE 'libellé Voie erronée (%) avec espace(s) superflus!', name;
        name := TRIM(name);
    END IF;

    _words := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _words_len := ARRAY_LENGTH(_words, 1);

    FOR _i IN 1 .. _words_len
    LOOP
        _kw_nwords := 1;
        _init_by_descriptor := FALSE;
        IF _i < _words_skip THEN
            CONTINUE;
        END IF;

        IF raise_notice THEN RAISE NOTICE ' word=%, i=%', _words[_i], _i; END IF;

        -- number
        IF fr.is_normalized_number(_words[_i])
            AND NOT fr.is_normalized_article(_words[_i]) THEN
            _words_d := CASE
                --WHEN _words[_i] = ANY('{D, L}') THEN 'A'
                WHEN _words[_i] = ANY('{C, M}') THEN 'N'
                -- exceptions: DI, LI, MI, CD, CL, ...
                WHEN fr.get_default_of_street_word(_words[_i]) != 'C' THEN
                    CASE
                    WHEN _i < _words_len THEN fr.get_default_of_street_word(_words[_i])
                    ELSE 'N'
                    END
                ELSE 'C'
                END
                ;
        ELSE
            _words_d := 'N';
            IF _i < _words_len THEN
                _with_exception := FALSE;

                -- keyword (title, type, extension or name)
                SELECT kw_group, kw, kw_abbreviated, kw_is_abbreviated, kw_nwords
                INTO _kw_group, _kw, _kw_abbreviated, _kw_is_abbreviated, _kw_nwords
                FROM fr.get_keyword_from_name(
                    name => name,
                    at_ => _i,
                    words => _words,
                    groups => 'STREET',
                    with_abbreviation => with_abbreviation
                );
                IF _kw IS NOT NULL THEN
                    _words_d := REPEAT(
                        CASE
                        -- up to last word, as name or name (w/ abbreviation)
                        WHEN ((_i + _kw_nwords -1) = _words_len) OR (_kw_group = 'NAME') THEN 'N'
                        -- type
                        WHEN _i = 1 AND _kw_group = 'TYPE' THEN 'V'
                        -- title
                        ELSE 'T'
                        END,
                        _kw_nwords
                    );
                    words_by_descriptor := ARRAY_APPEND(words_by_descriptor, _kw);
                    words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := _kw_abbreviated;
                    words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := CASE
                        WHEN _kw_is_abbreviated THEN '+'
                        ELSE '-'
                        END
                    ;
                    as_words := ARRAY_APPEND(as_words, _kw_nwords);
                    _init_by_descriptor := TRUE;
                ELSE
                    -- firstname
                    IF fr.is_normalized_firstname(_words[_i]) THEN
                        _words_d := 'P';
                        IF _i > 1
                            AND fr.is_normalized_article(_words[_i -1])
                            AND NOT (
                                (_words[_i -1] = ANY('{DE, ET}'))
                                OR
                                (_words[_i -1] = 'D' AND _words[_i] ~ '^[AEIOUY]')
                            ) THEN
                            _words_d := 'N';
                        END IF;
                    -- article
                    ELSIF fr.is_normalized_article(_words[_i]) THEN
                        /* RULE
                        exception for road as (A|D|N)# : highway, departmental, national
                        at end of name only, else counter examples
                            LA ROCHE A 7 HEURES
                            LA PLANCHE A 4 PIEDS
                        */
                        IF _words[_i] ~ '^A|D|N$' AND fr.is_normalized_number(
                            word => _words[_i +1],
                            only_digit => 'ARABIC'
                        ) AND _words_len = (_i +1) THEN
                            _words_d := 'N';
                        ELSE
                            _words_d := 'A';
                            _with_exception := TRUE;
                        END IF;
                    END IF;
                END IF;

                /* RULE
                (firstname|title) followed by a number only (at the end) is a name
                */
                IF _words_d ~ 'P|T' THEN
                    IF _words_d ~ 'P'
                        AND _words_len = (_i +1)
                        AND fr.is_normalized_number(_words[_i +1]) THEN
                        _words_d := REPEAT('N', LENGTH(_words_d));
                    ELSE
                        _with_exception := TRUE;
                    END IF;
                END IF;

                -- apply exception(s) if exist
                IF _with_exception AND LENGTH(_words_d) = 1 THEN
                    SELECT is_exception, descriptor
                    INTO _is_exception, _exception
                    FROM fr.get_descriptor_from_exception(
                        words => _words,
                        nwords => _words_len,
                        at_ => _i,
                        as_descriptor => SUBSTR(_words_d, 1, 1)
                    );
                    IF _is_exception THEN
                        IF ((_words_d ~ 'T')
                            AND
                            (words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] = '+')
                        ) THEN
                            -- reset
                            words_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)];
                            words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := NULL;
                            words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := NULL;
                        END IF;
                        _words_d := REPEAT(_exception, LENGTH(_words_d));
                    END IF;
                END IF;
            ELSIF fr.is_normalized_reserved_word(_words[_i]) THEN
                _words_d := 'E';
                SELECT kw_abbreviated
                INTO _abbr_e
                FROM fr.get_keyword_from_name(
                    name => name,
                    at_ => _i,
                    words => _words,
                    with_abbreviation => FALSE
                );
            END IF;
        END IF;

        IF NOT _init_by_descriptor THEN
            words_by_descriptor := ARRAY_APPEND(words_by_descriptor, _words[_i]);
            as_words := ARRAY_APPEND(as_words, 1);
        END IF;
        IF (_words_d = 'E' AND _abbr_e IS NOT NULL) THEN
            words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := _abbr_e;
            words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := '+';
        END IF;
        descriptors := CONCAT(descriptors, _words_d);
        _words_skip := _i;
        IF _kw_nwords > 1 THEN
            _words_skip := _words_skip + _kw_nwords;
        END IF;
    END LOOP;

    -- fix bad uses
    -- name of street (so descriptor) is ended by: number, reserved or name (CEN)
    IF descriptors !~ '[CEN]$' THEN
        descriptors := REGEXP_REPLACE(descriptors, '.$', 'N');

    /*
    -- nothing else than CN before last E (specially not title)
    ELSIF descriptors ~ '[^CN]E$' THEN
        descriptors := REGEXP_REPLACE(descriptors, '.E$', 'NE');
     */

    -- not title only (eventually followed by number), but name
    -- IMPASSE DU PASSAGE A NIVEAU 7, VANNNC
    -- and also successive V+ and T+
    -- PASSAGE A NIVEAU PASSAGE A NIVEAU 67, VVVNNNC
    ELSIF descriptors ~ 'T+C*$' THEN
        _descriptors_t := (REGEXP_MATCHES(descriptors, '(T+)(C*)$'))[1];
        _descriptors_c := (REGEXP_MATCHES(descriptors, '(T+)(C*)$'))[2];
        -- last title is one|many word(s)
        _len_c := LENGTH(COALESCE(_descriptors_c, ''));
        _last_t := ARRAY_LENGTH(words_by_descriptor, 1) - _len_c;
        IF raise_notice THEN
            RAISE NOTICE ' dt=%, dc=% #c=%', _descriptors_t, _descriptors_c, _len_c;
            RAISE NOTICE ' wbd=%, lt=%', words_by_descriptor, _last_t;
            RAISE NOTICE ' descriptors=%', descriptors;
        END IF;
        -- replace all T
        IF count_words(words_by_descriptor[_last_t]) = LENGTH(_descriptors_t) THEN
            IF raise_notice THEN RAISE NOTICE ' replace all'; END IF;
            descriptors := REGEXP_REPLACE(descriptors,
                'T+(C*)$',
                CONCAT(
                    REPEAT('N', LENGTH(_descriptors_t)),
                    '\1'
                )
            );
        -- replace only last T
        ELSE
            _last_t := LENGTH(descriptors) - _len_c;
            IF raise_notice THEN RAISE NOTICE ' replace last only, lt=%', _last_t; END IF;
            IF _last_t > 1 THEN
                descriptors := SUBSTR(descriptors, 1, _last_t -1);
            ELSE
                descriptors := NULL;
            END IF;
            descriptors := CONCAT(descriptors, 'N');
            IF _len_c > 0 THEN
                descriptors := CONCAT(descriptors, _descriptors_c);
            END IF;
        END IF;
    -- not type only (eventually followed by number, reserved), but name
    -- PASSAGE A NIVEAU 7, NNNC
    -- GRANDE RUE PROLONGEE
    ELSIF descriptors ~ 'V+[CE]*$' THEN
        _descriptors_v := (REGEXP_MATCHES(descriptors, '(V+)[CE]*$'))[1];
        descriptors := REGEXP_REPLACE(descriptors,
            'V+([CE]*)$',
            CONCAT(
                REPEAT('N', LENGTH(_descriptors_v)),
                '\1'
            )
        );
    /* neither firstname nor title
    VNC, AVENUE ALBERT 1ER
    VNCE, RUE ALBERT 1ER PROLONGEE
    VPCAN, AVENUE ALBERT 1ER DE BELGIQUE
     */
    ELSIF descriptors ~ '^V[PT]CE?$' THEN
        descriptors := REGEXP_REPLACE(descriptors, '(^V[PT]C)(E?)$', 'VNC\2');
    END IF;

    IF raise_notice THEN RAISE NOTICE ' descriptors=%', descriptors; END IF;
END
$func$ LANGUAGE plpgsql;
 */

/* TEST

-- difference from RAN !
SELECT
    descriptors_pow,
    descriptors_laposte,
    code,
    name
FROM (
    SELECT
        ds.descriptors descriptors_pow,
        lb_desc descriptors_laposte,
        co_cea code,
        lb_voie name
    FROM
        fr.laposte_address_street
            CROSS JOIN fr.get_descriptors_of_street(lb_voie) ds
    WHERE
        fl_active
    LIMIT
        1000
    ) t
WHERE
    descriptors_pow IS DISTINCT FROM descriptors_laposte
    ;
 */

-- fix address faults from list (manual corrections)
SELECT drop_all_functions_if_exists('fr', 'get_query_to_fix_from_manual_correction');
CREATE OR REPLACE FUNCTION fr.get_query_to_fix_from_manual_correction(
    element IN VARCHAR,
    fault IN VARCHAR,
    query_fix OUT TEXT
)
AS
$func$
DECLARE
    _exists BOOLEAN;
    _nrows INT;
BEGIN
    _exists := table_exists(
        schema_name => 'fr',
        table_name => 'laposte_address_fault_correction'
    );
    IF _exists THEN
        _nrows := (
            SELECT COUNT(*) FROM fr.laposte_address_fault_correction mc
            WHERE
                mc.element = get_query_to_fix_from_manual_correction.element
                AND
                mc.fault_key = fault
        );
    END IF;
    IF NOT _exists OR _nrows = 0 THEN
        RAISE 'Données de corrections manquantes (%)', fault;
    END IF;

    query_fix := CONCAT('
        UPDATE fr.', fr.get_table_name(element, 'UNIQ') , ' u SET
            name = mc.name_fixed
            FROM fr.laposte_address_fault_correction mc
            WHERE
                mc.element = ', quote_literal(element), '
                AND
                u.name = mc.name
                AND
                mc.fault_key = ', quote_literal(fault)
    );
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_table_name');
CREATE OR REPLACE FUNCTION fr.get_table_name(
    element IN VARCHAR,
    usecase IN VARCHAR,
    table_name OUT VARCHAR
)
AS
$func$
BEGIN
    table_name := CASE
        WHEN UPPER(usecase) ~ 'UNIQ|MEMBERSHIP|REFERENCE' THEN
            FORMAT('laposte_address_%s_%s',
                LOWER(element),
                LOWER(usecase)
            )
        WHEN UPPER(usecase) ~ 'ADDRESS' THEN
            FORMAT('laposte_address_%s',
                LOWER(element)
            )
        END
    ;
END
$func$ LANGUAGE plpgsql;

-- level-address subscript
SELECT drop_all_functions_if_exists('fr', 'get_subscript_of_level_address');
CREATE OR REPLACE FUNCTION fr.get_subscript_of_level_address(
    level IN VARCHAR,
    subscript OUT INT
)
AS
$func$
BEGIN
    subscript := CASE UPPER(level)
        WHEN 'AREA' THEN 1
        WHEN 'STREET' THEN 2
        WHEN 'HOUSENUMBER' THEN 3
        WHEN 'COMPLEMENT' THEN 4
        END
    ;
END
$func$ LANGUAGE plpgsql;

-- complement-descriptors subscript of group : [G1, G2, G3]
SELECT drop_all_functions_if_exists('fr', 'get_subscript_of_descriptor');
CREATE OR REPLACE FUNCTION fr.get_subscript_of_descriptor(
    descriptor IN VARCHAR,
    subscript OUT INT
)
AS
$func$
BEGIN
    -- I=>1, H=>2, G=>3
    subscript := ABS(ASCII(descriptor) - ASCII('I')) +1;
END
$func$ LANGUAGE plpgsql;

-- complement-group (keyword) from associated descriptor
SELECT drop_all_functions_if_exists('fr', 'get_group_of_descriptor');
CREATE OR REPLACE FUNCTION fr.get_group_of_descriptor(
    descriptor IN VARCHAR,
    group_ OUT VARCHAR
)
AS
$func$
BEGIN
    -- I=>GROUP1, H=>GROUP2, G=>GROUP3
    group_ := CASE descriptor
        WHEN 'G' THEN 'GROUP3'
        WHEN 'H' THEN 'GROUP2'
        WHEN 'I' THEN 'GROUP1'
        END
    ;
END
$func$ LANGUAGE plpgsql;

-- complement-descriptor for group (of a word)
SELECT drop_all_functions_if_exists('fr', 'get_descriptor_of_group');
CREATE OR REPLACE FUNCTION fr.get_descriptor_of_group(
    group_ IN VARCHAR,
    descriptors IN VARCHAR[],
    raise_notice IN BOOLEAN DEFAULT FALSE,
    descriptor OUT VARCHAR
)
AS
$func$
DECLARE
    _descriptor VARCHAR := CASE group_
        WHEN 'GROUP3' THEN 'G'
        WHEN 'GROUP2' THEN 'H'
        WHEN 'GROUP1' THEN 'I'
        END
    ;
    _i INT;
    _higher INT;
BEGIN
    FOR _i IN REVERSE 3 .. 1
    LOOP
        /* NOTE
        descriptors array contain in progess building descriptors
        of each group, in ascending order [G1, G2, G3]
        only first keyword (in each group) is affected by its descriptor,
        else other(s) are type-descriptor
         */
        IF descriptors[_i] IS NOT NULL THEN
            _higher := _i;
            EXIT;
        END IF;
    END LOOP;
    IF raise_notice THEN
        RAISE NOTICE ' DG: group=% descriptors=% higher=%', group_, descriptors, _higher;
    END IF;

    -- as type (V) if already defined
    descriptor := CASE
        WHEN fr.get_subscript_of_descriptor(
            descriptor => _descriptor
        ) > COALESCE(_higher, 0) THEN _descriptor
        ELSE 'V'
        END
    ;
    IF raise_notice THEN
        RAISE NOTICE ' DG: descriptor=%', descriptor;
    END IF;
END
$func$ LANGUAGE plpgsql;

-- get default of word
SELECT drop_all_functions_if_exists('fr', 'get_default_of_word');
CREATE OR REPLACE FUNCTION fr.get_default_of_word(
    element IN VARCHAR,                 -- STREET | COMPLEMENT
    word IN VARCHAR,
    as_default OUT VARCHAR
)
AS
$func$
BEGIN
    get_default_of_word.as_default := CASE element
        WHEN 'STREET' THEN
            (SELECT w.as_default
            FROM fr.laposte_address_street_word_descriptor w
            WHERE w.word = get_default_of_word.word)
        WHEN 'COMPLEMENT' THEN
            (SELECT w.as_default
            FROM fr.laposte_address_complement_word_descriptor w
            WHERE w.word = get_default_of_word.word)
        END
    ;
END
$func$ LANGUAGE plpgsql;

-- get descriptors from name (street, complement)
SELECT drop_all_functions_if_exists('fr', 'get_descriptors_from_name');
CREATE OR REPLACE FUNCTION fr.get_descriptors_from_name(
    element IN VARCHAR,                 -- STREET | COMPLEMENT
    name IN VARCHAR,
    with_abbreviation IN BOOLEAN DEFAULT FALSE,
    raise_notice IN BOOLEAN DEFAULT FALSE,
    descriptors OUT VARCHAR,
    words_by_descriptor OUT TEXT[],
    words_abbreviated_by_descriptor OUT TEXT[],
    words_todo_by_descriptor OUT TEXT[],
    as_words OUT INT[],
    as_groups OUT TEXT[]
)
AS
$func$
DECLARE
    _kw_group VARCHAR;
    _kw VARCHAR;
    _kw_abbreviated VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _descriptors_tmp VARCHAR;
    _descriptors_c VARCHAR;
    _descriptors_t VARCHAR;
    _descriptors_v VARCHAR;
    _groups_i INT;
    _words TEXT[];
    _words_len INT;
    _words_d VARCHAR;
    _words_skip INT := 0;
    _i INT;
    _len_c INT;
    _last_t INT;
    _with_exception BOOLEAN;
    _is_exception BOOLEAN;
    _exception VARCHAR;
    _init_by_descriptor BOOLEAN;
    _abbr VARCHAR;
    _word_default VARCHAR;
BEGIN
    IF raise_notice THEN RAISE NOTICE 'name="%"', name; END IF;

    IF name ~ '^ +' OR name ~ ' +$' THEN
        RAISE NOTICE 'libellé corrigé (%) avec espace(s) superflus!', name;
        name := TRIM(name);
    END IF;

    _words := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _words_len := ARRAY_LENGTH(_words, 1);

    FOR _i IN 1 .. _words_len
    LOOP
        _kw_nwords := 1;
        _init_by_descriptor := FALSE;
        IF _i < _words_skip THEN
            CONTINUE;
        END IF;

        IF raise_notice THEN RAISE NOTICE ' word=%, i=%', _words[_i], _i; END IF;

        -- number
        IF fr.is_normalized_number(_words[_i])
            AND NOT fr.is_normalized_article(_words[_i]) THEN
            _word_default := fr.get_default_of_word(
                element => element,
                word => _words[_i]
            );
            _words_d := CASE
                WHEN element = 'STREET' AND _word_default != 'C' THEN
                    CASE
                    WHEN _i < _words_len THEN _word_default
                    ELSE 'N'
                    END
                WHEN element = 'COMPLEMENT' AND _word_default != 'C' THEN
                    _word_default
                ELSE 'C'
                END
                ;
        ELSE
            _words_d := 'N';
            IF _i < _words_len THEN
                _with_exception := FALSE;

                -- keyword (title, type, extension or name)
                SELECT kw_group, kw, kw_abbreviated, kw_is_abbreviated, kw_nwords
                INTO _kw_group, _kw, _kw_abbreviated, _kw_is_abbreviated, _kw_nwords
                FROM fr.get_keyword_from_name(
                    name => name,
                    at_ => _i,
                    words => _words,
                    groups => element,
                    with_abbreviation => with_abbreviation
                );
                IF _kw IS NOT NULL THEN
                    _words_d := REPEAT(
                        CASE element
                        WHEN 'STREET' THEN
                            CASE
                            -- up to last word, as name or name (w/ abbreviation)
                            WHEN ((_i + _kw_nwords -1) = _words_len) OR (_kw_group = 'NAME') THEN 'N'
                            -- type
                            WHEN _i = 1 AND _kw_group = 'TYPE' THEN 'V'
                            -- title
                            ELSE 'T'
                            END
                        WHEN 'COMPLEMENT' THEN
                            CASE
                            -- as-type if group is preceded by an article
                            WHEN _kw_group ~ '^GROUP' AND descriptors IS NOT NULL AND RIGHT(descriptors, 1) = 'A' THEN 'V'
                            WHEN _kw_group ~ '^GROUP' THEN
                                fr.get_descriptor_of_group(
                                    group_ => _kw_group,
                                    descriptors => as_groups,
                                    raise_notice => raise_notice
                                )
                            -- up to last word, extension as name
                            WHEN ((_i + _kw_nwords -1) = _words_len) OR _kw_group = ANY('{NAME,EXT}') THEN 'N'
                            WHEN _kw_group = 'TYPE' THEN 'V'
                            ELSE 'T'
                            END
                        END,
                        _kw_nwords
                    );
                    words_by_descriptor := ARRAY_APPEND(words_by_descriptor, _kw);
                    words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := _kw_abbreviated;
                    words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := CASE
                        WHEN _kw_is_abbreviated THEN '+'
                        ELSE '-'
                        END
                    ;
                    as_words := ARRAY_APPEND(as_words, _kw_nwords);
                    _init_by_descriptor := TRUE;
                ELSE
                    -- firstname
                    IF fr.is_normalized_firstname(_words[_i]) THEN
                        _words_d := 'P';
                        IF _i > 1
                            AND fr.is_normalized_article(_words[_i -1])
                            AND NOT (
                                (_words[_i -1] = ANY('{DE, ET}'))
                                OR
                                (_words[_i -1] = 'D' AND _words[_i] ~ '^[AEIOUY]')
                            ) THEN
                            _words_d := 'N';
                        END IF;
                    ELSIF (
                        /* RULE
                        exception for road
                        (A|D|N)# : highway, departmental, national
                        at end of name only, else counter examples
                            LA ROCHE A 7 HEURES
                            LA PLANCHE A 4 PIEDS
                        */
                            _words[_i] ~ '^(A|D|N)$'
                            AND
                            fr.is_normalized_number(
                                word => _words[_i +1],
                                only_digit => 'ARABIC'
                            )
                            AND
                            _words_len = (_i +1)
                        ) THEN
                            _words_d := 'N';
                    -- article
                    ELSIF fr.is_normalized_article(_words[_i]) THEN
                        IF (
                            /* RULE
                            exception for complement, article as name
                            - building "number"
                            BATIMENT A 02
                            IMMEUBLE A 1
                            and special, w/ ET
                            BATIMENT A ET B
                            - between 2 groups
                            ENTREE A BATIMENT BLEU
                            BATIMENT A RESIDENCE LE VOLTAIRE
                             */
                            element = 'COMPLEMENT'
                            AND
                            _i < _words_len
                            AND
                            descriptors ~ '[GHI]$'
                            AND (
                                    fr.is_normalized_number(
                                        word => _words[_i +1],
                                        only_digit => 'ARABIC'
                                    )
                                OR (
                                    fr.get_descriptor_of_group(
                                        group_ => (
                                            SELECT kw_group
                                            FROM fr.get_keyword_from_name(
                                                name => name,
                                                at_ => _i +1,
                                                words => _words,
                                                groups => element,
                                                with_abbreviation => with_abbreviation
                                            )
                                        ),
                                        descriptors => as_groups,
                                        raise_notice => raise_notice
                                    ) ~ '[GHI]'
                                )
                                OR (
                                    _i < (_words_len -1)
                                    AND
                                    _words[_i +1] = 'ET'
                                    AND
                                    fr.get_default_of_word(
                                        element => element,
                                        word => _words[_i +2]
                                    ) = 'N'
                                )
                            )
                        ) THEN
                            _words_d := 'N';
                        ELSE
                            _words_d := 'A';
                            _with_exception := TRUE;
                        END IF;
                    END IF;
                END IF;

                /* RULE
                (firstname|title) followed by a number only (at the end) is a name
                */
                IF _words_d ~ 'P|T' THEN
                    IF _words_d ~ 'P'
                        AND _words_len = (_i +1)
                        AND fr.is_normalized_number(_words[_i +1]) THEN
                        _words_d := REPEAT('N', LENGTH(_words_d));
                    ELSE
                        _with_exception := TRUE;
                    END IF;
                END IF;

                -- apply exception(s) if exist
                IF _with_exception AND LENGTH(_words_d) = 1 THEN
                    SELECT is_exception, descriptor
                    INTO _is_exception, _exception
                    FROM fr.get_descriptor_from_exception(
                        words => _words,
                        nwords => _words_len,
                        at_ => _i,
                        as_descriptor => SUBSTR(_words_d, 1, 1)
                    );
                    IF _is_exception THEN
                        IF ((_words_d ~ 'T')
                            AND
                            (words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] = '+')
                        ) THEN
                            -- reset
                            words_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)];
                            words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := NULL;
                            words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := NULL;
                        END IF;
                        _words_d := REPEAT(_exception, LENGTH(_words_d));
                    END IF;
                END IF;
            ELSE
                _words_d := CASE
                    WHEN fr.is_normalized_reserved_word(_words[_i]) THEN 'E'
                    WHEN (element = 'COMPLEMENT') AND fr.is_normalized_title(
                        word => _words[_i],
                        groups => 'TYPE'
                    ) THEN 'V'
                    ELSE 'N'
                    END
                ;

                IF _words_d != 'N' THEN
                    SELECT kw_abbreviated
                    INTO _abbr
                    FROM fr.get_keyword_from_name(
                        name => name,
                        at_ => _i,
                        words => _words,
                        with_abbreviation => FALSE
                    );
                END IF;
            END IF;
        END IF;

        IF NOT _init_by_descriptor THEN
            words_by_descriptor := ARRAY_APPEND(words_by_descriptor, _words[_i]);
            as_words := ARRAY_APPEND(as_words, 1);
        END IF;
        IF (_words_d ~ '[EV]' AND _abbr IS NOT NULL) THEN
            words_abbreviated_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := _abbr;
            words_todo_by_descriptor[ARRAY_UPPER(words_by_descriptor, 1)] := '+';
        END IF;
        descriptors := CONCAT(descriptors, _words_d);
        IF element = 'COMPLEMENT' THEN
            IF _words_d ~ '[GHI]' THEN
                _groups_i := (
                    SELECT fr.get_subscript_of_descriptor(
                        descriptor => _words_d
                    )
                );
            END IF;
            as_groups[COALESCE(_groups_i, 1)] := CONCAT(
                as_groups[COALESCE(_groups_i, 1)],
                _words_d
            );
            IF raise_notice THEN
                RAISE NOTICE ' group=%, descriptors=%',
                    COALESCE(_groups_i, 1),
                    as_groups[COALESCE(_groups_i, 1)]
                ;
            END IF;
        END IF;
        _words_skip := _i;
        IF _kw_nwords > 1 THEN
            _words_skip := _words_skip + _kw_nwords;
        END IF;
    END LOOP;

    -- fix bad uses
    IF (element = 'STREET') THEN
        -- name of street (so descriptors) is ended by: number, reserved or name (CEN)
        IF descriptors !~ '[CEN]$' THEN
            descriptors := REGEXP_REPLACE(descriptors, '.$', 'N');

        /*
        -- nothing else than CN before last E (specially not title)
        ELSIF descriptors ~ '[^CN]E$' THEN
            descriptors := REGEXP_REPLACE(descriptors, '.E$', 'NE');
         */

        /* not title only (eventually followed by number), but name
        IMPASSE DU PASSAGE A NIVEAU 7, VANNNC
        and also successive V+ and T+
        PASSAGE A NIVEAU PASSAGE A NIVEAU 67, VVVNNNC
         */
        ELSIF descriptors ~ 'T+C*$' THEN
            _descriptors_t := (REGEXP_MATCHES(descriptors, '(T+)(C*)$'))[1];
            _descriptors_c := (REGEXP_MATCHES(descriptors, '(T+)(C*)$'))[2];
            -- last title is one|many word(s)
            _len_c := LENGTH(COALESCE(_descriptors_c, ''));
            _last_t := ARRAY_LENGTH(words_by_descriptor, 1) - _len_c;
            IF raise_notice THEN
                RAISE NOTICE ' dt=%, dc=% #c=%', _descriptors_t, _descriptors_c, _len_c;
                RAISE NOTICE ' wbd=%, lt=%', words_by_descriptor, _last_t;
                RAISE NOTICE ' descriptors=%', descriptors;
            END IF;
            -- replace all T
            IF count_words(words_by_descriptor[_last_t]) = LENGTH(_descriptors_t) THEN
                IF raise_notice THEN RAISE NOTICE ' replace all'; END IF;
                descriptors := REGEXP_REPLACE(descriptors,
                    'T+(C*)$',
                    CONCAT(
                        REPEAT('N', LENGTH(_descriptors_t)),
                        '\1'
                    )
                );
            -- replace only last T
            ELSE
                _last_t := LENGTH(descriptors) - _len_c;
                IF raise_notice THEN RAISE NOTICE ' replace last only, lt=%', _last_t; END IF;
                IF _last_t > 1 THEN
                    descriptors := SUBSTR(descriptors, 1, _last_t -1);
                ELSE
                    descriptors := NULL;
                END IF;
                descriptors := CONCAT(descriptors, 'N');
                IF _len_c > 0 THEN
                    descriptors := CONCAT(descriptors, _descriptors_c);
                END IF;
            END IF;
        /* not type only (eventually followed by number, reserved), but name
        PASSAGE A NIVEAU 7, NNNC
        GRANDE RUE PROLONGEE
         */
        ELSIF descriptors ~ 'V+[CE]*$' THEN
            _descriptors_v := (REGEXP_MATCHES(descriptors, '(V+)[CE]*$'))[1];
            descriptors := REGEXP_REPLACE(descriptors,
                'V+([CE]*)$',
                CONCAT(
                    REPEAT('N', LENGTH(_descriptors_v)),
                    '\1'
                )
            );
        /* neither firstname nor title
        VNC, AVENUE ALBERT 1ER
        VNCE, RUE ALBERT 1ER PROLONGEE
        VPCAN, AVENUE ALBERT 1ER DE BELGIQUE
         */
        ELSIF descriptors ~ '^V[PT]CE?$' THEN
            descriptors := REGEXP_REPLACE(descriptors, '(^V[PT]C)(E?)$', 'VNC\2');
        END IF;
    ELSE
        -- name of complement (so descriptors) is ended by: number, reserved, name or type (CENV)
        IF descriptors !~ '[CENV]$' THEN
            descriptors := REGEXP_REPLACE(descriptors, '.$', 'N');
        END IF;
    END IF;

    IF raise_notice THEN RAISE NOTICE ' descriptors=%', descriptors; END IF;
END
$func$ LANGUAGE plpgsql;

-- analyze differences of descriptors
SELECT drop_all_functions_if_exists('fr', 'get_differences_between_descriptors');
CREATE OR REPLACE FUNCTION fr.get_differences_between_descriptors(
    reference VARCHAR,
    other VARCHAR,
    raise_notice IN BOOLEAN DEFAULT FALSE,
    differences OUT VARCHAR[]
)
AS
$func$
DECLARE
    _ref_len INT := LENGTH(reference);
    _other_len INT := LENGTH(other);
    _ref_i CHAR(1);
    _other_i CHAR(1);
    _i INT;
    _usecase VARCHAR;
    _descriptor VARCHAR;
BEGIN
    IF _ref_len != _other_len THEN
        differences := ARRAY_APPEND(differences, CONCAT_WS('-',
            'LEN',
            CASE
                WHEN _ref_len > _other_len THEN 'GT'
                ELSE 'LT'
                END
            )
        );
    END IF;

    FOR _i IN 1 .. LEAST(_ref_len, _other_len)
    LOOP
        _ref_i := SUBSTR(reference, _i, 1);
        _other_i := SUBSTR(other, _i, 1);
        IF (_ref_i = _other_i) THEN CONTINUE; END IF;

        _usecase := CASE
            WHEN (_ref_i != _other_i) AND (_ref_i != 'N') THEN 'MISS'
            WHEN (_ref_i != _other_i) AND (_ref_i = 'N') THEN 'WRONG'
            END
            ;
        _descriptor := CASE
            WHEN (_ref_i != _other_i) AND (_ref_i != 'N') THEN _ref_i
            WHEN (_ref_i != _other_i) AND (_ref_i = 'N') THEN _other_i
            END
            ;
        differences := ARRAY_APPEND(differences, CONCAT_WS('-',
            _usecase,
            _descriptor,
            _i
            )
        );
    END LOOP;
END
$func$ LANGUAGE plpgsql;

/* TEST
view test_normalize.sh : option DESCRIPTORS_DIFF
 */

-- analyze differences of normalized name
SELECT drop_all_functions_if_exists('fr', 'get_differences_between_normalized_name');
CREATE OR REPLACE FUNCTION fr.get_differences_between_normalized_name(
    name_as_words IN TEXT[],
    descriptors_as_words IN TEXT[],
    nwords IN INT,
    reference_name_normalized_as_words IN TEXT[],
    reference_descriptors_normalized_as_words IN TEXT[],
    other_name_normalized_as_words IN TEXT[],
    other_descriptors_normalized_as_words IN TEXT[],
    raise_notice IN BOOLEAN DEFAULT FALSE,
    differences OUT VARCHAR[]
)
AS
$func$
DECLARE
    _reference_descriptors VARCHAR;
    _other_descriptors VARCHAR;
    _i INT;
    _usecase VARCHAR;
    _descriptor VARCHAR;
    _reference_unabbreviated BOOLEAN;
    _other_unabbreviated BOOLEAN;
BEGIN
    /* NOTE
    search for differences, basing w/ other
     */
    FOR _i IN 1 .. nwords
    LOOP
        _descriptor := SUBSTR(descriptors_as_words[_i], 1, 1);

        -- delete word (article)
        IF _descriptor = 'A' THEN
            _usecase := CASE
                WHEN (reference_name_normalized_as_words[_i] IS NULL) AND (other_name_normalized_as_words[_i] IS NOT NULL) THEN 'MORE'
                WHEN (reference_name_normalized_as_words[_i] IS NOT NULL) AND (other_name_normalized_as_words[_i] IS NULL) THEN 'LESS'
                ELSE 'OK'
                END
                ;
            IF _usecase = 'OK' THEN CONTINUE; END IF;

        -- abbreviate word
        ELSE
            _reference_unabbreviated := (name_as_words[_i] = reference_name_normalized_as_words[_i]);
            _other_unabbreviated := (name_as_words[_i] = other_name_normalized_as_words[_i]);

            _usecase := CASE
                WHEN (NOT _reference_unabbreviated) AND (_other_unabbreviated) THEN 'UNABBR'
                WHEN (_reference_unabbreviated) AND (NOT _other_unabbreviated) THEN 'ABBR'
                ELSE 'OK'
                END
                ;
            IF _usecase = 'OK' THEN CONTINUE; END IF;
        END IF;

        differences := ARRAY_APPEND(differences, CONCAT_WS('-',
            _descriptor,
            _usecase,
            _i
            )
        );
    END LOOP;

    /*
    _reference_descriptors := ARRAY_TO_STRING(reference_descriptors_normalized_as_words, '');
    _other_descriptors := ARRAY_TO_STRING(other_descriptors_normalized_as_words, '');
    IF LENGTH(_reference_descriptors) != LENGTH(_other_descriptors) THEN
        differences := ARRAY_APPEND(differences, CONCAT_WS('-',
                'D',
                _reference_descriptors,
                _other_descriptors
            )
        );
    END IF;
     */
END
$func$ LANGUAGE plpgsql;

/* TEST
view test_normalize.sh : option NAME_DIFF
 */

/* NOTE
old functions built to help, but useful now ?
 */

-- get title(s) from name of street
SELECT drop_all_functions_if_exists('fr', 'get_titles_from_name');
CREATE OR REPLACE FUNCTION fr.get_titles_from_name(
    name IN VARCHAR,
    descriptors IN VARCHAR,
    titles OUT TEXT[]
)
AS
$func$
DECLARE
    _words TEXT[] := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _title TEXT;
    _descriptors_len INT := LENGTH(descriptors);
    _descriptor VARCHAR;
    _descriptor_prev VARCHAR := 'Z';
    _i INT;
BEGIN
    FOR _i IN 1 .. _descriptors_len
    LOOP
        _descriptor := SUBSTR(descriptors, _i, 1);
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

-- count potential number of words (w/ descriptors)
SELECT drop_all_functions_if_exists('fr', 'count_potential_nof_words');
CREATE OR REPLACE FUNCTION fr.count_potential_nof_words(
    descriptors IN VARCHAR,
    nof OUT INT
)
AS
$func$
DECLARE
    _descriptors_len INT := LENGTH(descriptors);
    _descriptor VARCHAR;
    _descriptor_prev VARCHAR := 'Z';
    _i INT;
BEGIN
    nof := 0;
    FOR _i IN 1 .. _descriptors_len
    LOOP
        _descriptor := SUBSTR(descriptors, _i, 1);
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
    name IN VARCHAR,
    descriptors_in IN VARCHAR,
    is_normalized IN BOOLEAN DEFAULT FALSE,
    split_only IN VARCHAR DEFAULT NULL,       -- specific descriptor: A, C, E, N, P, T, V
    raise_notice IN BOOLEAN DEFAULT FALSE,
    words OUT TEXT[],
    descriptors OUT TEXT[]
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
    _descriptors_len INT := LENGTH(descriptors_in);
    _descriptor VARCHAR;
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
    _descriptor_wo_a VARCHAR;
    _descriptor_only_a VARCHAR;
    _descriptor_start_a VARCHAR;
    _descriptor_tmp VARCHAR;
    --_descriptor_next VARCHAR;
    --_descriptor_only_t VARCHAR;
    _kw VARCHAR;
    _kw_more VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
BEGIN
    /* NOTE
    when is_normalized, normalized name (eventually w/ deleted article, or abbreviated _words)
    is_normalized => TRUE

    -- deleted article (one)
    AVENUE DE LA 9E DIVISION INFANTERIE DE CAVALERIE
        name => 'AV LA 9E DIV INFANT DE CAVALERIE',
        descriptors_in => 'VAACTTAN'
    -- deleted article (all)
    CHEMIN D EXPLOITATION DU MAS SAINT PAUL
        name => 'CHEMIN EXPLOITATION MAS ST PAUL',
        descriptors_in => 'VANATTN'
    CHEMIN DE NOTRE DAME DES CHAMPS ET DES VIGNES
        name => 'CHEMIN ND CHAMPS ET DES VIGNES',
        descriptors_in => 'VATTANAAN'
    -- not an article!
    PARC D ACTIVITES NURIEUX CROIX CHALON'
        name => 'PARC A NURIEUX CRX CHALON',
        descriptors_in => 'VANNTN'
    AVENUE DES ANCIENS COMBATTANTS FRANCAIS D INDOCHINE
        name => 'AV A COMBATTANTS FR INDOCHINE',
        descriptors_in => 'VANNTAN'

    -- deleted (word of) title
    CHEMIN RURAL DIT ANCIEN CHEMIN DE BRISON A THUET
        name => 'CHEM R DIT ANCIEN BRISON THUET',
        descriptors_in => 'VNNTTANAN'

    -- abbreviated title
    ZONE ARTISANALE CENTRE COMMERCIAL BEAUGE
        name => 'ZONE ARTISANALE CCIAL BEAUGE',
        descriptors_in => 'VVTTN'
    PLACE NOTRE DAME DE LA LEGION D HONNEUR
        name => 'PL ND DE LA LEGION D HONNEUR',
        descriptors_in => 'VTTAANAN'
    -- w/ deleted article (prev='T')
    CHEMIN DE NOTRE DAME DES CHAMPS ET DES VIGNES
        name => 'CHEMIN ND CHAMPS ET DES VIGNES',
        descriptors_in => 'VATTANAAN'

    -- abbreviated type
    LIEU DIT LE GRAND BOIS DE LA DURANDIERE
        name => 'LD LE GD BOIS DE LA DURANDIERE',
        descriptors_in => 'VVATTAAN'
     */

    _n := ARRAY_LENGTH(_words, 1);
    IF raise_notice THEN RAISE NOTICE '#d=%, #w=%', _descriptors_len, _n; END IF;
    FOR _i IN 1 .. _n
    LOOP
        _k := (_i + _offset -1);
        IF _k > _descriptors_len THEN
            EXIT;
        END IF;
        IF (_i = _n) AND (_k < _descriptors_len) THEN
            IF raise_notice THEN RAISE NOTICE ' ajustement k=% len=%', _k, _descriptors_len; END IF;
            _k := _descriptors_len;
        END IF;
        _descriptor := SUBSTR(descriptors_in, _k, 1);
        IF raise_notice THEN RAISE NOTICE '(i=% ofs=% k=%): d=% (pd=%), w=%', _i, _offset, _k, _descriptor, _descriptor_prev, _words[_i]; END IF;

        -- article
        IF (is_normalized
            AND _n < _descriptors_len
            AND _descriptor = 'A'
        ) THEN
            IF (_descriptors_len - _k) != (_n - _i) THEN
                --_descriptor_from := CASE WHEN _descriptor_prev = 'A' THEN _k ELSE _k +1 END;
                _descriptor_from := _k +1;
                _descriptor_remainder := SUBSTR(descriptors_in, _descriptor_from);
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
                IF raise_notice THEN RAISE NOTICE ' usecase article=%', _usecase_article; END IF;

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

                    IF raise_notice THEN
                        RAISE NOTICE ' remainder=%', _descriptor_remainder;
                        RAISE NOTICE ' only_a=%', _descriptor_only_a;
                        RAISE NOTICE ' wo_a=%', _descriptor_wo_a;
                        RAISE NOTICE ' start_a=%', _descriptor_start_a;
                        RAISE NOTICE ' offset=%', _offset;
                    END IF;
                -- deleted article
                ELSIF _usecase_article = ANY('{2}') THEN
                    FOR _j IN (_k + 1) .. _descriptors_len
                    LOOP
                        _descriptor := SUBSTR(descriptors_in, _j, 1);
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

                            IF raise_notice THEN
                                RAISE NOTICE ' descriptors_in=%', _descriptor;
                                RAISE NOTICE ' offset=%', _offset;
                            END IF;
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
            AND _n < _descriptors_len
            AND _descriptor = 'V'
            AND _i = 1
        ) THEN
            SELECT kw, kw_is_abbreviated, kw_nwords
            INTO _kw, _kw_is_abbreviated, _kw_nwords
            FROM fr.get_type_of_street(
                name => name,
                with_abbreviation => is_normalized
            )
            ;
            -- abbreviated ?
            IF _kw_is_abbreviated THEN
                _descriptor_type := (REGEXP_MATCHES(descriptors_in, '^(V+)([^V])'))[1];
                -- be careful w/ multiple abbreviated type (as ZA, PCH) !
                IF count_words(_kw) != LENGTH(_descriptor_type) THEN
                    SELECT k.name
                    INTO _kw_more
                    FROM fr.laposte_address_keyword k
                    WHERE k.group = 'TYPE'
                    AND k.name_abbreviated = _words[_i]
                    AND count_words(k.name) = LENGTH(_descriptor_type)
                    ORDER BY occurs DESC
                    LIMIT 1;
                    IF FOUND THEN
                        _kw := _kw_more;
                    ELSE
                        IF raise_notice THEN RAISE NOTICE 'indécision libellé normalisé (lib=%, abr=%)', name, _kw_is_abbreviated; END IF;
                    END IF;
                END IF;
                _offset := count_words(_kw);
                _descriptor_word := _words[_i];
                _descriptor_same := _descriptor;
                CONTINUE;
            END IF;
        -- title
        ELSIF (is_normalized
            AND _n < _descriptors_len
            AND _descriptor = 'T'
        ) THEN
            _descriptor_from := _k;

            -- previous deleted article (one descriptors_in T already consumed)
            _descriptor_before := SUBSTR(descriptors_in, 1, _descriptor_from);
            _descriptor_with_a := (REGEXP_MATCHES(_descriptor_before, '(A+)(T+)$'))[1];
            IF (_descriptor_with_a IS NOT NULL
                AND
                (descriptors[ARRAY_UPPER(descriptors, 1)] !~ 'A')
            ) THEN
                _descriptor_from := _descriptor_from -1;
            END IF;

            _descriptor_remainder := SUBSTR(descriptors_in, _descriptor_from);
            _descriptor_title := (REGEXP_MATCHES(_descriptor_remainder, '(T+)([^T])'))[1];
            _descriptor_others := (REGEXP_MATCHES(_descriptor_remainder, '(T+)(.*)$'))[2];
            _descriptor_wo_a := REGEXP_REPLACE(_descriptor_others, '[A]', '', 'gi');
            _descriptor_only_a := REGEXP_REPLACE(_descriptor_others, '[^A]', '', 'gi');
            --_descriptor_only_t := REGEXP_REPLACE(_descriptor_others, '[^T]', '', 'gi');
            --_descriptor_next := SUBSTR(descriptors_in, (_descriptor_from + LENGTH(_descriptor_title)), 1);

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
            IF raise_notice THEN RAISE NOTICE ' usecase title=%', _usecase_title; END IF;

            -- as such number of words
            IF _usecase_title = ANY('{1, 2, 3, 5, 6}') THEN
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
                IF ((_usecase_title = ANY('{2, 3}'))
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
                -- search for next descriptors_in by adjusting offset
                FOR _j IN (_descriptor_from + LENGTH(_descriptor_title)) .. _descriptors_len
                LOOP
                    _descriptor_tmp := SUBSTR(descriptors_in, _j, 1);
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

                IF raise_notice THEN
                    RAISE NOTICE ' remainder=%', _descriptor_remainder;
                    RAISE NOTICE ' others=%', _descriptor_others;
                    RAISE NOTICE ' only_a=%', _descriptor_only_a;
                    RAISE NOTICE ' wo_a=%', _descriptor_wo_a;
                    RAISE NOTICE ' offset=%', _offset;
                END IF;
                CONTINUE;
            END IF;
        END IF;

        IF (_descriptor != _descriptor_prev) THEN
            IF _descriptor_word IS NOT NULL AND (
                (split_only IS NULL)
                OR
                (_descriptor_same ~ split_only)
            ) THEN
                -- last item already w/ same descriptors_in
                IF (descriptors[ARRAY_UPPER(descriptors, 1)] ~ SUBSTR(_descriptor_same, 1, 1)) THEN
                    words[ARRAY_UPPER(words, 1)] := CONCAT(
                        words[ARRAY_UPPER(words, 1)],
                        ' ',
                        _descriptor_word
                    );
                    descriptors[ARRAY_UPPER(descriptors, 1)] := CONCAT(
                        descriptors[ARRAY_UPPER(descriptors, 1)],
                        _descriptor_same
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
        -- last item already w/ same descriptors_in
        IF (descriptors[ARRAY_UPPER(descriptors, 1)] ~ SUBSTR(_descriptor_same, 1, 1)) THEN
            words[ARRAY_UPPER(words, 1)] := CONCAT(
                words[ARRAY_UPPER(words, 1)],
                ' ',
                _descriptor_word
            );
            descriptors[ARRAY_UPPER(descriptors, 1)] := CONCAT(
                descriptors[ARRAY_UPPER(descriptors, 1)],
                _descriptor_same
            );
        ELSE
            words := ARRAY_APPEND(words, _descriptor_word);
            descriptors := ARRAY_APPEND(descriptors, _descriptor_same);
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

/*
-- get keyword(s) from name of street
SELECT drop_all_functions_if_exists('fr', 'get_keywords_from_name');
CREATE OR REPLACE FUNCTION fr.get_keywords_from_name(
    descriptor IN VARCHAR,
    descriptors IN VARCHAR,
    name IN VARCHAR DEFAULT NULL,
    words IN TEXT[] DEFAULT NULL,
    keywords OUT TEXT[]
)
AS
$func$
DECLARE
    _words TEXT[];
    _kw TEXT;
    _descriptors_len INT := LENGTH(descriptors);
    _descriptor VARCHAR;
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
    IF _descriptors_len != ARRAY_UPPER(_words, 1) THEN
        RAISE 'traiter le nom de la voie avec un descripteur de même taille';
    END IF;

    FOR _i IN 1 .. _descriptors_len
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
