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

-- split name of street as words (w/ same descriptor)
SELECT drop_all_functions_if_exists('fr', 'split_name_of_street_as_descriptor');
CREATE OR REPLACE FUNCTION fr.split_name_of_street_as_descriptor(
    --words IN TEXT[]
    name IN VARCHAR
    , descriptor IN VARCHAR
    , fullname IN BOOLEAN DEFAULT TRUE
    , set_w OUT TEXT[]
    , set_d OUT TEXT[]
)
AS
$func$
DECLARE
    _words TEXT[] := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _kw VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _descriptor VARCHAR;
    _descriptor_len INT := LENGTH(descriptor);
    _descriptor_prev VARCHAR := 'Z';
    _descriptor_word VARCHAR := NULL;
    _descriptor_same VARCHAR := NULL;
    _i INT;
    _n INT;
    _offset INT;
    _last BOOLEAN := FALSE;
    _articles TEXT[] := '{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}'::TEXT[];
BEGIN
    /* NOTE
    not fullname, as normalized name (eventually w/ deleted article, or abbreviated _words)

    -- deleted article
    AVENUE DE LA 9E DIVISION INFANTERIE DE CAVALERIE
        descriptor=VAACTTAN
        normalized=AV LA 9E DIV INFANT DE CAVALERIE
    CHEMIN D EXPLOITATION DU MAS SAINT PAUL
        descriptor=VANATTN
        normalized=CHEMIN EXPLOITATION MAS ST PAUL

    -- abbreviated title
    ZONE ARTISANALE CENTRE COMMERCIAL BEAUGE
        descriptor=VVTTN
        normalized=ZONE ARTISANALE CCIAL BEAUGE
    PLACE NOTRE DAME DE LA LEGION D HONNEUR
        descriptor=VTTAANAN
        normalized=PL ND DE LA LEGION D HONNEUR

    -- abbreviated type
    LIEU DIT LE GRAND BOIS DE LA DURANDIERE
        descriptor=VVATTAAN
        normalized=LD LE GD BOIS DE LA DURANDIERE
     */
    _offset := 1;
    _n := ARRAY_LENGTH(_words, 1);
    FOR _i IN 1 .. _n
    LOOP
        IF (_i + _offset -1) > _descriptor_len THEN
            EXIT;
        END IF;

        RAISE NOTICE 'i=% ofs=% d=%', _i, _offset, _descriptor;
        _descriptor := SUBSTR(descriptor, (_i + _offset -1), 1);
        IF ((NOT fullname)
                AND _n < _descriptor_len
                AND _descriptor = 'A'
                AND (NOT _words[_i] = ANY(_articles))) THEN
            FOR _j IN (_i + _offset) .. _n
            LOOP
                _descriptor := SUBSTR(descriptor, _j, 1);
                _offset := _offset +1;
                IF RIGHT(_descriptor_same, 1) = 'A' THEN
                    _descriptor_same := CONCAT(_descriptor_same, 'A');
                END IF;
                IF _descriptor != 'A' THEN
                    EXIT;
                END IF;
            END LOOP;
            IF (_i + _offset) > _descriptor_len THEN
                RAISE 'découpage libellé % en erreur (desc=%)', _words, descriptor;
            END IF;
        ELSIF ((NOT fullname)
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
                    _offset := count_words(_kw);
                    _descriptor_word := _words[_i];
                    _descriptor_same := REPEAT('V', _offset);
                    CONTINUE;
                END IF;
        ELSIF ((NOT fullname)
                AND _n < _descriptor_len
                AND _descriptor = 'T'
                AND _i = _n) THEN
            _descriptor = 'N';
            _descriptor_same := CONCAT(_descriptor_same, 'T');
        END IF;

        IF (_descriptor != _descriptor_prev) THEN
            IF _descriptor_word IS NOT NULL THEN
                set_w := ARRAY_APPEND(set_w, _descriptor_word);
                set_d := ARRAY_APPEND(set_d, _descriptor_same);
            END IF;
            _descriptor_word := _words[_i];
            _descriptor_same := _descriptor;
        ELSE
            _descriptor_word := CONCAT(_descriptor_word, ' ', _words[_i]);
            _descriptor_same := CONCAT(_descriptor_same, _descriptor);
            IF _i = _n THEN
                set_w := ARRAY_APPEND(set_w, _descriptor_word);
                set_d := ARRAY_APPEND(set_d, _descriptor_same);
                _last := TRUE;
            END IF;
        END IF;
        _descriptor_prev := _descriptor;
    END LOOP;
    IF NOT _last THEN
        set_w := ARRAY_APPEND(set_w, _descriptor_word);
        set_d := ARRAY_APPEND(set_d, _descriptor_same);
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
    _articles TEXT[] := '{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}'::TEXT[];
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

        -- roman number
        -- https://www.geeksforgeeks.org/validating-roman-numerals-using-regular-expression/
        IF _words[_i] ~ '^1(ER)?|[2-9][0-9]*(E|EME)?$'
            OR
            _words[_i] ~ '^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$' THEN
            IF _i > 1 AND RIGHT(_descriptor, 1) = ANY('{A,V}') AND _words[_i] = ANY('{D,L}') THEN
                _words_d := 'A';
            ELSE
                _words_d := 'C';
            END IF;
        ELSIF _words[_i] = ANY(_articles) THEN
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
                        SELECT EXISTS(
                            SELECT 1
                            FROM fr.constant
                            WHERE
                                usecase = 'LAPOSTE_STREET_FIRSTNAME'
                                AND
                                key = _words[_i]
                        )
                        INTO _found
                        ;
                        IF _found THEN
                            _words_d := 'P';
                        END IF;
                    END IF;
                END IF;
            ELSIF _words[_i] ~ '^(INFERIEUR|SUPERIEUR|PROLONGE)E?S?$' THEN
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
