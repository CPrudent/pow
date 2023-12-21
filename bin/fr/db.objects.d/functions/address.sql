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

-- get type of street (from full name)
SELECT drop_all_functions_if_exists('fr', 'get_type_of_street');
CREATE OR REPLACE FUNCTION fr.get_type_of_street(
    name IN VARCHAR                   -- name of street
)
RETURNS RECORD AS
$func$
/*
DECLARE
    -- 1st word = type of street, eventually abbreviated
    _first_word VARCHAR := (REGEXP_MATCH(name, '^\S+'))[1];
    _type RECORD;
    _exists BOOLEAN := TRUE;
    _found BOOLEAN := FALSE;
 */
BEGIN
/*
    --RAISE NOTICE 'name=% word1=%', name, _first_word;

    SELECT *
    INTO _type
    FROM fr.laposte_address_street_type
    WHERE type_abbreviated = _first_word
    ORDER BY occurs DESC
    LIMIT 1;
    IF FOUND THEN
        --RAISE NOTICE 'type=%', _type;
        IF _type.type != _type.type_abbreviated THEN
            RETURN (_type.type, _type.type_abbreviated, TRUE);
        END IF;
    ELSE
        SELECT EXISTS(
            SELECT 1 FROM fr.laposte_address_street_type
            WHERE first_word = _first_word
        ) INTO _exists;
    END IF;

    IF _exists THEN
        FOR _type IN (
            SELECT * FROM fr.laposte_address_street_type
            WHERE first_word = _first_word
            ORDER BY LENGTH(type) DESC
        )
        LOOP
            IF name ~ CONCAT('^', _type.type, '[ ]+') THEN
                _found := TRUE;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    IF NOT _found THEN
        RETURN (NULL::VARCHAR, NULL::VARCHAR, FALSE);
    ELSE
        RETURN (_type.type, _type.type_abbreviated
            , (_first_word IS NOT DISTINCT FROM _type.type_abbreviated) AND (_type.type != _type.type_abbreviated)
        );
    END IF;
 */
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
    , type IN VARCHAR DEFAULT NULL
)
RETURNS VARCHAR AS
$func$
DECLARE
    _type VARCHAR;
    _type_abbreviated VARCHAR;
    _type_is_abbreviated BOOLEAN := TRUE;
    _descriptor VARCHAR := '';
    _words TEXT[];
    _words_i INT := 0;
    _words_len INT;
    _words_desc VARCHAR;
    _i INT;
    _articles TEXT[] := '{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}'::TEXT[];
    _titles VARCHAR[] :=
        ARRAY(SELECT key FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_TITLE');
    _found BOOLEAN;
BEGIN
    RAISE NOTICE 'name= %', name;
    IF type IS NULL THEN
        SELECT type_, type_abbreviated, type_is_abbreviated
        INTO _type, _type_abbreviated, _type_is_abbreviated
        FROM fr.get_type_of_street(name)
        AS (type_ VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
    ELSE
        _type := type;
    END IF;
    IF _type IS NOT NULL THEN
        _words_i := (LENGTH(_type) - LENGTH(REPLACE(_type, ' ', ''))) +1;
        _descriptor := LPAD(_descriptor, _words_i, 'V');
        _words_i := _words_i +1;
    ELSE
        _words_i := 1;
    END IF;
    RAISE NOTICE ' descriptor= %, _words_i=%', _descriptor, _words_i;
    _words := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    _words_len := ARRAY_LENGTH(_words, 1);
    FOR _i IN _words_i .. _words_len
    LOOP
        RAISE NOTICE ' word= %, i=%', _words[_i], _i;
        -- roman number
        -- https://www.geeksforgeeks.org/validating-roman-numerals-using-regular-expression/
        IF _words[_i] ~ '^[0-9]+$' OR _words[_i] ~ '^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$' THEN
            IF _i > 1 AND RIGHT(_descriptor, 1) = ANY('{A,V}') AND _words[_i] = ANY('{D,L}') THEN
                _words_desc := 'A';
            ELSE
                _words_desc := 'C';
            END IF;
        ELSE
            _words_desc := 'N';
            IF _i < _words_len THEN
                IF _words[_i] = ANY(_articles) THEN
                    -- not if previous is firstname
                    RAISE NOTICE ' last= %', RIGHT(_descriptor, 1);
                    IF _i > 1 AND RIGHT(_descriptor, 1) = 'P' THEN
                        _words_desc := 'N';
                    ELSE
                        _words_desc := 'A';
                    END IF;
                ELSIF _words[_i] = ANY(_titles) THEN
                    _words_desc := 'T';
                ELSE
                    -- not if previous is (article|number)
                    RAISE NOTICE ' last= %', RIGHT(_descriptor, 1);
                    IF _i > 1 AND RIGHT(_descriptor, 1) = ANY('{A,C}') THEN
                        _words_desc := 'N';
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
                            _words_desc := 'P';
                        END IF;
                    END IF;
                END IF;
            -- as last word
            ELSIF _words[_i] ~ '^(INFERIEUR|SUPERIEUR|PROLONGE)(ES)?$' THEN
                _words_desc := 'E';
            END IF;
        END IF;

        -- fix bad uses
        -- 'LA METAIRIE D EN HAUT' not roman number D, but article
        IF _i > 1
            AND _words_desc = ANY('{A,N}')
            AND LEFT(_words[_i], 1) = ANY('{A,E,I,O,U,Y}')
            AND RIGHT(_descriptor, 1) = 'C'
            AND _words[_i -1] = ANY('{D,L}') THEN
            _descriptor := CONCAT(
                SUBSTR(_descriptor, 1, LENGTH(_descriptor) - 1)
                , 'A'
            );
        END IF;
        _descriptor := CONCAT(_descriptor, _words_desc);
    END LOOP;

    RAISE NOTICE 'descriptor= %', _descriptor;
    RETURN _descriptor;
END
$func$ LANGUAGE plpgsql;
