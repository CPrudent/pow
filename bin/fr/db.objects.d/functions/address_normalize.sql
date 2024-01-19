/***
 * add FR-ADDRESS facilities (normalized label, following AFNOR NF Z 10-011 (1/2013))
 */

/* NOTE
LAPOSTE descriptor items
 A article
 C number
 E reserved word
 N name
 P firstname
 T title
 V type
 */

-- is number (date, roman, arabic)
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_number');
CREATE OR REPLACE FUNCTION fr.is_normalized_number(
    word VARCHAR
    , only_digit VARCHAR DEFAULT 'ALL'   -- ARABIC|DATE|HOUSENUMBER|ROAD_NETWORK|ROMAN
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_number BOOLEAN;
    _only VARCHAR[];
    _i INT;
    _re VARCHAR;
BEGIN
    IF LENGTH(word) = 0 THEN RETURN FALSE; END IF;

    IF only_digit = 'ALL' THEN
        _only := '{ARABIC,DATE,HOUSENUMBER,ROAD_NETWORK,ROMAN}'::VARCHAR[];
    ELSE
        _only := STRING_TO_ARRAY(only_digit, ',');
    END IF;

    -- roman number
    -- https://www.geeksforgeeks.org/validating-roman-numerals-using-regular-expression/

    FOR _i IN 1 .. ARRAY_LENGTH(_only, 1)
    LOOP
        IF UPPER(_only[_i]) = 'HOUSENUMBER' THEN
            SELECT
                ARRAY_TO_STRING(
                    ARRAY_AGG(name ORDER BY CASE
                        -- A .. Z
                        WHEN LENGTH(name) = 1   THEN ASCII(name) - ASCII('A') + 1
                        -- EXT before LETTER (BIS before B)
                        WHEN name = 'BIS'       THEN 2 - .1
                        WHEN name = 'TER'       THEN 3 - .1
                        WHEN name = 'QUATER'    THEN 4 - .1
                        WHEN name = 'QUINQUIES' THEN 5 - .1
                        WHEN name = 'SEXTO'     THEN 6 - .1
                        END
                    )
                    , '|'
                )
            INTO
                _re
            FROM
                fr.laposte_address_street_keyword
            WHERE
                "group" = 'EXT'
                ;
        END IF;

        _is_number := CASE
            WHEN UPPER(_only[_i]) = 'ARABIC' THEN (word ~ '^[0-9]+$')
            -- FR and UK dates
            WHEN UPPER(_only[_i]) = 'DATE' THEN (word ~ '^(1(ER|ST)?|([2-9][0-9]*|1[0-9]+)*I?(E|EME)?|2ND|3RD|([4-9]+|[1-9]+[0-9]+)TH)$')
            WHEN UPPER(_only[_i]) = 'HOUSENUMBER' THEN (word ~ CONCAT('^[0-9]+(', _re, ')$'))
            WHEN UPPER(_only[_i]) = 'ROMAN' THEN (word ~ '^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')
            WHEN UPPER(_only[_i]) = 'ROAD_NETWORK' THEN (word ~ '^(A|B|CD|CR|D|N|V|GR|R|RD|RN|VC)[0-9]+$')
        END IF;
        IF _is_number THEN
            RETURN TRUE;
        END IF;
    END LOOP;

    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

-- is article
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_article');
CREATE OR REPLACE FUNCTION fr.is_normalized_article(
    word VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _articles TEXT[] := '{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}'::TEXT[];
    _is_article BOOLEAN := FALSE;
BEGIN
    IF word = ANY(_articles) THEN
        _is_article := TRUE;
    END IF;
    RETURN _is_article;
END
$func$ LANGUAGE plpgsql;

-- is reserved word
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_reserved_word');
CREATE OR REPLACE FUNCTION fr.is_normalized_reserved_word(
    word VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_reserved BOOLEAN := FALSE;
BEGIN
    IF word ~ '^(INFERIEUR|SUPERIEUR|PROLONGE)E?S?$' THEN
        _is_reserved := TRUE;
    END IF;
    RETURN _is_reserved;
END
$func$ LANGUAGE plpgsql;

-- is firstname
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_firstname');
CREATE OR REPLACE FUNCTION fr.is_normalized_firstname(
    word VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_firstname BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1
        FROM fr.constant c JOIN fr.laposte_address_street_word w ON c.key = w.word
        WHERE
            c.usecase = 'LAPOSTE_STREET_FIRSTNAME'
            AND
            w.word = is_normalized_firstname.word
            AND
            -- at least 5%, others are ignored
            (	as_fname >= (
                    COALESCE(as_name, 0)
                    + COALESCE(as_reserved, 0)
                    + COALESCE(as_article, 0)
                    + COALESCE(as_number, 0)
                    + COALESCE(as_title, 0)
                    + COALESCE(as_type, 0)
                ) * 0.05
            )

    )
    INTO _is_firstname
    ;

    RETURN _is_firstname;
END
$func$ LANGUAGE plpgsql;

-- is title
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_title');
CREATE OR REPLACE FUNCTION fr.is_normalized_title(
    word VARCHAR
    , groups VARCHAR DEFAULT 'ALL'   -- TITLE|TYPE|EXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_title BOOLEAN;
    _groups VARCHAR[];
BEGIN
    IF groups = 'ALL' THEN
        _groups := '{TITLE,TYPE,EXT}'::VARCHAR[];
    ELSE
        _groups := STRING_TO_ARRAY(groups, ',');
    END IF;

    SELECT EXISTS(
        SELECT 1
        FROM fr.laposte_address_street_keyword k
        WHERE
            ARRAY_POSITION(_groups, k.group) > 0
            AND
            k.name = is_normalized_title.word
    )
    INTO _is_title
    ;

    RETURN _is_title;
END
$func$ LANGUAGE plpgsql;

-- abbreviate holy word(s)
SELECT drop_all_functions_if_exists('fr', 'normalize_abbreviate_holy');
CREATE OR REPLACE FUNCTION fr.normalize_abbreviate_holy(
    name IN VARCHAR
)
RETURNS VARCHAR AS
$func$
BEGIN
    -- replace (SAINT|SAINTE)
    IF name LIKE '% SAINT' OR name LIKE '% SAINTE' THEN
        -- exception if it's the name itself
        RETURN name;
    END IF;
    -- as starting word
    IF name LIKE 'SAINT %' THEN
        name := CONCAT('ST ', SUBSTR(name, 7));
    ELSIF name LIKE 'SAINTE %' THEN
        name := CONCAT('STE ', SUBSTR(name, 8));
    END IF;
    -- else anywhere (but at the end)
    RETURN REPLACE(REPLACE(name, ' SAINTE ', ' STE '), ' SAINT ', ' ST ');

    /* NOTE
DECLARE
    _name VARCHAR;
    _name_normalized VARCHAR;
    _words TEXT[];
    _words_normalized VARCHAR[];
    _word_end VARCHAR;

     avoid REGEX because too expansive! in run-time
    _words := REGEXP_SPLIT_TO_ARRAY(_name, '\s+');
    FOR _i IN 1..ARRAY_LENGTH(_words, 1) LOOP
        IF _words[_i] ~* '^SAINT[E]?$' THEN
            -- exception if it's the name itself
            IF _i = 2 THEN
                IF _words[_i -1] ~* '^(LE|LA)$' THEN
                    _words_normalized := ARRAY_APPEND(_words_normalized, _words[_i]);
                    CONTINUE;
                END IF;
            END IF;
            _word_end := (REGEXP_MATCH(_words[_i], 'SAINT([E]?)', 'i'))[1];
            _words_normalized := ARRAY_APPEND(_words_normalized, CONCAT('ST', UPPER(_word_end)));
            CONTINUE;
        END IF;
        _words_normalized := ARRAY_APPEND(_words_normalized, _words[_i]);
    END LOOP;
    RETURN ARRAY_TO_STRING(_words_normalized, ' ');
     */
END
$func$ LANGUAGE plpgsql;

-- normalize name of municipality
SELECT public.drop_all_functions_if_exists('fr', 'normalize_municipality_name');
CREATE OR REPLACE FUNCTION fr.normalize_municipality_name(
    code VARCHAR
    , name VARCHAR
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _name_normalized VARCHAR;
    _name VARCHAR;
BEGIN
    -- deal w/ exceptions
    SELECT value
    INTO _name_normalized
    FROM fr.constant
    WHERE
        usecase = 'LAPOSTE_MUNICIPALITY_EXCEPTION'
        AND
        key = code
        ;
    IF FOUND THEN
        RETURN _name_normalized;
    END IF;

    -- only upper and not special characters
    _name := clean_address_label(name);
    -- replace (SAINT|SAINTE)
    RETURN fr.normalize_abbreviate_holy(_name);
END
$func$ LANGUAGE plpgsql;

/* TEST
-- municipality differences
SELECT *
FROM (
    SELECT
        za.co_insee_commune AS municipality_code
        , c.nom AS name
        , fr.normalize_municipality_name(c.insee_com, c.nom) AS name_normalized
        , za.lb_ach_nn AS name_normalized_laposte
    FROM
        fr.laposte_address_area za
            JOIN fr.ign_municipality c ON za.co_insee_commune = c.insee_com
    WHERE
        za.fl_active
        AND
        za.lb_l5_nn IS NULL
    ) t
WHERE
    name_normalized != name_normalized_laposte
ORDER BY
    1
    ;
 */

-- normalize name of street
SELECT public.drop_all_functions_if_exists('fr', 'normalize_street_name');
CREATE OR REPLACE FUNCTION fr.normalize_street_name(
    name VARCHAR
    , raise_notice IN BOOLEAN DEFAULT FALSE
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _name VARCHAR;
    _name_tmp VARCHAR;
    _name_rebuild BOOLEAN;
    _type VARCHAR;
    _type_abbreviated VARCHAR;
    _type_is_abbreviated BOOLEAN := TRUE;
    _type_diff INT;
    _len INT;
    _words TEXT[];
    _words_i INT := 0;
    _words_len INT;
    _words_rebuild BOOLEAN;
    _i INT;
    _j INT;
    _found BOOLEAN;
    _ABBR_HOLY INT               := 1;
    _ABBR_TYPE INT               := 2;
    _ABBR_FIRSTNAME INT          := 3;
    _DELETE_ARTICLE INT          := 4;
    _ABBR_TITLE INT              := 5;
    _steps INT[];
    _step_change BOOLEAN;
    _step_i INT := 1;
    _step_words TEXT[];
    _step_words_len INT;
    _steps_done BOOLEAN[];
    _titles VARCHAR[] :=
        ARRAY(SELECT key FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_TITLE');
    _titles_abbr VARCHAR[] :=
        ARRAY(SELECT value FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_TITLE');
    _titles_i INT;
    _titles_diff INT;
    _types VARCHAR[] :=
        ARRAY(SELECT type FROM fr.laposte_address_street_type);
    _types_abbr VARCHAR[] :=
        ARRAY(SELECT type_abbreviated FROM fr.laposte_address_street_type);
    _types_1st_word VARCHAR[] :=
        ARRAY(SELECT first_word FROM fr.laposte_address_street_type);
    _types_i INT;
BEGIN
    -- only upper and not special characters
    _name := clean_address_label(name);

    -- unabbreviated type of street
    SELECT type, type_abbreviated, type_is_abbreviated
    INTO _type, _type_abbreviated, _type_is_abbreviated
    FROM fr.get_type_of_street(_name)
    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
    IF _type_is_abbreviated THEN
        _name_tmp := REGEXP_REPLACE(_name, '^\S+', _type);
        IF LENGTH(_name_tmp) <= 32 THEN
            RETURN _name_tmp;
        END IF;
        _name := _name_tmp;
    END IF;

    -- already ok ?
    _len := LENGTH(_name);
    IF _len <= 32 THEN
        RETURN _name;
    END IF;

    IF raise_notice THEN RAISE NOTICE 'name=% len=%', _name, _len; END IF;
    _words := REGEXP_SPLIT_TO_ARRAY(_name, '\s+');
    _words_len := ARRAY_LENGTH(_words, 1);
    -- dynamic steps!
    _type_diff := LENGTH(COALESCE(_type, '')) - LENGTH(COALESCE(_type_abbreviated, ''));
    _steps := CASE
        -- abbreviate type suffisant?
        --WHEN ((_len - _type_diff) <= 32) AND (_type_diff > 2) THEN
        WHEN (_len - _type_diff) <= 32 THEN
            ARRAY[2,3,5,4,1]::INT[]
        WHEN
            (COALESCE(ARRAY_POSITION(_titles, _words[1]), 0) > 0
            AND
            COALESCE(ARRAY_POSITION(_types, _words[1]), 0) > 0) THEN
            ARRAY[3,5,4,2,1]::INT[]
        --WHEN _type_diff < 3 THEN
        ELSE
            ARRAY[3,5,2,4,1]::INT[]
    END;
    _steps_done := ARRAY_FILL(FALSE, ARRAY[ARRAY_LENGTH(_steps, 1)]);

    WHILE _len > 32 AND _step_i < ARRAY_LENGTH(_steps, 1) LOOP
        _step_change := TRUE;
        _found := FALSE;
        _name_rebuild := FALSE;
        _words_rebuild := FALSE;

        IF NOT _steps_done[_step_i] THEN
            IF raise_notice THEN RAISE NOTICE 'step: %', _steps[_step_i]; END IF;

            -- abbreviate title(s), if not type (of street!)
            IF _steps[_step_i] = _ABBR_TITLE THEN
                IF _words_i = 0 THEN _words_i := 1; END IF;
                FOR _i IN _words_i .. _words_len
                LOOP
                    -- TODO include types of street (into name, not at beginning)
                    IF _words[_i] = ANY(_titles) THEN
                        _titles_i := ARRAY_POSITION(_titles, _words[_i]);
                        IF raise_notice THEN RAISE NOTICE ' title=% i=% titles_i=% (_titles_abbr=%)', _words[_i], _i, _titles_i, _titles_abbr[_titles_i]; END IF;
                        _titles_diff := LENGTH(_words[_i]) - LENGTH(_titles_abbr[_titles_i]);
                        IF raise_notice THEN RAISE NOTICE ' titles_diff=% (type_diff=%)', _titles_diff, _type_diff; END IF;

                        /*
                        IF (NOT _steps_done[_ABBR_TYPE] AND (_titles_diff >= COALESCE(_type_diff, 0))) OR _steps_done[_ABBR_TYPE] THEN
                         */
                            _types_i := ARRAY_POSITION(
                                _types
                                , public.items_of_array_to_string(
                                    elements => _words
                                    , to_ => _i
                                )
                            );
                            IF raise_notice THEN RAISE NOTICE ' types_i=%', _types_i; END IF;
                            -- replace title if not type too
                            IF COALESCE(_types_i, 0) = 0  AND _titles_i > 0 THEN
                                _found := TRUE;
                                _words_i := _i;
                                IF raise_notice THEN RAISE NOTICE ' title (at %) replaced', _words_i; END IF;
                                EXIT;
                            END IF;
                        /*
                        END IF;
                         */
                    END IF;
                END LOOP;
                IF _found THEN
                    _len := _len - _titles_diff;
                    _words[_words_i] := _titles_abbr[_titles_i];
                    IF raise_notice THEN RAISE NOTICE ' words=% len=%', _words, _len; END IF;
                    _name_rebuild := TRUE;
                    _step_change := FALSE;
                END IF;

            -- abbreviate type of street
            ELSIF _steps[_step_i] = _ABBR_TYPE THEN
                IF NOT _type_is_abbreviated AND _type_abbreviated IS NOT NULL THEN
                    _name := CONCAT(_type_abbreviated, SUBSTR(_name, LENGTH(_type) +1));
                    _len := LENGTH(_name);
                    IF raise_notice THEN RAISE NOTICE ' name=% len=%', _name, _len; END IF;
                    _words_rebuild := TRUE;
                END IF;

            -- abbreviate holy word(s)
            ELSIF _steps[_step_i] = _ABBR_HOLY THEN
                _name := fr.normalize_abbreviate_holy(_name);
                _len := LENGTH(_name);
                IF raise_notice THEN RAISE NOTICE ' name=% len=%', _name, _len; END IF;
                _words_rebuild := TRUE;

            -- abbreviate firstname
            ELSIF _steps[_step_i] = _ABBR_FIRSTNAME THEN
                --IF _words_i = 0 THEN _words_i := 1; END IF;
                _words_i := _words_i +1;
                -- firstname can't be last word!
                FOR _i IN _words_i .. _words_len -1
                LOOP
                    IF raise_notice THEN RAISE NOTICE ' search firstname : %', _words[_i]; END IF;
                    _found := fr.is_normalized_firstname(_words[_i]);
                    IF _found THEN
                        _words_i := _i;
                        IF raise_notice THEN RAISE NOTICE ' firstname (at %)', _words_i; END IF;
                        EXIT;
                    END IF;
                END LOOP;
                IF _found THEN
                    _len := _len - LENGTH(_words[_words_i]) +1;                    _words[_words_i] := SUBSTR(_words[_words_i], 1, 1);
                    IF raise_notice THEN RAISE NOTICE ' words=% len=%', _words, _len; END IF;
                    _name_rebuild := TRUE;
                    _step_change := FALSE;
                END IF;

            -- delete _article(s)
            ELSIF _steps[_step_i] = _DELETE_ARTICLE THEN
                IF raise_notice THEN RAISE NOTICE ' words=% len=%', _words, _len; END IF;
                IF _words_i = 0 THEN
                    _words_i := 1;
                    _name_tmp := _name;
                    _name := '';
                ELSE
                    _name := _words[1];
                    FOR _i IN 2 .. _words_i -1
                    LOOP
                        _name := CONCAT(_name, ' ', _words[_i]);
                    END LOOP;
                END IF;
                IF raise_notice THEN RAISE NOTICE ' search article from : %', _words_i; END IF;

                FOR _i IN _words_i .. _words_len
                LOOP
                    _found := fr.is_normalized_article(_words[_i]);
                    IF _found THEN
                        IF raise_notice THEN RAISE NOTICE ' article=% i=%', _words[_i], _i; END IF;
                        _words_i := _i;
                        EXIT;
                    ELSE
                        IF LENGTH(_name) > 0 THEN
                            _name := CONCAT(_name, ' ', _words[_i]);
                        ELSE
                            _name := _words[_i];
                        END IF;
                    END IF;
                END LOOP;
                IF _found THEN
                    _step_change := FALSE;
                    -- delete array item
                    -- https://dba.stackexchange.com/questions/94639/delete-array-element-by-index
                    _words := _words[:_words_i-1] || _words[_words_i+1:];
                    _words_len := _words_len -1;
                    IF raise_notice THEN RAISE NOTICE ' name=%', _name; END IF;
                    FOR _j IN _words_i .. _words_len
                    LOOP
                        _name := CONCAT(_name, ' ', _words[_j]);
                        IF raise_notice THEN RAISE NOTICE ' name=%', _name; END IF;
                    END LOOP;
                END IF;

                _len := LENGTH(_name);
                IF raise_notice THEN RAISE NOTICE ' words=% len=%', _words, _len; END IF;
            END IF;
        END IF;

        IF _name_rebuild THEN
            _name := ARRAY_TO_STRING(_words, ' ');
        END IF;
        IF _words_rebuild THEN
            _words := REGEXP_SPLIT_TO_ARRAY(_name, '\s+');
            _words_len := ARRAY_LENGTH(_words, 1);
        END IF;
        IF _step_change THEN
            _steps_done[_step_i] := TRUE;
            _step_words := _words;
            _step_words_len := _words_len;
            _step_i := _step_i +1;
            _words_i := 0;
        END IF;
    END LOOP;

    RETURN _name;
END
$func$ LANGUAGE plpgsql;

/* TEST
-- street differences
SELECT *
FROM (
    SELECT
        co_cea code
        , lb_voie name
        , fr.normalize_street_name(lb_voie) AS name_normalized
        , lb_voie_normalise AS name_normalized_laposte
    FROM
        fr.laposte_address_street
    WHERE
        lb_voie_normalise IS DISTINCT FROM lb_voie
    LIMIT
        100
    ) t
WHERE
    name_normalized != name_normalized_laposte
ORDER BY
    1
    ;
 */

-- normalize one address
SELECT drop_all_functions_if_exists('fr', 'normalize_address');
CREATE OR REPLACE FUNCTION fr.normalize_address(
    address IN RECORD                   -- address to normalize
    , columns_map IN HSTORE             -- mapping address(client)/address(reference)
)
RETURNS fr.address_normalized AS
$func$
DECLARE
    _address_normalized fr.address_normalized;
    _column_map VARCHAR[];
    _geom GEOMETRY;
    _geom_x DOUBLE PRECISION;
    _geom_y DOUBLE PRECISION;
    _geom_srid SMALLINT;
    _geom_srid_default SMALLINT := 2154;
    _cadastre_parcel_number VARCHAR;
    _cadastre_parcel_section VARCHAR;
    _cadastre_parcel_prefix CHAR(3);
    _street_type_is_abbreviated BOOLEAN;
    _exists BOOLEAN;
BEGIN
    FOREACH _column_map SLICE 1 IN ARRAY %# columns_map LOOP
        _column_map[2] := CONCAT('$1.', _column_map[2]);
        BEGIN
            CASE _column_map[1]
                WHEN 'id' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.id
                        USING address;
                WHEN 'complement' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.complement
                        USING address;
                    _address_normalized.complement := NULLIF(TRIM(public.clean_address_label(_address_normalized.complement)), '');
                WHEN 'housenumber' THEN
                    EXECUTE CONCAT('SELECT NULLIF(TRIM(', _column_map[2], '::TEXT), '''')::INTEGER')
                        INTO _address_normalized.housenumber
                        USING address;
                    --SELECT '33' ~ '^[0-9]*$'
                    --A ETUDIER : ne permet plus de forcer la recherche d'un numéro si activé
                    --_address_normalized.housenumber := NULLIF(_address_normalized.housenumber, 0);
                    IF _address_normalized.housenumber::VARCHAR !~ '^[0-9]*$' THEN
                        RAISE NOTICE 'Numéro de voie ignoré car invalide : %', _address_normalized.housenumber;
                        _address_normalized.housenumber := NULL;
                    END IF;
                WHEN 'housenumber_extension' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.housenumber_extension
                        USING address;
                    _address_normalized.housenumber_extension := NULLIF(TRIM(public.clean_address_label(_address_normalized.housenumber_extension)), '');
                WHEN 'street' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.street
                        USING address;

                    _address_normalized.street := NULLIF(TRIM(public.clean_address_label(_address_normalized.street)), '');

                    SELECT type, type_abbreviated, type_is_abbreviated
                    INTO _address_normalized.street_type
                        , _address_normalized.street_type_short
                        , _street_type_is_abbreviated
                    FROM fr.get_type_of_street(_address_normalized.street)
                    AS (type VARCHAR, type_abbreviated VARCHAR, type_is_abbreviated BOOLEAN);
                    IF _street_type_is_abbreviated THEN
                        _address_normalized.street := REGEXP_REPLACE(_address_normalized.street, '^\S+', _address_normalized.street_type);
                    END IF;
                WHEN 'municipality_code' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.municipality_code
                        USING address;

                    SELECT EXISTS(
                        SELECT 1 FROM fr.laposte_address_area
                        WHERE co_insee_commune = _address_normalized.municipality_code
                        AND fl_active)
                    INTO _exists;
                    IF NOT _exists THEN
                        RAISE NOTICE 'Code INSEE commune ignoré car invalide : %', _address_normalized.municipality_code;
                        _address_normalized.municipality_code := NULL;
                    END IF;
                WHEN 'postcode' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.postcode
                        USING address;
                    SELECT EXISTS(
                        SELECT 1 FROM fr.laposte_address_area
                        WHERE co_postal = _address_normalized.postcode
                        AND fl_active)
                    INTO _exists;
                    IF NOT _exists THEN
                        RAISE NOTICE 'Code Postal commune ignoré car invalide : %', _address_normalized.postcode;
                        _address_normalized.postcode := NULL;
                    END IF;
                WHEN 'municipality_name' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.municipality_name
                        USING address;
                -- TODO à intégrer dans lb_ligneX
                -- mention CEDEX OU libellé Ancienne Commune OU les 2 accollées
                -- RE=^((BP|CS|CE|CP) *[0-9]+)? *([A-Z ]+)?$
                WHEN 'municipality_old_name' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.municipality_old_name
                        USING address;

                WHEN 'geo_xy' THEN
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',1)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',2)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                WHEN 'geo_latlon' THEN
                    -- latitude = Y, longitude = X
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',2)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',1)::VARCHAR, '','' ,''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                    _geom_srid_default := 4326;
                WHEN 'geo_x' THEN
                    EXECUTE CONCAT('SELECT REPLACE(', _column_map[2], '::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                WHEN 'geo_y' THEN
                    EXECUTE CONCAT('SELECT REPLACE(', _column_map[2], '::VARCHAR, '','' ,''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                WHEN 'geo_srid' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2], '::SMALLINT')
                        INTO _geom_srid
                        USING address;
                WHEN 'geo_wkt' THEN
                    EXECUTE CONCAT('SELECT ST_PointFromText(', _column_map[2], ')')
                        INTO _geom
                        USING address;
                WHEN 'geo_json' THEN
                    EXECUTE CONCAT('SELECT ST_GeomFromGeoJSON(', _column_map[2], ')')
                        INTO _geom
                        USING address;
                WHEN 'geo' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _geom
                        USING address;

                WHEN 'cadastre_parcel_number' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2], '::INTEGER::VARCHAR')
                        INTO _cadastre_parcel_number
                        USING address;
                WHEN 'cadastre_parcel_section' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _cadastre_parcel_section
                        USING address;
                    --On enlève les éventuel 0 préfixant l'identifiant de section cadastrale
                    --Alternative : ne prendre que les lettre alphabéthiques ?
                    _cadastre_parcel_section := REPLACE(_cadastre_parcel_section, '0', '');
                WHEN 'cadastre_parcel_prefix' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _cadastre_parcel_prefix
                        USING address;
            ELSE
                RAISE NOTICE 'Attribut % ignoré car inconnu', _column_map[1];
            END CASE;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Attribut % ignoré car provoquant une erreur à l''évaluation de % : %', _column_map[1], _column_map[2], SQLERRM;
        END;
    END LOOP;

    IF _address_normalized.id IS NULL THEN
        RAISE 'Vous devez spécifier un code identifiant de l''adresse';
    END IF;

    _address_normalized.municipality_name := fr.normalize_municipality_name(
        code => _address_normalized.municipality_code
        , name => _address_normalized.municipality_name
    );
    IF _address_normalized.municipality_old_name IS NOT NULL THEN
        _address_normalized.municipality_old_name := fr.normalize_municipality_name(
            name => _address_normalized.municipality_old_name
        );
    END IF;
    IF _address_normalized.municipality_code IS NULL AND _address_normalized.municipality_name IS NOT NULL THEN
        BEGIN
            SELECT DISTINCT co_insee_commune
            INTO _address_normalized.municipality_code
            FROM fr.laposte_address_area
            WHERE lb_ach_nn = _address_normalized.municipality_name AND fl_active;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Déduction code Commune à partir du nom % provoquant une erreur : %', _address_normalized.municipality_name,  SQLERRM;
        END;
    END IF;
    IF _address_normalized.municipality_name IS NULL AND _address_normalized.municipality_code IS NOT NULL THEN
        BEGIN
            SELECT DISTINCT lb_ach_nn
            INTO _address_normalized.municipality_name
            FROM fr.laposte_address_area
            WHERE co_insee_commune = _address_normalized.municipality_code AND fl_active;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Déduction libellé Commune à partir du code % provoquant une erreur : %', _address_normalized.municipality_code,  SQLERRM;
        END;
    END IF;

    IF _geom IS NULL
    AND _geom_x IS NOT NULL
    AND _geom_y IS NOT NULL THEN
        _geom := ST_MakePoint(_geom_x,_geom_y);
    END IF;

    IF _geom IS NOT NULL THEN
        IF ST_SRID(_geom) = 0 THEN
            _geom := ST_SetSRID(_geom, COALESCE(_geom_srid, _geom_srid_default));
        END IF;
        IF NOT public.is_valid_geometry_in_SRID_bounds(_geom) THEN
            RAISE NOTICE 'Coordonnées en dehors des limites du système de projection : %, SRID %', ST_AsText(_geom), ST_SRID(_geom);
        ELSE
            _address_normalized.geom := ST_Transform(_geom, 3857);
        END IF;
    END IF;

    _address_normalized.level :=
    CASE
        WHEN _address_normalized.complement IS NOT NULL THEN 'L3'
        WHEN _address_normalized.housenumber IS NOT NULL THEN 'NUMERO'
        WHEN _address_normalized.street IS NOT NULL THEN 'VOIE'
        WHEN _address_normalized.municipality_code IS NOT NULL THEN 'ZA'
    END;

    IF _address_normalized.postcode IS NOT NULL
    OR _address_normalized.municipality_code IS NOT NULL
    OR _address_normalized.municipality_name IS NOT NULL
    THEN
        _address_normalized._order_code_area := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name));
        IF _address_normalized.street IS NOT NULL THEN
            _address_normalized._order_code_street := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name, _address_normalized.street));
            IF _address_normalized.housenumber IS NOT NULL THEN
                _address_normalized._order_code_housenumber := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name, _address_normalized.street, _address_normalized.housenumber, _address_normalized.housenumber_extension));
            END IF;
            IF _address_normalized.complement IS NOT NULL THEN
                _address_normalized._order_code_complement := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name, _address_normalized.street, _address_normalized.housenumber, _address_normalized.housenumber_extension, _address_normalized.complement));
            END IF;
        END IF;
    END IF;

    /*
    -- calcul mot directeur, si absent
    IF _address_normalized.lb_voie_mot_directeur IS NULL AND _address_normalized.street IS NOT NULL THEN
        _address_normalized.lb_voie_mot_directeur := getVoieMotDirecteur(_address_normalized.street);
    END IF;
     */

    RETURN _address_normalized;
END
$func$ LANGUAGE plpgsql;
