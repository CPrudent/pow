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
            WHEN UPPER(_only[_i]) = 'DATE' THEN
                (
                    -- UK dates
                    word ~ '^([2-9][0-9]*)?(1ST|2ND|3RD|([4-9]|[1-9]+[0-9]*0?)TH)$'
                )
                OR
                (
                    -- FR dates
                    word ~ '^(1(ERE?)?|([2-9][0-9]*|1[0-9]+)*I?(E|EME)?)$'
                )
            WHEN UPPER(_only[_i]) = 'HOUSENUMBER' THEN (word ~ CONCAT('^[0-9]+(', _re, ')$'))
            WHEN UPPER(_only[_i]) = 'ROMAN' THEN (word ~ '^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')
            WHEN UPPER(_only[_i]) = 'ROAD_NETWORK' THEN (word ~ '^(A|B|CD?|CR|D|E?V|GR?|N|R|RD|RN|S|T|VC)?([0-9]+(E[0-9]*)?|E[0-9]*)$')
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

-- abbreviate keyword
SELECT drop_all_functions_if_exists('fr', 'normalize_abbreviate_keyword');
CREATE OR REPLACE FUNCTION fr.normalize_abbreviate_keyword(
    name IN VARCHAR
    , iter IN INTEGER DEFAULT 1
    , words IN TEXT[] DEFAULT NULL
    , name_abbreviated OUT VARCHAR
    , one_more_time OUT BOOLEAN
)
AS
$func$
DECLARE
    _name_abbreviated VARCHAR;
    _one_more_time BOOLEAN := FALSE;
    _words TEXT[];
    _nwords INT;
    _i INT;
BEGIN
    SELECT
        k.name_abbreviated
    INTO
        _name_abbreviated
    FROM
        fr.laposte_address_street_keyword k
    WHERE
        k.name = normalize_abbreviate_keyword.name
        AND
        k.name_abbreviated IS NOT NULL
    ;
    IF NOT FOUND THEN
        IF words IS NULL THEN
            RAISE 'renseigner les mots du mot-clé (%) via option words', name;
        END IF;
        _nwords := count_words(name);
        IF iter <= _nwords THEN
            _one_more_time := (iter < _nwords);
            FOR _i IN 1 .. _nwords
            LOOP
                IF _i <= iter THEN
                    SELECT
                        k.name_abbreviated
                    INTO
                        _name_abbreviated
                    FROM
                        fr.laposte_address_street_keyword k
                    WHERE
                        k.name = words[_i]
                    ;
                    _words := ARRAY_APPEND(_words, COALESCE(_name_abbreviated, words[_i]));
                ELSE
                    _words := ARRAY_APPEND(_words, words[_i]);
                END IF;
            END LOOP;
            _name_abbreviated := ARRAY_TO_STRING(_words, ' ');
        --ELSE
        END IF;
    END IF;
    one_more_time := _one_more_time;
    name_abbreviated := _name_abbreviated;
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
    name IN VARCHAR
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , name_normalized OUT VARCHAR
    , descriptors OUT VARCHAR
)
AS
$func$
DECLARE
    _name VARCHAR;
    _len INT;
    _len_normalized INT;
    _words TEXT[];
    _words_abbreviated TEXT[];
    _words_todo TEXT[];
    _words_normalized TEXT[];
    _words_len INT;
    _words_normalized_len INT;
    _descriptors TEXT[];
    _descriptor VARCHAR;

    _i INT;
    _j INT;
    _nchanges INT := 0;
    _position INT;
    _position_a INT := 1;
    _position_p INT := 1;
    _position_t INT := 1;
    _position_v INT := 1;
    _positions_a INT[];
    _positions_p INT[];
    _positions_t INT[];
    _positions_v INT[];
    _earn_sz_a INT[];
    _earn_sz_p INT[];
    _earn_sz_t INT[];
    _earn_sz_v INT[];
    _done_a BOOLEAN[];
    _done_p BOOLEAN[];
    _done_t BOOLEAN[];
    _done_v BOOLEAN[];
    _words_t TEXT[];
    _more_t BOOLEAN[];
    _again_t BOOLEAN;
    _factor INT;

    _tmp_t VARCHAR;
    _tmp_v VARCHAR;
    _tmp_name VARCHAR;
BEGIN
    -- only upper and not special characters
    _name := clean_address_label(name);
    _len := LENGTH(_name);
    IF raise_notice THEN RAISE NOTICE 'N=% #=%', _name, _len; END IF;

    -- descriptors, words (by descriptor)
    SELECT
        ds.descriptors
        , ds.words_by_descriptor
        , ds.words_abbreviated_by_descriptor
        , ds.words_todo_by_descriptor
    INTO
        normalize_street_name.descriptors
        , _words
        , _words_abbreviated
        , _words_todo
    FROM
        fr.get_descriptors_of_street(
            name => _name
            , with_abbreviation => TRUE
        ) ds;
    _words_len := ARRAY_LENGTH(_words, 1);
    -- descriptors as array
    SELECT
        as_array
    INTO
        _descriptors
    FROM
        fr.split_descriptors_as_array(
            descriptors => normalize_street_name.descriptors
            , words => _words
            , nwords => _words_len
        ) da;

    -- store position(s) for each word of each descriptor
    FOR _i IN 1 .. _words_len
    LOOP
        IF _descriptors[_i] ~ 'A' THEN
            _positions_a := ARRAY_APPEND(_positions_a, _i);
        --ELSIF _descriptors[_i] ~ 'N' THEN
        ELSIF _descriptors[_i] ~ 'P' THEN
            _positions_p := ARRAY_APPEND(_positions_p, _i);
        ELSIF _descriptors[_i] ~ 'T' THEN
            _positions_t := ARRAY_APPEND(_positions_t, _i);
        ELSIF _descriptors[_i] ~ 'V' THEN
            _positions_v := ARRAY_APPEND(_positions_v, _i);
        END IF;
    END LOOP;

    -- eval earnings for each word of each descriptor
    IF _positions_a IS NOT NULL THEN
        _nchanges := _nchanges + ARRAY_LENGTH(_positions_a, 1);
        FOR _i IN 1 .. ARRAY_LENGTH(_positions_a, 1)
        LOOP
            -- delete article : don't forget to count space (as separator)!
            _earn_sz_a[_i] := LENGTH(_words[_positions_a[_i]]) +1;
        END LOOP;
        _done_a := ARRAY_FILL(FALSE, ARRAY[ARRAY_LENGTH(_positions_a, 1)]);
    END IF;
    IF _positions_p IS NOT NULL THEN
        _nchanges := _nchanges + ARRAY_LENGTH(_positions_p, 1);
        FOR _i IN 1 .. ARRAY_LENGTH(_positions_p, 1)
        LOOP
            -- remain 1st letter only
            _earn_sz_p[_i] := LENGTH(_words[_positions_p[_i]]) -1;
        END LOOP;
        _done_p := ARRAY_FILL(FALSE, ARRAY[ARRAY_LENGTH(_positions_p, 1)]);
    END IF;
    IF _positions_t IS NOT NULL THEN
        _nchanges := _nchanges + ARRAY_LENGTH(_positions_t, 1);
        FOR _i IN 1 .. ARRAY_LENGTH(_positions_t, 1)
        LOOP
            IF _words_abbreviated[_positions_t[_i]] IS NULL THEN
                _words_t := REGEXP_SPLIT_TO_ARRAY(_words[_positions_t[_i]], '\s+');
                SELECT name_abbreviated, one_more_time
                INTO _tmp_t, _again_t
                FROM fr.normalize_abbreviate_keyword(
                    name => _words[_positions_t[_i]]
                    , words => _words_t
                );
                _words_abbreviated[_positions_t[_i]] := _tmp_t;
                _more_t[_i] := _again_t;
            END IF;
            -- replace w/ abbreviation
            _earn_sz_t[_i] := LENGTH(_words[_positions_t[_i]]) - LENGTH(_words_abbreviated[_positions_t[_i]]);
            _factor := CASE
                WHEN _words_todo[_positions_t[_i]] = '-' THEN 1
                ELSE -1
                END
            ;
            _earn_sz_t[_i] := _earn_sz_t[_i] * _factor;
        END LOOP;
        _done_t := ARRAY_FILL(FALSE, ARRAY[ARRAY_LENGTH(_positions_t, 1)]);
    END IF;
    IF _positions_v IS NOT NULL THEN
        _nchanges := _nchanges + ARRAY_LENGTH(_positions_v, 1);
        FOR _i IN 1 .. ARRAY_LENGTH(_positions_v, 1)
        LOOP
            -- replace w/ abbreviation
            _earn_sz_v[_i] := LENGTH(_words[_positions_v[_i]]) - LENGTH(_words_abbreviated[_positions_v[_i]]);
            _factor := CASE
                WHEN _words_todo[_positions_t[_i]] = '-' THEN 1
                ELSE -1
                END
            ;
            _earn_sz_v[_i] := _earn_sz_v[_i] * _factor;
        END LOOP;
        _done_v := ARRAY_FILL(FALSE, ARRAY[ARRAY_LENGTH(_positions_v, 1)]);
    END IF;

    _words_normalized := _words;
    _words_normalized_len := _words_len;
    _len_normalized := (
        SELECT SUM(LENGTH(w)) FROM UNNEST(_words_normalized) w
    ) + (_words_len -1);
    FOR _i IN 1 .. _nchanges
    LOOP
        WITH
        earn_descriptor(descriptor, earn, i) AS (
            SELECT
                'A' descriptor
                , o.earn
                , o.i
                , _done_a[o.i] done
            FROM
                UNNEST(_earn_sz_a) WITH ORDINALITY AS o(earn, i)
            UNION
            SELECT
                'P'
                , o.earn
                , o.i
                , _done_p[o.i] done
            FROM
                UNNEST(_earn_sz_p) WITH ORDINALITY AS o(earn, i)
            UNION
            SELECT
                'T'
                , o.earn
                , o.i
                , _done_t[o.i] done
            FROM
                UNNEST(_earn_sz_t) WITH ORDINALITY AS o(earn, i)
            UNION
            SELECT
                'V'
                , o.earn
                , o.i
                , _done_v[o.i] done
            FROM
                UNNEST(_earn_sz_v) WITH ORDINALITY AS o(earn, i)
            ORDER BY
                earn DESC
            /*
            VALUES
                ('A', _earn_sz_a[_position_a])
                , ('P', _earn_sz_p[_position_p])
                , ('T', _earn_sz_t[_position_t])
                , ('V', _earn_sz_v[_position_v])
             */
        )
        SELECT
            descriptor
            , i
        INTO
            _descriptor
            , _position
        FROM
            earn_descriptor
        WHERE
            NOT done
        ORDER BY
            earn DESC
        LIMIT
            1
        ;
        IF _descriptor IS NULL THEN
            RAISE 'changement %/% non trouvé!', _i, _nchanges;
        ELSE
            IF raise_notice THEN RAISE NOTICE 'changement %/% : %', _i, _nchanges, _descriptor; END IF;
        END IF;

        IF _descriptor = 'A' THEN
            -- https://dba.stackexchange.com/questions/94639/delete-array-element-by-index
            _words_normalized := CASE
                WHEN _positions_a[_position] > 1 THEN _words_normalized[:_positions_a[_position]-1] || _words_normalized[_positions_a[_position]+1:]
                ELSE _words_normalized[_positions_a[_position]+1:]
                END
            ;
            _words_normalized_len := _words_normalized_len -1;
            _len_normalized := _len_normalized - _earn_sz_a[_position];
            _done_a[_position] := TRUE;
            FOR _j IN _position +1 .. ARRAY_LENGTH(_positions_a, 1)
            LOOP
                -- update position (following delete)!
                _positions_a[_j] := _positions_a[_j] -1;
            END LOOP;
        ELSIF _descriptor = 'P' THEN
            _words_normalized[_positions_p[_position]] := SUBSTR(_words_normalized[_positions_p[_position]], 1, 1);
            _len_normalized := _len_normalized - _earn_sz_p[_position];
            _done_p[_position] := TRUE;
        ELSIF _descriptor = 'T' THEN
            _words_normalized[_positions_t[_position]] := _words_abbreviated[_positions_t[_position]];
            _len_normalized := _len_normalized - _earn_sz_t[_position];
            _done_t[_position] := TRUE;
        ELSIF _descriptor = 'V' THEN
            _words_normalized[_positions_v[_position]] := _words_abbreviated[_positions_v[_position]];
            _len_normalized := _len_normalized - _earn_sz_v[_position];
            _done_v[_position] := TRUE;
        END IF;

        IF raise_notice THEN RAISE NOTICE 'NN=% #=%', _words_normalized, _len_normalized; END IF;
        IF _len_normalized <= 32 THEN
            name_normalized := ARRAY_TO_STRING(_words_normalized, ' ');
            RETURN;
        END IF;
    END LOOP;

    name_normalized := NULL;
    RAISE NOTICE 'pas de normalisation (%) : NN=% #=%', name, _words_normalized, _len_normalized;
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
