/***
 * FR: add LAPOSTE/RAN street keywords
 */

-- to store keywords
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_keyword (
    "group" VARCHAR NOT NULL
    , name VARCHAR NOT NULL
    , name_abbreviated VARCHAR
    , first_word VARCHAR
    , occurs INT
)
;

SELECT drop_all_functions_if_exists('fr', 'get_keyword_of_street');
CREATE OR REPLACE FUNCTION fr.get_keyword_of_street(
    name VARCHAR                   -- name of street
    , at_ INT DEFAULT 1
    , words TEXT[] DEFAULT NULL
    , group_ VARCHAR DEFAULT NULL
)
RETURNS RECORD AS
$func$
DECLARE
    _word VARCHAR;
    _kw RECORD;
    _exists BOOLEAN := TRUE;
    _found BOOLEAN := FALSE;
    _begin VARCHAR;
BEGIN
    IF group_ != 'TYPE' AND words IS NULL THEN
        RAISE 'recherche mot clé VOIE nécessite: liste des mots de la voie (TITLE,EXT)';
    END IF;

    _word :=
        CASE WHEN group_ = 'TYPE' AND at_ = 1 THEN (REGEXP_MATCH(name, '^\S+'))[1]
        ELSE words[at_]
        END;

    --RAISE NOTICE 'name=% word1=%', name, _word;

    SELECT *
    INTO _kw
    FROM fr.laposte_address_street_keyword k
    WHERE k.name_abbreviated = _word
    AND k.group = group_
    ORDER BY k.occurs DESC
    LIMIT 1;
    IF FOUND THEN
        --RAISE NOTICE 'type=%', _kw;
        IF _kw.name != _kw.name_abbreviated THEN
            RETURN (_kw.name, _kw.name_abbreviated, TRUE);
        END IF;
    ELSE
        SELECT EXISTS(
            SELECT 1 FROM fr.laposte_address_street_keyword k
            WHERE k.first_word = _word
            AND k.group = group_
        ) INTO _exists;
    END IF;

    _begin :=
        CASE WHEN group_ = 'TYPE' AND at_ = 1 THEN NULL
        ELSE public.items_of_array_to_string(
                elements => words
                , from_ => 1
                , to_ => at_ -1
            )
        END;
    IF _begin IS NOT NULL THEN
        _begin := CONCAT(_begin, ' ');
    END IF;

    IF _exists THEN
        FOR _kw IN (
            SELECT * FROM fr.laposte_address_street_keyword k
            WHERE k.first_word = _word
            AND k.group = group_
            -- keyword composed by many words (decreasing order)
            ORDER BY (LENGTH(k.name) - LENGTH(REPLACE(k.name, ' ', ''))) DESC
        )
        LOOP
            IF name ~ CONCAT('^', _begin, _kw.name, ' ?') THEN
                _found := TRUE;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    IF NOT _found THEN
        RETURN (NULL::VARCHAR, NULL::VARCHAR, FALSE);
    ELSE
        RETURN (_kw.name, _kw.name_abbreviated
            , (_word IS NOT DISTINCT FROM _kw.name_abbreviated) AND (_kw.name != _kw.name_abbreviated)
        );
    END IF;
END
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fr.get_keyword_of_street(
    name VARCHAR
    , groups VARCHAR[]
    , at_ INT DEFAULT 1
    , words TEXT[] DEFAULT NULL
)
RETURNS RECORD AS
$func$
DECLARE
    _kw VARCHAR;
    _kw_abbreviated VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _i INT;
BEGIN
    IF groups IS NULL THEN
        RAISE 'recherche mot clé VOIE nécessite: liste des mots-clé (TYPE,TITLE,EXT)';
    END IF;

    FOR _i IN 1 .. ARRAY_LENGTH(groups, 1)
    LOOP
        SELECT kw, kw_abbreviated, kw_is_abbreviated
        INTO _kw, _kw_abbreviated, _kw_is_abbreviated
        FROM fr.get_keyword_of_street(
            name => name
            , at_ => at_
            , words => words
            , group_ => groups[_i]
        )
        AS (kw VARCHAR, kw_abbreviated VARCHAR, kw_is_abbreviated BOOLEAN);

        IF _kw IS NOT NULL THEN
            RETURN (_kw, _kw_abbreviated, _kw_is_abbreviated);
        END IF;
    END LOOP;
    RETURN (NULL::VARCHAR, NULL::VARCHAR, FALSE);
END
$func$ LANGUAGE plpgsql;
