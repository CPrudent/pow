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
    IF NOT (group_ = 'TYPE' AND at_ = 1) AND words IS NULL THEN
        RAISE 'recherche mot clé VOIE nécessite: liste des mots de la voie (TITLE,EXT,TYPE)';
    END IF;

    _word :=
        -- 1st word, eventually abbreviated
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
        --RAISE NOTICE 'kw=%', _kw;
        IF _kw.name != _kw.name_abbreviated THEN
            RETURN (group_, _kw.name, _kw.name_abbreviated, TRUE, public.count_words(_kw.name));
        END IF;
    ELSE
        SELECT EXISTS(
            SELECT 1 FROM fr.laposte_address_street_keyword k
            WHERE COALESCE(k.first_word, k.name) = _word
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
            WHERE COALESCE(k.first_word, k.name) = _word
            AND k.group = group_
            AND (
                ((group_ = 'EXT') AND (LENGTH(_word) > 1))
                OR
                (group_ != 'EXT')
            )
            -- keyword composed by many words (decreasing order)
            ORDER BY public.count_words(k.name) DESC
        )
        LOOP
            IF name ~ CONCAT('^', _begin, _kw.name, ' ?') THEN
                _found := TRUE;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    IF NOT _found THEN
        RETURN (group_, NULL::VARCHAR, NULL::VARCHAR, FALSE, NULL::INT);
    ELSE
        RETURN (group_, _kw.name, _kw.name_abbreviated
            , (_word IS NOT DISTINCT FROM _kw.name_abbreviated) AND (_kw.name != _kw.name_abbreviated)
            , public.count_words(_kw.name)
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
    _kw_group VARCHAR;
    _kw VARCHAR;
    _kw_abbreviated VARCHAR;
    _kw_is_abbreviated BOOLEAN;
    _kw_nwords INT;
    _i INT;
BEGIN
    IF groups IS NULL THEN
        RAISE 'recherche mot clé VOIE nécessite: liste des mots-clé {TYPE,TITLE,EXT}';
    END IF;

    FOR _i IN 1 .. ARRAY_LENGTH(groups, 1)
    LOOP
        SELECT kw_group, kw, kw_abbreviated, kw_is_abbreviated, kw_nwords
        INTO _kw_group, _kw, _kw_abbreviated, _kw_is_abbreviated, _kw_nwords
        FROM fr.get_keyword_of_street(
            name => name
            , at_ => at_
            , words => words
            , group_ => groups[_i]
        )
        AS (kw_group VARCHAR, kw VARCHAR, kw_abbreviated VARCHAR, kw_is_abbreviated BOOLEAN, kw_nwords INT);

        IF _kw IS NOT NULL THEN
            RETURN (_kw_group, _kw, _kw_abbreviated, _kw_is_abbreviated, _kw_nwords);
        END IF;
    END LOOP;
    RETURN (NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, FALSE, NULL::INT);
END
$func$ LANGUAGE plpgsql;
