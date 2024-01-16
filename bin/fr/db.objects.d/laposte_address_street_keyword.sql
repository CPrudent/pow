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
    name IN VARCHAR                   -- name of street
    , group_ IN VARCHAR DEFAULT NULL
    , at_ IN INT DEFAULT 1
    , words IN TEXT[] DEFAULT NULL
    , with_abbreviation IN BOOLEAN DEFAULT TRUE
    , kw_group OUT VARCHAR
    , kw OUT VARCHAR
    , kw_abbreviated OUT VARCHAR
    , kw_is_abbreviated OUT BOOLEAN
    , kw_nwords OUT INT
)
AS
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

    kw_group := group_;
    _word :=
        -- 1st word, eventually abbreviated
        CASE WHEN group_ = 'TYPE' AND at_ = 1 THEN (REGEXP_MATCH(name, '^\S+'))[1]
        ELSE words[at_]
        END;

    --RAISE NOTICE 'name=% word1=%', name, _word;

    IF with_abbreviation THEN
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
                kw := _kw.name;
                kw_abbreviated := _kw.name_abbreviated;
                kw_is_abbreviated := TRUE;
                kw_nwords := count_words(_kw.name);
                RETURN;
            END IF;
        END IF;
    END IF;

    SELECT EXISTS(
        SELECT 1 FROM fr.laposte_address_street_keyword k
        WHERE COALESCE(k.first_word, k.name) = _word
        AND k.group = group_
    ) INTO _exists;
    _begin :=
        CASE WHEN group_ = 'TYPE' AND at_ = 1 THEN NULL
        ELSE items_of_array_to_string(
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
            -- exclude one-char EXT keywords (A..Z)
            AND (
                ((group_ = 'EXT') AND (LENGTH(k.name) > 1))
                OR
                (group_ != 'EXT')
            )
            -- keyword composed by many words (decreasing order)
            ORDER BY count_words(k.name) DESC
        )
        LOOP
            IF ((name ~ CONCAT('^', _begin, _kw.name, ' +'))
                OR
                (name ~ CONCAT('^', _begin, _kw.name, '$'))
                /*
                (name = _kw.name)
                OR
                -- not last word!
                (name ~ CONCAT('^', _begin, _kw.name, ' +'))
                 */
            ) THEN
                _found := TRUE;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    IF NOT _found THEN
        kw := NULL::VARCHAR;
        kw_abbreviated := NULL::VARCHAR;
        kw_is_abbreviated := FALSE;
        kw_nwords := NULL::INT;
    ELSE
        kw := _kw.name;
        kw_abbreviated := _kw.name_abbreviated;
        kw_is_abbreviated := (
            (_word IS NOT DISTINCT FROM _kw.name_abbreviated)
            AND
            (_kw.name != _kw.name_abbreviated)
        );
        kw_nwords := count_words(_kw.name);
    END IF;
END
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fr.get_keyword_of_street(
    name IN VARCHAR
    , groups IN VARCHAR[]
    , at_ IN INT DEFAULT 1
    , words IN TEXT[] DEFAULT NULL
    , with_abbreviation IN BOOLEAN DEFAULT TRUE
    , kw_group OUT VARCHAR
    , kw OUT VARCHAR
    , kw_abbreviated OUT VARCHAR
    , kw_is_abbreviated OUT BOOLEAN
    , kw_nwords OUT INT
)
AS
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
        RAISE 'recherche mot clé VOIE nécessite: liste des mots-clé (TYPE,TITLE,EXT)';
    END IF;

    FOR _i IN 1 .. ARRAY_LENGTH(groups, 1)
    LOOP
        SELECT ks.kw_group, ks.kw, ks.kw_abbreviated, ks.kw_is_abbreviated, ks.kw_nwords
        INTO
            get_keyword_of_street.kw_group
            , get_keyword_of_street.kw
            , get_keyword_of_street.kw_abbreviated
            , get_keyword_of_street.kw_is_abbreviated
            , get_keyword_of_street.kw_nwords
        FROM fr.get_keyword_of_street(
            name => name
            , group_ => groups[_i]
            , at_ => at_
            , words => words
            , with_abbreviation => with_abbreviation
        ) ks
        ;

        IF get_keyword_of_street.kw IS NOT NULL THEN
            RETURN;
        END IF;
    END LOOP;

    -- not found
    get_keyword_of_street.kw_group := NULL::VARCHAR;
    get_keyword_of_street.kw := NULL::VARCHAR;
    get_keyword_of_street.kw_abbreviated := NULL::VARCHAR;
    get_keyword_of_street.kw_is_abbreviated := FALSE;
    get_keyword_of_street.kw_nwords := NULL::INT;
END
$func$ LANGUAGE plpgsql;
