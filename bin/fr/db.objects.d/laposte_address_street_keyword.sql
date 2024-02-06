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
    name IN VARCHAR
    , words IN TEXT[] DEFAULT NULL
    , at_ IN INT DEFAULT 1
    , groups VARCHAR DEFAULT 'ALL'
    , with_abbreviation IN BOOLEAN DEFAULT TRUE
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , kw_group OUT VARCHAR
    , kw OUT VARCHAR
    , kw_abbreviated OUT VARCHAR
    , kw_is_abbreviated OUT BOOLEAN
    , kw_nwords OUT INT
)
AS
$func$
DECLARE
    _groups VARCHAR[];
    _kw RECORD;
    _is_abbreviated BOOLEAN := FALSE;
    _exists BOOLEAN := TRUE;
    _found BOOLEAN := FALSE;
    _begin VARCHAR;
    _name VARCHAR;
BEGIN
    -- mandatory words
    IF words IS NULL THEN
        words := REGEXP_SPLIT_TO_ARRAY(name, '\s+');
    END IF;

    IF groups = 'ALL' THEN
        _groups := '{TITLE,TYPE,EXT,NAME}'::VARCHAR[];
    ELSE
        _groups := STRING_TO_ARRAY(groups, ',');
    END IF;
    IF raise_notice THEN
        RAISE NOTICE ' name=% (w=%, g=%)', name, words, _groups;
    END IF;

    IF with_abbreviation THEN
        IF (SELECT COUNT(*)
            FROM fr.laposte_address_street_keyword k
            WHERE k.name_abbreviated = words[at_]
            AND LENGTH(k.name_abbreviated) > 1
        ) > 1 THEN
            _is_abbreviated := TRUE;
        ELSE
            SELECT *
            INTO _kw
            FROM fr.laposte_address_street_keyword k
            WHERE k.name_abbreviated = words[at_]
            AND LENGTH(k.name_abbreviated) > 1
            AND ARRAY_POSITION(_groups, k.group) > 0
            ORDER BY k.occurs DESC
            LIMIT 1;
            IF FOUND THEN
                IF raise_notice THEN RAISE NOTICE ' kw=%', _kw; END IF;
                IF _kw.name != _kw.name_abbreviated THEN
                    kw_group := _kw.group;
                    kw := _kw.name;
                    kw_abbreviated := _kw.name_abbreviated;
                    kw_is_abbreviated := TRUE;
                    kw_nwords := count_words(_kw.name);
                    RETURN;
                END IF;
            END IF;
        END IF;
    END IF;

    IF NOT _is_abbreviated THEN
        SELECT EXISTS(
            SELECT 1 FROM fr.laposte_address_street_keyword k
            WHERE COALESCE(k.first_word, k.name) = words[at_]
            AND ARRAY_POSITION(_groups, k.group) > 0
        ) INTO _exists;
    END IF;
    IF raise_notice THEN
        RAISE NOTICE ' exists=%, is_abbreviated=%', _exists, _is_abbreviated;
    END IF;

    _begin :=
        CASE WHEN at_ = 1 THEN NULL
        ELSE items_of_array_to_string(
                elements => words
                , from_ => 1
                , to_ => at_ -1
            )
        END;
    IF _begin IS NOT NULL THEN
        _begin := CONCAT(_begin, ' ');
    END IF;
    IF raise_notice THEN RAISE NOTICE ' begin=%', _begin; END IF;

    IF _exists THEN
        IF raise_notice THEN RAISE NOTICE ' recherche "%"', words[at_]; END IF;
        FOR _kw IN (
            SELECT * FROM fr.laposte_address_street_keyword k
            WHERE
                -- matching word
                (
                    (
                        _is_abbreviated
                        AND
                        (k.name_abbreviated ~ CONCAT('^', words[at_]))
                    )
                    OR
                    (
                        NOT _is_abbreviated
                        AND
                        (COALESCE(k.first_word, k.name) = words[at_])
                    )
                )
                -- among group(s)
                AND (
                    ARRAY_POSITION(_groups, k.group) > 0
                )
                -- exclude one-char EXT keywords (A..Z), NAME 1-letter abbreviation (A, ...)
                AND (
                    (LENGTH(k.name) > 1)
                )
            -- keyword composed by many words (decreasing order)
            ORDER BY
                count_words(k.name) DESC
                , CASE
                    WHEN at_ = 1 THEN
                        CASE WHEN k.group = 'TYPE' THEN 2
                        ELSE 1
                        END
                    ELSE
                        CASE WHEN k.group = 'TITLE' THEN 1
                        ELSE 0
                        END
                    END DESC
                , k.occurs DESC
        )
        LOOP
            IF raise_notice THEN RAISE NOTICE ' kw=%', _kw; END IF;
            _name := CASE
                WHEN _is_abbreviated THEN _kw.name_abbreviated
                ELSE _kw.name
                END
                ;
            IF raise_notice THEN RAISE NOTICE ' word=%', _name; END IF;

            IF ((name ~ CONCAT('^', _begin, _name, ' +'))
                OR
                (name ~ CONCAT('^', _begin, _name, '$'))
            ) THEN
                _found := TRUE;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    IF NOT _found THEN
        kw_group := NULL::VARCHAR;
        kw := NULL::VARCHAR;
        kw_abbreviated := NULL::VARCHAR;
        kw_is_abbreviated := FALSE;
        kw_nwords := NULL::INT;
    ELSE
        kw_group := _kw.group;
        kw := _kw.name;
        kw_abbreviated := _kw.name_abbreviated;
        kw_is_abbreviated := CASE
            WHEN _is_abbreviated THEN TRUE
            ELSE
                (
                    (words[at_] IS NOT DISTINCT FROM _kw.name_abbreviated)
                    AND
                    (_kw.name != _kw.name_abbreviated)
                )
            END
            ;
        kw_nwords := count_words(_kw.name);
    END IF;
END
$func$ LANGUAGE plpgsql;
