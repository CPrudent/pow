/***
 * FR: add LAPOSTE/RAN keywords management (street, complement)
 */

DO $$
BEGIN
    IF table_exists('fr', 'laposte_address_street_keyword') THEN
        ALTER TABLE fr.laposte_address_street_keyword RENAME TO laposte_address_keyword;
    END IF;
END $$;

-- to store keywords
CREATE TABLE IF NOT EXISTS fr.laposte_address_keyword (
    "group" VARCHAR NOT NULL
    , name VARCHAR NOT NULL
    , name_abbreviated VARCHAR
    , first_word VARCHAR
    , occurs INT
)
;

-- find keyword (at given position) into a name
SELECT drop_all_functions_if_exists('fr', 'get_keyword_from_name');
CREATE OR REPLACE FUNCTION fr.get_keyword_from_name(
    name IN VARCHAR
    , words IN TEXT[] DEFAULT NULL
    , at_ IN INT DEFAULT 1
    , groups IN VARCHAR DEFAULT 'ALL'
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
    _with_complement BOOLEAN;
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

    _with_complement := (groups ~ '(ALL|COMPLEMENT|GROUP)');
    _groups := CASE groups
        WHEN 'ALL' THEN '{GROUP1,GROUP2,GROUP3,TITLE,TYPE,EXT,NAME}'::VARCHAR[]
        WHEN 'COMPLEMENT' THEN '{GROUP1,GROUP2,GROUP3,TITLE,TYPE,EXT,NAME}'::VARCHAR[]
        WHEN 'STREET' THEN '{TITLE,TYPE,EXT,NAME}'::VARCHAR[]
        ELSE STRING_TO_ARRAY(groups, ',')
        END
    ;
    IF raise_notice THEN
        RAISE NOTICE ' name=% (w=%, g=%)', name, words, _groups;
    END IF;

    -- input name w/ abbreviation ?
    IF (with_abbreviation
        -- not article: EN abbreviation of ENCEINTE !
        AND NOT fr.is_normalized_article(words[at_])
        /* NOTE
        exception if word default is (name or title)
            ARC => ARCADE, GARE => GARENNE, PORT => PORTE, BAS => BASSE, CAMP => CAMPAGNE
        except ZA* or ZI, and PETI (typo error to right)
        but not BAT, RES (else word will be unabbreviated!)
            RUE DE BAT L EAU, CHEMIN BAT PRIBETTE, RUE BAT DE L ORGE, ...
            RUE DES RES DE DURSAT, BONNE RES, LE RES
         */
        AND NOT EXISTS(
            SELECT 1
            FROM fr.laposte_address_street_word_descriptor
            WHERE word = words[at_] AND as_default ~ 'N|T'
            -- except ZA, ZI, PETI
            AND words[at_] !~ '^(Z[AI]|PETI)$'
            -- trick because exists BAT, RES as name|title !
            AND NOT _with_complement
        )
    ) THEN
        /* NOTE
        1/ if many cases, choice better later (w/ loop), just remember is abbreviated
        2/ novelty w/ complement: ZONE ARTISANALE abbreviated to ZONE (as ZONE)!
           add filter: name_abbreviated != first_word
        3/ name_abbreviated can be composed by many words!
           starts w/ word (not equality) : case of abbreviated 'PETI ROUTE'
         */
        IF (SELECT COUNT(*)
            FROM fr.laposte_address_keyword k
            WHERE --k.name_abbreviated = words[at_]
            (
                (count_words(k.name) = 1 AND k.name_abbreviated = words[at_])
                OR
                (count_words(k.name) > 1 AND k.name_abbreviated ~ CONCAT('^', words[at_]))
            )
            AND LENGTH(k.name_abbreviated) > 1
            AND k.name_abbreviated != COALESCE(k.first_word, k.name)
            AND NOT fr.is_normalized_article(k.name_abbreviated)
        ) > 1 THEN
            IF raise_notice THEN RAISE NOTICE ' abbr, too many %', words[at_]; END IF;
            _is_abbreviated := TRUE;
        ELSE
            SELECT *
            INTO _kw
            FROM fr.laposte_address_keyword k
            WHERE --k.name_abbreviated = words[at_]
            k.name_abbreviated = items_of_array_to_string(
                elements => words
                , from_ => at_
                , to_ => (at_ + count_words(k.name_abbreviated) -1)
            )
            AND LENGTH(k.name_abbreviated) > 1
            AND k.name_abbreviated != COALESCE(k.first_word, k.name)
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
            SELECT 1 FROM fr.laposte_address_keyword k
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
        IF raise_notice THEN RAISE NOTICE ' search "%"', words[at_]; END IF;
        FOR _kw IN (
            SELECT * FROM fr.laposte_address_keyword k
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
                    WHEN _with_complement AND k.group ~ '^GROUP' THEN 3
                    WHEN at_ = 1 THEN
                        CASE WHEN k.group = 'TYPE' THEN 2
                        ELSE 1
                        END
                    ELSE
                        CASE
                            WHEN _with_complement AND k.group ~ 'TYPE' THEN 2
                            WHEN k.group = 'TITLE' THEN 1
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
        /* NOTE
        1/ event if is_abbreviated is TRUE, eval kw to verify if distinct
           counter example: (COUR, abbr COUR) return TRUE!
           because many kw w/ COUR as abbr
        2/ name_abbreviated can be composed by many words: no test on a single word!
         */
        kw_is_abbreviated := (
            -- new usecase w/ GROUP3, as ZONE ARTISANALE (abbreviated to ZONE)
            _is_abbreviated
            --AND
            --(words[at_] IS NOT DISTINCT FROM _kw.name_abbreviated)
            AND
            (_kw.name != _kw.name_abbreviated)
        );
        kw_nwords := count_words(_kw.name);
    END IF;
END
$func$ LANGUAGE plpgsql;
