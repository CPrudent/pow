/***
 * add FR-ADDRESS facilities (matching address)
 */

-- remove article(s) from name
SELECT drop_all_functions_if_exists('fr', 'get_street_name_without_article');
CREATE OR REPLACE FUNCTION fr.get_street_name_without_article(
    words IN TEXT[]
    , nwords IN INT
    , descriptors IN VARCHAR DEFAULT NULL
    , without_article OUT TEXT[]
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

-- status of match element
SELECT drop_all_functions_if_exists('fr', 'match_element_status');
CREATE OR REPLACE FUNCTION fr.match_element_status(
    search IN VARCHAR
    , matched_element INOUT fr.matched_element
)
AS
$func$
BEGIN
    IF ARRAY_LENGTH(matched_element.codes_address, 1) = 1 THEN
        matched_element.status := CASE search
            WHEN 'STRICT' THEN (SELECT CURRENT_SETTING('fr.address.match.strict'))
            ELSE (SELECT CURRENT_SETTING('fr.address.match.near'))
            END
        ;
    ELSIF matched_element.codes_address IS NOT NULL THEN
        matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.too_many'));
    ELSE
        matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.not_found'));
    END IF;
END
$func$ LANGUAGE plpgsql;

-- get query for matching area
SELECT drop_all_functions_if_exists('fr', 'query_match_area');
CREATE OR REPLACE FUNCTION fr.query_match_area(
    search IN VARCHAR
    , with_postcode IN BOOLEAN
    , query_area OUT TEXT
)
AS
$func$
BEGIN
    query_area := '
        SELECT
            ARRAY_AGG(area.co_adr)
        FROM
            fr.area_view area
        WHERE
            -- municipality code (if defined)
            (
                ($1 IS NULL)
                OR
                (area.co_insee_commune = $1)
            )
        '
    ;
    IF with_postcode THEN
        query_area := CONCAT(
            query_area
            , '
            -- postcode (if defined)
            AND (
                ($4 IS NULL)
                OR
                (area.co_postal = $4)
            )
            '
        );
    END IF;

    query_area := CONCAT(
        query_area
        , CASE UPPER(search)
            WHEN 'STRICT' THEN
                '
                -- municipality name (if defined and not defined code)
                AND (
                    ($1 IS NOT NULL)
                    OR
                    ($2 IS NULL)
                    OR
                    (area.lb_acheminement = $2)
                    OR
                    (area.lb_ligne5 = $2)
                )
                -- municipality old name (if defined)
                AND (
                    ($3 IS NULL)
                    OR
                    (area.lb_ligne5 = $3)
                )
                '
            ELSE
                '
                -- municipality name (if defined)
                AND (
                    ($2 IS NULL)
                    OR
                    (get_similarity($2, area.lb_acheminement) >= $5)
                    OR
                    (get_similarity($2, area.lb_ligne5) >= $5)
                )
                -- municipality old name (if defined)
                AND (
                    ($3 IS NULL)
                    OR
                    (get_similarity($3, area.lb_ligne5) >= $5)
                )
                '
        END
    );
END
$func$ LANGUAGE plpgsql;

-- match one element
SELECT drop_all_functions_if_exists('fr', 'match_element');
CREATE OR REPLACE FUNCTION fr.match_element(
    level IN VARCHAR
    , standardized_address IN fr.standardized_address
        /* NOTE
        HSTORE parameter to custom properties, as:
        '"AREA_THRESHOLD" => 0.6, "STREET_THRESHOLD" => 0.75, "STREET_RATIO" => 0.2'::HSTORE
        defaults are defined as global variables, view constant.sql
         */
    , matched_parent INOUT fr.matched_element
    , similarity IN HSTORE DEFAULT NULL
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , matched_element OUT fr.matched_element
)
AS
$func$
DECLARE
    _SEARCHS VARCHAR[] := ARRAY['STRICT', 'NEAR'];
    _LOOP_LIMIT INT := 4;
    _similarity_threshold REAL;
    _similarity_ratio REAL;
    _street_ratio REAL;
    _search VARCHAR;
    _todo BOOLEAN;
    _query TEXT;
    _order_word INT;
    _better_word TEXT;
    _street RECORD;
    _previous_street RECORD;
    _with_near BOOLEAN := TRUE;
    _abbr VARCHAR;
    _ext VARCHAR;
BEGIN
    IF level = 'AREA' THEN
        _SEARCHS := ARRAY_APPEND(_SEARCHS, 'NEAR_WO_POSTCODE');
        FOREACH _search IN ARRAY _SEARCHS
        LOOP
            _todo := CASE _search
                WHEN 'STRICT' THEN
                    (
                        (standardized_address).municipality_code IS NOT NULL
                        OR
                        (standardized_address).postcode IS NOT NULL
                        OR
                        (standardized_address).municipality_name IS NOT NULL
                        OR
                        (standardized_address).municipality_old_name IS NOT NULL
                    )
                /* NOTE
                similarity w/ low value (default to 0.5) to match:
                ('ST MEDARD', 'ST MEDARD EN JALLES')
                 */
                WHEN 'NEAR' THEN
                    (
                        ((standardized_address).municipality_name IS NOT NULL)
                        OR
                        ((standardized_address).municipality_old_name IS NOT NULL)
                    )
                /* NOTE
                postcode can be wrong! search for w/o it
                 */
                WHEN 'NEAR_WO_POSTCODE' THEN
                    ((
                            (standardized_address).municipality_code IS NOT NULL
                            OR
                            ((standardized_address).municipality_name IS NOT NULL)
                            OR
                            ((standardized_address).municipality_old_name IS NOT NULL)
                        )
                        AND
                        ((standardized_address).postcode IS NOT NULL)
                    )
            END;

            IF _todo THEN
                _query := fr.query_match_area(
                    search => _search
                    , with_postcode => CASE _search WHEN 'NEAR_WO_POSTCODE' THEN FALSE ELSE TRUE END
                );

                IF _search != 'STRICT' AND _similarity_threshold IS NULL THEN
                    _similarity_threshold := fr.get_similarity_property(
                        similarity => similarity
                        , level => level
                        , key => 'THRESHOLD'
                    );
                    --SET pg_trgm.similarity_threshold = _similarity_threshold;
                    _similarity_threshold := set_limit(_similarity_threshold);
                END IF;

                EXECUTE _query
                    INTO matched_element.codes_address
                    USING
                        (standardized_address).municipality_code
                        , (standardized_address).municipality_name
                        , (standardized_address).municipality_old_name
                        , (standardized_address).postcode
                        , _similarity_threshold
                    ;

                matched_element := (
                    SELECT fr.match_element_status(
                        search => _search
                        , matched_element => matched_element
                    )
                );
                IF (
                    matched_element.status != (SELECT (CURRENT_SETTING('fr.address.match.not_found')))
                ) THEN
                    EXIT;
                END IF;
            END IF;
        END LOOP;
    ELSIF level = 'STREET' THEN
        -- parent found (area)
        IF LEFT(matched_parent.status, 2) = 'OK' THEN
            -- strict search
            SELECT ARRAY_AGG(co_adr)
            INTO matched_element.codes_address
            FROM fr.street_view
            WHERE
                lb_voie = (standardized_address).street
                AND
                co_adr_za = ANY((matched_parent).codes_address)
            ;
            matched_element := (
                SELECT fr.match_element_status(
                    search => 'STRICT'
                    , matched_element => matched_element
                )
            );

            -- not found: try w/ near approach
            IF (
                matched_element.status = (SELECT CURRENT_SETTING('fr.address.match.not_found'))
            ) THEN
                _similarity_threshold := fr.get_similarity_property(
                    similarity => similarity
                    , level => level
                    , key => 'THRESHOLD'
                );
                /* FIXME don't accept variable!
                SET pg_trgm.similarity_threshold = _similarity_threshold;
                set_limit() obsolete!
                 */
                _similarity_threshold := set_limit(_similarity_threshold);
                _street_ratio := fr.get_similarity_property(
                    similarity => similarity
                    , level => level
                    , key => 'RATIO'
                );

                /* NOTE
                retrieve word w/ better similarity and rarity
                 */
                SELECT
                    better_word
                INTO
                    _better_word
                FROM
                    fr.get_better_word_with_similarity_criteria(
                        words => (standardized_address).words
                        , level => 'ZA'
                        , codes => (matched_parent).codes_address
                        , raise_notice => raise_notice
                    )
                ;
                /* NOTE
                https://stackoverflow.com/questions/40078047/sql-weighted-average
                 */
                /* NOTE
                _LOOP_LIMIT
                same street can be delivered by many areas (postcode, district, ...)
                to the maximum it exists a street w/ 3 areas!
                this loop aims to find gap between two successive streets, so 3 +1
                 */
                IF _better_word IS NOT NULL THEN
                    _previous_street := ROW(NULL);
                    matched_element.status := NULL;
                    FOR _street IN (
                        WITH
                        potential_streets AS (
                            SELECT
                                s.co_adr
                                , s.co_adr_za
                                , s.co_voie
                                , s.lb_voie
                            FROM
                                fr.street_view s
                                    JOIN fr.laposte_address_street_reference sr ON s.co_adr = sr.address_id
                                    JOIN fr.laposte_address_street_membership sm ON sr.name_id = sm.name_id
                            WHERE
                                sm.word = _better_word
                                AND
                                -- can verify area (known as ZA)
                                s.co_insee_commune = (standardized_address).municipality_code
                        )
                        , similarity_streets AS (
                            SELECT
                                co_adr
                                , co_adr_za
                                , co_voie
                                , lb_voie
                                , fr.get_similarity_words(
                                    words_a => (standardized_address).words
                                    , words_b => su.words
                                    , descriptors_a => (standardized_address).descriptors
                                    , descriptors_b => su.descriptors
                                ) similarity
                            FROM
                                potential_streets ps
                                    JOIN fr.laposte_address_street_uniq su ON ps.lb_voie = su.name
                        )
                        SELECT
                            *
                        FROM
                            similarity_streets
                        ORDER BY
                            similarity DESC
                        LIMIT
                            _LOOP_LIMIT
                    )
                    LOOP
                        IF raise_notice THEN
                            RAISE NOTICE 'DATA(%)', _street;
                        END IF;

                        IF (_previous_street IS NULL) THEN
                            IF raise_notice THEN
                                RAISE NOTICE 'VOIE(%) INSEE(%)'
                                    , (standardized_address).street
                                    , (standardized_address).municipality_code
                                ;
                            END IF;
                            IF (_street.similarity < _similarity_threshold) THEN
                                IF raise_notice THEN
                                    RAISE NOTICE ' premier choix trop faible VOIE(%) [sim=%]'
                                        , _street.lb_voie
                                        , ROUND(_street.similarity, 5)
                                    ;
                                END IF;
                                matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.not_near'));
                                EXIT;
                            ELSE
                                IF raise_notice THEN
                                    RAISE NOTICE ' premier choix ok VOIE(%) [sim=%]'
                                        , _street.lb_voie
                                        , ROUND(_street.similarity, 5)
                                    ;
                                END IF;
                                matched_element.codes_address := ARRAY[_street.co_adr]::VARCHAR[];
                                matched_element.similarity := _street.similarity;
                                IF (_street.co_adr_za != matched_parent.codes_address[1]) THEN
                                    RAISE NOTICE ' mais sur ZA différente (%/%)'
                                        , _street.co_adr_za
                                        , matched_parent.codes_address
                                    ;
                                    matched_parent.codes_address := ARRAY[_street.co_adr_za]::VARCHAR[];
                                    matched_parent := (
                                        SELECT fr.match_element_status(
                                            search => 'NEAR'
                                            , matched_element => matched_parent
                                        )
                                    );
                                END IF;
                            END IF;
                        ELSE
                            /* NOTE
                            OK if second|third choice far enough (15%)
                            minimum gap between 2 results ascending when similarity decrease
                             */
                            _street_ratio := (_previous_street.similarity / _street.similarity);
                            IF NOT (_street_ratio > _similarity_ratio) THEN
                                -- same street, but w/ {postcode, district, ...} difference
                                IF _previous_street.co_voie = _street.co_voie THEN
                                    IF raise_notice THEN
                                        RAISE NOTICE ' deuxième choix même voie CODE(%) [CEA=%]'
                                            , _street.co_voie
                                            , _street.co_adr
                                        ;
                                    END IF;
                                    matched_element.codes_address := ARRAY_APPEND(matched_element.codes_address, _street.co_adr);
                                ELSE
                                    IF raise_notice THEN
                                        RAISE NOTICE ' deuxième choix trop proche VOIE(%) [sim=%,ratio=%]'
                                            , _street.lb_voie
                                            , ROUND(_street.similarity, 5)
                                            , ROUND(_street_ratio, 2)
                                        ;
                                    END IF;
                                    matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.too_similar'));
                                    EXIT;
                                END IF;
                            ELSE
                                IF raise_notice THEN
                                    RAISE NOTICE ' deuxième choix suffisamment éloigné VOIE(%) [sim=%,ratio=%]'
                                        , _street.lb_voie
                                        , ROUND(_street.similarity, 5)
                                        , ROUND(_street_ratio, 2)
                                    ;
                                END IF;
                                EXIT;
                            END IF;
                        END IF;

                        _previous_street := _street;
                    END LOOP;

                    IF matched_element.status IS NULL THEN
                        matched_element := (
                            SELECT fr.match_element_status(
                                search => 'NEAR'
                                , matched_element => matched_element
                            )
                        );
                    END IF;
                END IF;
            END IF;
        END IF;
    ELSIF level = 'HOUSENUMBER' THEN
        -- parent found (street)
        IF LEFT(matched_parent.status, 2) = 'OK' THEN
            IF (standardized_address).extension IS NOT NULL THEN
                _abbr := (
                    SELECT fr.normalize_abbreviate_keyword(
                        name => (standardized_address).extension
                        , groups => 'EXT'
                    )
                );
            END IF;
            _with_near := (((standardized_address).extension IS NULL) OR (_abbr IS NULL));
            IF NOT _with_near THEN
                _SEARCHS := ARRAY_REMOVE(_SEARCHS, 'NEAR');
            END IF;

            FOREACH _search IN ARRAY _SEARCHS
            LOOP
                _ext := CASE _search
                    WHEN 'STRICT' THEN (standardized_address).extension
                    ELSE _abbr
                    END
                ;

                SELECT ARRAY_AGG(co_adr)
                INTO matched_element.codes_address
                FROM fr.address_view
                WHERE
                    co_adr_parent = ANY((matched_parent).codes_address)
                    AND
                    co_adr_l3 IS NULL
                    AND
                    no_numero = (standardized_address).housenumber
                    AND (
                        (_ext IS NULL)
                        OR
                        (lb_extension_numero = _ext)
                    )
                ;
                matched_element := (
                    SELECT fr.match_element_status(
                        search => _search
                        , matched_element => matched_element
                    )
                );

                IF (
                    matched_element.status != (SELECT CURRENT_SETTING('fr.address.match.not_found'))
                ) THEN
                    EXIT;
                END IF;
            END LOOP;
        END IF;
    ELSIF level = 'COMPLEMENT' THEN
        -- parent (street or housenumber) found
        IF LEFT(matched_parent.status, 2) = 'OK' THEN
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;

/*
-- match one address
SELECT drop_all_functions_if_exists('fr', 'match_address');
CREATE OR REPLACE FUNCTION fr.match_address(
    address_normalized IN fr.address_normalized           -- address to match
    --, address_matched OUT fr.address_matched              -- address matched
)
RETURNS fr.address_matched AS
$func$
DECLARE
    _address_matched fr.address_matched;
BEGIN
    -- basic algorithm
    SELECT
        ARRAY_AGG(a.co_adr)
    INTO
        _address_matched.codes_area_possible
    FROM
        fr.address_view a
    WHERE
        a.co_niveau = 'ZA'
        --Recherche par code postal exact, à moins qu'il ne soit pas indiqué
        AND (
            (address_normalized.postcode IS NULL)
            OR
            (a.co_postal = address_normalized.postcode)
        )
        --Recherche par code INSEE commune exact, à moins qu'il ne soit pas indiqué
        AND (
            (address_normalized.municipality_code IS NULL)
            OR
            (a.co_insee_commune = address_normalized.municipality_code)
        )
        --Recherche par libellé localité exact, à moins qu'il ne soit pas indiqué OU à moins que le code INSEE soit indiqué
        AND (
            (address_normalized.municipality_name IS NULL)
            OR
            (address_normalized.municipality_code IS NOT NULL)
            OR
            (a.lb_acheminement = address_normalized.municipality_name)
            OR
            (a.lb_ligne5 = address_normalized.municipality_name)
        )
    ;

    IF ARRAY_LENGTH(_address_matched.codes_area_possible, 1) = 1 THEN
        _address_matched.status := 1;
    ELSIF _address_matched.codes_area_possible IS NOT NULL THEN
        _address_matched.status := 22;
    ELSE
        _address_matched.status := 21;
    END IF;

    RAISE NOTICE 'address_matched= %', _address_matched;
    RETURN _address_matched;
END
$func$ LANGUAGE plpgsql;
 */
