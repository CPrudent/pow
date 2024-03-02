/***
 * add FR-ADDRESS facilities (matching address)
 */

-- to define global varibales
-- https://stackoverflow.com/questions/31316053/is-it-possible-to-define-global-variables-in-postgresql

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

/* NOTE
AREA: can be factorize!
    fr.match_element_area(
        search IN VARCHAR (STRICT|NEAR)
        , with_postcode IN BOOLEAN
    )

    fr.match_element()
    _SEARCHS VARCHAR[] := ARRAY['STRICT', 'NEAR', 'NEAR_WO_POSTCODE'];
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
            WHEN 'NEAR' THEN
                (
                    matched_element.code_area IS NULL
                    AND (
                        ((standardized_address).municipality_name IS NOT NULL)
                        OR
                        ((standardized_address).municipality_old_name IS NOT NULL)
                    )
                )
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
            END
        ;
        IF _todo THEN
            _query := fr.match_element_area(
                method => _search
                , CASE WHEN _search = 'NEAR_WO_POSTCODE' THEN FALSE ELSE TRUE END
            )
            ;
            EXECUTE _query INTO matched_element.codes_address;

            matched_element := (
                SELECT fr.match_element_status(
                    search => _search
                    , matched_element => matched_element
                )
            );
            IF matched_element.status = 1 THEN
                EXIT;
            END IF;
        END IF;
    END LOOP;
 */
-- match one element
SELECT drop_all_functions_if_exists('fr', 'match_element');
CREATE OR REPLACE FUNCTION fr.match_element(
    level IN VARCHAR
    , standardized_address IN fr.standardized_address
    , matched_parent IN fr.matched_element
    , similarity_threshold IN REAL DEFAULT 0.7
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , matched_element OUT fr.matched_element
)
AS
$func$
DECLARE
    _similarity_limit REAL;
    _words TEXT[];
    _orders INT[];
    _matched_words TEXT[];
    _street RECORD;
    _previous_street RECORD;
BEGIN
    IF level = 'AREA' THEN
        IF (
            (standardized_address).municipality_code IS NOT NULL
            OR
            (standardized_address).postcode IS NOT NULL
            OR
            (standardized_address).municipality_name IS NOT NULL
            OR
            (standardized_address).municipality_old_name IS NOT NULL
        ) THEN
            -- strict search
            SELECT
                ARRAY_AGG(area.co_adr)
            INTO
                matched_element.codes_address
            FROM
                fr.area_view area
            WHERE
                -- postcode (if defined)
                (
                    ((standardized_address).postcode IS NULL)
                    OR
                    (area.co_postal = (standardized_address).postcode)
                )
                -- municipality code (if defined)
                AND (
                    ((standardized_address).municipality_code IS NULL)
                    OR
                    (area.co_insee_commune = (standardized_address).municipality_code)
                )
                -- municipality name (if defined and not defined code)
                AND (
                    ((standardized_address).municipality_code IS NOT NULL)
                    OR
                    ((standardized_address).municipality_name IS NULL)
                    OR
                    (area.lb_acheminement = (standardized_address).municipality_name)
                    OR
                    (area.lb_ligne5 = (standardized_address).municipality_name)
                )
                -- municipality old name (if defined)
                AND (
                    ((standardized_address).municipality_old_name IS NULL)
                    OR
                    (area.lb_ligne5 = (standardized_address).municipality_old_name)
                )
            ;

            matched_element := (
                SELECT fr.match_element_status(
                    search => 'STRICT'
                    , matched_element => matched_element
                )
            );

            -- near search (if not already found)
            IF (
                matched_element.status = (SELECT (CURRENT_SETTING('fr.address.match.not_found')))
                AND (
                    ((standardized_address).municipality_name IS NOT NULL)
                    OR
                    ((standardized_address).municipality_old_name IS NOT NULL)
                )
            ) THEN
                /* NOTE
                to match: 'ST MEDARD' ~= 'ST MEDARD EN JALLES'
                 */
                _similarity_limit := 0.5;
                _similarity_limit := set_limit(_similarity_limit);

                SELECT
                    ARRAY_AGG(area.co_adr)
                INTO
                    matched_element.codes_address
                FROM
                    fr.area_view area
                WHERE
                    -- postcode (if defined)
                    (
                        ((standardized_address).postcode IS NULL)
                        OR
                        (area.co_postal = (standardized_address).postcode)
                    )
                    -- municipality code (if defined)
                    AND (
                        ((standardized_address).municipality_code IS NULL)
                        OR
                        (area.co_insee_commune = (standardized_address).municipality_code)
                    )
                    -- municipality name (if defined)
                    AND (
                        ((standardized_address).municipality_name IS NULL)
                        OR
                        (get_similarity((standardized_address).municipality_name, area.lb_acheminement) >= _similarity_limit)
                        OR
                        (get_similarity((standardized_address).municipality_name, area.lb_ligne5) >= _similarity_limit)
                    )
                    -- municipality old name (if defined)
                    AND (
                        ((standardized_address).municipality_old_name IS NULL)
                        OR
                        (get_similarity((standardized_address).municipality_old_name, area.lb_ligne5) >= _similarity_limit)
                    )
                ;

                matched_element := (
                    SELECT fr.match_element_status(
                        search => 'NEAR'
                        , matched_element => matched_element
                    )
                );

                -- search w/ (municipality code | municipality name | municipality old name) AND (postcode)
                IF (
                    (matched_element.status = (SELECT CURRENT_SETTING('fr.address.match.not_found')))
                    AND (
                            (
                                (standardized_address).municipality_code IS NOT NULL
                                OR
                                ((standardized_address).municipality_name IS NOT NULL)
                                OR
                                ((standardized_address).municipality_old_name IS NOT NULL)
                            )
                            AND
                            ((standardized_address).postcode IS NOT NULL)
                    )
                ) THEN
                    /* NOTE
                    postcode can be wrong! search for w/o it
                    */
                    SELECT
                        ARRAY_AGG(area.co_adr)
                    INTO
                        matched_element.codes_address
                    FROM
                        fr.area_view area
                    WHERE
                        -- municipality code (if defined)
                        (
                            ((standardized_address).municipality_code IS NULL)
                            OR
                            (area.co_insee_commune = (standardized_address).municipality_code)
                        )
                        -- municipality name (if defined)
                        AND (
                            ((standardized_address).municipality_name IS NULL)
                            OR
                            (get_similarity((standardized_address).municipality_name, area.lb_acheminement) >= _similarity_limit)
                            OR
                            (get_similarity((standardized_address).municipality_name, area.lb_ligne5) >= _similarity_limit)
                        )
                        -- municipality old name (if defined)
                        AND (
                            ((standardized_address).municipality_old_name IS NULL)
                            OR
                            (get_similarity((standardized_address).municipality_old_name, area.lb_ligne5) >= _similarity_limit)
                        )
                    ;

                    matched_element := (
                        SELECT fr.match_element_status(
                            search => 'NEAR'
                            , matched_element => matched_element
                        )
                    );
                END IF;
            END IF;
        END IF;
    ELSIF level = 'STREET' THEN
        -- uniq area found
        IF LENGTH(matched_parent.status) = 1 THEN
            SELECT ARRAY_AGG(co_adr)
            INTO matched_element.codes_address
            FROM fr.street_view
            WHERE
                lb_voie = (standardized_address).street
                AND
                co_adr_za = matched_parent.codes_address[1]
            --LIMIT 1
            ;
            matched_element := (
                SELECT fr.match_element_status(
                    search => 'STRICT'
                    , matched_element => matched_element
                )
            );

            -- not found: try w/ near approach
            IF matched_element.status = (SELECT CURRENT_SETTING('fr.address.match.not_found')) THEN
                /* IDEA
                step to do when normalization ?
                 */
                _words := STRING_TO_ARRAY((standardized_address).street, ' ');

                /* NOTE
                retrieve orders of words w/ better similarity and rarity
                as well as referential words that match them
                 */
                SELECT
                    orders
                    , matched_words
                INTO
                    _orders
                    , _matched_words
                FROM
                    fr.get_ordered_words_with_similarity_criteria(
                        words => _words
                        , municipality_code => (standardized_address).municipality_code
                    )
                ;
                /* NOTE
                https://stackoverflow.com/questions/40078047/sql-weighted-average
                 */
                IF _orders IS NOT NULL THEN
                    _previous_street := ROW(NULL);
                    matched_element.status := NULL;
                    FOR _street IN (
                        WITH
                        potential_streets AS (
                            SELECT
                                s.co_adr
                                , s.co_adr_za
                                , s.co_voie
                                , s.lb_voie_desc descriptors
                            FROM
                                fr.street_view s
                                    JOIN fr.laposte_address_street_reference sr ON s.co_adr = sr.address_id
                                    JOIN fr.laposte_address_street_membership sm ON sr.name_id = sm.name_id
                            WHERE
                                sm.word = _matched_words[_orders[1]]
                                AND
                                -- can verify area (known as ZA)
                                s.co_insee_commune = (standardized_address).municipality_code
                        )
                        --SELECT * FROM potential_streets
                        , word_similarity_streets AS (
                            SELECT
                                co_adr
                                , co_adr_za
                                , co_voie
                                , get_similarity(w.word, _words[i]) similarity
                                , fr.get_descriptor_factor(SUBSTR(descriptors, w.i::INT, 1)) descriptor_factor
                            FROM
                                potential_streets
                                    JOIN LATERAL UNNEST(_words) WITH ORDINALITY AS w(word, i) ON TRUE
                            WHERE
                                -- not article
                                SUBSTR(descriptors, w.i::INT, 1) != 'A'
                        )
                        , sum_similarity_streets AS (
                            SELECT
                                co_adr
                                , co_adr_za
                                , co_voie
                                -- weighted average
                                , (SUM(similarity * descriptor_factor) / SUM(descriptor_factor)) similarity
                            FROM
                                word_similarity_streets
                            GROUP BY
                                co_adr
                                , co_adr_za
                                , co_voie
                        )
                        SELECT
                            *
                        FROM
                            sum_similarity_streets
                        ORDER BY
                            similarity DESC
                        LIMIT
                            3
                    )
                    LOOP
                        --RAISE NOTICE 'test(%)', _street.co_adr;

                        IF (_previous_street IS NULL) THEN
                            IF (_street.similarity < similarity_threshold) THEN
                                IF raise_notice THEN
                                    RAISE NOTICE 'premier choix VOIE(%) INSEE(%) : trop faible,  CEA(%) [sim=%]'
                                        , (standardized_address).street
                                        , (standardized_address).municipality_code
                                        , _street.co_adr
                                        , _street.similarity
                                    ;
                                END IF;
                                EXIT;
                            ELSE
                                IF raise_notice THEN
                                    RAISE NOTICE 'premier choix VOIE(%) INSEE(%) : ok, CEA(%) [sim=%]'
                                        , (standardized_address).street
                                        , (standardized_address).municipality_code
                                        , _street.co_adr
                                        , _street.similarity
                                    ;
                                END IF;
                                matched_element.codes_address := ARRAY[_street.co_adr]::VARCHAR[];
                                matched_element.similarity := _street.similarity;
                                IF (_street.co_adr_za != matched_parent.codes_address[1]) THEN
                                    RAISE NOTICE 'mais sur ZA(%) : origine ZA(%)'
                                        , _street.co_adr_za
                                        , matched_parent.codes_address[1]
                                    ;
                                    matched_parent.codes_address := ARRAY[_street.co_adr_za]::VARCHAR[];
                                END IF;
                            END IF;
                        ELSE
                            /* NOTE
                            OK if second|third choice far enough
                            minimum gap between 2 results ascending when similarity decrease
                            1st match w/ score of 0.9 needs 2nd match inferior to 0.8
                            1st match w/ score of 0.7 needs 2nd match inferior to 0.4
                             */
                            IF NOT ((_previous_street.similarity - _street.similarity) > (1 - _previous_street.similarity)) THEN
                                -- same street, but w/ {postcode, district, ...} difference
                                IF _previous_street.co_voie = _street.co_voie THEN
                                    IF raise_notice THEN
                                        RAISE NOTICE 'deuxième choix VOIE(%) INSEE(%) : même voie CEA(%) [sim=%]'
                                            , (standardized_address).street
                                            , (standardized_address).municipality_code
                                            , _street.co_adr
                                            , _street.similarity
                                        ;
                                    END IF;
                                ELSE
                                    IF raise_notice THEN
                                        RAISE NOTICE 'deuxième choix VOIE(%) INSEE(%) : trop proche CEA(%) [sim=%]'
                                            , (standardized_address).street
                                            , (standardized_address).municipality_code
                                            , _street.co_adr
                                            , _street.similarity
                                        ;
                                    END IF;
                                    matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.too_similar'));
                                    EXIT;
                                END IF;
                            ELSE
                                IF raise_notice THEN
                                    RAISE NOTICE 'deuxième choix VOIE(%) INSEE(%) : ok CEA(%) [sim=%]'
                                        , (standardized_address).street
                                        , (standardized_address).municipality_code
                                        , _street.co_adr
                                        , _street.similarity
                                    ;
                                END IF;
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
    ELSIF level = 'COMPLEMENT' THEN
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
