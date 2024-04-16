/***
 * add FR-ADDRESS facilities (matching address)
 */

-- set status of matching element
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

-- find if level contains uncommon item (word, number)
SELECT drop_all_functions_if_exists('fr', 'contains_uncommon_value');
CREATE OR REPLACE FUNCTION fr.contains_uncommon_value(
      level IN VARCHAR
    , standardized_address INOUT fr.standardized_address
    , parameters IN HSTORE DEFAULT NULL
    , simulation IN BOOLEAN DEFAULT FALSE
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , with_uncommon OUT BOOLEAN
)
AS
$func$
DECLARE
    _query TEXT;
    _level_up VARCHAR := UPPER(level);
    _level_low VARCHAR := LOWER(level);
    _column_uncommon VARCHAR := CASE _level_up
        WHEN 'HOUSENUMBER' THEN 'u.id'
        ELSE 'wd.word'
        END
        ;
    -- has to cast array as TEXT[], else error!
    _from VARCHAR := CASE _level_up
        WHEN 'HOUSENUMBER' THEN 'fr.laposte_address_housenumber_uniq u'
        ELSE CONCAT('UNNEST($2::TEXT[]) AS w(word)
                JOIN fr.laposte_address_', _level_low, '_word_descriptor wd ON w.word = wd.word
                JOIN fr.laposte_address_', _level_low, '_membership m ON wd.word = m.word
                JOIN fr.laposte_address_', _level_low, '_uniq u ON m.name_id = u.id')
        END
        ;
    _where VARCHAR := CASE _level_up
        WHEN 'HOUSENUMBER' THEN
            '
            u.number = $2
            AND
            COALESCE(u.extension, '''') = COALESCE($3, '''')
            AND
            u.occurs <= $1
            '
        ELSE
            CONCAT('
                as_default = ''N''
                AND
            ', CASE _level_up
                WHEN 'STREET' THEN '(as_name + as_last)'
                ELSE 'as_name'
                END
            , ' <= $1
            '
            )
        END
        ;
    _nrows INT;
    _uncommon BOOLEAN := FALSE;
    _occur INT;
    _max_occurs INT;
    _value VARCHAR;
BEGIN
    _query := CONCAT(
        '
        SELECT
              u.occurs
            , ', _column_uncommon, '
        FROM ', _from, '
        WHERE ', _where, '
        ORDER BY
            1
        LIMIT
            1
        '
    );

    --RAISE NOTICE 'parameters=%', parameters;
    IF NOT simulation THEN
        _max_occurs := CAST(fr.get_parameter_value(
            parameters => parameters
            , category => 'max'
            , level => level
            , key => 'occurs'
        ) AS INTEGER);

        IF _level_up = 'HOUSENUMBER' THEN
            EXECUTE _query
                INTO _occur, _value
                USING _max_occurs
                    , standardized_address.housenumber
                    , standardized_address.extension
                ;
        ELSE
            EXECUTE _query
                INTO _occur, _value
                USING _max_occurs
                    , CASE _level_up
                        WHEN 'STREET' THEN standardized_address.street_words
                        ELSE standardized_address.complement_words
                        END
                ;
        END IF;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        IF _nrows = 1 THEN
            _uncommon := TRUE;
            IF raise_notice THEN
                RAISE NOTICE ' level %, uncommon item found "%" (occurs %, max %)'
                    , level
                    , CASE _level_up
                        WHEN 'HOUSENUMBER' THEN CONCAT(standardized_address.housenumber, standardized_address.extension)
                        ELSE _value
                        END
                    , _occur
                    , _max_occurs
                    ;
            END IF;
            IF _level_up = 'STREET' THEN
                standardized_address.street_uncommon_value := _value;
                standardized_address.street_uncommon_occur := _occur;
            ELSIF _level_up = 'COMPLEMENT' THEN
                standardized_address.complement_uncommon_value := _value;
                standardized_address.complement_uncommon_occur := _occur;
            ELSE
                standardized_address.housenumber_uncommon_id := _value::INT;
                standardized_address.housenumber_uncommon_occur := _occur;
            END IF;
        END IF;
    ELSE
        RAISE NOTICE ' query=%', _query;
    END IF;
    with_uncommon := _uncommon;
END
$func$ LANGUAGE plpgsql;

/*
get query for matching area
parameters
    1 w/ uncommon
    2 uniq uncommon
    3 w/ postcode
 */
SELECT drop_all_functions_if_exists('fr', 'get_query_match');
CREATE OR REPLACE FUNCTION fr.get_query_match(
      level IN VARCHAR
    , search IN VARCHAR
    , parameters IN INT
    , query_match OUT TEXT
)
AS
$func$
BEGIN
    level := UPPER(level);
    search := UPPER(search);
    query_match := CASE
        WHEN level = 'AREA' AND parameters & 1 = 0 THEN
            /* NOTE
            $1 municipality_code
            $2 municipality_name
            $3 municipality_old_name
            $4 postcode
            */
            CONCAT(
                '
                SELECT
                '
                , CASE search
                    WHEN 'STRICT' THEN
                        '
                        ARRAY_AGG(area.co_adr)
                        '
                    ELSE
                        '
                        area.co_adr
                        , area.co_insee_commune
                        , area.co_postal
                        , area.lb_acheminement
                        , area.lb_ligne5
                        , CASE
                            WHEN $2 IS NOT NULL THEN
                                GREATEST(
                                    get_similarity($2, area.lb_acheminement)
                                    , get_similarity($2, area.lb_ligne5)
                                )
                            END similarity_1
                        , CASE
                            WHEN $3 IS NOT NULL THEN
                                get_similarity($3, area.lb_ligne5)
                            END similarity_2
                        '
                    END
                , '
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
                , CASE
                    WHEN parameters & 4 = 4 THEN
                        '
                        -- postcode (if defined)
                        AND (
                            ($4 IS NULL)
                            OR
                            (area.co_postal = $4)
                        )
                        '
                    END
                , CASE search
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
                            (($3 IS NULL) AND (area.lb_ligne5 IS NULL))
                            OR
                            (area.lb_ligne5 = $3)
                        )
                        '
                    ELSE
                        '
                        ORDER BY
                            GREATEST(
                                CASE
                                    WHEN $2 IS NOT NULL THEN
                                        GREATEST(
                                            get_similarity($2, area.lb_acheminement)
                                            , get_similarity($2, area.lb_ligne5)
                                        )
                                    END
                                , CASE
                                    WHEN $3 IS NOT NULL THEN
                                        get_similarity($3, area.lb_ligne5)
                                    END
                            )
                        '
                    END
            )
        WHEN level = 'STREET' AND (
            (parameters & 1 = 0) OR (parameters & 2 = 0)
            ) THEN
            'TODO'
        WHEN level = 'STREET' AND parameters & 2 = 2 THEN
            'TODO'
        END
        ;
END
$func$ LANGUAGE plpgsql;

-- match element (of address) w/ referential
/* NOTE
HSTORE parameters to custom properties, as:
'"AREA_THRESHOLD" => 0.6, "STREET_THRESHOLD" => 0.75, "STREET_RATIO" => 0.2'::HSTORE
defaults are defined as global variables, view constant.sql
 */
SELECT drop_all_functions_if_exists('fr', 'match_element');
CREATE OR REPLACE FUNCTION fr.match_element(
      level IN VARCHAR
    , step IN INT
    , standardized_address IN fr.standardized_address
    , matched_parents INOUT fr.matched_element[]
    , parameters IN HSTORE DEFAULT NULL
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , update_parent OUT BOOLEAN
    , matched_element OUT fr.matched_element
)
AS
$func$
DECLARE
    _SEARCHS VARCHAR[] := ARRAY['STRICT', 'NEAR'];
    _limits_by_level INT[4] := ARRAY[2, 4, 0, 2];
    _similarity_threshold REAL;
    _similarity_ratio REAL;
    _element_ratio REAL;
    _search VARCHAR;
    _todo BOOLEAN;
    _query TEXT;
    _query_parameters INT := 0;
    _word TEXT;
    _element_current RECORD;
    _element_previous RECORD;
    _with_near BOOLEAN := TRUE;
    _abbr VARCHAR;
    _ext VARCHAR;
    _timestamp TIMESTAMP := clock_timestamp();
BEGIN
    update_parent := FALSE;
    -- level w/ uncommon item
    IF step = 1 THEN
        _query_parameters := _query_parameters | 1;
        IF (
            (fr._get_value_from_standardized_address(
                  standardized_address => standardized_address
                , key => CONCAT(LOWER(level), '_uncommon_occur')
            ))::INT = 1
        ) THEN
            _query_parameters := _query_parameters | 2;
        END IF;
        -- not uniq
        IF _query_parameters & 2 = 0 THEN
        END IF;
    ELSE
    END IF;


    matched_element.elapsed_time := clock_timestamp() - _timestamp;

    /*
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
                _query := fr.get_query_match(
                    search => _search
                    , with_postcode => CASE _search WHEN 'NEAR_WO_POSTCODE' THEN FALSE ELSE TRUE END
                );

                IF _search != 'STRICT' AND _similarity_threshold IS NULL THEN
                    /* NOTE
                    similarity w/ low value (default to 0.5) to match:
                    ('ST MEDARD', 'ST MEDARD EN JALLES')
                     */
                    _similarity_threshold := fr.get_parameter_value(
                        parameters => parameters
                        , category => 'similarity'
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

                matched_element := fr.match_element_status(
                    search => _search
                    , matched_element => matched_element
                );
                IF (
                    matched_element.status != (SELECT (CURRENT_SETTING('fr.address.match.not_found')))
                ) THEN
                    EXIT;
                END IF;
            END IF;
        END LOOP;
        matched_element.elapsed_time := clock_timestamp() - _timestamp;
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
            matched_element := fr.match_element_status(
                search => 'STRICT'
                , matched_element => matched_element
            );

            -- not found: try w/ near approach
            IF (
                matched_element.status = (SELECT CURRENT_SETTING('fr.address.match.not_found'))
            ) THEN
                _similarity_threshold := fr.get_parameter_value(
                    parameters => parameters
                    , category => 'similarity'
                    , level => level
                    , key => 'THRESHOLD'
                );
                /* FIXME don't accept variable!
                SET pg_trgm.similarity_threshold = _similarity_threshold;
                set_limit() obsolete!
                 */
                _similarity_threshold := set_limit(_similarity_threshold);

                _similarity_ratio := fr.get_parameter_value(
                    parameters => parameters
                    , category => 'similarity'
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
                                matched_element.similarity_1 := _street.similarity;
                                IF (_street.co_adr_za != matched_parent.codes_address[1]) THEN
                                    RAISE NOTICE ' mais sur ZA différente (%/%)'
                                        , _street.co_adr_za
                                        , matched_parent.codes_address
                                    ;
                                    update_parent := TRUE;
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
     */
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
