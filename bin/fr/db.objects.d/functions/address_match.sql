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
get query for matching element
parameters
    1 w/ uncommon
    2 uniq uncommon
    3 w/ postcode
    4 w/ multiple element (of uncommon)
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
DECLARE
    _level_up VARCHAR := UPPER(level);
    _level_low VARCHAR := LOWER(level);
    _where_area VARCHAR;
    _columns VARCHAR;
BEGIN
    _where_area := CONCAT(
        '
        -- municipality code (if defined)
        (
            ($1 IS NULL)
            OR
            (area.co_insee_commune = $1)
        )
        '
        , CASE
            -- w/ postcode
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
            END
        );
    IF (parameters & 1 = 0) OR (parameters & 2 = 0) THEN
        _columns := CASE _level_up
            WHEN 'STREET' THEN 'co_voie'
            WHEN 'COMPLEMENT' THEN 'co_adr_voie, co_adr_numero'
            END
            ;
    END IF;
    query_match := CASE
        /* NOTE
        $1 municipality code
        $2 municipality name
        $3 municipality old name
        $4 postcode
         */
        WHEN _level_up = 'AREA' AND parameters & 1 = 0 THEN
            CONCAT(
                '
                SELECT
                '
                , CASE search
                    WHEN 'STRICT' THEN
                        '
                        ARRAY_AGG(area.co_adr) codes_address
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
                ', _where_area
                , CASE search
                    WHEN 'NEAR' THEN
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
        /* NOTE
        STRICT
        $1 parent code(s)
        $2 name

        NEAR
        $1 municipality code
        $2 municipality name
        $3 municipality old name
        $4 postcode
        $5 better word, or (multiple parent) parent address codes
        $6 words (name)
        $7 descriptors
        $8 limit
         */
        WHEN (((_level_up = 'STREET') OR (_level_up = 'COMPLEMENT')) AND (
            (parameters & 1 = 0) OR (parameters & 2 = 0)
        )) THEN
            CASE search
                WHEN 'STRICT' THEN
                    CONCAT(
                        '
                        SELECT
                            ARRAY_AGG(co_adr) codes_address
                        FROM
                            fr.', _level_low, '_dict_view
                        WHERE
                            name = $2
                            AND
                        ', CASE _level_up
                            WHEN 'STREET' THEN 'co_adr_za = ANY($1)'
                            ELSE '(
                                (co_adr_voie = ANY($1))
                                OR
                                (co_adr_numero = ANY($1))
                            )'
                            END
                    )
                ELSE
                    CONCAT(
                        '
                        WITH
                        potential_elements AS (
                            SELECT
                                  a.co_adr
                                , a.co_adr_za
                                , a.name
                                ', alias_words(_columns, ',[ ]*', 'a'), '
                                , r.name_id
                            FROM
                                fr.', _level_low, '_view a
                                    JOIN fr.laposte_address_', _level_low, '_reference r ON a.co_adr = r.address_id
                                    JOIN fr.laposte_address_', _level_low, '_membership m ON r.name_id = m.name_id
                            WHERE
                        '
                        , CASE
                            WHEN parameters & 8 = 0 THEN
                                '
                                m.word = $5
                                '
                            -- w/ multiple element (of uncommon)
                            ELSE
                                '
                                a.co_adr = ANY($5)
                                '
                            END
                        , ' AND '
                        , CASE
                            -- not uniq uncommon
                            WHEN parameters & 2 = 0 THEN
                                '
                                -- can verify area (known as ZA)
                                a.co_insee_commune = $1
                                '
                            ELSE
                                _where_area
                            END
                        , '
                        )
                        , similarity_elements AS (
                            SELECT
                                  co_adr
                                , co_adr_za
                                ', _columns, '
                                , name
                                , fr.get_similarity_words(
                                      words_a => $6
                                    , words_b => u.words
                                    , descriptors_a => $7
                                    , descriptors_b => u.descriptors
                                ) similarity
                            FROM
                                potential_elements p
                                    JOIN fr.laposte_address_', _level_low, '_uniq u ON p.name_id = u.id
                        )
                        SELECT
                            *
                        FROM
                            similarity_elements
                        ORDER BY
                            similarity DESC
                        LIMIT
                            $8
                        '
                    )
                END
        /* NOTE
        $1 uniq uncommon
         */
        WHEN _level_up = 'STREET' AND parameters & 2 = 2 THEN
            '
            SELECT
                  d.co_adr
                , d.co_adr_za
            FROM
                fr.laposte_address_street_membership m
                    JOIN fr.laposte_address_street_reference r ON m.name_id = r.name_id
                    JOIN fr.street_dict_view d ON r.name_id = d.id
            WHERE
                m.word = $1
            '
        /* NOTE
        $1 uncommon word
         */
        WHEN _level_up = 'COMPLEMENT' AND parameters & 2 = 2 THEN
            '
            SELECT
                  d.co_adr
                , d.co_adr_za
                , d.co_adr_voie
                , d.co_adr_numero
            FROM
                fr.laposte_address_complement_membership m
                    JOIN fr.laposte_address_complement_reference r ON m.name_id = r.name_id
                    JOIN fr.complement_dict_view d ON r.name_id = d.id
            WHERE
                m.word = $1
            '
        /* NOTE
        $1 parent code
        $2 housenumber
        $3 extension (STRICT, else abbreviated as NEAR)
         */
        WHEN _level_up = 'HOUSENUMBER' AND parameters & 1 = 0 THEN
            '
            SELECT
                ARRAY_AGG(co_adr) codes_address
            FROM
                fr.address_view
            WHERE
                co_adr_parent = ANY($1)
                AND
                co_adr_l3 IS NULL
                AND
                no_numero = $2
                AND (
                    ($3 IS NULL)
                    OR
                    (lb_extension_numero = $3)
                )
            '
        /* NOTE
        $1 housenumber id (uniq uncommon)
         */
        WHEN _level_up = 'HOUSENUMBER' AND parameters & 1 = 1 THEN
            '
            SELECT
                  co_adr
                , co_adr_za
                , co_adr_voie
            FROM
                fr.housenumber_dict_view
            WHERE
                id = $1
            '
        END
        ;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'exec_query_match');
CREATE OR REPLACE FUNCTION fr.exec_query_match(
      level IN VARCHAR
    , search IN VARCHAR
    , parameters IN INT
    , standardized_address IN fr.standardized_address
    , match_parameters IN fr.match_parameters
)
RETURNS SETOF RECORD
AS
$func$
DECLARE
    _query TEXT;
BEGIN
    _query := fr.get_query_match(
          level => level
        , search => _search
        , parameters => parameters
    );

    IF level = 'AREA' AND parameters & 1 = 0 THEN
        RETURN QUERY EXECUTE _query USING
              (standardized_address).municipality_code
            , (standardized_address).municipality_name
            , (standardized_address).municipality_old_name
            , (standardized_address).postcode
            ;
    ELSIF (((level = 'STREET') OR (level = 'COMPLEMENT')) AND (
            (parameters & 1 = 0) OR (parameters & 2 = 0))) THEN
        IF search = 'STRICT' THEN
            RETURN QUERY EXECUTE _query USING
                  (match_parameters).codes_address
                , fr._get_value_from_standardized_address(
                      standardized_address => standardized_address
                    , key => CONCAT(LOWER(level), '_name')
                )
                ;
        ELSE
            RETURN QUERY EXECUTE _query USING
                  (standardized_address).municipality_code
                , (standardized_address).municipality_name
                , (standardized_address).municipality_old_name
                , (standardized_address).postcode
                , CASE
                    WHEN parameters & 8 = 0 THEN
                        (match_parameters).word
                    -- w/ multiple element (of uncommon)
                    ELSE
                        (match_parameters).codes_address
                    END
                , fr._get_value_from_standardized_address(
                      standardized_address => standardized_address
                    , key => CONCAT(LOWER(level), '_words')
                )
                , fr._get_value_from_standardized_address(
                      standardized_address => standardized_address
                    , key => CONCAT(LOWER(level), '_descriptors')
                )
                , (match_parameters).limit
                ;
        END IF;
    ELSIF (((level = 'STREET') OR (level = 'COMPLEMENT')) AND (parameters & 2 = 2)) THEN
        RETURN QUERY EXECUTE _query USING
            (match_parameters).word
            ;
    ELSIF level = 'HOUSENUMBER' AND parameters & 1 = 0 THEN
        RETURN QUERY EXECUTE _query USING
              (match_parameters).codes_address[1]
            , (standardized_address).housenumber
            , CASE search
                WHEN 'STRICT' THEN (standardized_address).extension
                ELSE (match_parameters).abbreviated_extension
                END
            ;
    ELSIF level = 'HOUSENUMBER' AND parameters & 1 = 1 THEN
        RETURN QUERY EXECUTE _query USING
            (match_parameters).uncommon_id
            ;
    ELSE
        RAISE 'exec_query_match: usecase not defined!';
    END IF;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'is_match_todo');
CREATE OR REPLACE FUNCTION fr.is_match_todo(
      level IN VARCHAR
    , search IN VARCHAR
    , standardized_address IN fr.standardized_address
    , is_todo OUT BOOLEAN
)
AS
$func$
BEGIN
    is_todo := CASE level
        WHEN 'AREA' THEN
            CASE search
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
                            (standardized_address).municipality_name IS NOT NULL
                            OR
                            (standardized_address).municipality_old_name IS NOT NULL
                        )
                        AND
                        ((standardized_address).postcode IS NOT NULL)
                    )
            END
        WHEN 'STREET' THEN
            (standardized_address).street_name IS NOT NULL
        WHEN 'HOUSENUMBER' THEN
            (standardized_address).housenumber IS NOT NULL
        WHEN 'COMPLEMENT' THEN
            (standardized_address).complement_name IS NOT NULL
        END
        ;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'notice_match');
CREATE OR REPLACE PROCEDURE fr.notice_match(
      level IN VARCHAR
    , search IN VARCHAR
    , standardized_address IN fr.standardized_address
    , usecase IN VARCHAR
    , current IN RECORD
    , ratio IN REAL DEFAULT 1
    , raise_notice IN BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _notice VARCHAR;
BEGIN
    IF raise_notice THEN
        _notice := CASE
            WHEN usecase = level THEN
                CASE level
                    WHEN 'AREA' THEN
                        FORMAT('%s: MUNICIPALITY(%s) OLD(%s) POSTCODE(%s) NAME(%s)'
                            , level
                            , (standardized_address).municipality_code
                            , (standardized_address).municipality_old_name
                            , (standardized_address).postcode
                            , (standardized_address).municipality_name
                        )
                    WHEN 'STREET' THEN
                        FORMAT('%s: MUNICIPALITY(%s) NAME(%s)'
                            , level
                            , (standardized_address).municipality_code
                            , (standardized_address).street_name
                        )
                    WHEN 'HOUSENUMBER' THEN
                        FORMAT('%s: MUNICIPALITY(%s) NUMBER(%s) EXTENSION(%s)'
                            , level
                            , (standardized_address).municipality_code
                            , (standardized_address).housenumber
                            , (standardized_address).extension
                        )
                    WHEN 'COMPLEMENT' THEN
                        FORMAT('%s: MUNICIPALITY(%s) NAME(%s)'
                            , level
                            , (standardized_address).municipality_code
                            , (standardized_address).complement_name
                        )
                    END
            WHEN usecase = '1ST_NOT_NEAR_1' THEN
                FORMAT('%s: first choice too low NAME(%s) [SIMILARITY=%s]'
                    , level
                    , (current).lb_acheminement
                    , ROUND(current.similarity_1, 5)
                )
            WHEN usecase = '1ST_NOT_NEAR_2' THEN
                FORMAT('%s: first choice too low OLD_NAME(%s) [SIMILARITY=%s]'
                    , level
                    , (current).lb_ligne5
                    , ROUND(current.similarity_2, 5)
                )
            WHEN usecase = '1ST_NOT_NEAR' THEN
                FORMAT('%s: first choice too low NAME(%s) [SIMILARITY=%s]'
                    , level
                    , (current).name
                    , ROUND(current.similarity, 5)
                )
            WHEN usecase = '1ST_OK_1' THEN
                FORMAT('%s: first choice ok NAME(%s) [SIMILARITY=%s]'
                    , level
                    , (current).lb_acheminement
                    , ROUND(current.similarity_1, 5)
                )
            WHEN usecase = '1ST_OK_2' THEN
                FORMAT('%s: first choice ok OLD_NAME(%s) [SIMILARITY=%s]'
                    , level
                    , (current).lb_ligne5
                    , ROUND(current.similarity_2, 5)
                )
            WHEN usecase = '1ST_OK' THEN
                FORMAT('%s: first choice ok NAME(%s) [SIMILARITY=%s]'
                    , level
                    , (current).name
                    , ROUND(current.similarity, 5)
                )
            WHEN usecase = '2ND_SAME_CODE' THEN
                FORMAT('%s: second choice same CODE(%s) [ADDRESS=%s]'
                    , level
                    , (current).co_voie
                    , (current).co_adr
                )
            WHEN usecase = '2ND_TOO_SIMILAR_1' THEN
                FORMAT('%s: second choice too similar NAME(%s) [SIMILARITY=%s,RATIO=%s]'
                    , level
                    , (current).lb_acheminement
                    , ROUND(current.similarity_1, 5)
                    , ROUND(ratio, 2)
                )
            WHEN usecase = '2ND_TOO_SIMILAR_2' THEN
                FORMAT('%s: second choice too similar OLD_NAME(%s) [SIMILARITY=%s,RATIO=%s]'
                    , level
                    , (current).lb_ligne5
                    , ROUND(current.similarity_2, 5)
                    , ROUND(ratio, 2)
                )
            WHEN usecase = '2ND_TOO_SIMILAR' THEN
                FORMAT('%s: second choice too similar NAME(%s) [SIMILARITY=%s,RATIO=%s]'
                    , level
                    , (current).name
                    , ROUND(current.similarity, 5)
                    , ROUND(ratio, 2)
                )
            WHEN usecase = '2ND_OK_1' THEN
                FORMAT('%s: second choice ok NAME(%s) [SIMILARITY=%s,RATIO=%s]'
                    , level
                    , (current).name
                    , ROUND(current.similarity_1, 5)
                    , ROUND(ratio, 2)
                )
            WHEN usecase = '2ND_OK_2' THEN
                FORMAT('%s: second choice ok NAME(%s) [SIMILARITY=%s,RATIO=%s]'
                    , level
                    , (current).name
                    , ROUND(current.similarity_2, 5)
                    , ROUND(ratio, 2)
                )
            WHEN usecase = '2ND_OK' THEN
                FORMAT('%s: second choice ok NAME(%s) [SIMILARITY=%s,RATIO=%s]'
                    , level
                    , (current).name
                    , ROUND(current.similarity, 5)
                    , ROUND(ratio, 2)
                )
            END
            ;
        RAISE NOTICE ' %', _notice;
    END IF;
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'analyze_matched_elements');
CREATE OR REPLACE FUNCTION fr.analyze_matched_elements(
      level IN VARCHAR
    , search IN VARCHAR
    , parameters IN INT
    , standardized_address IN fr.standardized_address
    , matched_parent IN fr.matched_element
    , current IN RECORD
    , previous IN RECORD
    , similarity_threshold IN REAL DEFAULT 0.5
    , similarity_ratio IN REAL DEFAULT 0.5
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , matched_parents OUT fr.matched_element[]
    , matched_element OUT fr.matched_element
)
AS
$func$
DECLARE
    _element_ratio REAL;
BEGIN
    IF search = 'STRICT' THEN
        matched_element.codes_address := current.codes_address;
        matched_element := fr.match_element_status(
              search => search
            , matched_element => matched_element
        );
    ELSE
        IF (previous IS NULL) THEN
            CALL fr.notice_match(
                  level => level
                , search => search
                , standardized_address => standardized_address
                , usecase => level
                , current => current
            );
            IF ((
                    (level = 'AREA')
                    AND
                    (COALESCE(current.similarity_1, 0) < similarity_threshold)
                    AND
                    (COALESCE(current.similarity_2, 0) < similarity_threshold)
                ) OR (
                    (level != 'AREA')
                    AND
                    (COALESCE(current.similarity, 0) < similarity_threshold)
                )
            ) THEN
                CALL fr.notice_match(
                      level => level
                    , search => search
                    , standardized_address => standardized_address
                    , usecase => CASE
                        WHEN level = 'AREA' AND standardized_address.municipality_name IS NOT NULL AND current.similarity_1 < similarity_threshold THEN
                            '1ST_NOT_NEAR_1'
                        WHEN level = 'AREA' AND standardized_address.municipality_old_name IS NOT NULL AND current.similarity_2 < similarity_threshold THEN
                            '1ST_NOT_NEAR_2'
                        ELSE '1ST_NOT_NEAR'
                        END
                    , current => current
                );
                matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.not_near'));
            ELSE
                CALL fr.notice_match(
                      level => level
                    , search => search
                    , standardized_address => standardized_address
                    , usecase => '1ST_OK'
                    , current => current
                );
                matched_element.codes_address := ARRAY[current.co_adr]::VARCHAR[];
                matched_element.similarity_1 := CASE
                    WHEN level = 'AREA' THEN current.similarity_1
                    ELSE current.similarity
                    END;
                IF level = 'AREA' AND standardized_address.municipality_old_name IS NOT NULL THEN
                    matched_element.similarity_2 := current.similarity_2;
                END IF;

                /* TODO STREET, else ...
                IF (current.co_adr_za != matched_parent.codes_address[1]) THEN
                    RAISE NOTICE ' mais sur ZA différente (%/%)'
                        , current.co_adr_za
                        , matched_parent.codes_address
                    ;
                    matched_parent.codes_address := ARRAY[current.co_adr_za]::VARCHAR[];
                    matched_element := fr.match_element_status(
                        search => search
                        , matched_element => matched_element
                    );
                    matched_parent := (
                        SELECT fr.match_element_status(
                            search => 'NEAR'
                            , matched_element => matched_parent
                        )
                    );
                END IF;
                 */
            END IF;
        ELSE
            IF (level = 'STREET'
                AND
                previous.co_voie = current.co_voie
            ) THEN
                CALL fr.notice_match(
                      level => level
                    , search => search
                    , standardized_address => standardized_address
                    , usecase => '2ND_SAME_CODE'
                    , current => current
                );
                -- TODO test postcode!
                matched_element.codes_address := ARRAY_APPEND(matched_element.codes_address, current.co_adr);
                RETURN;
            END IF;

            /* NOTE
            OK if second choice far enough (15%)
            minimum gap between 2 results ascending when similarity decrease
            */
            _element_ratio := (previous.similarity / current.similarity);
            IF NOT (_element_ratio > similarity_ratio) THEN
                CALL fr.notice_match(
                      level => level
                    , search => search
                    , standardized_address => standardized_address
                    , usecase => '2ND_TOO_SIMILAR'
                    , current => current
                    , ratio => _element_ratio
                );
                matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.too_similar'));
            ELSE
                CALL fr.notice_match(
                      level => level
                    , search => search
                    , standardized_address => standardized_address
                    , usecase => '2ND_OK'
                    , current => current
                    , ratio => _element_ratio
                );
                matched_element.status := (SELECT CURRENT_SETTING('fr.address.match.near'));
            END IF;
        END IF;
    END IF;
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
    , matched_parent IN fr.matched_element
    , parameters IN HSTORE DEFAULT NULL
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , matched_parents OUT fr.matched_element[]
    , matched_element OUT fr.matched_element
)
AS
$func$
DECLARE
    _searchs VARCHAR[] := ARRAY['STRICT', 'NEAR'];
    _search VARCHAR;
    _limits_by_level INT[4] := ARRAY[2, 4, 0, 2];
    _similarity_threshold REAL;
    _similarity_ratio REAL;
    _query TEXT;
    _query_parameters INT := 0;
    _element_current RECORD;
    _element_previous RECORD;
    _with_near BOOLEAN := TRUE;
    _match_parameters fr.match_parameters;
    _match_result RECORD;
    _timestamp TIMESTAMP := clock_timestamp();
BEGIN
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
    END IF;
    -- w/ postcode
    _query_parameters := _query_parameters | 4;
    -- parent codes address
    _match_parameters.codes_address := matched_parent.codes_address;

    IF level = 'AREA' THEN
        _searchs := ARRAY_APPEND(_searchs, 'NEAR_WO_POSTCODE');
    ELSIF level = 'HOUSENUMBER' THEN
        IF (standardized_address).extension IS NOT NULL THEN
            _match_parameters.abbreviated_extension := fr.normalize_abbreviate_keyword(
                  name => (standardized_address).extension
                , groups => 'EXT'
            );
        END IF;
        IF _match_parameters.abbreviated_extension IS NULL THEN
            _searchs := ARRAY_REMOVE(_searchs, 'NEAR');
        END IF;
        IF _query_parameters & 1 = 1 THEN
            _match_parameters.uncommon_id := (standardized_address).housenumber_uncommon_id;
        END IF;
    END IF;

    FOREACH _search IN ARRAY _searchs
    LOOP
        IF _search = 'NEAR_WO_POSTCODE' THEN
            -- XOR better but!
            _query_parameters := _query_parameters - 4;
        END IF;

        IF fr.is_match_todo(
              level => level
            , search => _search
            , standardized_address => standardized_address
        ) THEN
            -- near match, set threshold, ratio, better word
            IF _search != 'STRICT' THEN
                IF _similarity_threshold IS NULL THEN
                    /* NOTE
                    AREA
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

                    _similarity_ratio := fr.get_parameter_value(
                          parameters => parameters
                        , category => 'similarity'
                        , level => level
                        , key => 'RATIO'
                    );
                END IF;

                IF ((level = 'STREET') OR (level = 'COMPLEMENT')) THEN
                    _match_parameters.word := CASE
                        WHEN _query_parameters & 1 = 0 THEN
                            -- retrieve word w/ better similarity and rarity
                            fr.get_better_word_with_similarity_criteria(
                                  level => level
                                , words => fr._get_value_from_standardized_address(
                                    standardized_address => standardized_address
                                    , key => CONCAT(LOWER(level), '_words')
                                )
                                , zone => 'ZA'
                                , codes => (matched_parent).codes_address
                                , raise_notice => raise_notice
                            )
                        ELSE
                            -- uncommon word
                            fr._get_value_from_standardized_address(
                                  standardized_address => standardized_address
                                , key => CONCAT(LOWER(level), '_uncommon_value')
                            )
                        END
                        ;
                    IF _match_parameters.word IS NULL THEN
                        RAISE NOTICE 'match_element: no better word!';
                        RAISE NOTICE ' level(%), search(%), data(%)'
                            , level
                            , _search
                            , standardized_address
                            ;
                        CONTINUE;
                    END IF;
                END IF;

                /* NOTE
                for STREET, limit set to 4
                same street can be delivered by many areas (postcode, district, ...)
                to the maximum it exists a street w/ 3 areas!
                this loop aims to find gap between two successive streets, so 3 +1
                for others, limit 2 is ok
                 */
                _match_parameters.limit := _limits_by_level[fr.get_subscript_of_level_address(level)];
            END IF;

            _element_previous := ROW(NULL);
            matched_element.status := NULL;
            _query := CONCAT(
                '
                SELECT * FROM fr.exec_query_match(
                      level => $1
                    , search => $2
                    , parameters => $3
                    , standardized_address => $4
                    , match_parameters => $5
                ) AS t('
                ,
                CASE
                    WHEN level = 'AREA' AND _query_parameters & 1 = 0 THEN
                        CASE _search
                            WHEN 'STRICT' THEN
                                '
                                codes_address CHAR(10)[]
                                '
                            ELSE
                                '
                                  co_adr CHAR(10)
                                , co_insee_commune CHAR(5)
                                , co_postal CHAR(5)
                                , lb_acheminement VARCHAR
                                , lb_ligne5 VARCHAR
                                , similarity_1 REAL
                                , similarity_2 REAL
                                '
                            END
                    WHEN ((level = 'STREET') AND ((parameters & 1 = 0) OR (parameters & 2 = 0))) THEN
                        CASE _search
                            WHEN 'STRICT' THEN
                                '
                                codes_address CHAR(10)[]
                                '
                            ELSE
                                '
                                  co_adr CHAR(10)
                                , co_adr_za CHAR(10)
                                , co_voie INT
                                , name VARCHAR
                                , similarity REAL
                                '
                            END
                    WHEN ((level = 'COMPLEMENT') AND ((parameters & 1 = 0) OR (parameters & 2 = 0))) THEN
                        CASE _search
                            WHEN 'STRICT' THEN
                                '
                                codes_address CHAR(10)[]
                                '
                            ELSE
                                '
                                  co_adr CHAR(10)
                                , co_adr_za CHAR(10)
                                , co_adr_voie CHAR(10)
                                , co_adr_numero CHAR(10)
                                , name VARCHAR
                                , similarity REAL
                                '
                            END
                    WHEN level = 'STREET' AND parameters & 2 = 2 THEN
                        '
                          co_adr CHAR(10)
                        , co_adr_za CHAR(10)
                        '
                    WHEN level = 'COMPLEMENT' AND parameters & 2 = 2 THEN
                        '
                          co_adr CHAR(10)
                        , co_adr_za CHAR(10)
                        , co_adr_voie CHAR(10)
                        , co_adr_numero CHAR(10)
                        '
                    WHEN level = 'HOUSENUMBER' AND parameters & 1 = 0 THEN
                        '
                        codes_address CHAR(10)[]
                        '
                    WHEN level = 'HOUSENUMBER' AND parameters & 1 = 1 THEN
                        '
                          co_adr CHAR(10)
                        , co_adr_za CHAR(10)
                        , co_adr_voie CHAR(10)
                        '
                    END
                , ')'
                )
                ;
            FOR _element_current IN EXECUTE _query USING
                  level
                , _search
                , _query_parameters
                , standardized_address
                , _match_parameters
            LOOP
                _match_result := fr.analyze_matched_elements(
                      level => level
                    , search => _search
                    , parameters => _query_parameters
                    , standardized_address => standardized_address
                    , matched_parent => matched_parent
                    , current => _element_current
                    , previous => _element_previous
                    , similarity_threshold => _similarity_threshold
                    , similarity_ratio => _similarity_ratio
                    , raise_notice => raise_notice
                );
                matched_parents := _match_result.matched_parents;
                matched_element := _match_result.matched_element;
                IF matched_element.status IS NOT NULL THEN
                    EXIT;
                END IF;

                _element_previous := _element_current;
            -- loop elements
            END LOOP;
        ELSE
            RAISE NOTICE 'match_element: not todo!';
            RAISE NOTICE ' level(%), search(%), data(%)'
                , level
                , _search
                , standardized_address
                ;
        END IF;
    -- loop searchs
    END LOOP;

    matched_element.elapsed_time := clock_timestamp() - _timestamp;
    IF matched_element.status IS NULL THEN
        matched_element := fr.match_element_status(
                search => 'NEAR'
            , matched_element => matched_element
        );
    END IF;


    /*
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
                _searchs := ARRAY_REMOVE(_searchs, 'NEAR');
            END IF;

            FOREACH _search IN ARRAY _searchs
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
