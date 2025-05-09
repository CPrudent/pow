/***
 * FR-ADDRESS matching address (element)
 */

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'matched_element')
    OR NOT EXISTS (
        SELECT 1 FROM information_schema.attributes
        WHERE udt_name = 'matched_element' AND attribute_name = 'similarity_1')
    THEN
        DROP TYPE IF EXISTS fr.matched_element CASCADE;
        CREATE TYPE fr.matched_element AS (
            codes_address CHAR(10)[],
            elapsed_time INTERVAL,
            status VARCHAR,
            similarity_1 NUMERIC,
            similarity_2 NUMERIC              -- AREA (municipality_name AND old_name)
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'match_parameters')
    OR NOT EXISTS (
        SELECT 1 FROM information_schema.attributes
        WHERE udt_name = 'match_parameters' AND attribute_name = 'rating')
    THEN
        DROP TYPE IF EXISTS fr.match_parameters CASCADE;
        CREATE TYPE fr.match_parameters AS (
            codes_address CHAR(10)[],
            word VARCHAR,
            rating NUMERIC,
            "limit" INT,
            abbreviated_extension VARCHAR,
            uncommon_id INT
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT column_exists('fr', 'address_match_element', 'matched_element') THEN
        -- has to be rebuild!
        DROP TABLE IF EXISTS fr.address_match_element;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.address_match_element (
    id SERIAL NOT NULL,
    level VARCHAR,
    match_code VARCHAR,
    matched_element fr.matched_element
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_element_id ON fr.address_match_element(id);
CREATE INDEX IF NOT EXISTS ix_address_match_element_match_code ON fr.address_match_element(match_code);

-- match each element of address (of a matching request)
SELECT drop_all_functions_if_exists('fr', 'set_match_element');
CREATE OR REPLACE PROCEDURE fr.set_match_element(
    id IN INT,
    force IN BOOLEAN DEFAULT FALSE,
    raise_notice IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _is_match_element BOOLEAN;
    _parameters HSTORE;
    _nrows INTEGER;
    _info VARCHAR;
    _step INT;
    _levels VARCHAR[] := ARRAY['AREA', 'STREET', 'HOUSENUMBER', 'COMPLEMENT'];
    _level VARCHAR;
    _query TEXT;
    _element RECORD;
    _matched_element fr.matched_element;
    _init_ranks BOOLEAN := FALSE;
BEGIN
    SELECT is_match_element, parameters
    INTO _is_match_element, _parameters
    FROM fr.address_match_request mr
    WHERE mr.id = set_match_element.id
    ;

    IF _is_match_element IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée pour ID ''%''', id;
    END IF;

    _info := CONCAT('gestion ELEMENT demande Rapprochement (', id, ')');
    IF force OR NOT _is_match_element THEN
        CALL public.log_info(_info);

        WITH
        request_mc(match_code) AS (
            SELECT match_code_element
            FROM fr.address_match_code
            WHERE id_request = set_match_element.id
        )
        DELETE FROM fr.address_match_element me
            USING request_mc mr
            WHERE me.match_code = mr.match_code
        ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        IF _nrows > 0 THEN
            CALL public.log_info(CONCAT('===[PURGE] #', _nrows));
        END IF;

        FOREACH _level IN ARRAY _levels
        LOOP
            IF raise_notice THEN
                CALL public.log_info(
                    FORMAT('===[LEVEL=%s]%s', _level, REPEAT('=', 70))
                );
            END IF;

            IF NOT _init_ranks AND _level != 'AREA' THEN
                DROP TABLE IF EXISTS tmp_fr_match_municipality;
                CREATE TEMPORARY TABLE tmp_fr_match_municipality AS
                    WITH
                    municipalities(code) AS (
                        SELECT DISTINCT
                            (mr.standardized_address).municipality_code
                        FROM
                            fr.address_match_result mr
                                LEFT OUTER JOIN fr.address_match_element me ON ((mr.standardized_address).level, (mr.standardized_address).match_code_area) = (me.level, me.match_code)
                        WHERE
                            mr.id_request = set_match_element.id
                    )
                    SELECT
                        m.code,
                        'STREET' level,
                        MAX(wl.rank) nranks
                    FROM
                        fr.laposte_address_street_word_level wl
                            JOIN municipalities m ON m.code = wl.codgeo
                    WHERE
                        wl.nivgeo = 'COM'
                    GROUP BY
                        m.code
                    UNION
                    SELECT
                        m.code,
                        'COMPLEMENT' level,
                        MAX(wl.rank) nranks
                    FROM
                        fr.laposte_address_complement_word_level wl
                            JOIN municipalities m ON m.code = wl.codgeo
                    WHERE
                        wl.nivgeo = 'COM'
                    GROUP BY
                        m.code
                ;

                IF raise_notice THEN
                    CALL public.log_info(' RANKS');
                END IF;
                _init_ranks := TRUE;
            END IF;

            -- search for element not already matched (w/ its matched parent if exists)
            _query := CONCAT(
                '
                SELECT
                    mc.level,
                    mc.match_code_element,
                ',
                CASE _level
                    WHEN 'AREA' THEN 'ARRAY[NULL]::VARCHAR[]'
                    ELSE 'ARRAY[mc.match_code_parent]'
                    END, ' match_code_parents, ',
                CASE _level
                    WHEN 'AREA' THEN 'NULL::fr.matched_element'
                    ELSE 'me.matched_element'
                    END, ' matched_parent,
                    (
                        SELECT standardized_address
                        FROM fr.address_match_result
                        WHERE id_request = $1
                        AND
                        (standardized_address).match_code_', LOWER(_level), ' =
                        mc.match_code_element
                        LIMIT 1
                    ) standardized_address
                FROM
                    fr.address_match_code mc
                '
            );
            IF _level != 'AREA' THEN
                _query := CONCAT(_query
                    , '
                        LEFT OUTER JOIN fr.address_match_element me
                            ON me.match_code = mc.match_code_parent
                    '
                );
            END IF;
            _query := CONCAT(_query,
                '
                WHERE
                    mc.id_request = $1
                    AND
                    mc.level = $2
                '
            );
            IF NOT force THEN
                _query := CONCAT(_query,
                    '
                    AND
                    NOT EXISTS(
                        SELECT 1
                        FROM fr.address_match_element me2
                        WHERE mc.match_code_element = me2.match_code
                    )
                    '
                );
            END IF;

            _nrows := 0;
            FOR _element IN EXECUTE _query USING id, _level
            LOOP
                IF raise_notice THEN
                    CALL public.log_info(
                        FORMAT('---(%s=%s)',
                            _level,
                            CASE _level
                            WHEN 'AREA' THEN
                                CONCAT_WS('-',
                                    fr._get_value_from_standardized_address(
                                        standardized_address => _element.standardized_address,
                                        key => 'municipality_old_name'
                                    ),
                                    fr._get_value_from_standardized_address(
                                        standardized_address => _element.standardized_address,
                                        key => 'postcode'
                                    ),
                                    fr._get_value_from_standardized_address(
                                        standardized_address => _element.standardized_address,
                                        key => 'municipality_name'
                                    )
                                )
                            WHEN 'STREET' THEN
                                fr._get_value_from_standardized_address(
                                    standardized_address => _element.standardized_address,
                                    key => 'street_name'
                                )
                            WHEN 'HOUSENUMBER' THEN
                                CONCAT(
                                    fr._get_value_from_standardized_address(
                                        standardized_address => _element.standardized_address,
                                        key => 'housenumber'
                                    ),
                                    fr._get_value_from_standardized_address(
                                        standardized_address => _element.standardized_address,
                                        key => 'extension'
                                    )
                                )
                            WHEN 'COMPLEMENT' THEN
                                fr._get_value_from_standardized_address(
                                    standardized_address => _element.standardized_address,
                                    key => 'complement_name'
                                )
                            END
                        )
                    );
                    CALL public.log_info(
                        FORMAT(' ELEMENT=%s', _element)
                    );
                END IF;

                -- match element
                _matched_element := fr.match_element(
                    level => _element.level,
                    standardized_address => _element.standardized_address,
                    matched_parent => _element.matched_parent,
                    parameters => _parameters,
                    raise_notice => raise_notice
                );

                -- save matched element
                WITH element(level, match_code) AS (
                    VALUES (_element.level, _element.match_code_element)
                )
                MERGE INTO fr.address_match_element me
                USING element t ON
                    me.match_code = t.match_code
                    AND
                    me.level = t.level
                WHEN MATCHED /*AND force*/ THEN
                    UPDATE SET
                        matched_element = _matched_element
                WHEN NOT MATCHED THEN
                    INSERT (
                        level,
                        match_code,
                        matched_element
                    )
                    VALUES(
                        _element.level,
                        _element.match_code_element,
                        _matched_element
                    )
                ;

                _nrows := _nrows +1;
            END LOOP;
            CALL public.log_info(
                FORMAT('===[LEVEL=%s] #%s', _level, _nrows)
            );
        END LOOP;

        UPDATE fr.address_match_request mr SET
            is_match_element = TRUE
            WHERE
                mr.id = set_match_element.id
        ;
    ELSE
        CALL public.log_info(CONCAT(_info, ' : déjà traitée (option force disponible)'));
    END IF;
END
$proc$ LANGUAGE plpgsql;
