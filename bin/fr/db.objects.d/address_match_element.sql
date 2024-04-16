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
              codes_address CHAR(10)[]
            , elapsed_time INTERVAL
            , status VARCHAR
            , similarity_1 NUMERIC
            , similarity_2 NUMERIC              -- AREA (municipality_name AND old_name)
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
      id SERIAL NOT NULL
    , level VARCHAR
    , match_code VARCHAR
    , matched_element fr.matched_element
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_element_id ON fr.address_match_element(id);
CREATE INDEX IF NOT EXISTS ix_address_match_element_match_code ON fr.address_match_element(match_code);

-- match each element of address (of a matching request)
SELECT drop_all_functions_if_exists('fr', 'set_match_element');
CREATE OR REPLACE PROCEDURE fr.set_match_element(
      id IN INT
    , force IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _is_match_element BOOLEAN;
    _parameters HSTORE;
    _nrows INTEGER;
    _info VARCHAR := CONCAT('rapprochement ELEMENT demande Rapprochement (', id, ')');
    _step INT;
    _levels VARCHAR[] := ARRAY['AREA', 'STREET', 'HOUSENUMBER', 'COMPLEMENT'];
    _level VARCHAR;
    _query TEXT;
    _element RECORD;
    _record RECORD;
BEGIN
    SELECT is_match_element, parameters
    INTO _is_match_element, _parameters
    FROM fr.address_match_request mr
    WHERE mr.id = set_match_element.id
    ;

    IF _is_match_element IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée "%"', id;
    END IF;

    IF force OR NOT _is_match_element THEN
        -- step 1 (uncommon), step 2 (others)
        FOR _step IN 1 .. 2
        LOOP
            FOREACH _level IN ARRAY _levels
            LOOP
                IF _step = 1 AND _level = 'AREA' THEN
                    CONTINUE;
                END IF;

                -- search for element not already matched (w/ its matched parent if exists)
                _query := CONCAT(
                    '
                    SELECT
                        mc.level
                        , mc.match_code_element
                        , '
                    , CASE _step
                        WHEN 1 THEN
                            CONCAT('(SELECT
                                ARRAY[
                                    (standardized_address).match_code_area
                                    , (standardized_address).match_code_street
                                    , (standardized_address).match_code_housenumber
                                    , (standardized_address).match_code_complement
                                ]
                                FROM fr.address_match_result
                                WHERE id_request = $1
                                AND
                                (standardized_address).match_code_', LOWER(_level), ' =
                                mc.match_code_element
                                LIMIT 1
                            )')
                        ELSE
                            CASE _level
                                WHEN 'AREA' THEN 'ARRAY[NULL]::VARCHAR[]'
                                ELSE 'ARRAY[mc.match_code_parent]'
                                END
                        END, ' match_code_parents, '
                    , CASE _level
                        WHEN 'AREA' THEN 'ARRAY[NULL]'
                        ELSE 'ARRAY[me.matched_element]'
                        END, '::fr.matched_element[] matched_parents
                        , (SELECT standardized_address
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
                _query := CONCAT(_query
                    , '
                    WHERE
                        mc.id_request = $1
                        AND
                        mc.level = $2
                        AND
                        NOT EXISTS(
                            SELECT 1
                            FROM fr.address_match_element me2
                            WHERE mc.match_code_element = me2.match_code
                        )
                    '
                );
                IF _step = 1 THEN
                    _query := CONCAT(_query
                        , '
                        AND (
                            (SELECT (standardized_address).'
                        , CASE _level
                            WHEN 'STREET' THEN 'street_uncommon_value'
                            WHEN 'HOUSENUMBER' THEN 'housenumber_uncommon_id'
                            WHEN 'COMPLEMENT' THEN 'complement_uncommon_value'
                            END
                        , '
                                FROM fr.address_match_result
                                WHERE id_request = $1
                                AND
                                (standardized_address).match_code_', LOWER(_level), ' =
                                mc.match_code_element
                                LIMIT 1
                            ) IS NOT NULL
                        )
                        '
                    );
                END IF;

                _nrows := 0;
                FOR _element IN EXECUTE _query USING id, _level
                LOOP
                    -- match element
                    _record := fr.match_element(
                          level => _element.level
                        , step => _step
                        , standardized_address => _element.standardized_address
                        , matched_parents => _element.matched_parents
                        , parameters => _parameters
                    );

                    -- new matched element w/ its match code
                    INSERT INTO fr.address_match_element(
                        level
                        , match_code
                        , matched_element
                    )
                    VALUES(
                        _element.level
                        , _element.match_code_element
                        , _record.matched_element
                    )
                    ;

                    IF _record.update_parent THEN
                        UPDATE fr.address_match_element SET
                            matched_element = _record.matched_parent
                            WHERE
                                match_code = _element.match_code_parent
                            ;
                    END IF;

                    _nrows := nrows +1;
                END LOOP;
                CALL public.log_info(CONCAT(_info, ' [STEP=%] : #%=%', _step, _level, _nrows));
            END LOOP;
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
