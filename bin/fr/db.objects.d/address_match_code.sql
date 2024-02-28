/***
 * FR-ADDRESS matching address (match code)
 */

CREATE TABLE IF NOT EXISTS fr.address_match_code (
      id_request INTEGER NOT NULL
    , level VARCHAR
    , match_code_element VARCHAR
    , match_code_parent VARCHAR
    , standardized_address fr.standardized_address
);

CREATE INDEX IF NOT EXISTS ix_address_match_code_id_level ON fr.address_match_code(id, level);
CREATE INDEX IF NOT EXISTS ix_address_match_code_element_parent ON fr.address_match_code(match_code_element, match_code_parent);

SELECT drop_all_functions_if_exists('fr', 'get_match_code');
CREATE OR REPLACE FUNCTION fr.get_match_code(
    level IN VARCHAR
    , standardized_address IN fr.standardized_address
    , match_code OUT VARCHAR
)
AS $$
BEGIN
    match_code := MD5(CASE level
        WHEN 'AREA' THEN
            CONCAT(
                  (standardized_address).municipality_old_name
                , (standardized_address).postcode
                , (standardized_address).municipality_code
                , (standardized_address).municipality_name
            )
        WHEN 'STREET' THEN
            CONCAT(
                  (standardized_address).municipality_old_name
                , (standardized_address).postcode
                , (standardized_address).municipality_code
                , (standardized_address).municipality_name
                , (standardized_address).street
            )
        WHEN 'HOUSENUMBER' THEN
            CONCAT(
                  (standardized_address).municipality_old_name
                , (standardized_address).postcode
                , (standardized_address).municipality_code
                , (standardized_address).municipality_name
                , (standardized_address).street
                , (standardized_address).housenumber
                , (standardized_address).housenumber_extension
            )
        WHEN 'COMPLEMENT' THEN
            CONCAT(
                  (standardized_address).municipality_old_name
                , (standardized_address).postcode
                , (standardized_address).municipality_code
                , (standardized_address).municipality_name
                , (standardized_address).street
                , (standardized_address).housenumber
                , (standardized_address).housenumber_extension
                , (standardized_address).complement
            )
        END
    );
END $$ LANGUAGE plpgsql;

-- eval distinct match codes (of a matching request)
SELECT drop_all_functions_if_exists('fr', 'set_match_code');
CREATE OR REPLACE PROCEDURE fr.set_match_code(
      id IN INT
    , force IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _is_match_code BOOLEAN;
    _query TEXT;
    _nrows INTEGER;
    _info VARCHAR := CONCAT('gestion MATCH CODE demande Rapprochement (', id, ')');
BEGIN
    SELECT is_match_code
    INTO _is_match_code
    FROM fr.address_match_request mr
    WHERE mr.id = set_match_code.id
    ;

    IF _is_match_code IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée ''%''', id;
    END IF;

    IF force OR NOT _is_match_code THEN
        DELETE FROM fr.address_match_code WHERE id_request = id;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        IF _nrows > 0 THEN
            CALL public.log_info(CONCAT(_info, ' - PURGE : #', _nrows));
        END IF;

        _query :=
            '
            WITH
            match_codes AS (
                SELECT DISTINCT
                      ''AREA'' level
                    ,(standardized_address).match_code_area match_code_element
                    , NULL match_code_parent
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                UNION
                SELECT DISTINCT
                      ''STREET'' level
                    ,(standardized_address).match_code_street match_code_element
                    ,(standardized_address).match_code_area match_code_parent
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                UNION
                SELECT DISTINCT
                      ''HOUSENUMBER'' level
                    ,(standardized_address).match_code_housenumber match_code_element
                    ,(standardized_address).match_code_street match_code_parent
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                    AND
                    (standardized_address).housenumber IS NOT NULL
                UNION
                SELECT DISTINCT
                      ''COMPLEMENT'' level
                    ,(standardized_address).match_code_complement match_code_element
                    ,(standardized_address).match_code_housenumber match_code_parent
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                    AND
                    (standardized_address).complement IS NOT NULL
                    AND
                    (standardized_address).housenumber IS NOT NULL
                UNION
                SELECT DISTINCT
                      ''COMPLEMENT'' level
                    ,(standardized_address).match_code_complement match_code_element
                    ,(standardized_address).match_code_street match_code_parent
                    , mc.standardized_address
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                    AND
                    (standardized_address).complement IS NOT NULL
                    AND
                    (standardized_address).housenumber IS NULL
            )
            INSERT INTO fr.address_match_code(
                id_request
                , level
                , match_code_element
                , match_code_parent
            )
            (
                SELECT
                    $1
                    , level
                    , match_code_element
                    , match_code_parent
                FROM
                    match_codes
            )
            '
        ;
        EXECUTE _query USING id;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(_info, ' : #', _nrows));

        UPDATE fr.address_match_request mr SET
            is_match_code = TRUE
            WHERE
                mr.id = set_match_code.id
        ;
    ELSE
        CALL public.log_info(CONCAT(_info, ' : déjà traitée (option force disponible)'));
    END IF;
END
$proc$ LANGUAGE plpgsql;
