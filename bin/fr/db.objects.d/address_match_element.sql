/***
 * FR-ADDRESS matching address (element)
 */

CREATE TABLE IF NOT EXISTS fr.address_match_element (
      id SERIAL NOT NULL
    , level VARCHAR
    , match_code VARCHAR
    , matched_element fr.matched_element
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_element_id_level ON fr.address_match_element(id, level);

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
    _query TEXT;
    _nrows INTEGER;
    _info VARCHAR := CONCAT('rapprochement ELEMENT demande Rapprochement (', id, ')');
    _element RECORD;
BEGIN
    SELECT is_match_element
    INTO _is_match_element
    FROM fr.address_match_request mr
    WHERE mr.id = set_match_element.id
    ;

    IF _is_match_element IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée ''%''', id;
    END IF;

    IF force OR NOT _is_match_element THEN
        FOR _element IN (
            SELECT mc.level, mc.match_code_element, mc.match_code_parent
            FROM fr.address_match_code mc
                LEFT OUTER JOIN fr.address_match_element me
                ON (me.match_code_element, me.match_code_parent) = (mc.match_code_element, mc.match_code_parent)
            WHERE mc.id_request = id AND me.match_code_element IS NULL
        )
        LOOP

        END LOOP;

        DELETE FROM fr.address_match_code WHERE id_request = id;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        IF _nrows > 0 THEN
            CALL public.log_info(CONCAT(_info, ' - PURGE : #', _nrows));
        END IF;

        _query := CONCAT(
            '
            WITH
            match_code AS (
                SELECT DISTINCT
                    ''AREA'' level
                    , CONCAT(
                          (standardized_address).municipality_old_name
                        , (standardized_address).postcode
                        , (standardized_address).municipality_code
                        , (standardized_address).municipality_name
                    ) match_code
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                UNION
                SELECT DISTINCT
                    ''STREET'' level
                    , CONCAT(
                          (standardized_address).municipality_old_name
                        , (standardized_address).postcode
                        , (standardized_address).municipality_code
                        , (standardized_address).municipality_name
                        , (standardized_address).street
                    ) match_code
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                UNION
                SELECT DISTINCT
                    ''HOUSENUMBER'' level
                    , CONCAT(
                          (standardized_address).municipality_old_name
                        , (standardized_address).postcode
                        , (standardized_address).municipality_code
                        , (standardized_address).municipality_name
                        , (standardized_address).street
                        , (standardized_address).housenumber
                        , (standardized_address).housenumber_extension
                    ) match_code
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                    AND
                    (standardized_address).housenumber IS NOT NULL
                UNION
                SELECT DISTINCT
                    ''COMPLEMENT'' level
                    , CONCAT(
                          (standardized_address).municipality_old_name
                        , (standardized_address).postcode
                        , (standardized_address).municipality_code
                        , (standardized_address).municipality_name
                        , (standardized_address).street
                        , (standardized_address).housenumber
                        , (standardized_address).housenumber_extension
                        , (standardized_address).complement
                    ) match_code
                FROM
                    fr.address_match_result
                WHERE
                    id_request = $1
                    AND
                    (standardized_address).complement IS NOT NULL
            )
            INSERT INTO fr.address_match_code(
                id_request
                , level
                , match_code
            )
            (
                SELECT
                    $1
                    , level
                    , MD5(match_code)
                FROM
                    match_code
            )
            '
        );
        EXECUTE _query USING id;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(_info, ' : #', _nrows));

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
