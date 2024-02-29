/***
 * FR-ADDRESS matching address (result)
 */

-- old name
DROP TYPE IF EXISTS fr.address_normalized CASCADE;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'standardized_address')
    OR EXISTS (
        SELECT 1 FROM information_schema.attributes
        WHERE udt_name = 'standardized_address' AND attribute_name = '_order_code_area')

    THEN
        DROP TYPE IF EXISTS fr.standardized_address CASCADE;
        CREATE TYPE fr.standardized_address AS (
              id VARCHAR                        -- client ID
            , level VARCHAR                     -- AREA|STREET|HOUSENUMBER|COMPLEMENT
            , elapsed_time INTERVAL             -- running time
            , match_code_area VARCHAR
            , match_code_street VARCHAR
            , match_code_housenumber VARCHAR
            , match_code_complement VARCHAR
            , complement VARCHAR                -- address complement (known as L3)
            , housenumber INTEGER
            , extension VARCHAR                 -- housenumber extension (BIS, ...)
            , street VARCHAR                    -- full name of street (w/o abbr)
            , descriptors VARCHAR               -- LAPOSTE/RAN classified words
            , as_words INT[]                    -- array of length of each item
            , strong_word VARCHAR               -- important word (generaly last one)
            /* useful ?
            , street_normalized VARCHAR         -- normalized name of street
            , descriptors_normalized VARCHAR
            , as_words_normalized INT[]
             */
            , postcode VARCHAR                  -- postal code
            , municipality_code VARCHAR         -- INSEE code (municipality)
            , municipality_name VARCHAR         -- normalized name of municipality
            , municipality_old_code VARCHAR     -- old municipality (known as L5)
            , municipality_old_name VARCHAR
            , geom GEOMETRY(POINT, 3857)        -- WGS84-proj geometry
        );

        -- has to be rebuild!
        DROP TABLE IF EXISTS fr.address_match_result;
    END IF;
END $$;

/*
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'address_matched')
    THEN
        DROP TYPE IF EXISTS fr.address_matched CASCADE;
        CREATE TYPE fr.address_matched AS (
            code_area CHAR(10)
            , codes_area_possible CHAR(10)[]
            , code_street CHAR(10)
            , code_housenumber CHAR(10)
            , code_complement CHAR(10)
            , elapsed_time INTERVAL
            , search_area INT
            , search_street INT
            , search_housenumber INT
            , search_complement INT
            , similarity NUMERIC
            , similarity_semantic NUMERIC
            , similarity_phonetic NUMERIC
            , similarity_geometry NUMERIC
        );
    END IF;

    IF NOT column_exists('fr', 'address_match_result', 'id_address') THEN
        -- has to be rebuild!
        DROP TABLE IF EXISTS fr.address_match_result;
    END IF;
END $$;
 */

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'matched_element')
    THEN
        --DROP TYPE IF EXISTS fr.matched_element CASCADE;
        CREATE TYPE fr.matched_element AS (
              codes_address CHAR(10)[]
            , level VARCHAR                     -- AREA|STREET|HOUSENUMBER|COMPLEMENT
            , elapsed_time INTERVAL
            , status INT
            , similarity NUMERIC
            , similarity_semantic NUMERIC
            , similarity_phonetic NUMERIC
            , similarity_geometry NUMERIC
        );
    END IF;

    IF NOT column_exists('fr', 'address_match_result', 'id_matched_area') THEN
        -- has to be rebuild!
        DROP TABLE IF EXISTS fr.address_match_result;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.address_match_result (
    id SERIAL NOT NULL
    , id_request INTEGER NOT NULL
    , id_address INT NOT NULL
    , standardized_address fr.standardized_address
    , code_address CHAR(10)
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_normalize_id ON fr.address_match_result(id);
CREATE UNIQUE INDEX IF NOT EXISTS ix_address_match_result_ids ON fr.address_match_result(id_request, id_address);

-- normalize (standardize) addresses
/* NOTE
here goal is to standardize address (upcase, w/o abbr, ...) before matching step
not really obtain normalized name, by example, for street
 */
SELECT drop_all_functions_if_exists('fr', 'set_normalize');
CREATE OR REPLACE PROCEDURE fr.set_normalize(
    file_path IN VARCHAR
    , mapping IN HSTORE
    , force IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _id_request INTEGER;
    _suffix VARCHAR;
    _is_normalized BOOLEAN;
    _query TEXT;
    _table VARCHAR;
    _nrows INTEGER;
    _info VARCHAR;
BEGIN
    SELECT id, suffix, is_normalized
    INTO _id_request, _suffix, _is_normalized
    FROM fr.address_match_request mr
    WHERE mr.file_path = set_normalize.file_path
    ;

    IF _id_request IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée pour le fichier ''%''', file_path;
    END IF;

    _info := CONCAT('standardisation demande Rapprochement (', _id_request, ')');
    IF force OR NOT _is_normalized THEN
        DELETE FROM fr.address_match_result WHERE id_request = _id_request;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        IF _nrows > 0 THEN
            CALL public.log_info(CONCAT(_info, ' - PURGE : #', _nrows));
        END IF;

        _table := CONCAT('address_match_', _suffix);
        _query := CONCAT(
            '
            INSERT INTO fr.address_match_result(
                id_request
                , id_address
                , standardized_address
            )
            (
                SELECT
                    $1
                    , d.rowid
                    , ROW(na.*)::standardized_address
                FROM fr.
                '
                , _table, ' d
                    LEFT OUTER JOIN fr.standardize_address(
                        address =>  d
                        , columns_map => $2
                    ) AS na ON TRUE
            )
            '
        );
        EXECUTE _query USING _id_request, mapping;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(_info, ' : #', _nrows));

        UPDATE fr.address_match_request SET
            is_normalized = TRUE
            WHERE
                id = _id_request
        ;
    ELSE
        CALL public.log_info(CONCAT(_info, ' : déjà traitée (option force disponible)'));
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- match addresses
SELECT drop_all_functions_if_exists('fr', 'set_match');
CREATE OR REPLACE PROCEDURE fr.set_match(
    file_path IN VARCHAR
    , force IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _id_request INTEGER;
    _is_matched BOOLEAN;
    _query TEXT;
    _nrows INTEGER;
BEGIN
    SELECT id, is_matched
    INTO _id_request, _is_matched
    FROM fr.address_match_request mr
    WHERE mr.file_path = set_match.file_path
    ;

    IF _id_request IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée pour le fichier ''%''', file_path;
    END IF;

    IF force OR NOT _is_matched THEN
        -- take addresses ordered by same element(s)
        _query := CONCAT(
            '
            WITH
            ordered_addresses AS (
                SELECT
                    na.id_request
                    , na.id_address
                    , ROW(ma.*)::fr.address_matched address_matched
                FROM
                    fr.address_match_result na
                        CROSS JOIN fr.match_address(
                                standardized_address => na.standardized_address
                            ) ma
                WHERE
                    na.id_request = $1
                ORDER BY
                    (na.standardized_address)._order_code_area
                    , (na.standardized_address)._order_code_street
                    , (na.standardized_address)._order_code_housenumber
                    , (na.standardized_address)._order_code_complement

            )
            UPDATE fr.address_match_result r SET
                address_matched = oa.address_matched
                FROM
                    ordered_addresses oa
                WHERE
                    (r.id_request, r.id_address) = (oa.id_request, oa.id_address)
            '
        );
        EXECUTE _query USING _id_request;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT('traitement demande Rapprochement (', _id_request, ') : #', _nrows));

        UPDATE fr.address_match_request SET
            is_matched = TRUE
            WHERE
                id = _id_request
        ;
    ELSE
        CALL public.log_info(CONCAT('demande Rapprochement (', _id_request, ') : déjà traitée (option force disponible)'));
    END IF;
END
$proc$ LANGUAGE plpgsql;
