/***
 * FR-ADDRESS matching address (result)
 */

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'standardized_address')
    OR NOT EXISTS (
        SELECT 1 FROM information_schema.attributes
        WHERE udt_name = 'standardized_address' AND attribute_name = 'housenumber_uncommon_id')
    THEN
        DROP TYPE IF EXISTS fr.standardized_address CASCADE;
        CREATE TYPE fr.standardized_address AS (
            id VARCHAR,                       -- client ID
            level VARCHAR,                    -- AREA|STREET|HOUSENUMBER|COMPLEMENT
            elapsed_time INTERVAL,            -- running time
            match_code_area VARCHAR,
            match_code_street VARCHAR,
            match_code_housenumber VARCHAR,
            match_code_complement VARCHAR,
            complement_name VARCHAR,          -- address complement (known as L3)
            complement_descriptors VARCHAR,   -- LAPOSTE/RAN classified words
            complement_as_words INT[],        -- array of length of each item
            complement_words TEXT[],          -- array of each words
            complement_uncommon_value VARCHAR,
            complement_uncommon_occur INT,
            housenumber INT,
            extension VARCHAR,                -- housenumber extension (BIS, ...)
            housenumber_uncommon_id INT,
            housenumber_uncommon_occur INT,
            street_name VARCHAR,              -- full name of street (w/o abbr)
            street_descriptors VARCHAR,       -- LAPOSTE/RAN classified words
            street_as_words INT[],            -- array of length of each item
            street_words TEXT[],              -- array of each words
            street_uncommon_value VARCHAR,
            street_uncommon_occur INT,
            /* useful ?
            street_normalized VARCHAR,        -- normalized name of street
            street_descriptors_normalized VARCHAR,
            street_as_words_normalized INT[],
             */
            postcode VARCHAR,                 -- postal code
            municipality_code VARCHAR,        -- INSEE code (municipality)
            municipality_name VARCHAR,        -- normalized name of municipality
            municipality_old_code VARCHAR,    -- old municipality (known as L5)
            municipality_old_name VARCHAR,
            geom GEOMETRY(POINT, 3857)        -- WGS84-proj geometry
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
            code_area CHAR(10),
            codes_area_possible CHAR(10)[],
            code_street CHAR(10),
            code_housenumber CHAR(10),
            code_complement CHAR(10),
            elapsed_time INTERVAL,
            search_area INT,
            search_street INT,
            search_housenumber INT,
            search_complement INT,
            similarity NUMERIC,
            similarity_semantic NUMERIC,
            similarity_phonetic NUMERIC,
            similarity_geometry NUMERIC
        );
    END IF;

    IF NOT column_exists('fr', 'address_match_result', 'id_address') THEN
        -- has to be rebuild!
        DROP TABLE IF EXISTS fr.address_match_result;
    END IF;
END $$;
 */

CREATE TABLE IF NOT EXISTS fr.address_match_result (
    id SERIAL NOT NULL,
    id_request INTEGER NOT NULL,
    id_address INT NOT NULL,
    standardized_address fr.standardized_address,
    code_address CHAR(10)
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_normalize_id ON fr.address_match_result(id);
CREATE UNIQUE INDEX IF NOT EXISTS ix_address_match_result_ids ON fr.address_match_result(id_request, id_address);

-- get value from standardized_address record
SELECT drop_all_functions_if_exists('fr', '_get_value_from_standardized_address');
CREATE OR REPLACE FUNCTION fr._get_value_from_standardized_address(
    standardized_address IN fr.standardized_address,
    key IN VARCHAR,
    value OUT VARCHAR
)
AS
$func$
BEGIN
    EXECUTE CONCAT('SELECT $1.', key)
        INTO value
        USING standardized_address;
END
$func$ LANGUAGE plpgsql;

/* NOTE
standardize addresses
here goal is to standardize address (upcase, w/o abbr, ...) before matching step
not really obtain normalized name, by example, for street
 */
SELECT drop_all_functions_if_exists('fr', 'set_match_standardize');
CREATE OR REPLACE PROCEDURE fr.set_match_standardize(
    file_path IN VARCHAR,
    mapping IN HSTORE,
    force IN BOOLEAN DEFAULT FALSE,
    raise_notice IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _id_request INTEGER;
    _suffix VARCHAR;
    _is_normalized BOOLEAN;
    _matching HSTORE;
    _query TEXT;
    _table VARCHAR;
    _nrows INTEGER;
    _info VARCHAR;
BEGIN
    SELECT id, suffix, is_normalized, parameters
    INTO _id_request, _suffix, _is_normalized, _matching
    FROM fr.address_match_request mr
    WHERE mr.file_path = set_match_standardize.file_path
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
                    , ROW(sa.*)::fr.standardized_address
                FROM fr.
                '
                , _table, ' d
                    LEFT OUTER JOIN fr.standardize_address(
                        address =>  d
                        , mapping => $2
                        , matching => $3
                        , raise_notice => $4
                    ) sa ON TRUE
            )
            '
        );
        EXECUTE _query USING _id_request, mapping, _matching, raise_notice;
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
    file_path IN VARCHAR,
    force IN BOOLEAN DEFAULT FALSE
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
                    mr.id_request
                    , mr.id_address
                    , ROW(ma.*)::fr.address_matched address_matched
                FROM
                    fr.address_match_result mr
                        CROSS JOIN fr.match_address(
                            standardized_address => mr.standardized_address
                        ) ma
                WHERE
                    mr.id_request = $1
                ORDER BY
                    (mr.standardized_address).match_code_area,
                    (mr.standardized_address).match_code_street,
                    (mr.standardized_address).match_code_housenumber,
                    (mr.standardized_address).match_code_complement

            )
            UPDATE fr.address_match_result mr SET
                address_matched = oa.address_matched
                FROM
                    ordered_addresses oa
                WHERE
                    (mr.id_request, mr.id_address) = (oa.id_request, oa.id_address)
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
