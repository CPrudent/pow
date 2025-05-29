/***
 * FR-ADDRESS matching address (result)
 */

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'standardized_address')
    OR NOT EXISTS (
        SELECT 1 FROM information_schema.attributes
        WHERE udt_name = 'standardized_address' AND attribute_name = 'street_nwords_xa')
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
            complement_nwords_xa INT,         -- nof words (w/o article)
            --complement_uncommon_value VARCHAR,
            --complement_uncommon_occur INT,
            housenumber INT,
            extension VARCHAR,                -- housenumber extension (BIS, ...)
            --housenumber_uncommon_id INT,
            --housenumber_uncommon_occur INT,
            street_name VARCHAR,              -- full name of street (w/o abbr)
            street_descriptors VARCHAR,       -- LAPOSTE/RAN classified words
            street_as_words INT[],            -- array of length of each item
            street_words TEXT[],              -- array of each words
            street_nwords_xa INT,             -- nof words (w/o article)
            --street_uncommon_value VARCHAR,
            --street_uncommon_occur INT,
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
    id IN INTEGER,
    mapping IN HSTORE,
    force IN BOOLEAN DEFAULT FALSE,
    raise_notice IN BOOLEAN DEFAULT FALSE,
    simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _import VARCHAR;
    _source_name VARCHAR;
    _source_kind VARCHAR;
    _source_filter VARCHAR;
    _source_query VARCHAR;
    _is_normalized BOOLEAN;
    _matching HSTORE;
    _table VARCHAR;
    _query TEXT;
    _nrows INTEGER;
    _info VARCHAR;
BEGIN
    SELECT import_name, source_name, source_kind, source_filter, source_query, is_normalized, parameters
    INTO _import, _source_name, _source_kind, _source_filter, _source_query, _is_normalized, _matching
    FROM fr.address_match_request mr
    WHERE mr.id = set_match_standardize.id
    ;
    IF NOT FOUND THEN
        RAISE 'aucune demande de Rapprochement trouvée pour ID ''%''', id;
    END IF;

    _info := CONCAT('standardisation demande Rapprochement (', id, ')');
    IF force OR NOT _is_normalized THEN
        IF NOT simulation THEN
            DELETE FROM fr.address_match_result WHERE id_request = set_match_standardize.id;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            IF _nrows > 0 THEN
                CALL public.log_info(CONCAT(_info, ' - PURGE : #', _nrows));
            END IF;
        END IF;

        _table := CASE _source_kind
            WHEN 'FILE' THEN
                CONCAT_WS('.', 'fr', _import)
            WHEN 'TABLE' THEN
                CONCAT_WS('.', 'fr', _source_name)
            WHEN 'QUERY' THEN
                _source_name
            END
        ;

        IF _source_kind = 'QUERY' THEN
            _query := CONCAT(
                '
                WITH
                ',
                _table,
                ' AS (',
                _source_query,
                ')
                '
            );
        END IF;

        _query := CONCAT(
            _query,
            '
            INSERT INTO fr.address_match_result(
                id_request,
                id_address,
                standardized_address
            )
            (
                SELECT
                    $1,
                    d.rowid,
                    ROW(sa.*)::fr.standardized_address
                FROM
                '
                , _table, ' d
                    LEFT OUTER JOIN fr.standardize_address(
                        address =>  TO_JSON(d),
                        mapping => $2,
                        matching => $3,
                        raise_notice => $4
                    ) sa ON TRUE
            '
        );

        IF _source_filter IS NOT NULL THEN
            _query := CONCAT(
                _query,
                '
                WHERE
                ',
                _source_filter
            );
        END IF;
        _query := CONCAT(
            _query,
            '
            )
            '
        );

        IF raise_notice THEN
            CALL public.log_info(FORMAT('ID(%s): mapping %s', id, mapping));
            CALL public.log_info(FORMAT('ID(%s): query %s', id, _query));
            CALL public.log_info(FORMAT('ID(%s): matching %s', id, _matching));
        END IF;

        IF simulation THEN
            RAISE NOTICE 'query=%', _query;
            RETURN;
        END IF;

        EXECUTE _query USING id, mapping, _matching, raise_notice;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(_info, ' : #', _nrows));

        UPDATE fr.address_match_request mr SET
            is_normalized = TRUE
            WHERE
                mr.id = set_match_standardize.id
        ;
    ELSE
        CALL public.log_info(CONCAT(_info, ' : déjà traitée (option force disponible)'));
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- eval result as counters
SELECT drop_all_functions_if_exists('fr', 'set_match_result');
CREATE OR REPLACE FUNCTION fr.set_match_result(
    id IN INT,
    counters OUT NUMERIC[]
)
AS
$func$
DECLARE
    _count1 NUMERIC;
    _count2 NUMERIC;
    _count3 NUMERIC;
    _count4 NUMERIC;
    _count5 NUMERIC;
    _count6 NUMERIC;
    _count7 NUMERIC;
BEGIN
    SELECT
        COUNT(1) total,
        SUM(CASE WHEN (me.matched_element).status = 'OK_1' THEN 1 ELSE 0 END) ok_strict,
        ROUND((SUM(CASE WHEN (me.matched_element).status = 'OK_1' THEN 1 ELSE 0 END)::NUMERIC * 100) / COUNT(1), 1) percent_ok_strict,
        SUM(CASE WHEN (me.matched_element).status = 'OK_2' THEN 1 ELSE 0 END) ok_near,
        ROUND((SUM(CASE WHEN (me.matched_element).status = 'OK_2' THEN 1 ELSE 0 END)::NUMERIC * 100) / COUNT(1), 1) percent_ok_near,
        SUM(CASE WHEN (me.matched_element).status ~ '^KO' THEN 1 ELSE 0 END) ko,
        ROUND((SUM(CASE WHEN (me.matched_element).status ~ '^KO' THEN 1 ELSE 0 END)::NUMERIC * 100) / COUNT(1), 1) percent_ko
    INTO
        _count1,
        _count2,
        _count3,
        _count4,
        _count5,
        _count6,
        _count7
    FROM
        fr.address_match_result mr
            LEFT OUTER JOIN fr.address_match_element me
            ON (
                ((mr.standardized_address).level = me.level)
                AND (
                    ((mr.standardized_address).match_code_street = me.match_code)
                    OR
                    ((mr.standardized_address).match_code_housenumber = me.match_code)
                    OR
                    ((mr.standardized_address).match_code_complement = me.match_code)
                )
            )
    WHERE
        mr.id_request = set_match_result.id
    ;

    counters[1] := _count1;
    counters[2] := _count2;
    counters[3] := _count3;
    counters[4] := _count4;
    counters[5] := _count5;
    counters[6] := _count6;
    counters[7] := _count7;
END
$func$ LANGUAGE plpgsql;

-- build cross reference between SOURCE and LAPOSTE
SELECT drop_all_functions_if_exists('fr', 'set_match_cross_reference');
CREATE OR REPLACE FUNCTION fr.set_match_cross_reference(
    id IN INTEGER,                          -- match request ID
    municipality_code IN VARCHAR,
    table_name INOUT VARCHAR DEFAULT NULL   -- result table
)
AS
$func$
DECLARE
    _import VARCHAR;
    _source_name VARCHAR;
    _source_kind VARCHAR;
    _source_query VARCHAR;
    _is_match_element BOOLEAN;
    _query TEXT;
    _nrows INTEGER;
BEGIN
    SELECT import_name, source_name, source_kind, source_query, is_match_element
    INTO _import, _source_name, _source_kind, _source_query, _is_match_element
    FROM fr.address_match_request mr
    WHERE mr.id = set_match_cross_reference.id
    ;
    IF NOT FOUND THEN
        RAISE 'aucune demande de Rapprochement trouvée pour ID ''%''', id;
    END IF;
    IF NOT _is_match_element THEN
        RAISE 'demande de Rapprochement ID ''%'' non terminée (MATCH_ELEMENT manquant)', id;
    END IF;

    _query :=
        CASE _source_kind
        WHEN 'FILE' THEN CONCAT('SELECT * FROM fr.', _import)
        WHEN 'TABLE' THEN CONCAT('SELECT * FROM fr.', _source_name)
        WHEN 'QUERY' THEN _source_query
        END
    ;
    table_name := COALESCE(table_name,
        CASE _source_kind
        WHEN 'FILE' THEN CONCAT(_import, '_crossref')
        ELSE CONCAT(LOWER(_source_name), '_crossref')
        END
    );

    IF NOT table_exists(schema_name => 'fr', table_name => table_name) THEN
        _query := CONCAT(
            'CREATE TABLE fr.', table_name,' AS
            WITH
            source_data AS (', _query, '),
            laposte_data AS (
                SELECT
                    a.co_adr ref_code_address,
                    COALESCE(a.lb_ligne3_normalise, a.lb_ligne3) ref_complement,
                    a.no_numero ref_number,
                    a.lb_extension_numero ref_extension,
                    COALESCE(a.lb_voie_normalise, a.lb_voie) ref_street,
                    a.lb_ligne5 ref_area,
                    a.co_postal ref_postcode,
                    a.lb_acheminement ref_municipality,
                    a.no_type_localisation_coord ref_location,
                    ST_Transform(a.gm_coord, 4326) ref_geom
                FROM
                    fr.address_view a
                WHERE
                    a.co_insee_commune = $2
                    AND
                    a.co_niveau != ''ZA''
            ),
            match_data AS (
                SELECT
                    mr.id_address code_source,
                    (me.matched_element).codes_address[1] code_laposte
                FROM
                    fr.address_match_result mr
                        JOIN fr.address_match_element me
                        ON (
                            ((mr.standardized_address).level = me.level)
                            AND (
                                ((mr.standardized_address).match_code_street = me.match_code)
                                OR
                                ((mr.standardized_address).match_code_housenumber = me.match_code)
                                OR
                                ((mr.standardized_address).match_code_complement = me.match_code)
                            )
                        )
                WHERE
                    mr.id_request = $1
            )
            SELECT
                s.*,
                p.*
            FROM
                source_data s
                    LEFT OUTER JOIN match_data m ON s.rowid = m.code_source
                    FULL OUTER JOIN laposte_data p ON p.ref_code_address = m.code_laposte
            '
        );
        EXECUTE _query
            USING set_match_cross_reference.id, set_match_cross_reference.municipality_code
            ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(
            FORMAT('CROSS REFERENCE (MATCH-REQUEST-ID=%s): NROWS=%s',
                set_match_cross_reference.id,
                _nrows
            )
        );
    END IF;
END
$func$ LANGUAGE plpgsql;
