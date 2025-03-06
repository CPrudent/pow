/***
 * FR-ADDRESS matching address (request)
 */

CREATE TABLE IF NOT EXISTS fr.address_match_request (
    id SERIAL NOT NULL,
    source_name VARCHAR NOT NULL,
    source_kind VARCHAR NOT NULL,
    source_filter VARCHAR NULL,
    source_query VARCHAR NULL,
    date_create TIMESTAMP NOT NULL,
    is_normalized BOOLEAN DEFAULT FALSE,
    is_match_code BOOLEAN DEFAULT FALSE,
    is_match_element BOOLEAN DEFAULT FALSE,
    is_match_address BOOLEAN DEFAULT FALSE,
    parameters HSTORE NULL,
    import_name VARCHAR NULL,
    match_version VARCHAR NULL
);

DO $$
BEGIN
    IF NOT column_exists('fr', 'address_match_request', 'is_normalized') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN is_normalized BOOLEAN DEFAULT FALSE;
    END IF;
    IF column_exists('fr', 'address_match_request', 'is_matched') THEN
        ALTER TABLE fr.address_match_request DROP COLUMN is_matched;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'is_match_code') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN is_match_code BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'is_match_element') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN is_match_element BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'is_match_address') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN is_match_address BOOLEAN DEFAULT FALSE;
    END IF;

    IF NOT column_exists('fr', 'address_match_request', 'parameters') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN "parameters" HSTORE;
    END IF;

    IF column_exists('fr', 'address_match_request', 'file_path') THEN
        ALTER TABLE fr.address_match_request RENAME COLUMN file_path TO source_name;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'source_kind') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN source_kind VARCHAR DEFAULT 'FILE';
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'source_filter') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN source_filter VARCHAR;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'source_query') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN source_query VARCHAR;
    END IF;
    IF column_exists('fr', 'address_match_request', 'suffix') THEN
        ALTER TABLE fr.address_match_request DROP COLUMN suffix;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'import_name') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN import_name VARCHAR;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'match_version') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN match_version VARCHAR;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_request_id ON fr.address_match_request(id);

-- set request (if not exists)
SELECT drop_all_functions_if_exists('fr', 'set_match_request');
CREATE OR REPLACE FUNCTION fr.set_match_request(
    source_name IN VARCHAR,
    source_kind IN VARCHAR DEFAULT 'FILE',
    source_filter IN VARCHAR DEFAULT NULL,
    source_query IN VARCHAR DEFAULT NULL,
    parameters IN HSTORE DEFAULT NULL,
    id OUT INT,                                            -- ID request
    import_name OUT VARCHAR                                -- table name to import data (if needed)
)
AS $$
DECLARE
    _id INTEGER;
    _import VARCHAR;
BEGIN
    IF NOT source_kind = ANY('{FILE,TABLE,QUERY}') THEN
        RAISE 'type source ''%'' non géré! (%)', source_kind, source_name;
    END IF;

    SELECT
        mr.id,
        mr.import_name
    INTO
        _id,
        _import
    FROM
        fr.address_match_request mr
    WHERE
        mr.source_name = set_match_request.source_name
        AND
        mr.source_kind = set_match_request.source_kind
        AND
        mr.source_filter IS NOT DISTINCT FROM set_match_request.source_filter
    ;

    -- already exists?
    IF _id IS NOT NULL THEN
        set_match_request.id := _id;
        set_match_request.import_name := _import;
    ELSE
        INSERT INTO fr.address_match_request(
            source_name,
            source_kind,
            source_filter,
            source_query,
            parameters,
            date_create,
            import_name,
            match_version
        )
        VALUES(
            set_match_request.source_name,
            set_match_request.source_kind,
            set_match_request.source_filter,
            set_match_request.source_query,
            set_match_request.parameters,
            NOW(),
            CASE set_match_request.source_kind
                WHEN 'FILE' THEN
                    CONCAT(
                        'address_match_',
                        MD5(set_match_request.source_name)
                    )
            END,
            fr.match_version()
        )
        RETURNING
            address_match_request.id,
            address_match_request.import_name
        INTO
            set_match_request.id,
            set_match_request.import_name
        ;
    END IF;
END $$ LANGUAGE plpgsql;

/* TEST
 */
