/***
 * FR-ADDRESS matching address (request)
 */

CREATE TABLE IF NOT EXISTS fr.address_match_request (
    id SERIAL NOT NULL
    , file_path VARCHAR NOT NULL
    , date_create TIMESTAMP NOT NULL
    , suffix VARCHAR NOT NULL
    , is_normalized BOOLEAN DEFAULT FALSE
    , is_match_code BOOLEAN DEFAULT FALSE
    , is_match_element BOOLEAN DEFAULT FALSE
    , is_match_address BOOLEAN DEFAULT FALSE
    , parameters HSTORE
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
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_request_id ON fr.address_match_request(id);

-- add request (if not exists)
SELECT drop_all_functions_if_exists('fr', 'add_match_request');
CREATE OR REPLACE FUNCTION fr.add_match_request(
    file_path IN VARCHAR
    , parameters IN HSTORE DEFAULT NULL
    , suffix INOUT VARCHAR DEFAULT NULL     -- loaded data, as address_match_<SUFFIX>
    , id OUT INT                            -- ID request
    , new_request OUT BOOLEAN
)
AS $$
DECLARE
    _id INTEGER;
    _suffix VARCHAR;
BEGIN
    SELECT
          mr.id
        , mr.suffix
    INTO
          _id
        , _suffix
    FROM
        fr.address_match_request mr
    WHERE
        mr.file_path = add_match_request.file_path
        ;
    -- already exists?
    IF _id IS NOT NULL THEN
        add_match_request.id = _id;
        add_match_request.suffix := _suffix;
        new_request := FALSE;
    ELSE
        INSERT INTO fr.address_match_request(
              file_path
            , parameters
            , date_create
            , suffix
        )
        VALUES(
              add_match_request.file_path
            , add_match_request.parameters
            , NOW()
            , COALESCE(add_match_request.suffix, MD5(file_path))
        )
        RETURNING
              address_match_request.id
            , address_match_request.suffix
        INTO
              add_match_request.id
            , add_match_request.suffix
            ;
        new_request := TRUE;
    END IF;
END $$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM fr.add_match_request('dir1/file1.csv');
SELECT * FROM fr.add_match_request(file_path => 'dir1/file2.csv', suffix => 'test1');
-- already exists
SELECT * FROM fr.add_match_request('dir1/file1.csv');
 */
