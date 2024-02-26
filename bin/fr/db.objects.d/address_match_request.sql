/***
 * FR-MATCH address (request manager)
 */

CREATE TABLE IF NOT EXISTS fr.address_match_request (
    id SERIAL NOT NULL
    , file_path VARCHAR NOT NULL
    , date_create TIMESTAMP NOT NULL
    , suffix VARCHAR NOT NULL
    , is_normalized BOOLEAN DEFAULT FALSE
    , is_matched BOOLEAN DEFAULT FALSE
);

DO $$
BEGIN
    IF NOT column_exists('fr', 'address_match_request', 'is_normalized') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN is_normalized BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT column_exists('fr', 'address_match_request', 'is_matched') THEN
        ALTER TABLE fr.address_match_request ADD COLUMN is_matched BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_request_id ON fr.address_match_request(id);

SELECT drop_all_functions_if_exists('fr', 'add_address_match');
CREATE OR REPLACE FUNCTION fr.add_address_match(
    file_path IN VARCHAR
    , suffix INOUT VARCHAR DEFAULT NULL -- loaded data, as address_match_<SUFFIX>
    , id OUT INT                        -- ID request
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
        mr.file_path = add_address_match.file_path
        ;
    -- already exists?
    IF _id IS NOT NULL THEN
        add_address_match.id = _id;
        add_address_match.suffix := _suffix;
        new_request := FALSE;
    ELSE
        INSERT INTO fr.address_match_request(file_path, date_create, suffix)
            VALUES(
                add_address_match.file_path
                , NOW()
                , COALESCE(add_address_match.suffix, MD5(file_path))
            )
            RETURNING id, suffix INTO add_address_match.id, add_address_match.suffix
            ;
        new_request := TRUE;
    END IF;
END $$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM fr.add_address_match('dir1/file1.csv');
SELECT * FROM fr.add_address_match('dir1/file2.csv', 'test1');
-- already exists
SELECT * FROM fr.add_address_match('dir1/file1.csv');
 */
