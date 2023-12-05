/***
 * FR-MATCH address (request manager)
 */

CREATE TABLE IF NOT EXISTS fr.address_match_request (
    id SERIAL NOT NULL
    , file_path VARCHAR NOT NULL
    , date_create TIMESTAMP NOT NULL
    , suffix VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_request_id ON fr.address_match_request(id);

SELECT drop_all_functions_if_exists('fr', 'add_address_match');
CREATE OR REPLACE FUNCTION fr.add_address_match(
    file_path VARCHAR
    , suffix VARCHAR DEFAULT gen_random_uuid()      --can be: bal_<<INSEE>>
)
RETURNS RECORD
AS $$
DECLARE
    _id INTEGER;
    _suffix VARCHAR;
    _result RECORD;
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
        _result := (_id, _suffix);
    ELSE
        INSERT INTO fr.address_match_request(file_path, date_create, suffix)
            VALUES(add_address_match.file_path, NOW(), add_address_match.suffix)
            RETURNING id INTO _id
            ;
        _result := (_id, add_address_match.suffix);
    END IF;

    RETURN _result;
END $$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM fr.add_address_match('dir1/file1.csv') AS (id INT, suffix VARCHAR);
SELECT * FROM fr.add_address_match('dir1/file2.csv', 'test1') AS (id INT, suffix VARCHAR);
-- already exists
SELECT fr.add_address_match('dir1/file1.csv') AS (id INT, suffix VARCHAR);
 */
