/***
 * FR-MATCH address (normalize)
 */

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'address_normalized')
    OR NOT EXISTS (
        SELECT 1 FROM information_schema.attributes
        WHERE udt_name = 'address_normalized' AND attribute_name = 'municipality_name')
    THEN
        DROP TYPE IF EXISTS fr.address_normalized CASCADE;
        CREATE TYPE fr.address_normalized AS (
            _order_code_area VARCHAR
            , _order_code_street VARCHAR
            , _order_code_housenumber VARCHAR
            , _order_code_complement VARCHAR
            , id VARCHAR
            , complement VARCHAR
            , housenumber INTEGER
            , housenumber_extension VARCHAR
            , street VARCHAR
            , street_type VARCHAR
            , street_type_short VARCHAR
            , geom GEOMETRY(POINT, 3857)
            , level VARCHAR
            , postcode VARCHAR
            , municipality_old_name VARCHAR
            , municipality_code VARCHAR
            , municipality_name VARCHAR
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.address_match_normalize (
    id SERIAL NOT NULL
    , id_request INTEGER NOT NULL
    , address address_normalized
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_address_match_normalize_id ON fr.address_match_normalize(id);
CREATE INDEX IF NOT EXISTS ix_address_match_normalize_id_request ON fr.address_match_normalize(id_request);

SELECT drop_all_functions_if_exists('fr', 'set_normalize');
CREATE OR REPLACE PROCEDURE fr.set_normalize(
    file_path VARCHAR
    , mapping TEXT[]
)
AS
$proc$
DECLARE
    _id_request INTEGER;
    _suffix VARCHAR;
    _query TEXT;
    _table VARCHAR;
    _nrows INTEGER;
BEGIN
    SELECT id, suffix
    INTO _id_request, _suffix
    FROM fr.address_match_request mr
    WHERE mr.file_path = set_normalize.file_path
    ;

    IF _id_request IS NULL THEN
        RAISE 'aucune demande de Rapprochement trouvée pour le fichier ''%''', file_path;
    END IF;

    DELETE FROM fr.address_match_normalize WHERE id_request = _id_request;
    _table := CONCAT('fr.address_match_', _suffix);
    _query := CONCAT(
        '
        INSERT INTO fr.address_match_normalize(
            id_request
            , address
        )
        (
            SELECT
        '
        , _id_request
        , '
        , na.*
            FROM
        '
        , _table
        , '
            LEFT OUTER JOIN fr.normalize_address(
                address => ', _table, '
                , columns_map => ', mapping, '
            ) AS na ON TRUE
        )
        '
    );
    EXECUTE _query;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT('demande Rapprochement (', _id_request, ') : #', _nrows));
END
$proc$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE fr.set_normalize(
    file_path VARCHAR
    , mapping HSTORE
)
AS
$proc$
DECLARE
    _map_array TEXT[] := %# mapping;
BEGIN
    CALL fr.set_normalize(file_path => file_path, mapping => _map_array);
END
$proc$ LANGUAGE plpgsql;
