/***
 * FR: add BAL housenumber
 */

DO $$
BEGIN
    -- old structure inherited from BCAA
    IF column_exists('fr', 'bal_housenumber', 'dt_derniere_maj') THEN
        DROP TABLE fr.bal_housenumber;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.bal_housenumber (
    id SERIAL NOT NULL,
    id_street INT NOT NULL,
    code VARCHAR NOT NULL,
    number INTEGER NOT NULL,
    extension VARCHAR,
    postcode VARCHAR NOT NULL,
    area VARCHAR,
    parcels VARCHAR[],
    geom FLOAT[],
    location VARCHAR,
    last_update TIMESTAMP WITHOUT TIME ZONE
);

SELECT drop_all_functions_if_exists('fr', 'set_bal_housenumber_index');
CREATE OR REPLACE PROCEDURE fr.set_bal_housenumber_index()
AS
$proc$
BEGIN
    -- uniq ID, code
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_housenumber_id ON fr.bal_housenumber (id);
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_housenumber_code ON fr.bal_housenumber (code);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_housenumber_index();

    -- add integrity constraint on table fr.bal_housenumber
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
                ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
                ON nsp.oid = connamespace
        WHERE
            nsp.nspname = 'fr'
            AND rel.relname = 'bal_housenumber'
            AND con.contype = 'f'
    ) THEN
        ALTER TABLE fr.bal_housenumber ADD FOREIGN KEY (id_street) REFERENCES fr.bal_street (id);
    END IF;
END $$;
