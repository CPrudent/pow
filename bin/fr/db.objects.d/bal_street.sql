/***
 * FR: add BAL street
 */

DO $$
BEGIN
    -- old structure inherited from BCAA
    IF column_exists('fr', 'bal_street', 'dt_derniere_maj') THEN
        DROP TABLE fr.bal_street CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.bal_street (
    id SERIAL NOT NULL,
    id_municipality INT NOT NULL,
    code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    sources VARCHAR[],
    housenumbers INTEGER,
    housenumbers_auth INTEGER,
    last_update TIMESTAMP WITHOUT TIME ZONE
);

DO $$
BEGIN
    IF NOT column_exists('fr', 'bal_street', 'housenumbers') THEN
        ALTER TABLE fr.bal_street ADD COLUMN housenumbers INT;
    END IF;
    IF NOT column_exists('fr', 'bal_street', 'source') THEN
        ALTER TABLE fr.bal_street ADD COLUMN source TEXT[];
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_bal_street_index');
CREATE OR REPLACE PROCEDURE fr.set_bal_street_index()
AS
$proc$
BEGIN
    -- uniq ID, code
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_street_id ON fr.bal_street (id);
    CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_street_code ON fr.bal_street (code);
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_street_index();

    -- add integrity constraint
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
                ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
                ON nsp.oid = connamespace
        WHERE
            nsp.nspname = 'fr'
            AND rel.relname = 'bal_street'
            AND con.contype = 'f'
    ) THEN
        ALTER TABLE fr.bal_street ADD FOREIGN KEY (id_municipality) REFERENCES fr.bal_municipality (id);
    END IF;
END
$$;
