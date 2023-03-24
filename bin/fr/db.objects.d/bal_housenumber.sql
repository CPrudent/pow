/***
 * FR: add BAL housenumber
 */

CREATE TABLE IF NOT EXISTS fr.bal_housenumber (
    id_bal_numero VARCHAR NOT NULL
    , numero INTEGER NOT NULL
    , suffixe VARCHAR
    , id_bal_voie VARCHAR NOT NULL
    , libelle_ancienne_commune VARCHAR
    , parcelles VARCHAR[]
    , coordonnees FLOAT[]
    , co_postal VARCHAR NOT NULL
    , type_position VARCHAR
    , dt_derniere_maj TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

SELECT drop_all_functions_if_exists('fr', 'set_bal_housenumber_index');
CREATE OR REPLACE PROCEDURE fr.set_bal_housenumber_index()
AS
$proc$
BEGIN
    -- uniq ID
    IF index_exists('fr', 'idx_numero_id_bal_numero') AND NOT index_exists('fr', 'iux_bal_housenumber_id_bal_numero') THEN
        ALTER INDEX idx_numero_id_bal_numero RENAME TO iux_bal_housenumber_id_bal_numero;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_housenumber_id_bal_numero ON fr.bal_housenumber (id_bal_numero);
    END IF;
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
        ALTER TABLE fr.bal_housenumber ADD FOREIGN KEY (id_bal_voie) REFERENCES fr.bal_street (id_bal_voie);
    END IF;
END $$;
