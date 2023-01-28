/***
 * add BAL housenumber
 */

CREATE TABLE IF NOT EXISTS bal.numero (
  id_bal_numero VARCHAR NOT NULL,
  numero INTEGER NOT NULL,
  suffixe VARCHAR,
  id_bal_voie VARCHAR NOT NULL,
  libelle_ancienne_commune VARCHAR,
  parcelles VARCHAR[],
  coordonnees FLOAT[],
  co_postal VARCHAR NOT NULL,
  type_position VARCHAR,
  dt_derniere_maj TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE /*UNIQUE*/ INDEX IF NOT EXISTS ix_numero_id_bal_numero ON bal.numero(id_bal_numero);

DO $$
BEGIN
    -- add integrity constraint on table bal.voie
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
                ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
                ON nsp.oid = connamespace
        WHERE
            nsp.nspname = 'bal'
            AND rel.relname = 'numero'
            AND con.contype = 'f'
    ) THEN
        ALTER TABLE bal.numero ADD FOREIGN KEY (id_bal_voie) REFERENCES bal.voie (id_bal_voie);
    END IF;
END $$;
