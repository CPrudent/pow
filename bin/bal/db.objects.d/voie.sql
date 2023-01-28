/***
 * add BAL street
 */

CREATE TABLE IF NOT EXISTS bal.voie (
  id_bal_voie VARCHAR NOT NULL,
  type_voie VARCHAR NOT NULL,
  libelle_voie VARCHAR NOT NULL,
  nb_numeros_certifies INTEGER NOT NULL,
  id_bal_commune VARCHAR NOT NULL,
  libelle_commune VARCHAR NOT NULL,
  co_insee_commune VARCHAR NOT NULL,
  dt_derniere_maj TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_voie_lower_id_bal_voie ON bal.voie(LOWER(id_bal_voie));
CREATE UNIQUE INDEX IF NOT EXISTS iux_voie_id_bal_voie ON bal.voie(id_bal_voie);
