/***
 * FR: add BAL street
 */

CREATE TABLE IF NOT EXISTS fr.bal_street (
    id_bal_voie VARCHAR NOT NULL
    , type_voie VARCHAR NOT NULL
    , libelle_voie VARCHAR NOT NULL
    , nb_numeros_certifies INTEGER NOT NULL
    , id_bal_commune VARCHAR NOT NULL
    , libelle_commune VARCHAR NOT NULL
    , co_insee_commune VARCHAR NOT NULL
    , dt_derniere_maj TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

SELECT drop_all_functions_if_exists('fr', 'setBalIndexStreet');
CREATE OR REPLACE PROCEDURE fr.setBalIndexStreet()
AS
$proc$
BEGIN
    -- uniq ID
    IF index_exists('fr', 'iux_voie_id_bal_voie') AND NOT index_exists('fr', 'iux_bal_street_id_bal_voie') THEN
        ALTER INDEX iux_voie_id_bal_voie RENAME TO iux_bal_street_id_bal_voie;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_street_id_bal_voie ON fr.bal_street (id_bal_voie);
    END IF;
    IF index_exists('fr', 'idx_voie_id_bal_voie') AND NOT index_exists('fr', 'iux_bal_street_id_bal_voie') THEN
        ALTER INDEX idx_voie_id_bal_voie RENAME TO iux_bal_street_id_bal_voie;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_street_id_bal_voie ON fr.bal_street (id_bal_voie);
    END IF;

    /*
    -- uniq lower ID
    IF index_exists('fr', 'iux_voie_lower_id_bal_voie') AND NOT index_exists('fr', 'iux_bal_street_lower_id_bal_voie') THEN
        ALTER INDEX iux_voie_lower_id_bal_voie RENAME TO iux_bal_street_lower_id_bal_voie;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_street_lower_id_bal_voie ON fr.bal_street (LOWER(id_bal_voie));
    END IF;
     */
    DROP INDEX IF EXISTS idx_voie_lower_id_bal_voie;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.setBalIndexStreet();
END
$$;
