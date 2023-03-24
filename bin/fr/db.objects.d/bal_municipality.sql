/***
 * FR: add BAL municipality
 */

CREATE TABLE IF NOT EXISTS fr.bal_municipality (
    code_commune CHAR(5)
    , nom_commune VARCHAR
    , departement VARCHAR(3)
    , region CHAR(3)
    , population INTEGER
    , type_composition VARCHAR
    , nb_lieux_dits INTEGER
    , nb_voies INTEGER
    , nb_numeros INTEGER
    , nb_numeros_certifies INTEGER
    , analyse_adressage_nb_adresses_attendues INTEGER
    , analyse_adressage_ratio INTEGER
    , analyse_adressage_deficit_adresses INTEGER
    , composed_at TIMESTAMP WITHOUT TIME ZONE
)
;

SELECT drop_all_functions_if_exists('fr', 'set_bal_municipality_index');
CREATE OR REPLACE PROCEDURE fr.set_bal_municipality_index()
AS
$proc$
BEGIN
    -- uniq ID
    IF index_exists('fr', 'idx_communes_summary_code_commune') AND NOT index_exists('fr', 'iux_bal_municipality_code_commune') THEN
        ALTER INDEX idx_communes_summary_code_commune RENAME TO iux_bal_municipality_id_bal_voie;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_bal_municipality_code_commune ON fr.bal_municipality (code_commune);
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.set_bal_municipality_index();
END
$$;
