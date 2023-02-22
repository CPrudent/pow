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

