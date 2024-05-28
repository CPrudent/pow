/***
 * FR: add INSEE administrative cuttings (municipality w/ municipal district and supra)
 */

DO $$
BEGIN
    ALTER TABLE IF EXISTS fr.insee_administrative_cutting_municipality_and_district RENAME TO insee_municipality;
    ALTER INDEX IF EXISTS fr.iux_insee_administrative_cutting_municipality_and_district_codg RENAME TO iux_insee_municipality_codgeo_millesime;

    ALTER TABLE IF EXISTS fr.insee_administrative_cutting_supra RENAME TO insee_supra;
    ALTER INDEX IF EXISTS fr.iux_insee_administrative_cutting_supra_codgeo RENAME TO iux_insee_supra_nivgeo_codgeo_millesime;
END $$;

CREATE TABLE IF NOT EXISTS fr.insee_municipality(
    millesime INTEGER NOT NULL,
    codgeo VARCHAR NOT NULL,
    libgeo VARCHAR,
    com VARCHAR,
    dep VARCHAR,
    reg VARCHAR,
    epci VARCHAR,
    nature_epci VARCHAR,
    arr VARCHAR,
    cv VARCHAR
);

CREATE TABLE IF NOT EXISTS fr.insee_supra(
    millesime INTEGER NOT NULL,
    nivgeo VARCHAR NOT NULL,
    codgeo VARCHAR,
    libgeo VARCHAR,
    nb_com VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_insee_municipality_codgeo_millesime ON fr.insee_municipality (codgeo, millesime);
CREATE UNIQUE INDEX IF NOT EXISTS iux_insee_supra_nivgeo_codgeo_millesime ON fr.insee_supra (nivgeo, codgeo, millesime);

SELECT set_table_comment(
    'fr',
    'insee_municipality',
    'Découpage administratif - communes & arrondissements municipaux',
    'Table d''appartenance géographique des communes - Communes et arrondissements municipaux',
    ''
);

SELECT set_table_comment(
    'fr',
    'insee_supra',
    'Découpage administratif - zones supra-communales',
    'Table d''appartenance géographique des communes - Zones supra-communales',
    ''
);
