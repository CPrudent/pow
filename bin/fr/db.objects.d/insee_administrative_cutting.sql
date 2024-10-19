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

DO $$
BEGIN
    IF column_exists('fr', 'insee_municipality', 'millesime') THEN
        ALTER TABLE fr.insee_municipality DROP COLUMN millesime;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fr.insee_supra(
    nivgeo VARCHAR NOT NULL,
    codgeo VARCHAR,
    libgeo VARCHAR,
    nb_com VARCHAR
);

DO $$
BEGIN
    IF column_exists('fr', 'insee_supra', 'millesime') THEN
        ALTER TABLE fr.insee_supra DROP COLUMN millesime;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS iux_insee_municipality_codgeo ON fr.insee_municipality (codgeo);
CREATE UNIQUE INDEX IF NOT EXISTS iux_insee_supra_nivgeo_codgeo ON fr.insee_supra (nivgeo, codgeo);

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
