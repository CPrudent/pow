/***
 * DDL: INSEE administrative cuttings (municipality, district and supra)
 */

CREATE TABLE IF NOT EXISTS insee.administrative_cutting_municipality_and_district
(
    millesime INTEGER NOT NULL
    , codgeo VARCHAR NOT NULL
    , libgeo VARCHAR
    , com VARCHAR
    , dep VARCHAR
    , reg VARCHAR
    , epci VARCHAR
    , nature_epci VARCHAR
    , arr VARCHAR
    , cv VARCHAR
);

CREATE TABLE IF NOT EXISTS insee.administrative_cutting_supra
(
	millesime INTEGER NOT NULL
	, nivgeo VARCHAR NOT NULL
	, codgeo VARCHAR
	, libgeo VARCHAR
	, nb_com VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_decoupage_communal_com_arm_codgeo ON insee.administrative_cutting_municipality_and_district (codgeo, millesime);
CREATE UNIQUE INDEX IF NOT EXISTS idx_decoupage_communal_zsc_codgeo ON insee.administrative_cutting_supra (nivgeo, codgeo, millesime);

SELECT set_table_comment(
    'insee'
    , 'administrative_cutting_municipality_and_district'
    , 'Découpage communal - communes + arrondissements'
    , 'Table d''appartenance géographique des communes - Communes et arrondissements municipaux'
    , ''
);
