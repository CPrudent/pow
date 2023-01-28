-- On créé la table si elle n'existe pas
CREATE TABLE IF NOT EXISTS ran.coord_ra20
(
	dt_import_bcaa DATE NOT NULL DEFAULT NOW(),
	dt_extraction_ran DATE NOT NULL,
	no_version_ran INTEGER NOT NULL,
	co_insee CHAR(5) NOT NULL,
	co_cea CHAR(10) NULL,
	va_x CHARACTER VARYING /*NOT*/ NULL, 
	va_y CHARACTER VARYING /*NOT*/ NULL,
	no_type_localisation INTEGER /*NOT*/ NULL,
	lb_type_localisation CHARACTER VARYING(100) /*NOT*/ NULL,
	co_type_projection CHAR(1) /*NOT*/ NULL,
	lb_type_projection CHARACTER VARYING(100) /*NOT*/ NULL
)
WITH (
  OIDS=FALSE
);

COMMENT ON TABLE ran.coord_ra20 IS 'Coordonnées projetées
Cette table contient toutes les coordonnées projetées, existantes dans RAN, et pour les adresses numéro (dont le géocodage n''est pas stocké dans RAN), le géocodage de son adresse mère (adresse voie) s''il existe sinon le géocodage de la commune..
Cette table contient les géocodages de niveau Commune, voie ou numéro. 
Il n''y a pas de GEOCODAGE de niveau Ligne 3 dans RAN.
Ne concerne que les adresses diffusables';
COMMENT ON COLUMN ran.coord_ra20.co_insee IS 'Code INSEE de la commune';
COMMENT ON COLUMN ran.coord_ra20.co_cea IS 'CEA de la voie ou du numéro. Vide s''il s''agit d''une commune.';
COMMENT ON COLUMN ran.coord_ra20.va_x IS 'Cordonnées X';
COMMENT ON COLUMN ran.coord_ra20.va_y IS 'Cordonnées Y';
COMMENT ON COLUMN ran.coord_ra20.no_type_localisation IS 'Numéro identifiant du type de localisation des coordonnées (du moins précis au plus précis) :
1) Centre commune		Coordonnées du barycentre de la surface communale de l''adresse
2) Mairie				Coordonnées de la mairie de la commune 
3) Zone adressage		Coordonnées du barycentre de la surface du CP de l''adresse
4) Centre voie			Coordonnées du milieu de la somme de tous les tronçons de la même voie
5) Interpolation		Coordonnées du numéro en equi distance par rapport aux bornes du tronçon de rattachement
6) Tronçon de voie		Coordonnées du centre du tronc sur lequel se situe l''adresse
7) Projection centroïde Coordonnées de la projection orthogonale du barycentre de la parcelle cadastrale correspondant au numéro
8) Projection plaque	Coordonnées de la plaque du numéro , donc l''entrée dans la voie';
COMMENT ON COLUMN ran.coord_ra20.lb_type_localisation IS 'Libellé de localisation des coordonnées';
COMMENT ON COLUMN ran.coord_ra20.co_type_projection IS 'Code identifiant du type de projection des coordonnées
1) RGF93 / Lambert-93 -- France / EPSG:2154
2) RRAF 1991 / UTM zone 20N / EPSG:4559
3) NTF (Paris) / France II / EPSG:27582 DEPRECATED
4) RGFG95 / UTM zone 22N / EPSG:2972
5) RGR92 / UTM zone 40S / EPSG:2975
6) RGM04 / UTM zone 38S / EPSG:4471
7) RGSPM06 / UTM zone 21N / EPSG:4467';
COMMENT ON COLUMN ran.coord_ra20.lb_type_projection IS 'Libellé du type de projection des coordonnées';

-- On vide la table ou cas ou elle existait et contenait des données
TRUNCATE TABLE ran.coord_ra20;