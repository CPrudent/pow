DO $$
DECLARE
BEGIN
	IF table_exists('ran','coord') = FALSE THEN
		DROP TABLE IF EXISTS ran.coord_ra50 CASCADE;
	END IF;
END $$;

--Table contenant les données du fichiers RAN RA50
CREATE TABLE IF NOT EXISTS ran.coord_ra50(
	co_insee CHAR(5) NOT NULL,
	co_cea CHAR(10) NULL,
	va_x CHARACTER VARYING /*NOT*/ NULL, 
	va_y CHARACTER VARYING /*NOT*/ NULL,
	no_type_localisation INTEGER /*NOT*/ NULL,
	lb_type_localisation CHARACTER VARYING(100) /*NOT*/ NULL,
	co_type_projection CHAR(1) /*NOT*/ NULL,
	lb_type_projection CHARACTER VARYING(100) /*NOT*/ NULL,
	fl_diffusable INTEGER
)
WITH (
  OIDS=FALSE
);

--Table contenant les données révisées du fichiers RAN RA50
CREATE TABLE IF NOT EXISTS ran.coord
(
	co_insee CHAR(5) NOT NULL,
	co_cea CHAR(10) NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	va_x DOUBLE PRECISION /*NOT*/ NULL, 
	va_y DOUBLE PRECISION /*NOT*/ NULL,
	no_type_localisation INTEGER /*NOT*/ NULL,
	co_type_projection CHAR(1) /*NOT*/ NULL,
	gm_coord GEOMETRY(POINT,3857) /*NOT*/ NULL
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh)
ALTER TABLE ran.coord SET (
  autovacuum_enabled = false
);
--TODO : de même sur coord_ra50 ?

/*
COMMENT ON TABLE ran.coord IS 'Coordonnées projetées
Cette table contient toutes les coordonnées projetées, existantes dans RAN, et pour les adresses numéro (dont le géocodage n''est pas stocké dans RAN), le géocodage de son adresse mère (adresse voie) s''il existe sinon le géocodage de la commune..
Cette table contient les géocodages de niveau Commune, voie ou numéro. 
Il n''y a pas de GEOCODAGE de niveau Ligne 3 dans RAN.
Concerne toutes les adresses, y compris les non diffusables';
COMMENT ON COLUMN ran.coord_ra50.co_insee IS 'Code INSEE de la commune';
COMMENT ON COLUMN ran.coord_ra50.co_cea IS 'CEA de la voie ou du numéro. Vide s''il s''agit d''une commune.';
COMMENT ON COLUMN ran.coord_ra50.va_x IS 'Cordonnées X';
COMMENT ON COLUMN ran.coord_ra50.va_y IS 'Cordonnées Y';
COMMENT ON COLUMN ran.coord_ra50.no_type_localisation IS 'Numéro identifiant du type de localisation des coordonnées (du moins précis au plus précis) :
1) Centre commune		Coordonnées du barycentre de la surface communale de l''adresse
2) Mairie				Coordonnées de la mairie de la commune 
3) Zone adressage		Coordonnées du barycentre de la surface du CP de l''adresse
4) Centre voie			Coordonnées du milieu de la somme de tous les tronçons de la même voie
5) Interpolation		Coordonnées du numéro en equi distance par rapport aux bornes du tronçon de rattachement
6) Tronçon de voie		Coordonnées du centre du tronc sur lequel se situe l''adresse
7) Projection centroïde Coordonnées de la projection orthogonale du barycentre de la parcelle cadastrale correspondant au numéro
8) Projection plaque	Coordonnées de la plaque du numéro , donc l''entrée dans la voie';
COMMENT ON COLUMN ran.coord_ra50.lb_type_localisation IS 'Libellé de localisation des coordonnées';
COMMENT ON COLUMN ran.coord_ra50.co_type_projection IS 'Code identifiant du type de projection des coordonnées
1) RGF93 / Lambert-93 -- France / EPSG:2154
2) RRAF 1991 / UTM zone 20N / EPSG:4559
3) NTF (Paris) / France II / EPSG:27582 DEPRECATED
4) RGFG95 / UTM zone 22N / EPSG:2972
5) RGR92 / UTM zone 40S / EPSG:2975
6) RGM04 / UTM zone 38S / EPSG:4471
7) RGSPM06 / UTM zone 21N / EPSG:4467';
COMMENT ON COLUMN ran.coord_ra50.lb_type_projection IS 'Libellé du type de projection des coordonnées';
COMMENT ON COLUMN ran.coord_ra50.fl_diffusable IS 'Etat diffusable
1 = diffusable, 0 = non diffusable, NULL = pour les communes';
*/
-- Fonction remplacée par un appel à import_file
SELECT drop_all_functions_if_exists('ran','setCoordRa50FromRa50');

/*
TEST

Sur le serveur (REC) :
#extraction fichier ran
cd /data/bcaa/common_env/import/ran/
mkdir test
tar -C ./test/ -xzf raataaaa.bm_2017-09-30.tar.gz
#filtre département
head -1 ra50aaaa.bm > ra50aaaa.bm.tmp
grep '^50' ra50aaaa.bm >> ra50aaaa.bm.tmp
rm ra50aaaa.bm
mv ra50aaaa.bm.tmp ra50aaaa.bm
#chargement
SELECT ran.setCoordRa50FromRa50('/data/bcaa/common_env/import/ran/test/');
*/

SELECT drop_all_functions_if_exists('ran','getCoordFromCoordRa50');
CREATE OR REPLACE FUNCTION ran.getCoordFromCoordRa50(coord_ra50 IN ran.coord_ra50)
  RETURNS ran.coord AS
$func$
DECLARE
	v_coord ran.coord%ROWTYPE;
BEGIN
	v_coord.co_insee := coord_ra50.co_insee;
	v_coord.co_cea := coord_ra50.co_cea;
	v_coord.va_x := REPLACE(coord_ra50.va_x, ',', '.')::DOUBLE PRECISION;
	v_coord.va_y := REPLACE(coord_ra50.va_y, ',', '.')::DOUBLE PRECISION;
	v_coord.no_type_localisation := coord_ra50.no_type_localisation;
	v_coord.co_type_projection := coord_ra50.co_type_projection;
	v_coord.gm_coord := ST_Transform(
			ST_SetSRID(
				ST_MakePoint(
					v_coord.va_x
					,v_coord.va_y
				)
				,public.getSridCoordRanFromCodeInseeDepartement(public.getCodeInseeDepartementFromCodeInseeCommune(v_coord.co_insee))
			)
			,3857
		);
	RETURN v_coord;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT coord.*
FROM ran.coord_ra50
CROSS JOIN ran.getCoordFromCoordRa50(coord_ra50) AS coord
*/

SELECT drop_all_functions_if_exists('ran','getCoordDeltaAdresseFromRa50');
CREATE OR REPLACE FUNCTION ran.getCoordDeltaAdresseFromRa50(in_dt_reference DATE)
  RETURNS SETOF ran.coord AS
$func$
DECLARE
BEGIN
	RETURN QUERY
		SELECT 
			coord.co_insee
			,coord.co_cea
			,in_dt_reference AS dt_reference
			,CASE 
				--WHEN ??? THEN 'S'
				WHEN coord_avant.co_cea IS NULL OR coord_avant.co_mouvement = 'S' THEN 'C' /*recréation*/ 
				ELSE 'M' 
			END::CHAR(1) AS co_mouvement
			,coord.va_x
			,coord.va_y
			,coord.no_type_localisation
			,coord.co_type_projection
			,coord.gm_coord
		FROM ran.coord_ra50
		LEFT OUTER JOIN ran.coord AS coord_avant ON coord_avant.co_cea = coord_ra50.co_cea
		CROSS JOIN ran.getCoordFromCoordRa50(coord_ra50) AS coord
		WHERE coord.co_cea IS NOT NULL --coordonnées sur une adresse
		AND (
			--élément qui n'existait pas jusqu'ici
			coord_avant.co_cea IS NULL
			--OU élément qui a changé
			OR COALESCE(coord_avant.va_x, -1) != coord.va_x
			OR COALESCE(coord_avant.va_y, -1) != coord.va_y
			OR COALESCE(coord_avant.no_type_localisation, -1) != coord.no_type_localisation
			OR COALESCE(coord_avant.co_type_projection, '0') != coord.co_type_projection
		)
		;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('ran','getCoordDeltaCommuneFromRa50');
CREATE OR REPLACE FUNCTION ran.getCoordDeltaCommuneFromRa50(in_dt_reference DATE)
  RETURNS SETOF ran.coord AS
$func$
DECLARE
BEGIN
	RETURN QUERY
		SELECT 
			coord.co_insee
			,coord.co_cea
			,in_dt_reference AS dt_reference
			,CASE 
				--WHEN ??? THEN 'S'
				WHEN coord_avant.co_cea IS NULL OR coord_avant.co_mouvement = 'S' THEN 'C' /*recréation*/ 
				ELSE 'M' 
			END::CHAR(1) AS co_mouvement
			,coord.va_x
			,coord.va_y
			,coord.no_type_localisation
			,coord.co_type_projection
			,coord.gm_coord
		FROM ran.coord_ra50
		LEFT OUTER JOIN ran.coord AS coord_avant ON coord_avant.co_insee = coord_ra50.co_insee AND coord_avant.co_cea IS NULL
		CROSS JOIN ran.getCoordFromCoordRa50(coord_ra50) AS coord
		WHERE coord.co_cea IS NULL --coordonnées sur une commune
		AND (
			--élément qui n'existait pas jusqu'ici
			coord_avant.co_insee IS NULL
			--OU élément qui a changé
			OR COALESCE(coord_avant.va_x, -1) != coord.va_x
			OR COALESCE(coord_avant.va_y, -1) != coord.va_y
			OR COALESCE(coord_avant.no_type_localisation, -1) != coord.no_type_localisation
			OR COALESCE(coord_avant.co_type_projection, '0') != coord.co_type_projection
		)
		;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM ran.getCoordDeltaFromRa50(NOW()::DATE)

REC(50) : 10s
*/

SELECT drop_all_functions_if_exists('ran','setCoordFromRa50');
CREATE OR REPLACE FUNCTION ran.setCoordFromRa50(in_dt_reference IN DATE, in_en_mode_init BOOLEAN DEFAULT TRUE, in_avec_historique BOOLEAN DEFAULT TRUE)
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	--Note : à ne pas lancer 2 fois car on ne prévoit pas de réinit de coord_ra50 systématique
	--On ajoute les coordonnées sur les adresses L3 par héritage de leur adresse parent (VOIE ou NUMERO)
	INSERT INTO ran.coord_ra50 (
		SELECT 
			coord.co_insee
			,adresse_l3.co_cea_l3
			,coord.va_x
			,coord.va_y
			,coord.no_type_localisation
			,coord.lb_type_localisation
			,coord.co_type_projection
			,coord.lb_type_projection
			,coord.fl_diffusable
		FROM ran.coord_ra50 AS coord
		INNER JOIN ran.adresse AS adresse_l3 ON adresse_l3.co_cea_parent = coord.co_cea AND adresse_l3.co_niveau = 'L3'
	);
	--A ENVISAGER pour généraliser le processus : On y ajoute les coordonnées sur les adresses ZA par héritage de leur commune + suppression des coordonnées sur commune uniquement (co_cea sera alors TOUJOURS renseigné)
	--A ENVISAGER : héritage systématique sur tous les étages ?
	--A ENVISAGER : héritage géré par un lien co_adr_coord dans adresse ? cela éviterait de dupliquer une même coordonnée utilisée pour plusieurs adresses
	
	IF in_en_mode_init = TRUE THEN
		--Initialisation des coords
		TRUNCATE TABLE ran.coord;
		PERFORM public.drop_table_indexes('ran', 'coord');
		INSERT INTO ran.coord(
			co_insee
			,co_cea
			,dt_reference
			,co_mouvement
			,va_x
			,va_y
			,no_type_localisation
			,co_type_projection
			,gm_coord
		)
		(
			SELECT 
				coord.co_insee
				,coord.co_cea
				,in_dt_reference AS dt_reference
				/*,CASE 
					WHEN ??? THEN 'S' 
					ELSE 'I'  --INIT
				END AS co_mouvement
				*/
				,'I' AS co_mouvement
				,coord.va_x
				,coord.va_y
				,coord.no_type_localisation
				,coord.co_type_projection
				,coord.gm_coord
			FROM ran.coord_ra50
			CROSS JOIN ran.getCoordFromCoordRa50(coord_ra50) AS coord
		);
	ELSE
		INSERT INTO ran.coord(
			co_insee
			,co_cea
			,dt_reference
			,co_mouvement
			,va_x
			,va_y
			,no_type_localisation
			,co_type_projection
			,gm_coord
		)
		(
			SELECT
				co_insee
				,co_cea
				,dt_reference
				,co_mouvement
				,va_x
				,va_y
				,no_type_localisation
				,co_type_projection
				,gm_coord
			FROM ran.getCoordDeltaAdresseFromRa50(in_dt_reference)
		)
		ON CONFLICT(co_cea)
		DO UPDATE
			SET	dt_reference = EXCLUDED.dt_reference
				,co_mouvement = EXCLUDED.co_mouvement
				,va_x = EXCLUDED.va_x
				,va_y = EXCLUDED.va_y
				,no_type_localisation = EXCLUDED.no_type_localisation 
				,co_type_projection = EXCLUDED.co_type_projection
				,gm_coord = EXCLUDED.gm_coord
		;
		
		INSERT INTO ran.coord(
			co_insee
			,co_cea
			,dt_reference
			,co_mouvement
			,va_x
			,va_y
			,no_type_localisation
			,co_type_projection
			,gm_coord
		)
		(
			SELECT
				co_insee
				,co_cea
				,dt_reference
				,co_mouvement
				,va_x
				,va_y
				,no_type_localisation
				,co_type_projection
				,gm_coord
			FROM ran.getCoordDeltaCommuneFromRa50(in_dt_reference)
		)
		ON CONFLICT(co_insee) WHERE co_cea IS NULL
		DO UPDATE
			SET	dt_reference = EXCLUDED.dt_reference
				,co_mouvement = EXCLUDED.co_mouvement
				,va_x = EXCLUDED.va_x
				,va_y = EXCLUDED.va_y
				,no_type_localisation = EXCLUDED.no_type_localisation 
				,co_type_projection = EXCLUDED.co_type_projection
				,gm_coord = EXCLUDED.gm_coord
		;
	END IF;
	
	-- Création des indexes pour accélérer l'accès au données
	-- Controle d'unicité, et optimisation de filtre sur l'identifiant de l'adresse géolocalisée
	-- Les valeurs CEA sont uniques, en dehors des valeurs NULL (coordonnées sur commune)
	CREATE UNIQUE INDEX IF NOT EXISTS idx_ran_coord_co_cea ON ran.coord (co_cea);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_ran_coord_co_insee ON ran.coord (co_insee) WHERE co_cea IS NULL;
	-- Otimisation de filtre géographique
	CREATE INDEX IF NOT EXISTS idx_ran_coord_gm_coord ON ran.coord USING GIST(gm_coord);
	
	TRUNCATE TABLE ran.coord_ra50;
	
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST

SELECT ran.setCoordFromRa50('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, TRUE, TRUE);

SELECT ran.setCoordFromRa50('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, FALSE, FALSE);

*/
