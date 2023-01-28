DO $$
DECLARE
	v_information_schema_column information_schema.columns%ROWTYPE;
BEGIN
	IF table_exists('ran','adresse_ra49') = TRUE THEN
		v_information_schema_column := public.get_column_information('ran','adresse_ra49','co_cea_voie');
		--Réordonnement pour correspondre à l'ordre du fichier RA49
		IF v_information_schema_column.ordinal_position != 1 THEN
			DROP TABLE ran.adresse_ra49 CASCADE;
		END IF;
	END IF;
END $$;

--Table contenant les données du fichiers RAN RA49
CREATE TABLE IF NOT EXISTS ran.adresse_ra49(
	co_cea_voie CHAR(10),
	co_cea_numero CHAR(10),
	co_cea_l3 CHAR(10),
	co_cea_za CHAR(10) NOT NULL,
	fl_diffusable INTEGER NOT NULL
)
WITH (
  OIDS=FALSE
);

/*
COMMENT ON TABLE ran.adresse_ra49 IS 'Adresses RAN';
COMMENT ON COLUMN ran.adresse_ra49.co_cea_l3 IS 'CEA de l''adresse L3';
COMMENT ON COLUMN ran.adresse_ra49.co_cea_numero IS 'CEA de l''adresse NUMERO';
COMMENT ON COLUMN ran.adresse_ra49.co_cea_voie IS 'CEA de l''adresse VOIE';
COMMENT ON COLUMN ran.adresse_ra49.co_cea_za IS 'CEA de l''adresse ZA';
COMMENT ON COLUMN ran.adresse_ra49.fl_diffusable IS 'Etat diffusable
1 = diffusable, 0 = non diffusable';
*/

--Table contenant les données révisées du fichiers RAN RA49
CREATE TABLE IF NOT EXISTS ran.adresse
(
	co_cea_determinant CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL,
	fl_diffusable BOOLEAN NOT NULL,
	co_cea_parent CHAR(10) NULL,
	co_niveau VARCHAR(10) NOT NULL,
	co_cea_l3 CHAR(10) NULL,
	dt_reference_l3 DATE NULL,
	co_cea_numero CHAR(10) NULL,
	dt_reference_numero DATE NULL,
	co_cea_voie CHAR(10) NULL,
	dt_reference_voie DATE NULL,
	co_cea_za CHAR(10) NOT NULL,
	dt_reference_za DATE NOT NULL
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh)
ALTER TABLE ran.adresse_ra49 SET (
  autovacuum_enabled = false
);
--TODO : de même sur adresse_ra49 ?

-- On créé la table si elle n'existe pas
CREATE TABLE IF NOT EXISTS ran.adresse_histo
(
	co_cea_determinant CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL,
	fl_diffusable BOOLEAN NOT NULL,
	co_cea_parent CHAR(10) NULL,
	co_niveau VARCHAR(10) NOT NULL,
	co_cea_l3 CHAR(10) NULL,
	dt_reference_l3 DATE NULL,
	co_cea_numero CHAR(10) NULL,
	dt_reference_numero DATE NULL,
	co_cea_voie CHAR(10) NULL,
	dt_reference_voie DATE NULL,
	co_cea_za CHAR(10) NOT NULL,
	dt_reference_za DATE NOT NULL
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh), on préfère désactiver l'autovacuum de façon à ce qu'il n'occupe pas de ressource inutilement (exemple : lancement pendant une opération intermédiaire au vacuum manuel)
ALTER TABLE ran.adresse_histo SET (
  autovacuum_enabled = false
);

-- Fonction remplacée par un appel à import_file
SELECT drop_all_functions_if_exists('ran','setAdresseRa49FromRa49');

/*
TEST

Sur le serveur (REC) :
#extraction fichier ran
cd /data/bcaa/common_env/import/ran/
mkdir test
tar -C ./test/ -xzf raataaaa.bm_2017-09-30.tar.gz
#filtre département
head -1 ra49aaaa.bm > ra49aaaa.bm.tmp
grep '^49' ra49aaaa.bm >> ra49aaaa.bm.tmp
rm ra49aaaa.bm
mv ra49aaaa.bm.tmp ra49aaaa.bm
#chargement
*/

SELECT drop_all_functions_if_exists('ran','getAdresseFromAdresseRa49');
CREATE OR REPLACE FUNCTION ran.getAdresseFromAdresseRa49(adresse_ra49 IN ran.adresse_ra49, za IN ran.za, voie IN ran.voie, numero IN ran.numero, l3 IN ran.l3)
  RETURNS ran.adresse AS
$func$
DECLARE
	v_adresse ran.adresse%ROWTYPE;
BEGIN
	v_adresse.co_cea_determinant := COALESCE(adresse_ra49.co_cea_l3, adresse_ra49.co_cea_numero, adresse_ra49.co_cea_voie, adresse_ra49.co_cea_za);
	v_adresse.fl_active := LEAST(za.fl_active, voie.fl_active, numero.fl_active, l3.fl_active);
	--Les adresses enfants sont considérées non diffusables si leur adresse parent ne l'est pas.
	--Exemple : les lignes 3 d'un numéro non diffusable, sont considérées indirectement non diffusables, qu'elles soient marquées diffusables ou non directement
    --L'indicateur "adresse diffusable" aussi présent le fichier RA49 (parfois appelée adresses hiérarchisées) ne semble pas au point, il est préférable de l'ignorer
	v_adresse.fl_diffusable := LEAST(/*adresse_ra49.fl_diffusable::INTEGER::BOOLEAN, */ TRUE, voie.fl_diffusable, numero.fl_diffusable, l3.fl_diffusable);
	v_adresse.co_cea_parent := CASE
			WHEN (adresse_ra49.co_cea_l3 IS NOT NULL) THEN COALESCE(adresse_ra49.co_cea_numero, adresse_ra49.co_cea_voie)
			WHEN (adresse_ra49.co_cea_numero IS NOT NULL) THEN adresse_ra49.co_cea_voie
			WHEN (adresse_ra49.co_cea_voie IS NOT NULL) THEN adresse_ra49.co_cea_za
			ELSE NULL
		END;
	v_adresse.co_niveau := CASE
			WHEN (adresse_ra49.co_cea_l3 IS NOT NULL) THEN 'L3'
			WHEN (adresse_ra49.co_cea_numero IS NOT NULL) THEN 'NUMERO'
			WHEN (adresse_ra49.co_cea_voie IS NOT NULL) THEN 'VOIE'
			ELSE 'ZA'
		END;
	v_adresse.co_cea_l3 := adresse_ra49.co_cea_l3;
	v_adresse.dt_reference_l3 := l3.dt_reference;
	v_adresse.co_cea_numero := adresse_ra49.co_cea_numero;
	v_adresse.dt_reference_numero := numero.dt_reference;
	v_adresse.co_cea_voie := adresse_ra49.co_cea_voie;
	v_adresse.dt_reference_voie := voie.dt_reference;
	v_adresse.co_cea_za := adresse_ra49.co_cea_za;
	v_adresse.dt_reference_za := za.dt_reference;
	
	RETURN v_adresse;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT adresse.*
FROM ran.adresse_ra49
CROSS JOIN ran.getAdresseFromAdresseRa49(adresse_ra49) AS adresse
*/

SELECT drop_all_functions_if_exists('ran','getAdresseDeltaFromRa49');
CREATE OR REPLACE FUNCTION ran.getAdresseDeltaFromRa49(in_dt_reference DATE)
  RETURNS SETOF ran.adresse AS
$func$
DECLARE
BEGIN
	RETURN QUERY
		SELECT 
			adresse.co_cea_determinant
			,in_dt_reference AS dt_reference
			,CASE 
				WHEN adresse.fl_active = FALSE THEN 'S' 
				WHEN adresse_avant.co_cea_determinant IS NULL OR adresse_avant.co_mouvement = 'S' THEN 'C' /*recréation*/ 
				ELSE 'M' 
			END::CHAR(1) AS co_mouvement
			,adresse.fl_active
			,adresse.fl_diffusable
			,adresse.co_cea_parent
			,adresse.co_niveau
			,adresse.co_cea_l3
			,adresse.dt_reference_l3
			,adresse.co_cea_numero
			,adresse.dt_reference_numero
			,adresse.co_cea_voie
			,adresse.dt_reference_voie
			,adresse.co_cea_za
			,adresse.dt_reference_za
		FROM ran.adresse_ra49
		INNER JOIN ran.za ON za.co_cea = adresse_ra49.co_cea_za
		LEFT OUTER JOIN ran.voie ON voie.co_cea = adresse_ra49.co_cea_voie
		LEFT OUTER JOIN ran.numero ON numero.co_cea = adresse_ra49.co_cea_numero
		LEFT OUTER JOIN ran.l3 ON l3.co_cea = adresse_ra49.co_cea_l3
		/* Alternative intéressante en cas d'abandon de l'état actuel au profit de l'historique seul :
		LEFT OUTER JOIN ran.adresse_histo AS adresse_avant 
		ON adresse_avant.co_cea_determinant = adresse_ra49.co_cea_determinant 
		AND adresse_avant.dt_reference = (
			SELECT adresse_a_date.dt_reference
			FROM ran.adresse_histo AS adresse_a_date
			WHERE adresse_a_date.co_cea = adresse_avant.co_cea
			AND adresse_a_date.dt_reference < in_dt_reference
			ORDER BY adresse_a_date.dt_reference DESC
			LIMIT 1
		)
		*/
		CROSS JOIN ran.getAdresseFromAdresseRa49(adresse_ra49, za, voie, numero, l3) AS adresse
		LEFT OUTER JOIN ran.adresse AS adresse_avant ON adresse_avant.co_cea_determinant = adresse.co_cea_determinant
		WHERE 
		(
			--élément qui n'existait pas jusqu'ici
			adresse_avant.co_cea_determinant IS NULL
			--OU élément qui a changé
			OR adresse_avant.fl_active != adresse.fl_active
			OR adresse_avant.fl_diffusable != adresse.fl_diffusable
			OR adresse_avant.co_cea_za != adresse.co_cea_za
			--changement de voie liée, ou lien voie retiré (à priori pas possible)
			OR COALESCE(adresse_avant.co_cea_voie,'NULL') != COALESCE(adresse.co_cea_voie,'NULL')
			--changement de numéro lié, ou lien numero retiré (L3 anciennement lié à un NUMERO, désomais liée à une VOIE ?)
			OR COALESCE(adresse_avant.co_cea_numero,'NULL') != COALESCE(adresse.co_cea_numero,'NULL')
			OR adresse_avant.dt_reference_za != adresse.dt_reference_za
			OR adresse_avant.dt_reference_voie != adresse.dt_reference_voie
			OR adresse_avant.dt_reference_numero != adresse.dt_reference_numero
			OR adresse_avant.dt_reference_l3 != adresse.dt_reference_l3
		)
		;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM ran.getAdresseDeltaFromRa49(NOW()::DATE)
*/

SELECT drop_all_functions_if_exists('ran','setAdresseFromRa49');
CREATE OR REPLACE FUNCTION ran.setAdresseFromRa49(in_dt_reference IN DATE, in_en_mode_init BOOLEAN DEFAULT TRUE, in_avec_historique BOOLEAN DEFAULT TRUE)
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	IF in_en_mode_init = TRUE THEN
		--Initialisation des adresses
		TRUNCATE TABLE ran.adresse;
		PERFORM public.drop_table_indexes('ran', 'adresse');
		INSERT INTO ran.adresse(
			co_cea_determinant
			,dt_reference
			,co_mouvement
			,fl_active 
			,fl_diffusable
			,co_cea_parent
			,co_niveau
			,co_cea_l3
			,dt_reference_l3
			,co_cea_numero
			,dt_reference_numero
			,co_cea_voie
			,dt_reference_voie
			,co_cea_za
			,dt_reference_za
		)
		(
			SELECT 
				adresse.co_cea_determinant
				,in_dt_reference AS dt_reference
				,CASE 
					WHEN adresse.fl_active = FALSE THEN 'S' 
					ELSE 'I'  --INIT
				END AS co_mouvement
				,adresse.fl_active 
				,adresse.fl_diffusable
				,adresse.co_cea_parent
				,adresse.co_niveau
				,adresse.co_cea_l3
				,adresse.dt_reference_l3
				,adresse.co_cea_numero
				,adresse.dt_reference_numero
				,adresse.co_cea_voie
				,adresse.dt_reference_voie
				,adresse.co_cea_za
				,adresse.dt_reference_za
			FROM ran.adresse_ra49
			INNER JOIN ran.za ON za.co_cea = adresse_ra49.co_cea_za
			LEFT OUTER JOIN ran.voie ON voie.co_cea = adresse_ra49.co_cea_voie
			LEFT OUTER JOIN ran.numero ON numero.co_cea = adresse_ra49.co_cea_numero
			LEFT OUTER JOIN ran.l3 ON l3.co_cea = adresse_ra49.co_cea_l3
			CROSS JOIN ran.getAdresseFromAdresseRa49(adresse_ra49, za, voie, numero, l3) AS adresse
		);
		IF in_avec_historique = TRUE THEN
			--Initialisation de l'historique
			TRUNCATE TABLE ran.adresse_histo;
			PERFORM public.drop_table_indexes('ran', 'adresse_histo');
			INSERT INTO ran.adresse_histo(
				co_cea_determinant
				,dt_reference
				,co_mouvement
				,fl_active 
				,fl_diffusable
				,co_cea_parent
				,co_niveau
				,co_cea_l3
				,dt_reference_l3
				,co_cea_numero
				,dt_reference_numero
				,co_cea_voie
				,dt_reference_voie
				,co_cea_za
				,dt_reference_za
			)
			(
				SELECT 
					co_cea_determinant
					,dt_reference
					,co_mouvement
					,fl_active 
					,fl_diffusable
					,co_cea_parent
					,co_niveau
					,co_cea_l3
					,dt_reference_l3
					,co_cea_numero
					,dt_reference_numero
					,co_cea_voie
					,dt_reference_voie
					,co_cea_za
					,dt_reference_za
				FROM ran.adresse
			)
			;
		END IF;
	ELSE
		IF in_avec_historique = TRUE THEN
			--Calcul du DELTA dans une table temporaire, pour le réutiliser à la fois pour mettre à jour les adresses et l'historique
			DROP TABLE IF EXISTS tmp_ran_adresse_ra49_delta;
			CREATE TEMPORARY TABLE tmp_ran_adresse_ra49_delta AS TABLE ran.adresse WITH NO DATA;
			INSERT INTO tmp_ran_adresse_ra49_delta
			(
				co_cea_determinant
				,dt_reference
				,co_mouvement
				,fl_active 
				,fl_diffusable
				,co_cea_parent
				,co_niveau
				,co_cea_l3
				,dt_reference_l3
				,co_cea_numero
				,dt_reference_numero
				,co_cea_voie
				,dt_reference_voie
				,co_cea_za
				,dt_reference_za
			)
			(
				SELECT
					co_cea_determinant
					,dt_reference
					,co_mouvement
					,fl_active 
					,fl_diffusable
					,co_cea_parent
					,co_niveau
					,co_cea_l3
					,dt_reference_l3
					,co_cea_numero
					,dt_reference_numero
					,co_cea_voie
					,dt_reference_voie
					,co_cea_za
					,dt_reference_za
				FROM ran.getAdresseDeltaFromRa49(in_dt_reference)
			);
			
			--Mise à jour des adresses
			INSERT INTO ran.adresse (SELECT * FROM tmp_ran_adresse_ra49_delta)
			ON CONFLICT(co_cea_determinant)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active  = EXCLUDED.fl_active 
					,fl_diffusable = EXCLUDED.fl_diffusable
					,co_cea_parent = EXCLUDED.co_cea_parent
					,co_niveau = EXCLUDED.co_niveau
					,co_cea_l3 = EXCLUDED.co_cea_l3
					,dt_reference_l3 = EXCLUDED.dt_reference_l3
					,co_cea_numero = EXCLUDED.co_cea_numero
					,dt_reference_numero = EXCLUDED.dt_reference_numero
					,co_cea_voie = EXCLUDED.co_cea_voie
					,dt_reference_voie = EXCLUDED.dt_reference_voie
					,co_cea_za = EXCLUDED.co_cea_za
					,dt_reference_za = EXCLUDED.dt_reference_za
			;
			
			--Mise à jour de l'historique
			INSERT INTO ran.adresse_histo(
				co_cea_determinant
				,dt_reference
				,co_mouvement
				,fl_active 
				,fl_diffusable
				,co_cea_parent
				,co_niveau
				,co_cea_l3
				,dt_reference_l3
				,co_cea_numero
				,dt_reference_numero
				,co_cea_voie
				,dt_reference_voie
				,co_cea_za
				,dt_reference_za
			)
			(
				SELECT
					co_cea_determinant
					,dt_reference
					,co_mouvement
					,fl_active 
					,fl_diffusable
					,co_cea_parent
					,co_niveau
					,co_cea_l3
					,dt_reference_l3
					,co_cea_numero
					,dt_reference_numero
					,co_cea_voie
					,dt_reference_voie
					,co_cea_za
					,dt_reference_za
				FROM tmp_ran_adresse_ra49_delta
			)
			;
		ELSE
			INSERT INTO ran.adresse(
				co_cea_determinant
				,dt_reference
				,co_mouvement
				,fl_active 
				,fl_diffusable
				,co_cea_parent
				,co_niveau
				,co_cea_l3
				,dt_reference_l3
				,co_cea_numero
				,dt_reference_numero
				,co_cea_voie
				,dt_reference_voie
				,co_cea_za
				,dt_reference_za
			)
			(
				SELECT
					co_cea_determinant
					,dt_reference
					,co_mouvement
					,fl_active 
					,fl_diffusable
					,co_cea_parent
					,co_niveau
					,co_cea_l3
					,dt_reference_l3
					,co_cea_numero
					,dt_reference_numero
					,co_cea_voie
					,dt_reference_voie
					,co_cea_za
					,dt_reference_za
				FROM ran.getAdresseDeltaFromRa49(in_dt_reference)
			)
			ON CONFLICT(co_cea_determinant)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active  = EXCLUDED.fl_active 
					,fl_diffusable = EXCLUDED.fl_diffusable
					,co_cea_parent = EXCLUDED.co_cea_parent
					,co_niveau = EXCLUDED.co_niveau
					,co_cea_l3 = EXCLUDED.co_cea_l3
					,dt_reference_l3 = EXCLUDED.dt_reference_l3
					,co_cea_numero = EXCLUDED.co_cea_numero
					,dt_reference_numero = EXCLUDED.dt_reference_numero
					,co_cea_voie = EXCLUDED.co_cea_voie
					,dt_reference_voie = EXCLUDED.dt_reference_voie
					,co_cea_za = EXCLUDED.co_cea_za
					,dt_reference_za = EXCLUDED.dt_reference_za
			;
		END IF;
	END IF;
	
	-- Création des indexes pour accélérer l'accès au données
	-- Controle d'unicité, et optimisation de filtre sur l'identifiant d'une adresse (CEA déterminant)
	CREATE UNIQUE INDEX IF NOT EXISTS idx_adresse_co_cea_determinant ON ran.adresse (co_cea_determinant);
	-- Optimisation de filtre sur le niveau d'adresse
	CREATE INDEX IF NOT EXISTS idx_adresse_niveau ON ran.adresse (co_niveau);
	-- Optimisation de filtre sur le code adresse parent
	CREATE INDEX IF NOT EXISTS idx_adresse_co_cea_parent ON ran.adresse (co_cea_parent);
	-- Optimisation lorsque le filtre provient d'information propre à la ligne3 / numéro / voie / za
	-- Exemples : adresses dont la za est sur une commune, adresse dont le nom de la voie ressemble à, ...
	CREATE UNIQUE INDEX IF NOT EXISTS idx_adresse_co_cea_l3 ON ran.adresse (co_cea_l3); --WHERE co_cea_l3 IS NOT NULL ?
	CREATE INDEX IF NOT EXISTS idx_adresse_co_cea_numero ON ran.adresse (co_cea_numero); --WHERE co_cea_numero IS NOT NULL ?
	CREATE INDEX IF NOT EXISTS idx_adresse_co_cea_voie ON ran.adresse (co_cea_voie); --WHERE co_cea_voie IS NOT NULL ?
	CREATE INDEX IF NOT EXISTS idx_adresse_co_cea_za ON ran.adresse (co_cea_za);

	CREATE UNIQUE INDEX IF NOT EXISTS idx_adresse_histo_key ON ran.adresse_histo (co_cea_determinant, dt_reference);
	
	TRUNCATE TABLE ran.adresse_ra49;
	
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST

SELECT ran.setAdresseFromRa49('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, TRUE, TRUE);

SELECT ran.setAdresseFromRa49('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, FALSE, FALSE);

*/
