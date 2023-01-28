DO $$
DECLARE
BEGIN
	IF table_exists('ran','l3') = FALSE THEN
		DROP TABLE IF EXISTS ran.l3_ra34 CASCADE;
		DROP TABLE IF EXISTS ran.l3_ra34_histo CASCADE;
	END IF;
END $$;

--Table contenant les données du fichiers RAN RA33
CREATE TABLE IF NOT EXISTS ran.l3_ra34(
	co_cea CHAR(10) NOT NULL,
	id_type_groupe1_l3 INTEGER,
	lb_type_groupe1_l3 CHARACTER VARYING(38),
	lb_abrev_g1_an CHARACTER VARYING(10),
	lb_abrev_g1_nn CHARACTER VARYING(10),
	lb_groupe1 CHARACTER VARYING(38),
	id_type_groupe2_l3 INTEGER,
	lb_type_groupe2_l3 CHARACTER VARYING(38),
	lb_abrev_g2_an CHARACTER VARYING(10),
	lb_abrev_g2_nn CHARACTER VARYING(10),
	lb_groupe2 CHARACTER VARYING(38),
	id_type_groupe3_l3 INTEGER,
	lb_type_groupe3_l3 CHARACTER VARYING(38),
	lb_abrev_g3_an CHARACTER VARYING(10),
	lb_abrev_g3_nn CHARACTER VARYING(10),
	lb_groupe3 CHARACTER VARYING(38),
	lb_descr_an_groupe1 CHARACTER VARYING(10),
	lb_descr_nn_groupe1 CHARACTER VARYING(10),
	lb_mot_dir_groupe1 CHARACTER VARYING(38),
	lb_descr_an_groupe2 CHARACTER VARYING(10),
	lb_descr_nn_groupe2 CHARACTER VARYING(10),
	lb_mot_dir_groupe2 CHARACTER VARYING(38),
	lb_descr_an_groupe3 CHARACTER VARYING(10),
	lb_descr_nn_groupe3 CHARACTER VARYING(10),
	lb_mot_dir_groupe3 CHARACTER VARYING(38),
	fl_zone CHARACTER VARYING(1),
	lb_standard_an CHARACTER VARYING(32),
	lb_standard_nn CHARACTER VARYING(38) NOT NULL,
	fl_etat_adresse INTEGER NOT NULL,
	fl_diffusable INTEGER NOT NULL
)
WITH (
  OIDS=FALSE
);

--Table contenant les données révisées du fichiers RAN RA33
CREATE TABLE IF NOT EXISTS ran.l3
(
	co_cea CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL,
	fl_diffusable BOOLEAN NOT NULL,
	lb_standard_nn CHARACTER VARYING(38) NOT NULL,
	id_type_groupe1_l3 INTEGER,
	lb_type_groupe1_l3 CHARACTER VARYING(38),
	lb_abrev_g1_an CHARACTER VARYING(10),
	lb_abrev_g1_nn CHARACTER VARYING(10),
	lb_groupe1 CHARACTER VARYING(38),
	id_type_groupe2_l3 INTEGER,
	lb_type_groupe2_l3 CHARACTER VARYING(38),
	lb_abrev_g2_an CHARACTER VARYING(10),
	lb_abrev_g2_nn CHARACTER VARYING(10),
	lb_groupe2 CHARACTER VARYING(38),
	id_type_groupe3_l3 INTEGER,
	lb_type_groupe3_l3 CHARACTER VARYING(38),
	lb_abrev_g3_an CHARACTER VARYING(10),
	lb_abrev_g3_nn CHARACTER VARYING(10),
	lb_groupe3 CHARACTER VARYING(38),
	lb_descr_an_groupe1 CHARACTER VARYING(10),
	lb_descr_nn_groupe1 CHARACTER VARYING(10),
	lb_mot_dir_groupe1 CHARACTER VARYING(38),
	lb_descr_an_groupe2 CHARACTER VARYING(10),
	lb_descr_nn_groupe2 CHARACTER VARYING(10),
	lb_mot_dir_groupe2 CHARACTER VARYING(38),
	lb_descr_an_groupe3 CHARACTER VARYING(10),
	lb_descr_nn_groupe3 CHARACTER VARYING(10),
	lb_mot_dir_groupe3 CHARACTER VARYING(38)
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh)
ALTER TABLE ran.l3_ra34 SET (
  autovacuum_enabled = false
);
--TODO : de même sur l3_ra34 ?

-- On créé la table si elle n'existe pas
CREATE TABLE IF NOT EXISTS ran.l3_histo
(
	co_cea CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL,
	fl_diffusable BOOLEAN NOT NULL,
	lb_standard_nn CHARACTER VARYING(38) NOT NULL,
	id_type_groupe1_l3 INTEGER,
	lb_type_groupe1_l3 CHARACTER VARYING(38),
	lb_abrev_g1_an CHARACTER VARYING(10),
	lb_abrev_g1_nn CHARACTER VARYING(10),
	lb_groupe1 CHARACTER VARYING(38),
	id_type_groupe2_l3 INTEGER,
	lb_type_groupe2_l3 CHARACTER VARYING(38),
	lb_abrev_g2_an CHARACTER VARYING(10),
	lb_abrev_g2_nn CHARACTER VARYING(10),
	lb_groupe2 CHARACTER VARYING(38),
	id_type_groupe3_l3 INTEGER,
	lb_type_groupe3_l3 CHARACTER VARYING(38),
	lb_abrev_g3_an CHARACTER VARYING(10),
	lb_abrev_g3_nn CHARACTER VARYING(10),
	lb_groupe3 CHARACTER VARYING(38),
	lb_descr_an_groupe1 CHARACTER VARYING(10),
	lb_descr_nn_groupe1 CHARACTER VARYING(10),
	lb_mot_dir_groupe1 CHARACTER VARYING(38),
	lb_descr_an_groupe2 CHARACTER VARYING(10),
	lb_descr_nn_groupe2 CHARACTER VARYING(10),
	lb_mot_dir_groupe2 CHARACTER VARYING(38),
	lb_descr_an_groupe3 CHARACTER VARYING(10),
	lb_descr_nn_groupe3 CHARACTER VARYING(10),
	lb_mot_dir_groupe3 CHARACTER VARYING(38)
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh), on préfère désactiver l'autovacuum de façon à ce qu'il n'occupe pas de ressource inutilement (exemple : lancement pendant une opération intermédiaire au vacuum manuel)
ALTER TABLE ran.l3_histo SET (
  autovacuum_enabled = false
);

/*
COMMENT ON TABLE ran.l3_ra34 IS 'Adresses ligne 3';
COMMENT ON COLUMN ran.l3_ra34.co_cea IS 'CEA de l''adresse ligne 3';
COMMENT ON COLUMN ran.l3_ra34.id_type_groupe1_l3 IS 'Type groupe 1';
COMMENT ON COLUMN ran.l3_ra34.lb_type_groupe1_l3 IS 'Libellé type groupe 1';
COMMENT ON COLUMN ran.l3_ra34.lb_abrev_g1_an IS 'Libellé type groupe 1 abrégé AN';
COMMENT ON COLUMN ran.l3_ra34.lb_abrev_g1_nn IS 'Libellé type groupe 1 abrégé NN';
COMMENT ON COLUMN ran.l3_ra34.lb_groupe1 IS 'Libellé groupe 1';
COMMENT ON COLUMN ran.l3_ra34.id_type_groupe2_l3 IS 'Type groupe 2';
COMMENT ON COLUMN ran.l3_ra34.lb_type_groupe2_l3 IS 'Libellé type groupe 2';
COMMENT ON COLUMN ran.l3_ra34.lb_abrev_g2_an IS 'Libellé type groupe 2 abrégé AN';
COMMENT ON COLUMN ran.l3_ra34.lb_abrev_g2_nn IS 'Libellé type groupe 2 abrégé NN';
COMMENT ON COLUMN ran.l3_ra34.lb_groupe2 IS 'Libellé groupe 2';
COMMENT ON COLUMN ran.l3_ra34.id_type_groupe3_l3 IS 'Type groupe 3';
COMMENT ON COLUMN ran.l3_ra34.lb_type_groupe3_l3 IS 'Libellé type groupe 3';
COMMENT ON COLUMN ran.l3_ra34.lb_abrev_g3_an IS 'Libellé type groupe 3 abrégé AN';
COMMENT ON COLUMN ran.l3_ra34.lb_abrev_g3_nn IS 'Libellé type groupe 3 abrégé NN';
COMMENT ON COLUMN ran.l3_ra34.lb_groupe3 IS 'Libellé groupe 3';
COMMENT ON COLUMN ran.l3_ra34.lb_descr_an_groupe1 IS 'Libellé descripteur groupe 1 AN';
COMMENT ON COLUMN ran.l3_ra34.lb_descr_nn_groupe1 IS 'Libellé descripteur groupe 1 NN';
COMMENT ON COLUMN ran.l3_ra34.lb_mot_dir_groupe1 IS 'Libellé mot directeur groupe 1 NN';
COMMENT ON COLUMN ran.l3_ra34.lb_descr_an_groupe2 IS 'Libellé descripteur groupe 2 AN';
COMMENT ON COLUMN ran.l3_ra34.lb_descr_nn_groupe2 IS 'Libellé descripteur groupe 2 NN';
COMMENT ON COLUMN ran.l3_ra34.lb_mot_dir_groupe2 IS 'Libellé mot directeur groupe 2 NN';
COMMENT ON COLUMN ran.l3_ra34.lb_descr_an_groupe3 IS 'Libellé descripteur groupe 3 AN';
COMMENT ON COLUMN ran.l3_ra34.lb_descr_nn_groupe3 IS 'Libellé descripteur groupe 3 NN';
COMMENT ON COLUMN ran.l3_ra34.lb_mot_dir_groupe3 IS 'Libellé mot directeur groupe 3 NN';
COMMENT ON COLUMN ran.l3_ra34.fl_zone IS 'Type zone
Ce flag est mis à ''O'' si le libellé du type de groupe commence par ''PARC'' ou par ''ZONE''';
COMMENT ON COLUMN ran.l3_ra34.lb_standard_an IS 'Libellé normalisé AN';
COMMENT ON COLUMN ran.l3_ra34.lb_standard_nn IS 'Libellé normalisé NN';
COMMENT ON COLUMN ran.l3_ra34.fl_etat_adresse IS 'Etat de l''adresse
1 = actif ; 0 = inactif';
COMMENT ON COLUMN ran.l3_ra34.fl_diffusable IS 'Etat diffusable
1 = diffusable, 0 = non diffusable';
*/

-- Fonction remplacée par un appel à import_file
SELECT drop_all_functions_if_exists('ran','setL3Ra34FromRa34');

/*
TEST

Sur le serveur (REC) :
#extraction fichier ran
cd /data/bcaa/common_env/import/ran/
mkdir test
tar -C ./test/ -xzf raataaaa.bm_2017-09-30.tar.gz
#filtre département
head -1 ra34aaaa.bm > ra34aaaa.bm.tmp
grep '^33' ra34aaaa.bm >> ra34aaaa.bm.tmp
rm ra34aaaa.bm
mv ra34aaaa.bm.tmp ra34aaaa.bm
#chargement
SELECT ran.setL3Ra34FromRa34('/data/bcaa/common_env/import/ran/test/');
*/

SELECT drop_all_functions_if_exists('ran','getL3FromL3Ra34');
CREATE OR REPLACE FUNCTION ran.getL3FromL3Ra34(l3_ra34 IN ran.l3_ra34)
  RETURNS ran.l3 AS
$func$
DECLARE
	v_l3 ran.l3%ROWTYPE;
BEGIN
	v_l3.co_cea := l3_ra34.co_cea;
	v_l3.fl_active := l3_ra34.fl_etat_adresse::INTEGER::BOOLEAN;
	v_l3.fl_diffusable := l3_ra34.fl_diffusable::INTEGER::BOOLEAN;
	v_l3.lb_standard_nn := l3_ra34.lb_standard_nn;
	v_l3.id_type_groupe1_l3 := l3_ra34.id_type_groupe1_l3;
	v_l3.lb_type_groupe1_l3 := l3_ra34.lb_type_groupe1_l3;
	v_l3.lb_abrev_g1_an := l3_ra34.lb_abrev_g1_an;
	v_l3.lb_abrev_g1_nn := l3_ra34.lb_abrev_g1_nn;
	v_l3.lb_groupe1 := l3_ra34.lb_groupe1;
	v_l3.id_type_groupe2_l3 := l3_ra34.id_type_groupe2_l3;
	v_l3.lb_type_groupe2_l3 := l3_ra34.lb_type_groupe2_l3;
	v_l3.lb_abrev_g2_an := l3_ra34.lb_abrev_g2_an;
	v_l3.lb_abrev_g2_nn := l3_ra34.lb_abrev_g2_nn;
	v_l3.lb_groupe2 := l3_ra34.lb_groupe2;
	v_l3.id_type_groupe3_l3 := l3_ra34.id_type_groupe3_l3;
	v_l3.lb_type_groupe3_l3 := l3_ra34.lb_type_groupe3_l3;
	v_l3.lb_abrev_g3_an := l3_ra34.lb_abrev_g3_an;
	v_l3.lb_abrev_g3_nn := l3_ra34.lb_abrev_g3_nn;
	v_l3.lb_groupe3 := l3_ra34.lb_groupe3;
	v_l3.lb_descr_an_groupe1 := l3_ra34.lb_descr_an_groupe1;
	v_l3.lb_descr_nn_groupe1 := l3_ra34.lb_descr_nn_groupe1;
	v_l3.lb_mot_dir_groupe1 := l3_ra34.lb_mot_dir_groupe1;
	v_l3.lb_descr_an_groupe2 := l3_ra34.lb_descr_an_groupe2;
	v_l3.lb_descr_nn_groupe2 := l3_ra34.lb_descr_nn_groupe2;
	v_l3.lb_mot_dir_groupe2 := l3_ra34.lb_mot_dir_groupe2;
	v_l3.lb_descr_an_groupe3 := l3_ra34.lb_descr_an_groupe3;
	v_l3.lb_descr_nn_groupe3 := l3_ra34.lb_descr_nn_groupe3;
	v_l3.lb_mot_dir_groupe3 := l3_ra34.lb_mot_dir_groupe3;
	RETURN v_l3;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT l3.*
FROM ran.l3_ra34
CROSS JOIN ran.getL3FromL3Ra34(l3_ra34) AS l3
*/

SELECT drop_all_functions_if_exists('ran','getL3DeltaFromRa34');
CREATE OR REPLACE FUNCTION ran.getL3DeltaFromRa34(in_dt_reference DATE)
  RETURNS SETOF ran.l3 AS
$func$
DECLARE
BEGIN
	RETURN QUERY
		SELECT 
			l3.co_cea
			,in_dt_reference AS dt_reference
			,CASE 
				WHEN l3.fl_active = FALSE THEN 'S' 
				WHEN l3_avant.co_cea IS NULL OR l3_avant.co_mouvement = 'S' THEN 'C' /*recréation*/ 
				ELSE 'M' 
			END::CHAR(1) AS co_mouvement
			,l3.fl_active
			,l3.fl_diffusable
			,l3.lb_standard_nn
			,l3.id_type_groupe1_l3
			,l3.lb_type_groupe1_l3
			,l3.lb_abrev_g1_an
			,l3.lb_abrev_g1_nn
			,l3.lb_groupe1
			,l3.id_type_groupe2_l3
			,l3.lb_type_groupe2_l3
			,l3.lb_abrev_g2_an
			,l3.lb_abrev_g2_nn
			,l3.lb_groupe2
			,l3.id_type_groupe3_l3
			,l3.lb_type_groupe3_l3
			,l3.lb_abrev_g3_an
			,l3.lb_abrev_g3_nn
			,l3.lb_groupe3
			,l3.lb_descr_an_groupe1
			,l3.lb_descr_nn_groupe1
			,l3.lb_mot_dir_groupe1
			,l3.lb_descr_an_groupe2
			,l3.lb_descr_nn_groupe2
			,l3.lb_mot_dir_groupe2
			,l3.lb_descr_an_groupe3
			,l3.lb_descr_nn_groupe3
			,l3.lb_mot_dir_groupe3
		FROM ran.l3_ra34
		LEFT OUTER JOIN ran.l3 AS l3_avant ON l3_avant.co_cea = l3_ra34.co_cea
		/* Alternative intéressante en cas d'abandon de l'état actuel au profit de l'historique seul :
		LEFT OUTER JOIN ran.l3_histo AS l3_avant 
		ON l3_avant.co_cea = l3.co_cea 
		AND l3_avant.dt_reference = (
			SELECT l3_a_date.dt_reference
			FROM ran.l3_histo AS l3_a_date
			WHERE l3_a_date.co_cea = l3_avant.co_cea
			AND l3_a_date.dt_reference < in_dt_reference
			ORDER BY l3_a_date.dt_reference DESC
			LIMIT 1
		)
		*/
		CROSS JOIN ran.getL3FromL3Ra34(l3_ra34) AS l3
		WHERE 
		(
			--élément qui n'existait pas jusqu'ici
			l3_avant.co_cea IS NULL
			--OU élément qui a changé
			OR l3_avant.fl_active != l3.fl_active
			OR l3_avant.fl_diffusable != l3.fl_diffusable
			OR l3_avant.lb_standard_nn != l3.lb_standard_nn
		)
		;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM ran.getL3DeltaFromRa34(NOW()::DATE)

REC(33) : 10s
*/

SELECT drop_all_functions_if_exists('ran','setL3FromRa34');
CREATE OR REPLACE FUNCTION ran.setL3FromRa34(in_dt_reference IN DATE, in_en_mode_init BOOLEAN DEFAULT TRUE, in_avec_historique BOOLEAN DEFAULT TRUE)
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	IF in_en_mode_init = TRUE THEN
		--Initialisation des l3s
		TRUNCATE TABLE ran.l3;
		PERFORM public.drop_table_indexes('ran', 'l3');
		INSERT INTO ran.l3(
			co_cea
			,dt_reference
			,co_mouvement
			,fl_active
			,fl_diffusable
			,lb_standard_nn
			,id_type_groupe1_l3
			,lb_type_groupe1_l3
			,lb_abrev_g1_an
			,lb_abrev_g1_nn
			,lb_groupe1
			,id_type_groupe2_l3
			,lb_type_groupe2_l3
			,lb_abrev_g2_an
			,lb_abrev_g2_nn
			,lb_groupe2
			,id_type_groupe3_l3
			,lb_type_groupe3_l3
			,lb_abrev_g3_an
			,lb_abrev_g3_nn
			,lb_groupe3
			,lb_descr_an_groupe1
			,lb_descr_nn_groupe1
			,lb_mot_dir_groupe1
			,lb_descr_an_groupe2
			,lb_descr_nn_groupe2
			,lb_mot_dir_groupe2
			,lb_descr_an_groupe3
			,lb_descr_nn_groupe3
			,lb_mot_dir_groupe3
		)
		(
			SELECT 
				l3.co_cea
				,in_dt_reference AS dt_reference
				,CASE 
					WHEN l3.fl_active = FALSE THEN 'S' 
					ELSE 'I'  --INIT
				END AS co_mouvement
				,l3.fl_active
				,l3.fl_diffusable
				,l3.lb_standard_nn
				,l3.id_type_groupe1_l3
				,l3.lb_type_groupe1_l3
				,l3.lb_abrev_g1_an
				,l3.lb_abrev_g1_nn
				,l3.lb_groupe1
				,l3.id_type_groupe2_l3
				,l3.lb_type_groupe2_l3
				,l3.lb_abrev_g2_an
				,l3.lb_abrev_g2_nn
				,l3.lb_groupe2
				,l3.id_type_groupe3_l3
				,l3.lb_type_groupe3_l3
				,l3.lb_abrev_g3_an
				,l3.lb_abrev_g3_nn
				,l3.lb_groupe3
				,l3.lb_descr_an_groupe1
				,l3.lb_descr_nn_groupe1
				,l3.lb_mot_dir_groupe1
				,l3.lb_descr_an_groupe2
				,l3.lb_descr_nn_groupe2
				,l3.lb_mot_dir_groupe2
				,l3.lb_descr_an_groupe3
				,l3.lb_descr_nn_groupe3
				,l3.lb_mot_dir_groupe3
			FROM ran.l3_ra34
			CROSS JOIN ran.getL3FromL3Ra34(l3_ra34) AS l3
		);
		IF in_avec_historique = TRUE THEN
			--Initialisation de l'historique
			TRUNCATE TABLE ran.l3_histo;
			PERFORM public.drop_table_indexes('ran', 'l3_histo');
			INSERT INTO ran.l3_histo(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,lb_standard_nn
				,id_type_groupe1_l3
				,lb_type_groupe1_l3
				,lb_abrev_g1_an
				,lb_abrev_g1_nn
				,lb_groupe1
				,id_type_groupe2_l3
				,lb_type_groupe2_l3
				,lb_abrev_g2_an
				,lb_abrev_g2_nn
				,lb_groupe2
				,id_type_groupe3_l3
				,lb_type_groupe3_l3
				,lb_abrev_g3_an
				,lb_abrev_g3_nn
				,lb_groupe3
				,lb_descr_an_groupe1
				,lb_descr_nn_groupe1
				,lb_mot_dir_groupe1
				,lb_descr_an_groupe2
				,lb_descr_nn_groupe2
				,lb_mot_dir_groupe2
				,lb_descr_an_groupe3
				,lb_descr_nn_groupe3
				,lb_mot_dir_groupe3
			)
			(
				SELECT 
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,lb_standard_nn
					,id_type_groupe1_l3
					,lb_type_groupe1_l3
					,lb_abrev_g1_an
					,lb_abrev_g1_nn
					,lb_groupe1
					,id_type_groupe2_l3
					,lb_type_groupe2_l3
					,lb_abrev_g2_an
					,lb_abrev_g2_nn
					,lb_groupe2
					,id_type_groupe3_l3
					,lb_type_groupe3_l3
					,lb_abrev_g3_an
					,lb_abrev_g3_nn
					,lb_groupe3
					,lb_descr_an_groupe1
					,lb_descr_nn_groupe1
					,lb_mot_dir_groupe1
					,lb_descr_an_groupe2
					,lb_descr_nn_groupe2
					,lb_mot_dir_groupe2
					,lb_descr_an_groupe3
					,lb_descr_nn_groupe3
					,lb_mot_dir_groupe3
				FROM ran.l3
			)
			;
		END IF;
	ELSE
		IF in_avec_historique = TRUE THEN
			--Calcul du DELTA dans une table temporaire, pour le réutiliser à la fois pour mettre à jour les l3s et l'historique
			DROP TABLE IF EXISTS tmp_ran_l3_ra34_delta;
			CREATE TEMPORARY TABLE tmp_ran_l3_ra34_delta AS TABLE ran.l3 WITH NO DATA;
			INSERT INTO tmp_ran_l3_ra34_delta
			(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,lb_standard_nn
				,id_type_groupe1_l3
				,lb_type_groupe1_l3
				,lb_abrev_g1_an
				,lb_abrev_g1_nn
				,lb_groupe1
				,id_type_groupe2_l3
				,lb_type_groupe2_l3
				,lb_abrev_g2_an
				,lb_abrev_g2_nn
				,lb_groupe2
				,id_type_groupe3_l3
				,lb_type_groupe3_l3
				,lb_abrev_g3_an
				,lb_abrev_g3_nn
				,lb_groupe3
				,lb_descr_an_groupe1
				,lb_descr_nn_groupe1
				,lb_mot_dir_groupe1
				,lb_descr_an_groupe2
				,lb_descr_nn_groupe2
				,lb_mot_dir_groupe2
				,lb_descr_an_groupe3
				,lb_descr_nn_groupe3
				,lb_mot_dir_groupe3
			)
			(
				SELECT
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,lb_standard_nn
					,id_type_groupe1_l3
					,lb_type_groupe1_l3
					,lb_abrev_g1_an
					,lb_abrev_g1_nn
					,lb_groupe1
					,id_type_groupe2_l3
					,lb_type_groupe2_l3
					,lb_abrev_g2_an
					,lb_abrev_g2_nn
					,lb_groupe2
					,id_type_groupe3_l3
					,lb_type_groupe3_l3
					,lb_abrev_g3_an
					,lb_abrev_g3_nn
					,lb_groupe3
					,lb_descr_an_groupe1
					,lb_descr_nn_groupe1
					,lb_mot_dir_groupe1
					,lb_descr_an_groupe2
					,lb_descr_nn_groupe2
					,lb_mot_dir_groupe2
					,lb_descr_an_groupe3
					,lb_descr_nn_groupe3
					,lb_mot_dir_groupe3
				FROM ran.getL3DeltaFromRa34(in_dt_reference)
			);
			
			--Mise à jour des l3s
			INSERT INTO ran.l3 (SELECT * FROM tmp_ran_l3_ra34_delta)
			ON CONFLICT(co_cea)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active = EXCLUDED.fl_active
					,fl_diffusable = EXCLUDED.fl_diffusable
					,lb_standard_nn = EXCLUDED.lb_standard_nn
					,id_type_groupe1_l3 = EXCLUDED.id_type_groupe1_l3
					,lb_type_groupe1_l3 =  EXCLUDED.lb_type_groupe1_l3
					,lb_abrev_g1_an =  EXCLUDED.lb_abrev_g1_an
					,lb_abrev_g1_nn =  EXCLUDED.lb_abrev_g1_nn
					,lb_groupe1 =  EXCLUDED.lb_groupe1
					,id_type_groupe2_l3 =  EXCLUDED.id_type_groupe2_l3
					,lb_type_groupe2_l3 =  EXCLUDED.lb_type_groupe2_l3
					,lb_abrev_g2_an =  EXCLUDED.lb_abrev_g2_an
					,lb_abrev_g2_nn =  EXCLUDED.lb_abrev_g2_nn
					,lb_groupe2 =  EXCLUDED.lb_groupe2
					,id_type_groupe3_l3 =  EXCLUDED.id_type_groupe3_l3
					,lb_type_groupe3_l3 =  EXCLUDED.lb_type_groupe3_l3
					,lb_abrev_g3_an =  EXCLUDED.lb_abrev_g3_an
					,lb_abrev_g3_nn =  EXCLUDED.lb_abrev_g3_nn
					,lb_groupe3 =  EXCLUDED.lb_groupe3
					,lb_descr_an_groupe1 =  EXCLUDED.lb_descr_an_groupe1
					,lb_descr_nn_groupe1 =  EXCLUDED.lb_descr_nn_groupe1
					,lb_mot_dir_groupe1 =  EXCLUDED.lb_mot_dir_groupe1
					,lb_descr_an_groupe2 =  EXCLUDED.lb_descr_an_groupe2
					,lb_descr_nn_groupe2 =  EXCLUDED.lb_descr_nn_groupe2
					,lb_mot_dir_groupe2 =  EXCLUDED.lb_mot_dir_groupe2
					,lb_descr_an_groupe3 =  EXCLUDED.lb_descr_an_groupe3
					,lb_descr_nn_groupe3 =  EXCLUDED.lb_descr_nn_groupe3
					,lb_mot_dir_groupe3 =  EXCLUDED.lb_mot_dir_groupe3
			;
				
			--Mise à jour de l'historique
			INSERT INTO ran.l3_histo(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,lb_standard_nn
				,id_type_groupe1_l3
				,lb_type_groupe1_l3
				,lb_abrev_g1_an
				,lb_abrev_g1_nn
				,lb_groupe1
				,id_type_groupe2_l3
				,lb_type_groupe2_l3
				,lb_abrev_g2_an
				,lb_abrev_g2_nn
				,lb_groupe2
				,id_type_groupe3_l3
				,lb_type_groupe3_l3
				,lb_abrev_g3_an
				,lb_abrev_g3_nn
				,lb_groupe3
				,lb_descr_an_groupe1
				,lb_descr_nn_groupe1
				,lb_mot_dir_groupe1
				,lb_descr_an_groupe2
				,lb_descr_nn_groupe2
				,lb_mot_dir_groupe2
				,lb_descr_an_groupe3
				,lb_descr_nn_groupe3
				,lb_mot_dir_groupe3
			)
			(
				SELECT
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,lb_standard_nn
					,id_type_groupe1_l3
					,lb_type_groupe1_l3
					,lb_abrev_g1_an
					,lb_abrev_g1_nn
					,lb_groupe1
					,id_type_groupe2_l3
					,lb_type_groupe2_l3
					,lb_abrev_g2_an
					,lb_abrev_g2_nn
					,lb_groupe2
					,id_type_groupe3_l3
					,lb_type_groupe3_l3
					,lb_abrev_g3_an
					,lb_abrev_g3_nn
					,lb_groupe3
					,lb_descr_an_groupe1
					,lb_descr_nn_groupe1
					,lb_mot_dir_groupe1
					,lb_descr_an_groupe2
					,lb_descr_nn_groupe2
					,lb_mot_dir_groupe2
					,lb_descr_an_groupe3
					,lb_descr_nn_groupe3
					,lb_mot_dir_groupe3
				FROM tmp_ran_l3_ra34_delta
			)
			;
		ELSE
			INSERT INTO ran.l3(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,lb_standard_nn
				,id_type_groupe1_l3
				,lb_type_groupe1_l3
				,lb_abrev_g1_an
				,lb_abrev_g1_nn
				,lb_groupe1
				,id_type_groupe2_l3
				,lb_type_groupe2_l3
				,lb_abrev_g2_an
				,lb_abrev_g2_nn
				,lb_groupe2
				,id_type_groupe3_l3
				,lb_type_groupe3_l3
				,lb_abrev_g3_an
				,lb_abrev_g3_nn
				,lb_groupe3
				,lb_descr_an_groupe1
				,lb_descr_nn_groupe1
				,lb_mot_dir_groupe1
				,lb_descr_an_groupe2
				,lb_descr_nn_groupe2
				,lb_mot_dir_groupe2
				,lb_descr_an_groupe3
				,lb_descr_nn_groupe3
				,lb_mot_dir_groupe3
			)
			(
				SELECT
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,lb_standard_nn
					,id_type_groupe1_l3
					,lb_type_groupe1_l3
					,lb_abrev_g1_an
					,lb_abrev_g1_nn
					,lb_groupe1
					,id_type_groupe2_l3
					,lb_type_groupe2_l3
					,lb_abrev_g2_an
					,lb_abrev_g2_nn
					,lb_groupe2
					,id_type_groupe3_l3
					,lb_type_groupe3_l3
					,lb_abrev_g3_an
					,lb_abrev_g3_nn
					,lb_groupe3
					,lb_descr_an_groupe1
					,lb_descr_nn_groupe1
					,lb_mot_dir_groupe1
					,lb_descr_an_groupe2
					,lb_descr_nn_groupe2
					,lb_mot_dir_groupe2
					,lb_descr_an_groupe3
					,lb_descr_nn_groupe3
					,lb_mot_dir_groupe3
				FROM ran.getL3DeltaFromRa34(in_dt_reference)
			)
			ON CONFLICT(co_cea)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active = EXCLUDED.fl_active
					,fl_diffusable = EXCLUDED.fl_diffusable
					,lb_standard_nn = EXCLUDED.lb_standard_nn
					,id_type_groupe1_l3 = EXCLUDED.id_type_groupe1_l3
					,lb_type_groupe1_l3 =  EXCLUDED.lb_type_groupe1_l3
					,lb_abrev_g1_an =  EXCLUDED.lb_abrev_g1_an
					,lb_abrev_g1_nn =  EXCLUDED.lb_abrev_g1_nn
					,lb_groupe1 =  EXCLUDED.lb_groupe1
					,id_type_groupe2_l3 =  EXCLUDED.id_type_groupe2_l3
					,lb_type_groupe2_l3 =  EXCLUDED.lb_type_groupe2_l3
					,lb_abrev_g2_an =  EXCLUDED.lb_abrev_g2_an
					,lb_abrev_g2_nn =  EXCLUDED.lb_abrev_g2_nn
					,lb_groupe2 =  EXCLUDED.lb_groupe2
					,id_type_groupe3_l3 =  EXCLUDED.id_type_groupe3_l3
					,lb_type_groupe3_l3 =  EXCLUDED.lb_type_groupe3_l3
					,lb_abrev_g3_an =  EXCLUDED.lb_abrev_g3_an
					,lb_abrev_g3_nn =  EXCLUDED.lb_abrev_g3_nn
					,lb_groupe3 =  EXCLUDED.lb_groupe3
					,lb_descr_an_groupe1 =  EXCLUDED.lb_descr_an_groupe1
					,lb_descr_nn_groupe1 =  EXCLUDED.lb_descr_nn_groupe1
					,lb_mot_dir_groupe1 =  EXCLUDED.lb_mot_dir_groupe1
					,lb_descr_an_groupe2 =  EXCLUDED.lb_descr_an_groupe2
					,lb_descr_nn_groupe2 =  EXCLUDED.lb_descr_nn_groupe2
					,lb_mot_dir_groupe2 =  EXCLUDED.lb_mot_dir_groupe2
					,lb_descr_an_groupe3 =  EXCLUDED.lb_descr_an_groupe3
					,lb_descr_nn_groupe3 =  EXCLUDED.lb_descr_nn_groupe3
					,lb_mot_dir_groupe3 =  EXCLUDED.lb_mot_dir_groupe3
			;
		END IF;
	END IF;
	
	-- Création des indexes pour accélérer l'accès au données
	-- Controle d'unicité, et optimisation de filtre sur l'identifiant de la LIGNE 3
	CREATE UNIQUE INDEX IF NOT EXISTS idx_l3_co_cea ON ran.l3 (co_cea);
	-- Optimisation de filtre un libellé ligne3 nouvelle norme ressemblant à
	CREATE INDEX IF NOT EXISTS idx_l3_lb_standard_nn ON ran.l3  USING gin(lb_standard_nn gin_trgm_ops);

	CREATE UNIQUE INDEX IF NOT EXISTS idx_l3_histo_key ON ran.l3_histo (co_cea, dt_reference);

	TRUNCATE TABLE ran.l3_ra34;
	
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST

SELECT ran.setL3FromRa34('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, TRUE, TRUE);

SELECT ran.setL3FromRa34('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, FALSE, FALSE);

*/
