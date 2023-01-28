DO $$
DECLARE
BEGIN
	IF table_exists('ran','numero') = FALSE THEN
		DROP TABLE IF EXISTS ran.numero_ra33 CASCADE;
		DROP TABLE IF EXISTS ran.numero_ra33_histo CASCADE;
	END IF;
END $$;

--Table contenant les données du fichiers RAN RA33
CREATE TABLE IF NOT EXISTS ran.numero_ra33(
	co_cea CHAR(10) NOT NULL,
	no_voie INTEGER NOT NULL,
	lb_ext CHARACTER VARYING(10) NULL,
	lb_abr_an CHARACTER VARYING(1) NULL,
	lb_abr_nn CHARACTER VARYING(1) NULL,
	fl_etat INTEGER NOT NULL,
	fl_diffusable INTEGER NOT NULL
)
WITH (
  OIDS=FALSE
);

--Table contenant les données révisées du fichiers RAN RA33
CREATE TABLE IF NOT EXISTS ran.numero
(
	co_cea CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL, 
	fl_diffusable BOOLEAN NOT NULL,
	no_voie INTEGER NOT NULL,
	lb_ext CHARACTER VARYING(10) NULL,
	lb_abr_nn CHARACTER VARYING(1) NULL --FIXME : renommer lb_abr ?
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh)
ALTER TABLE ran.numero_ra33 SET (
  autovacuum_enabled = false
);
--TODO : de même sur numero_ra33 ?

-- On créé la table si elle n'existe pas
CREATE TABLE IF NOT EXISTS ran.numero_histo
(
	co_cea CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL, 
	fl_diffusable BOOLEAN NOT NULL,
	no_voie INTEGER NOT NULL,
	lb_ext CHARACTER VARYING(10) NULL,
	lb_abr_nn CHARACTER VARYING(1) NULL
)
WITH (
  OIDS=FALSE
);

--VACUUM géré manuellement (cf. ran/import.sh), on préfère désactiver l'autovacuum de façon à ce qu'il n'occupe pas de ressource inutilement (exemple : lancement pendant une opération intermédiaire au vacuum manuel)
ALTER TABLE ran.numero_histo SET (
  autovacuum_enabled = false
);

/*
COMMENT ON TABLE ran.numero_ra33 IS 'Adresses numéro';
COMMENT ON COLUMN ran.numero_ra33.co_cea IS 'CEA de l''adresse numéro';
COMMENT ON COLUMN ran.numero_ra33.no_voie IS 'Numéro dans la numero';
COMMENT ON COLUMN ran.numero_ra33.lb_ext IS 'Libellé extension longue
type BIS, TER, ...';
COMMENT ON COLUMN ran.numero_ra33.lb_abr_an IS 'Libellé extension abrégée AN
type B, T, ... ou A, B, ..., Z';
COMMENT ON COLUMN ran.numero_ra33.lb_abr_nn IS 'Libellé extension abrégée NN
type B, T, ... ou A, B, ..., Z';
COMMENT ON COLUMN ran.numero_ra33.fl_etat IS 'Etat de l''adresse
1 = actif ; 0 = inactif';
COMMENT ON COLUMN ran.numero_ra33.fl_diffusable IS 'Etat diffusable
1 = diffusable, 0 = non diffusable';
*/

-- Fonction remplacée par un appel à import_file
SELECT drop_all_functions_if_exists('ran','setNumeroRa33FromRa33');

/*
TEST

Sur le serveur (REC) :
#extraction fichier ran
cd /data/bcaa/common_env/import/ran/
mkdir test
tar -C ./test/ -xzf raataaaa.bm_2017-09-30.tar.gz
#filtre département
head -1 ra33aaaa.bm > ra33aaaa.bm.tmp
grep '^33' ra33aaaa.bm >> ra33aaaa.bm.tmp
rm ra33aaaa.bm
mv ra33aaaa.bm.tmp ra33aaaa.bm
#chargement
*/

SELECT drop_all_functions_if_exists('ran','getNumeroFromNumeroRa33');
CREATE OR REPLACE FUNCTION ran.getNumeroFromNumeroRa33(numero_ra33 IN ran.numero_ra33)
  RETURNS ran.numero AS
$func$
DECLARE
	v_numero ran.numero%ROWTYPE;
BEGIN
	v_numero.co_cea := numero_ra33.co_cea;
	v_numero.fl_active := numero_ra33.fl_etat::INTEGER::BOOLEAN;
	v_numero.fl_diffusable := numero_ra33.fl_diffusable::INTEGER::BOOLEAN;
	v_numero.no_voie := numero_ra33.no_voie;
	v_numero.lb_ext := numero_ra33.lb_ext;
	v_numero.lb_abr_nn := numero_ra33.lb_abr_nn;
	RETURN v_numero;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT numero.*
FROM ran.numero_ra33
CROSS JOIN ran.getNumeroFromNumeroRa33(numero_ra33) AS numero
*/

SELECT drop_all_functions_if_exists('ran','getNumeroDeltaFromRa33');
CREATE OR REPLACE FUNCTION ran.getNumeroDeltaFromRa33(in_dt_reference DATE)
  RETURNS SETOF ran.numero AS
$func$
DECLARE
BEGIN
	RETURN QUERY
		SELECT 
			numero.co_cea
			,in_dt_reference AS dt_reference
			,CASE 
				WHEN numero.fl_active = FALSE THEN 'S' 
				WHEN numero_avant.co_cea IS NULL OR numero_avant.co_mouvement = 'S' THEN 'C' /*recréation*/ 
				ELSE 'M' 
			END::CHAR(1) AS co_mouvement
			,numero.fl_active
			,numero.fl_diffusable
			,numero.no_voie
			,numero.lb_ext
			,numero.lb_abr_nn
		FROM ran.numero_ra33
		LEFT OUTER JOIN ran.numero AS numero_avant ON numero_avant.co_cea = numero_ra33.co_cea
		/* Alternative intéressante en cas d'abandon de l'état actuel au profit de l'historique seul :
		LEFT OUTER JOIN ran.numero_histo AS numero_avant 
		ON numero_avant.co_cea = numero.co_cea 
		AND numero_avant.dt_reference = (
			SELECT numero_a_date.dt_reference
			FROM ran.numero_histo AS numero_a_date
			WHERE numero_a_date.co_cea = numero_avant.co_cea
			AND numero_a_date.dt_reference < in_dt_reference
			ORDER BY numero_a_date.dt_reference DESC
			LIMIT 1
		)
		*/
		CROSS JOIN ran.getNumeroFromNumeroRa33(numero_ra33) AS numero
		WHERE 
		(
			--élément qui n'existait pas jusqu'ici
			numero_avant.co_cea IS NULL
			--OU élément qui a changé
			OR numero_avant.fl_active != numero.fl_active
			OR numero_avant.fl_diffusable != numero.fl_diffusable
			OR numero_avant.no_voie != numero.no_voie
			OR COALESCE(numero_avant.lb_ext, 'NULL') != COALESCE(numero.lb_ext, 'NULL') --NULLABLE
			OR COALESCE(numero_avant.lb_abr_nn, 'NULL') != COALESCE(numero.lb_abr_nn, 'NULL') --NULLABLE
		)
		;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM ran.getNumeroDeltaFromRa33(NOW()::DATE)

REC(33) : 10s
*/

SELECT drop_all_functions_if_exists('ran','setNumeroFromRa33');
CREATE OR REPLACE FUNCTION ran.setNumeroFromRa33(in_dt_reference IN DATE, in_en_mode_init BOOLEAN DEFAULT TRUE, in_avec_historique BOOLEAN DEFAULT TRUE)
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	IF in_en_mode_init = TRUE THEN
		--Initialisation des numeros
		TRUNCATE TABLE ran.numero;
		PERFORM public.drop_table_indexes('ran', 'numero');
		INSERT INTO ran.numero(
			co_cea
			,dt_reference
			,co_mouvement
			,fl_active
			,fl_diffusable
			,no_voie
			,lb_ext
			,lb_abr_nn
		)
		(
			SELECT 
				numero.co_cea
				,in_dt_reference AS dt_reference
				,CASE 
					WHEN numero.fl_active = FALSE THEN 'S' 
					ELSE 'I'  --INIT
				END AS co_mouvement
				,numero.fl_active
				,numero.fl_diffusable
				,numero.no_voie
				,numero.lb_ext
				,numero.lb_abr_nn
			FROM ran.numero_ra33
			CROSS JOIN ran.getNumeroFromNumeroRa33(numero_ra33) AS numero
		);
		IF in_avec_historique = TRUE THEN
			--Initialisation de l'historique
			TRUNCATE TABLE ran.numero_histo;
			PERFORM public.drop_table_indexes('ran', 'numero_histo');
			INSERT INTO ran.numero_histo(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,no_voie
				,lb_ext
				,lb_abr_nn
			)
			(
				SELECT 
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,no_voie
					,lb_ext
					,lb_abr_nn
				FROM ran.numero
			)
			;
		END IF;
	ELSE
		IF in_avec_historique = TRUE THEN
			--Calcul du DELTA dans une table temporaire, pour le réutiliser à la fois pour mettre à jour les numeros et l'historique
			DROP TABLE IF EXISTS tmp_ran_numero_ra33_delta;
			CREATE TEMPORARY TABLE tmp_ran_numero_ra33_delta AS TABLE ran.numero WITH NO DATA;
			INSERT INTO tmp_ran_numero_ra33_delta
			(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,no_voie
				,lb_ext
				,lb_abr_nn
			)
			(
				SELECT
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,no_voie
					,lb_ext
					,lb_abr_nn
				FROM ran.getNumeroDeltaFromRa33(in_dt_reference)
			);
			
			--Mise à jour des numeros
			INSERT INTO ran.numero (SELECT * FROM tmp_ran_numero_ra33_delta)
			ON CONFLICT(co_cea)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active = EXCLUDED.fl_active
					,fl_diffusable = EXCLUDED.fl_diffusable
					,no_voie = EXCLUDED.no_voie
					,lb_ext = EXCLUDED.lb_ext
					,lb_abr_nn = EXCLUDED.lb_abr_nn
			;
				
			--Mise à jour de l'historique
			INSERT INTO ran.numero_histo(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,no_voie
				,lb_ext
				,lb_abr_nn
			)
			(
				SELECT
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,no_voie
					,lb_ext
					,lb_abr_nn
				FROM tmp_ran_numero_ra33_delta
			)
			;
		ELSE
			INSERT INTO ran.numero(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,no_voie
				,lb_ext
				,lb_abr_nn
			)
			(
				SELECT
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,no_voie
					,lb_ext
					,lb_abr_nn
				FROM ran.getNumeroDeltaFromRa33(in_dt_reference)
			)
			ON CONFLICT(co_cea)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active = EXCLUDED.fl_active
					,fl_diffusable = EXCLUDED.fl_diffusable
					,no_voie = EXCLUDED.no_voie
					,lb_ext = EXCLUDED.lb_ext
					,lb_abr_nn = EXCLUDED.lb_abr_nn
			;
		END IF;
	END IF;
	
	-- Création des indexes pour accélérer l'accès au données
	-- Controle d'unicité, et optimisation de filtre sur l'identifiant de la NUMERO
	CREATE UNIQUE INDEX IF NOT EXISTS idx_ran_numero_co_cea ON ran.numero (co_cea);
	-- TODO : autres indexes à créer sur numéro? libellé extensions? ...

	CREATE UNIQUE INDEX IF NOT EXISTS idx_numero_histo_key ON ran.numero_histo (co_cea, dt_reference);

	TRUNCATE TABLE ran.numero_ra33;
	
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST

SELECT ran.setNumeroFromRa33('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, TRUE, TRUE);

SELECT ran.setNumeroFromRa33('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, FALSE, FALSE);

*/
