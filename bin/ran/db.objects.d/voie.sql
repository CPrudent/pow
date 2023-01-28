-- data from RAN-RA41 file
CREATE TABLE IF NOT EXISTS ran.voie_ra41(
    co_cea CHAR(10),
    co_voie NUMERIC(8,0) NOT NULL,
    co_insee CHARACTER VARYING(5) NOT NULL,
    lb_voie CHARACTER VARYING(60) NOT NULL,
    lb_voie_an CHARACTER VARYING(27),
    lb_voie_nn CHARACTER VARYING(32),
    lb_abr_an CHARACTER VARYING(4) NULL,
    lb_abr_nn CHARACTER VARYING(4) NULL,
    lb_desc_an CHARACTER VARYING(10) /*NOT*/ NULL,
    lb_desc_nn CHARACTER VARYING(10) /*NOT*/ NULL,
    lb_md CHARACTER VARYING(20) NULL,
    co_insee_anc CHARACTER VARYING(5),
    fl_etat NUMERIC(1,0) NOT NULL,
    fl_adr NUMERIC(1,0) NOT NULL,
    lb_in_ext_typ_voie CHARACTER VARYING(38) NULL,
    fl_diffusable NUMERIC(1,0) NOT NULL
)
;

DO $$
DECLARE
BEGIN
    -- error 25-01-2020
    PERFORM alter_column_drop_not_null('ran','voie_ra41','lb_desc_an');
    PERFORM alter_column_drop_not_null('ran','voie_ra41','lb_desc_nn');
END $$;


-- address-street with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.voie
(
    co_cea CHAR(10),
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    co_voie NUMERIC(8,0) NOT NULL,
    lb_voie CHARACTER VARYING(60) NOT NULL,
    lb_voie_normalise CHARACTER VARYING(32) NOT NULL,
    lb_type CHARACTER VARYING(38) NULL,
    lb_type_abrege CHARACTER VARYING(4) NULL,
    lb_md CHARACTER VARYING(20) NULL,
    lb_desc CHARACTER VARYING(10) NOT NULL,
    co_insee_commune CHAR(5) NOT NULL, --FIXME : anciennement nécessaire pour index mot directeur par commune
    co_cea_za CHAR(10) --NOTE : useful for index idx_voie_co_cea_za_lb_voie (trigrams)
)
;

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.voie SET (
    AUTOVACUUM_ENABLED = FALSE
);

DO $$
DECLARE
BEGIN
    IF column_exists('ran','voie','co_cea_za') = FALSE THEN
        ALTER TABLE ran.voie ADD COLUMN co_cea_za CHAR(10);

        UPDATE ran.voie SET co_cea_za = adresse.co_cea_za
        FROM ran.adresse
        WHERE adresse.co_cea_determinant = voie.co_cea;

        CREATE INDEX IF NOT EXISTS idx_voie_co_cea_za_lb_voie
            ON ran.voie USING GIST(co_cea_za, lb_voie gist_trgm_ops);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS ran.voie_histo
(
	co_cea CHAR(10),
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL, 
	fl_diffusable BOOLEAN NOT NULL,
	co_voie NUMERIC(8,0) NOT NULL, 
	lb_voie CHARACTER VARYING(60) NOT NULL,
	lb_voie_normalise CHARACTER VARYING(32) NOT NULL,
	lb_type CHARACTER VARYING(38) NULL,
	lb_type_abrege CHARACTER VARYING(4) NULL,
	lb_md CHARACTER VARYING(20) NULL, 
	lb_desc CHARACTER VARYING(10) NOT NULL
)
;

--VACUUM géré manuellement (cf. ran/import.sh), on préfère désactiver l'autovacuum de façon à ce qu'il n'occupe pas de ressource inutilement (exemple : lancement pendant une opération intermédiaire au vacuum manuel)
ALTER TABLE ran.voie_histo SET (
  autovacuum_enabled = false
);

COMMENT ON TABLE ran.voie_ra41 IS 'Adresses voie';
/* TODO : à actualiser
COMMENT ON COLUMN ran.voie_ra41.co_cea IS 'CEA de l''adresse voie';
COMMENT ON COLUMN ran.voie_ra41.co_voie IS 'Matricule de la voie';
COMMENT ON COLUMN ran.voie_ra41.co_insee IS 'Code INSEE';
COMMENT ON COLUMN ran.voie_ra41.lb_voie IS 'Libellé voie in extenso';
COMMENT ON COLUMN ran.voie_ra41.lb_voie_an IS 'Libellé normalisé NN
Ce champ n''est renseigné que si le libellé in extenso fait plus de 27 caractères';
COMMENT ON COLUMN ran.voie_ra41.lb_voie_an IS 'Libellé normalisé AN
Ce champ n''est renseigné que si le libellé in extenso fait plus de 32 caractères';
COMMENT ON COLUMN ran.voie_ra41.lb_in_ext_typ_voie IS 'Libellé type de voie in extenso
Ce champ n''est renseigné que s''il appartient à la liste des types de voies selon la norme de l''Adresse';
COMMENT ON COLUMN ran.voie_ra41.lb_abr_an IS 'Libellé type de voie abrégé NN
Ce champ n''est renseigné que s''il appartient à la liste des types de voies abrégeables selon la norme de l''Adresse.
Il est alors systématiquement renseigné même si le libellé normalisé AN ne l''a pas été';
COMMENT ON COLUMN ran.voie_ra41.lb_abr_nn IS 'Libellé type de voie abrégé AN
Ce champ n''est renseigné que s''il appartient à la liste des types de voies abrégeables selon la norme de l''Adresse.
Il est alors systématiquement renseigné même si le libellé normalisé NN ne l''a pas été';
COMMENT ON COLUMN ran.voie_ra41.lb_desc_an IS 'Libellé descripteur AN
Descripteurs du libellé in extenso de la voie (descripteur du type de voie+ descripteur du nom de voie)';
COMMENT ON COLUMN ran.voie_ra41.lb_desc_nn IS 'Libellé descripteur NN
Descripteurs du libellé in extenso de la voie (descripteur du type de voie + descripteur du nom de voie)';
COMMENT ON COLUMN ran.voie_ra41.lb_md IS 'Libellé mot directeur
Unique pour les 2 normes';
COMMENT ON COLUMN ran.voie_ra41.co_insee_anc IS 'Code INSEE (ancienne commune)
Facultatif, s''il y a un lien avec ancienne commune';
COMMENT ON COLUMN ran.voie_ra41.fl_etat IS 'Etat de la voie
1 = actif ; 0 = inactif (correspond à l''état de la voie ET NON à l''état de l''adresse ran.voie_ra41)';
COMMENT ON COLUMN ran.voie_ra41.fl_adr IS 'Etat de l''adresse
1 = actif ; 0 = inactif et vide = si pas encore d''adresse';
COMMENT ON COLUMN ran.voie_ra41.fl_diffusable IS 'Etat diffusable
1 = diffusable, 0 = non diffusable';
*/



-- Fonction remplacée par un appel à import_file
SELECT drop_all_functions_if_exists('ran','setVoieRa41FromRa41');

/*
TEST

Sur le serveur (REC) :
#extraction fichier ran
cd /data/bcaa/common_env/import/ran/
mkdir test
tar -C ./test/ -xzf raataaaa.bm_2017-09-30.tar.gz
#filtre département
head -1 ra41aaaa.bm > ra41aaaa.bm.tmp
grep '^33' ra41aaaa.bm >> ra41aaaa.bm.tmp
rm ra41aaaa.bm
mv ra41aaaa.bm.tmp ra41aaaa.bm
#chargement
*/

SELECT drop_all_functions_if_exists('ran','getVoieFromVoieRa41');
CREATE OR REPLACE FUNCTION ran.getVoieFromVoieRa41(voie_ra41 IN ran.voie_ra41)
  RETURNS ran.voie AS
$func$
DECLARE
	v_voie ran.voie%ROWTYPE;
	v_words TEXT[];
BEGIN
	v_voie.co_cea := voie_ra41.co_cea;
	v_voie.fl_active := CASE WHEN voie_ra41.fl_etat = 1 AND voie_ra41.fl_adr = 1 THEN TRUE ELSE FALSE END;
	v_voie.fl_diffusable := voie_ra41.fl_diffusable::INTEGER::BOOLEAN;
	v_voie.co_voie := voie_ra41.co_voie;
	v_voie.lb_voie := voie_ra41.lb_voie;
	
	--Cas sur fichier du 17-02-2020 : lb_voie de plus de 32 caractères et pas de lb_voie_nn renseigné
	IF LENGTH(COALESCE(voie_ra41.lb_voie_nn,voie_ra41.lb_voie)) > 32 THEN
		RAISE NOTICE 'Erreur : LENGTH(COALESCE(voie_ra41.lb_voie_nn,voie_ra41.lb_voie)) > 32 : %',voie_ra41;
		v_voie.lb_voie_normalise := LEFT(COALESCE(voie_ra41.lb_voie_nn,voie_ra41.lb_voie),32);
	ELSE
	v_voie.lb_voie_normalise := COALESCE(voie_ra41.lb_voie_nn,voie_ra41.lb_voie);
	END IF;
	
	v_voie.lb_type := voie_ra41.lb_in_ext_typ_voie;
	v_voie.lb_type_abrege := COALESCE(voie_ra41.lb_abr_nn,voie_ra41.lb_abr_an);
	
	IF voie_ra41.lb_md IS NULL THEN
		RAISE NOTICE 'Avertissement : voie_ra41.lb_md IS NULL : %',voie_ra41;
	END IF;
	v_voie.lb_md := voie_ra41.lb_md;
	
	--Cas sur fichier du 25-01-2020 : pas de descripteur
	IF voie_ra41.lb_desc_nn IS NULL THEN
		RAISE NOTICE 'Erreur : voie_ra41.lb_desc_nn IS NULL : %',voie_ra41;
		--Le descripteur décrit le libellé voie non normalisé, exemple : 793162227B
		--On découpe le libellé en mots
		SELECT ARRAY_AGG(regexp_matches[1])
		INTO v_words
		FROM regexp_matches(voie_ra41.lb_voie, '[^ ]+', 'g');
		--On met le descripteur X pour chaque mot (car trop compliqué à calculer pour le moment)
		v_voie.lb_desc := REPEAT('X',ARRAY_LENGTH(v_words,1));
	ELSE
	v_voie.lb_desc := voie_ra41.lb_desc_nn;
	END IF;
	v_voie.co_insee_commune := voie_ra41.co_insee;
	
	RETURN v_voie;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT voie.*
FROM ran.voie_ra41
CROSS JOIN ran.getVoieFromVoieRa41(voie_ra41) AS voie

REC(33) : 8s à 9s

Quasiment équivalent en temps à la requete :
SELECT 
	co_cea
	,CASE WHEN voie_ra41.fl_etat = 1 AND voie_ra41.fl_adr = 1 THEN TRUE ELSE FALSE END
	,voie_ra41.fl_diffusable::INTEGER::BOOLEAN
	,voie_ra41.co_voie
	,voie_ra41.lb_voie
	,COALESCE(voie_ra41.lb_voie_nn,voie_ra41.lb_voie)
	,voie_ra41.lb_in_ext_typ_voie
	,COALESCE(voie_ra41.lb_abr_nn,voie_ra41.lb_abr_an)
	,voie_ra41.lb_md
	,voie_ra41.lb_desc_nn
	,voie_ra41.co_insee
FROM ran.voie_ra41
*/

SELECT drop_all_functions_if_exists('ran','getVoieDeltaFromRa41');
CREATE OR REPLACE FUNCTION ran.getVoieDeltaFromRa41(in_dt_reference DATE)
  RETURNS SETOF ran.voie AS
$func$
DECLARE
BEGIN
	RETURN QUERY
		SELECT 
			voie.co_cea
			,in_dt_reference AS dt_reference
			,CASE 
				WHEN voie.fl_active = FALSE THEN 'S' 
				WHEN voie_avant.co_cea IS NULL OR voie_avant.co_mouvement = 'S' THEN 'C' /*recréation*/ 
				ELSE 'M' 
			END::CHAR(1) AS co_mouvement
			,voie.fl_active
			,voie.fl_diffusable
			,voie.co_voie
			,voie.lb_voie
			,voie.lb_voie_normalise
			,voie.lb_type
			,voie.lb_type_abrege
			,voie.lb_md
			,voie.lb_desc
			,voie.co_insee_commune
			,voie_avant.co_cea_za
		FROM ran.voie_ra41
		LEFT OUTER JOIN ran.voie AS voie_avant ON voie_avant.co_cea = voie_ra41.co_cea
		/* Alternative intéressante en cas d'abandon de l'état actuel au profit de l'historique seul :
		LEFT OUTER JOIN ran.voie_histo AS voie_avant 
		ON voie_avant.co_cea = voie.co_cea 
		AND voie_avant.dt_reference = (
			SELECT voie_a_date.dt_reference
			FROM ran.voie_histo AS voie_a_date
			WHERE voie_a_date.co_cea = voie_avant.co_cea
			AND voie_a_date.dt_reference < in_dt_reference
			ORDER BY voie_a_date.dt_reference DESC
			LIMIT 1
		)
		*/
		CROSS JOIN ran.getVoieFromVoieRa41(voie_ra41) AS voie
		WHERE 
		(
			--élément qui n'existait pas jusqu'ici
			voie_avant.co_cea IS NULL
			--OU élément qui a changé
			OR voie_avant.fl_active != voie.fl_active
			OR voie_avant.fl_diffusable != voie.fl_diffusable
			OR voie_avant.co_voie != voie.co_voie
			OR voie_avant.lb_voie != voie.lb_voie
			OR voie_avant.lb_voie_normalise != voie.lb_voie_normalise
			OR COALESCE(voie_avant.lb_type, 'NULL') != COALESCE(voie.lb_type, 'NULL') --NULLABLE
			OR COALESCE(voie_avant.lb_type_abrege, 'NULL') != COALESCE(voie.lb_type_abrege, 'NULL') --NULLABLE
			OR COALESCE(voie_avant.lb_md, 'NULL') != COALESCE(voie.lb_md, 'NULL') --NULLABLE
			OR COALESCE(voie_avant.lb_desc, 'NULL') != COALESCE(voie.lb_desc, 'NULL') --NULLABLE
		)
		;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM ran.getVoieDeltaFromRa41(NOW()::DATE)

REC(33) : 10s
*/

SELECT drop_all_functions_if_exists('ran','setVoieFromRa41');
CREATE OR REPLACE FUNCTION ran.setVoieFromRa41(in_dt_reference IN DATE, in_en_mode_init BOOLEAN DEFAULT TRUE, in_avec_historique BOOLEAN DEFAULT TRUE)
  RETURNS BOOLEAN AS
$func$
DECLARE
BEGIN
	IF in_en_mode_init = TRUE THEN
		--Initialisation des voies
		TRUNCATE TABLE ran.voie;
		PERFORM public.drop_table_indexes('ran', 'voie');
		INSERT INTO ran.voie(
			co_cea
			,dt_reference
			,co_mouvement
			,fl_active
			,fl_diffusable
			,co_voie
			,lb_voie
			,lb_voie_normalise
			,lb_type
			,lb_type_abrege
			,lb_md
			,lb_desc
			,co_insee_commune --nécessaire pour index mot directeur par commune
		)
		(
			SELECT 
				voie.co_cea
				,in_dt_reference AS dt_reference
				,CASE 
					WHEN voie.fl_active = FALSE THEN 'S' 
					ELSE 'I'  --INIT
				END AS co_mouvement
				,voie.fl_active
				,voie.fl_diffusable
				,voie.co_voie
				,voie.lb_voie
				,voie.lb_voie_normalise
				,voie.lb_type
				,voie.lb_type_abrege
				,voie.lb_md
				,voie.lb_desc
				,voie.co_insee_commune
			FROM ran.voie_ra41
			CROSS JOIN ran.getVoieFromVoieRa41(voie_ra41) AS voie
		);
		IF in_avec_historique = TRUE THEN
			--Initialisation de l'historique
			TRUNCATE TABLE ran.voie_histo;
			PERFORM public.drop_table_indexes('ran', 'voie_histo');
			INSERT INTO ran.voie_histo(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,co_voie
				,lb_voie
				,lb_voie_normalise
				,lb_type
				,lb_type_abrege
				,lb_md
				,lb_desc
				--,co_insee_commune
			)
			(
				SELECT 
					co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,co_voie
					,lb_voie
					,lb_voie_normalise
					,lb_type
					,lb_type_abrege
					,lb_md
					,lb_desc
					--,co_insee_commune
				FROM ran.voie
			)
			;
		END IF;
	ELSE
		IF in_avec_historique = TRUE THEN
			--Calcul du DELTA dans une table temporaire, pour le réutiliser à la fois pour mettre à jour les voies et l'historique
			DROP TABLE IF EXISTS tmp_ran_voie_ra41_delta;
			CREATE TEMPORARY TABLE tmp_ran_voie_ra41_delta AS TABLE ran.voie WITH NO DATA;
			INSERT INTO tmp_ran_voie_ra41_delta
			(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,co_voie
				,lb_voie
				,lb_voie_normalise
				,lb_type
				,lb_type_abrege
				,lb_md
				,lb_desc
				,co_insee_commune
				,co_cea_za
			)
			(
				SELECT	co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,co_voie
					,lb_voie
					,lb_voie_normalise
					,lb_type
					,lb_type_abrege
					,lb_md
					,lb_desc
					,co_insee_commune
					,co_cea_za
				FROM ran.getVoieDeltaFromRa41(in_dt_reference)
			);
			
			--Mise à jour des voies
			INSERT INTO ran.voie (SELECT * FROM tmp_ran_voie_ra41_delta)
			ON CONFLICT(co_cea)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active = EXCLUDED.fl_active
					,fl_diffusable = EXCLUDED.fl_diffusable
					,co_voie = EXCLUDED.co_voie
					,lb_voie = EXCLUDED.lb_voie
					,lb_voie_normalise = EXCLUDED.lb_voie_normalise
					,lb_type = EXCLUDED.lb_type
					,lb_type_abrege = EXCLUDED.lb_type_abrege
					,lb_md = EXCLUDED.lb_md
					,lb_desc = EXCLUDED.lb_desc
					,co_insee_commune = EXCLUDED.co_insee_commune
					,co_cea_za = EXCLUDED.co_insee_commune
			;

			--Mise à jour de l'historique
			INSERT INTO ran.voie_histo(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,co_voie
				,lb_voie
				,lb_voie_normalise
				,lb_type
				,lb_type_abrege
				,lb_md
				,lb_desc
				--,co_insee_commune
			)
			(
				SELECT	co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,co_voie
					,lb_voie
					,lb_voie_normalise
					,lb_type
					,lb_type_abrege
					,lb_md
					,lb_desc
					--,co_insee_commune
				FROM tmp_ran_voie_ra41_delta
			)
			;
		ELSE
			INSERT INTO ran.voie(
				co_cea
				,dt_reference
				,co_mouvement
				,fl_active
				,fl_diffusable
				,co_voie
				,lb_voie
				,lb_voie_normalise
				,lb_type
				,lb_type_abrege
				,lb_md
				,lb_desc
				,co_insee_commune --nécessaire pour index mot directeur par commune
				,co_cea_za
			)
			(
				SELECT	co_cea
					,dt_reference
					,co_mouvement
					,fl_active
					,fl_diffusable
					,co_voie
					,lb_voie
					,lb_voie_normalise
					,lb_type
					,lb_type_abrege
					,lb_md
					,lb_desc
					,co_insee_commune
					,co_cea_za
				FROM ran.getVoieDeltaFromRa41(in_dt_reference)
			)
			ON CONFLICT(co_cea)
			DO UPDATE
				SET	dt_reference = EXCLUDED.dt_reference
					,co_mouvement = EXCLUDED.co_mouvement
					,fl_active = EXCLUDED.fl_active
					,fl_diffusable = EXCLUDED.fl_diffusable
					,co_voie = EXCLUDED.co_voie
					,lb_voie = EXCLUDED.lb_voie
					,lb_voie_normalise = EXCLUDED.lb_voie_normalise
					,lb_type = EXCLUDED.lb_type
					,lb_type_abrege = EXCLUDED.lb_type_abrege
					,lb_md = EXCLUDED.lb_md
					,lb_desc = EXCLUDED.lb_desc
					,co_insee_commune = EXCLUDED.co_insee_commune
					,co_cea_za = EXCLUDED.co_cea_za
			;
		END IF;

		/* Suite problème de cohérence INSEE entre ZA et VOIE, on fait un UPDATE similaire dans import_end.sql
		--Particularité : on met à jour le code insee commune de la voie, mais on ne considère pas que c'est une modification de la voie (mais plutot de la za rattachée à la voie)
		-- BAN-1011 souci fusion 72176/72117
		-- à faire indépendamment de la gestion de l'historique
		UPDATE ran.voie
		SET co_insee_commune = voie_ra41.co_insee
		FROM ran.voie_ra41
		WHERE voie_ra41.co_cea = voie.co_cea
		AND voie_ra41.co_insee != voie.co_insee_commune;
		*/
	END IF;
	
	-- Controle d'unicité, et optimisation de filtre sur l'identifiant de la VOIE
	CREATE UNIQUE INDEX IF NOT EXISTS idx_voie_co_cea ON ran.voie (co_cea);
	-- Optimisation de filtre sur code inseee commune
	--CREATE INDEX idx_voie_co_insee_commune ON ran.voie (co_insee);
	-- + recherche libellé mot directeur approchant sur la commune
	-- cet index n'est plus indispensable car il servait à faire le diagnostic, mais désormais le diagnostic utilise un jeu d'adresses temporaire, avec ses propres index
	--CREATE INDEX IF NOT EXISTS idx_voie_co_insee_commune_lb_md ON ran.voie USING GIST(co_insee_commune, lb_md gist_trgm_ops);
	DROP INDEX IF EXISTS ran.idx_voie_co_insee_commune_lb_md;
	-- + recherche libellé mot directeur approchant sur le CP ? Nécessiterait de dénormaliser le CP de la ZA sur les communes
	--CREATE INDEX IF NOT EXISTS idx_voie_co_postal_lb_md ON ran.voie USING GIST(co_postal, lb_md gist_trgm_ops);
	CREATE INDEX IF NOT EXISTS idx_voie_co_insee_departement ON ran.voie (public.getCodeInseeDepartementFromCodeInseeCommune(co_insee_commune));
	-- Optimisation de filtre un libellé voie nouvelle norme ressemblant à
	CREATE INDEX IF NOT EXISTS idx_voie_lb_voie ON ran.voie USING gin(lb_voie gin_trgm_ops);
	-- Optimisation de filtre un libellé mot directeur ressemblant à
	--CREATE INDEX idx_voie_lb_md ON ran.voie USING gin(lb_md gin_trgm_ops);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_voie_histo_key ON ran.voie_histo (co_cea, dt_reference);

	TRUNCATE TABLE ran.voie_ra41;
	
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT ran.setVoieFromRa41('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, TRUE, TRUE);
SELECT ran.setVoieFromRa41('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, FALSE, FALSE);
 */
