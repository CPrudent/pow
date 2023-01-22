/*
apt-get --purge remove postgresql-9.6
apt autoremove

apt-get dist-upgrade
*/
DO
$$
DECLARE
	v_pg_extension pg_catalog.pg_extension%ROWTYPE;
	v_requete VARCHAR;
BEGIN
	IF extension_exists('postgis') = TRUE THEN
		--Mise à jour, au cas où cela serait nécessaire avant de déplacer postgis
		ALTER EXTENSION postgis UPDATE;
		IF extension_exists('postgis_topology') = TRUE THEN
			ALTER EXTENSION postgis_topology UPDATE;
		END IF;
		
		CREATE SCHEMA IF NOT EXISTS ext_postgis;
	
		SELECT * INTO v_pg_extension 
		FROM pg_catalog.pg_extension 
		WHERE extname = 'postgis';
	
		IF v_pg_extension.extname IS NOT NULL THEN
			IF v_pg_extension.extrelocatable != TRUE THEN
				UPDATE pg_extension 
					SET extrelocatable = TRUE 
				WHERE extname = 'postgis';
			END IF;

			ALTER EXTENSION postgis SET SCHEMA ext_postgis;

			v_requete := CONCAT('ALTER EXTENSION postgis UPDATE TO "',v_pg_extension.extversion,'next";');
			RAISE NOTICE 'requete : %', v_requete;
			EXECUTE v_requete;

			v_requete := CONCAT('ALTER EXTENSION postgis UPDATE TO "',v_pg_extension.extversion,'";');
			RAISE NOTICE 'requete : %', v_requete;
			EXECUTE v_requete;

			PERFORM add_to_search_path('ext_postgis');
			DROP SCHEMA IF EXISTS postgis;
			PERFORM remove_from_search_path('postgis');

			ALTER EXTENSION postgis UPDATE;
			RAISE NOTICE 'FIN MAJ postgis';
		END IF;
	END IF;
	
	IF extension_exists('postgis_topology') = TRUE THEN
		/* Il n'est pas possible de définir un autre schéma que topology pour l'extension
		ALTER EXTENSION postgis_topology SET SCHEMA ext_postgis_topology;
		PERFORM add_to_search_path('ext_postgis_topology');
		DROP SCHEMA IF EXISTS topology;
		PERFORM remove_from_search_path('postgis');
		*/
		ALTER EXTENSION postgis_topology UPDATE;
		RAISE NOTICE 'FIN MAJ postgis_topology';
	END IF;
	
	IF extension_exists('pg_trgm') = TRUE THEN
		CREATE SCHEMA IF NOT EXISTS ext_pg_trgm;
		ALTER EXTENSION pg_trgm SET SCHEMA ext_pg_trgm;
		PERFORM add_to_search_path('ext_pg_trgm');
		RAISE NOTICE 'FIN MAJ pg_trgm';
	END IF;
	
	IF extension_exists('btree_gist') = TRUE THEN
		CREATE SCHEMA IF NOT EXISTS ext_btree_gist;
		ALTER EXTENSION btree_gist SET SCHEMA ext_btree_gist;
		PERFORM add_to_search_path('ext_btree_gist');
		RAISE NOTICE 'FIN MAJ btree_gist';
	END IF;
	
	IF extension_exists('fuzzystrmatch') = TRUE THEN
		CREATE SCHEMA IF NOT EXISTS ext_fuzzystrmatch;
		ALTER EXTENSION fuzzystrmatch SET SCHEMA ext_fuzzystrmatch;
		PERFORM add_to_search_path('ext_fuzzystrmatch');
		RAISE NOTICE 'FIN MAJ fuzzystrmatch';
	END IF;
	
	IF extension_exists('pgcrypto') = TRUE THEN
		CREATE SCHEMA IF NOT EXISTS ext_pgcrypto;
		ALTER EXTENSION pgcrypto SET SCHEMA ext_pgcrypto;
		PERFORM add_to_search_path('ext_pgcrypto');
		RAISE NOTICE 'FIN MAJ pgcrypto';
	END IF;

	IF extension_exists('hstore') = TRUE THEN
		CREATE SCHEMA IF NOT EXISTS ext_hstore;
		ALTER EXTENSION hstore SET SCHEMA ext_hstore;
		PERFORM add_to_search_path('ext_hstore');
		RAISE NOTICE 'FIN MAJ hstore';
	END IF;
END
$$
