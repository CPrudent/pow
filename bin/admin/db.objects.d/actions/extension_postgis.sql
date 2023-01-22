--Script à exécuter séparement pour obliger une reconnexion au serveur de bdd et éviter un plantage du genre "type geometry not exists" pour les opérations suivantes

\include_relative ./functions/search_path.sql

CREATE SCHEMA IF NOT EXISTS ext_postgis;
SELECT add_to_search_path('ext_postgis');
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA ext_postgis; -- pour types et opérations géométriques
DROP VIEW IF EXISTS public.carreau_insee_apps_ciblage_view; --Fait planter l'update de 3.0 à 3.1 à cause de l'utilisation de ST_Intersection dans la vue
ALTER EXTENSION postgis UPDATE; --Mise à jour au cas où
--Suite maj (préconisé par le site officiel de PostGIS) : SELECT postgis_extensions_upgrade();
GRANT ALL PRIVILEGES ON SCHEMA ext_postgis TO public;
GRANT ALL PRIVILEGES ON TABLE ext_postgis.spatial_ref_sys TO public;