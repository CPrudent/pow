/***
 * DB: install EXTENSION postgis
 */

-- need this script to force 2-steps installation, and avoid "type geometry not exists"

\include_relative ../functions/search_path.sql

CREATE SCHEMA IF NOT EXISTS ext_postgis;
SELECT add_to_search_path('ext_postgis');
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA ext_postgis;

-- for updates
ALTER EXTENSION postgis UPDATE;
-- SELECT postgis_extensions_upgrade();

GRANT ALL PRIVILEGES ON SCHEMA ext_postgis TO public;
GRANT ALL PRIVILEGES ON TABLE ext_postgis.spatial_ref_sys TO public;
