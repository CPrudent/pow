/***
 * DB: add EXTENSIONS
 */

/* NOTE
 * no more useful, extensions are now created into final schema
\include_relative move_extensions_to_schema.sql
 */

/* NOTE
PostGIS: see extension_postgis.sql, which has been already executed
         to avoid bug as "type geometry not exists"
 */

/* NOTE
type & geometry

topology: can't add this extension in another schema
CREATE SCHEMA IF NOT EXISTS ext_topology;
SELECT add_to_search_path('ext_topology');
 */
CREATE EXTENSION IF NOT EXISTS postgis_topology;
ALTER EXTENSION postgis_topology UPDATE;
CREATE EXTENSION IF NOT EXISTS postgis_sfcgal WITH SCHEMA ext_postgis;
ALTER EXTENSION postgis_sfcgal UPDATE;

/* NOTE
indexes (trigrammes), perhaps text match
 */
CREATE SCHEMA IF NOT EXISTS ext_pg_trgm;
SELECT add_to_search_path('ext_pg_trgm');
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA ext_pg_trgm;
GRANT ALL PRIVILEGES ON SCHEMA ext_pg_trgm TO public;

/* NOTE
indexes mix(BTREE, GIST)
 */
CREATE SCHEMA IF NOT EXISTS ext_btree_gist;
SELECT add_to_search_path('ext_btree_gist');
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA ext_btree_gist;
GRANT ALL PRIVILEGES ON SCHEMA ext_btree_gist TO public;

/* NOTE
indexes mix(BTREE, GIN)
 */
CREATE SCHEMA IF NOT EXISTS ext_btree_gin;
SELECT add_to_search_path('ext_btree_gin');
CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA ext_btree_gin;
GRANT ALL PRIVILEGES ON SCHEMA ext_btree_gin TO public;

CREATE SCHEMA IF NOT EXISTS ext_fuzzystrmatch;
SELECT add_to_search_path('ext_fuzzystrmatch');
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch WITH SCHEMA ext_fuzzystrmatch;
GRANT ALL PRIVILEGES ON SCHEMA ext_fuzzystrmatch TO public;

/* NOTE
password
 */
CREATE SCHEMA IF NOT EXISTS ext_pgcrypto;
SELECT add_to_search_path('ext_pgcrypto');
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA ext_pgcrypto;
GRANT ALL PRIVILEGES ON SCHEMA ext_pgcrypto TO public;

/* NOTE
field as (key, value), for example into BAN's db
 */
CREATE SCHEMA IF NOT EXISTS ext_hstore;
SELECT add_to_search_path('ext_hstore');
CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA ext_hstore;
GRANT ALL PRIVILEGES ON SCHEMA ext_hstore TO public;

/* NOTE
crosstab
 */
CREATE SCHEMA IF NOT EXISTS ext_tablefunc;
SELECT add_to_search_path('ext_tablefunc');
CREATE EXTENSION IF NOT EXISTS tablefunc WITH SCHEMA ext_tablefunc;
GRANT ALL PRIVILEGES ON SCHEMA ext_tablefunc TO public;
