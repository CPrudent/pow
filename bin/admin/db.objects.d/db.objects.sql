/***
 * general SQL functions : need to be executed first
 */

/*
TODO
    - remove files NOT IN db.objects.sql
    - <FUNCTION> in minuscules, such: CONCAT, ARRAY_*, ST_*
    - TESTS en TEST
    - ,data as: , data (RE= ,([^ ]) w/ , \1)
 */

-- system functions
/* NOTE
begins w/ drop.sql due to drop_all_functions_if_exists() called for each function
 */
\include_relative ./functions/drop.sql
\include_relative ./functions/admin.sql
\include_relative ./functions/exists.sql
\include_relative ./functions/alter.sql
\include_relative ./functions/comments.sql
\include_relative ./functions/array.sql
\include_relative ./functions/json.sql
\include_relative ./functions/aggregate.sql
\include_relative ./functions/search_path.sql
\include_relative ./functions/random.sql
\include_relative ./functions/string.sql
\include_relative ./functions/internet.sql

\include_relative ./actions/extensions.sql
\include_relative ./actions/schemas_roles.sql

-- geometry functions
\include_relative ./functions/geometry.sql

/* INCLUDE
\include_relative ./functions/st_distance_exterior.sql
\include_relative ./functions/st_aslatlng.sql
\include_relative ./functions/st_removerepeatedpoints.sql
\include_relative ./functions/st_voronoipolygons.sql
\include_relative ./functions/st_splitfour.sql
\include_relative ./functions/coordisinsridbounds.sql
\include_relative ./functions/st_extendline.sql
\include_relative ./functions/st_makevalid2.sql
\include_relative ./functions/st_internalboundary.sql
 */

/* TODO or EXCLUDE
    \include_relative ./functions/st_addremovepointfrompoints.sql
    \include_relative ./functions/st_transform.sql
    \include_relative ./functions/st_simplifycreateandpreservetopology.sql
    \include_relative ./functions/st_rings.sql
    \include_relative ./functions/getprobabilitepolygoncontainpoint.sql
 */

-- business functions
\include_relative ./functions/address.sql
\include_relative territory_to_date.sql

/* INCLUDE
\include_relative ./functions/getcodeinseedepartementfromcodeinseecommune.sql
\include_relative ./functions/removemotsoutils.sql
\include_relative ./functions/uppernospecialscharsonlyalfanum.sql
 */

/* EXCLUDE
\include_relative ./functions/getsridcoordranfromcodeinseedepartement.sql
\include_relative ./functions/getsridcoordignfromcodeinseedepartement.sql
\include_relative ./functions/getsridcoordsourceorgafromlibelleprojection.sql
\include_relative ./functions/getsridcoordranfromcodeprojectionran.sql
\include_relative ./functions/getcodeprojectioncoordranfromcodeinseedepartement.sql
\include_relative ./functions/getnotypelocalisationcoordranfromlbtypelocalisationcoordign.sql
 */

/*
--FIXME : contournement de dépendances
\include_relative ../../public/structure/functions/historique_import.sql
 */
