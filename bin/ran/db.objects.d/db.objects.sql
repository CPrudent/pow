/***
 * import RAN definitions, which is LAPOSTE DELIVERY addresses referential
 */

-- level: district code/zipcode/district old name (or neighbourhood)/district name
\include_relative za.sql

-- level: street
\include_relative voie.sql

-- level: housenumber
\include_relative numero.sql

-- level: address complement
\include_relative l3.sql

-- union of all previous, with hierarchy
\include_relative adresse.sql

-- level: XY
\include_relative coord.sql
