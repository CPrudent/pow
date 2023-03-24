/***
 * schema FR : list of objects to create
 */

-- POW
\include_relative ./functions/geometry.sql
\include_relative ./functions/address.sql
\include_relative ./functions/address_normalize.sql

-- BAL

\include_relative bal_municipality.sql
\include_relative bal_street.sql
\include_relative bal_housenumber.sql

-- INSEE

\include_relative insee_municipality_event.sql
\include_relative insee_administrative_cutting.sql

-- LAPOSTE

\include_relative laposte_address.sql
\include_relative laposte_zone_address.sql
\include_relative laposte_street.sql
\include_relative laposte_housenumber.sql
\include_relative laposte_complement.sql
\include_relative laposte_xy.sql
\include_relative laposte_delivery_point.sql

-- POW
\include_relative constant.sql
\include_relative territory_level.sql
\include_relative territory_laposte.sql
\include_relative territory_to_date.sql
\include_relative territory_supra.sql
\include_relative territory_geometry.sql
\include_relative territory.sql
