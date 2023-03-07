/***
 * schema FR : list of objects to create
 */

\include_relative ./functions/address.sql

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
\include_relative territory_level.sql
