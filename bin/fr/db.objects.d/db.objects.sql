/***
 * schema FR : list of objects to create
 */

-- BAL

\include_relative bal_mumiciplality.sql
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
\include_relative laposte_pdi.sql
