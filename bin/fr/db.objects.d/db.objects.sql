/***
 * schema FR : list of objects to create
 */

-- POW
\include_relative ./functions/geometry.sql
\include_relative ./functions/address.sql

-- IO
\include_relative io.sql

-- BAL

\include_relative bal_municipality.sql
\include_relative bal_street.sql
\include_relative bal_housenumber.sql

-- INSEE

\include_relative insee_municipality_event.sql
\include_relative insee_administrative_cutting.sql

-- LAPOSTE

\include_relative laposte_address_area.sql
\include_relative laposte_address_street.sql
\include_relative laposte_address_street_uniq.sql
\include_relative laposte_address_street_reference.sql
\include_relative laposte_address_street_membership.sql
\include_relative laposte_address_street_word_descriptor.sql
\include_relative laposte_address_street_word_level.sql
\include_relative laposte_address_street_keyword.sql
\include_relative laposte_address_street_kw_exception.sql
\include_relative laposte_address_housenumber.sql
\include_relative laposte_address_complement.sql
\include_relative laposte_address_complement_uniq.sql
\include_relative laposte_address_complement_reference.sql
\include_relative laposte_address_complement_membership.sql
\include_relative laposte_address_complement_word_descriptor.sql
\include_relative laposte_address_xy.sql
\include_relative laposte_address.sql
\include_relative laposte_address_fault.sql
\include_relative laposte_address_fault_street.sql
\include_relative laposte_delivery_point.sql
\include_relative laposte_delivery_address.sql
\include_relative laposte_organization.sql
\include_relative laposte_address_redirect.sql
\include_relative laposte_address_history.sql

-- POW
\include_relative constant.sql
\include_relative territory_level.sql
\include_relative territory_laposte.sql
\include_relative territory_to_date.sql
\include_relative territory_supra.sql
\include_relative territory_geometry.sql
\include_relative territory.sql
\include_relative address.sql
\include_relative address_match_request.sql
\include_relative address_match_result.sql
\include_relative address_match_code.sql
\include_relative address_match_element.sql
\include_relative ./functions/similarity.sql
\include_relative ./functions/address_normalize.sql
\include_relative ./functions/address_match.sql

