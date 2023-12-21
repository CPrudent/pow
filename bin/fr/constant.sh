#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR's constants

# TODO check if current env is fr, else set_env --schema_name fr

execute_query \
    --name SET_CONSTANTS \
    --query "
        CALL fr.set_laposte_municipality_normalized_label_exception();
        CALL fr.set_laposte_address_street_type();
        CALL fr.set_laposte_address_extension_of_housenumber();
        CALL fr.set_laposte_address_titles();
        CALL fr.set_laposte_address_street_firstname();
        CALL fr.set_laposte_address_correction_list();
        CALL fr.set_territory_overseas();
        "
vacuum \
    --schema_name fr \
    --table_name constant \
    --mode ANALYZE || exit $ERROR_CODE

log_info "Import des constantes LAPOSTE avec succès"
exit $SUCCESS_CODE
