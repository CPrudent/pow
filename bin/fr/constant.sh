#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR's constants

# TODO check if current env is fr, else set_env --schema_code fr

execute_query \
    --name SET_CONSTANTS \
    --query "
        CALL fr.set_laposte_municipality_normalized_label_exception();
        CALL fr.set_laposte_street_type();
        CALL fr.set_laposte_street_firstname();
        DELETE FROM fr.constant WHERE list = 'LAPOSTE_STREET_TITLE';
        " &&
import_file \
    --file_path "$POW_DIR_COMMON_GLOBAL_SCHEMA/constant/laposte_title.csv" \
    --table_name constant \
    --load_mode APPEND || exit $ERROR_CODE

log_info "Import des constantes LAPOSTE avec succès"
exit $SUCCESS_CODE
