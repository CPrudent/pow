#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match FR addresses

bash_args \
    --args_p "
        force:Forcer le traitement même si celui-ci a déjà été fait;
        file_path:Fichier Adresses à rapprocher;
        format:Code SQL du format d adresse ou chemin absolu vers un fichier de format;
        import_options:Options import (du fichier) spécifiques à son type;
        import_limit:Limiter à n enregistrements
    " \
    --args_o '
        file_path
    ' \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

declare -A match_var=(
    [FORCE]=$get_arg_force
    [FILE_PATH]="$get_arg_file_path"
    [FORMAT]="$get_arg_format"
    [IMPORT_OPTIONS]="$get_arg_import_options"
    [IMPORT_LIMIT]=$get_arg_import_limit
    [TABLE_NAME]=''
    [FORMAT_PATH]=''
    [FORMAT_SQL]=''
)
_k=0
MATCH_REQUEST_ID=$((_k++))
MATCH_REQUEST_SUFFIX=$((_k++))
MATCH_REQUEST_NEW=$((_k++))
MATCH_REQUEST_ITEMS=$_k
declare -a match_request

expect file "${match_var[FILE_PATH]}" &&
set_env --schema_name fr &&
execute_query \
    --name ADD_MATCH_REQUEST \
    --query "
        SELECT CONCAT_WS(' ', id, suffix, new)
        FROM fr.add_address_match(file_path => '${match_var[FILE_PATH]}')
        AS (id INT, suffix VARCHAR, new BOOLEAN)" \
    --psql_arguments 'tuples-only:pset=format=unaligned' \
    --return _request &&
match_request=($_request) &&
[ ${#match_request[*]} -eq $MATCH_REQUEST_ITEMS ] || {
    log_error "demande Rapprochement fichier '${match_var[FILE_PATH]}' en erreur"
    exit $ERROR_CODE
}

match_var[TABLE_NAME]=address_match_${match_request[$MATCH_REQUEST_SUFFIX]}
match_var[FORMAT_PATH]="${POW_DIR_BIN}/${match_var[FORMAT]}_format.sql"
{
    ([ match_var[FORCE] = no ] && table_exists --schema_name fr --table_name ${match_var[TABLE_NAME]}) || {
        log_info "demande de Rapprochement (étape Chargement)" &&
        import_file \
            --file_path "${match_var[FILE_PATH]}" \
            --schema_name fr \
            --table_name ${match_var[TABLE_NAME]} \
            --load_mode OVERWRITE_TABLE \
            --limit "${match_var[IMPORT_LIMIT]}" \
            --import_options "${match_var[IMPORT_OPTIONS]}"
    }
} && {
    [ -f "${match_var[FORMAT_PATH]}" ] &&
    match_var[FORMAT_SQL]=$(cat "${match_var[FORMAT_PATH]}") || {
        [ -f "${match_var[FORMAT]}" ] &&
        match_var[FORMAT_SQL]=$(cat "${match_var[FORMAT]}") || {
            log_error "Le format ${match_var[FORMAT]} n'existe pas"
            false
        }
    }
} &&
log_info "demande de Rapprochement (étape Normalisation)" &&
execute_query \
    --name NORMALIZE_MATCH_REQUEST \
    --query "CALL fr.set_normalize(
        file_path => '${match_var[FILE_PATH]}'
        , mapping => '${match_var[FORMAT_SQL]}'::HSTORE
    )" || exit $ERROR_CODE

exit $SUCCESS_CODE
