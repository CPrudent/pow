#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match FR addresses

# log info about matching step
match_info() {
    bash_args \
        --args_p "
            steps_info:Table des libellés des étapes;
            steps_id:Entrée dans cette table
        " \
        --args_o '
            steps_info;
            steps_id
        ' \
        "$@" || return $ERROR_CODE

    local -n _steps_ref=$get_arg_steps_info

    log_info "demande de Rapprochement (étape '${_steps_ref[$get_arg_steps_id]}')"
    return $SUCCESS_CODE
}

# get defintion of property (format or parameters) w/ OS file
get_definition() {
    bash_args \
        --args_p "
            property:Propriété à définir;
            vars:Entité des variables globales
        " \
        --args_o '
            property;
            vars
        ' \
        --args_v '
            property:format|parameters
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars
    local _property=${get_arg_property^^}
    local _path=${_property}_PATH _sql=${_property}_SQL

    # defined property ?
    [ -n "${_vars_ref[${_property}]}" ] && {
        # default path
        _vars_ref[$_path]="${POW_DIR_BIN}/${_vars_ref[${_property}]}_${get_arg_property}.sql"
        [ -f "${_vars_ref[$_path]}" ] &&
        _vars_ref[$_sql]=$(cat "${_vars_ref[_path]}") || {
            # specific path
            [ -f "${_vars_ref[${_property}]}" ] &&
            _vars_ref[$_sql]=$(cat "${_vars_ref[${_property}]}") || {
                log_error "Le fichier ${_property} ${_vars_ref[${_property}]} n'existe pas"
                return $ERROR_CODE
            }
        }
    }

    return $SUCCESS_CODE
}

bash_args \
    --args_p "
        source_name:Fichier des Adresses à rapprocher;
        source_query:Requête à appliquer pour obtenir les données entrantes;
        source_filter:Filtre à appliquer sur les données entrantes;
        format:Définition du Format des Adresses (ou fichier du Format);
        parameters:Définition des Paramètres du Rapprochement (ou fichier des Paramètres);
        import_options:Options import (du fichier) spécifiques à son type;
        import_limit:Limiter à n enregistrements;
        steps:Ensemble des étapes à réaliser (séparées par une virgule, si plusieurs);
        info:Afficher les informations de la demande;
        force:Forcer le traitement même si celui-ci a déjà été fait;
        verbose:Ajouter des détails sur les traitements
    " \
    --args_o '
        source_name
    ' \
    --args_v '
        force:yes|no;
        info:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        force:no;
        info:no;
        verbose:no;
        steps:ALL
    ' \
    "$@" || exit $ERROR_CODE

declare -A match_vars=(
    [SOURCE_NAME]="$get_arg_source_name"
    [SOURCE_KIND]=
    [SOURCE_FILTER]="$get_arg_source_filter"
    [SOURCE_QUERY]="$get_arg_source_query"
    [FORCE]=$get_arg_force
    [FORMAT]="$get_arg_format"
    [PARAMETERS]="$get_arg_parameters"
    [IMPORT_OPTIONS]="$get_arg_import_options"
    [IMPORT_LIMIT]=$get_arg_import_limit
    [FORMAT_PATH]=''
    [FORMAT_SQL]=''
    [PARAMETERS_PATH]=''
    [PARAMETERS_SQL]=''
    [STEPS]=${get_arg_steps// /}
    [VERBOSE]=$get_arg_verbose
)
_k=0
MATCH_REQUEST_ID=$((_k++))
MATCH_REQUEST_IMPORT=$((_k++))
MATCH_REQUEST_ITEMS=$_k
declare -a match_request

MATCH_STEPS=IMPORT,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT,MATCH_ADDRESS,REPORT,STATS
declare -a match_steps
[ "${match_vars[STEPS]}" = ALL ] && match_vars[STEPS]=$MATCH_STEPS
match_steps=( ${match_vars[STEPS]//,/ } )
declare -a match_steps_info=(
    [0]=Chargement
    [1]=Standardisation
    [2]="Calcul MATCH CODE"
    [3]="Rapprochement ELEMENT"
    [4]="Rapprochement ADRESSE"
    [5]=Rapport
    [6]=Statistiques
)

set_env --schema_name fr &&
# determine kind of source
{
    [ -f "${match_vars[SOURCE_NAME]}" ] && {
        match_vars[SOURCE_KIND]=FILE
    } || {
        table_exists --schema_name fr --table_name "${match_vars[SOURCE_NAME]}" && match_vars[SOURCE_KIND]=TABLE || {
            [ -n "${match_vars[SOURCE_QUERY]}" ] && match_vars[SOURCE_KIND]=QUERY
        }
    }
} &&

get_definition --property parameters --vars match_vars &&

# ERROR if query has * (following select) which is replaced by bash_args as files!
execute_query \
    --name MATCH_REQUEST \
    --query "
        SELECT CONCAT_WS(' ', id, import_name)
        FROM fr.set_match_request(
            source_name => '${match_vars[SOURCE_NAME]}',
            source_kind => '${match_vars[SOURCE_KIND]}'
            $([ -n "${match_vars[SOURCE_FILTER]}" ] && echo ", source_filter => '${match_vars[SOURCE_FILTER]//\'/\'\'}'")
            $([ -n "${match_vars[SOURCE_QUERY]}" ] && echo ", source_query => '${match_vars[SOURCE_QUERY]//\'/\'\'}'")
            $([ -n "${match_vars[PARAMETERS_SQL]}" ] && echo ", parameters => '${match_vars[PARAMETERS_SQL]}'::HSTORE")
        )
    " \
    --psql_arguments 'tuples-only:pset=format=unaligned' \
    --return _request &&

match_request=($_request) &&

# ERROR if TABLE|QUERY as 2nd result (import_name) is null, and so array has only 1 element!
# declare -p match_request &&
# [ ${#match_request[*]} -eq $MATCH_REQUEST_ITEMS ] || {
#     log_error "demande Rapprochement données '${match_vars[SOURCE_NAME]}' en erreur"
#     exit $ERROR_CODE
# } &&

# only info
{
    [ "$get_arg_info" = yes ] && {
        declare -p match_request match_vars
        exit $SUCCESS_CODE
    } || true
} &&

# import todo? only for FILE input
{
    ([ "${match_vars[SOURCE_KIND]}" = FILE ] && in_array match_steps IMPORT _steps_id) && {
        ([ ${match_vars[FORCE]} = no ] && table_exists --schema_name fr --table_name ${match_request[MATCH_REQUEST_IMPORT]}) || {
            match_info --steps_info match_steps_info --steps_id $_steps_id &&
            import_file \
                --file_path "${match_vars[SOURCE_NAME]}" \
                --schema_name fr \
                --table_name "${match_request[MATCH_REQUEST_IMPORT]}" \
                --load_mode OVERWRITE_TABLE \
                --limit "${match_vars[IMPORT_LIMIT]}" \
                --import_options "${match_vars[IMPORT_OPTIONS]}"
        }
    } || true
} &&
{
    in_array match_steps STANDARDIZE _steps_id && {
        get_definition --property format --vars match_vars && {
            [ -n "${match_vars[FORMAT_SQL]}" ] && true || {
                log_error "manque définition du format (option --format)"
                exit $ERROR_CODE
            }
        } &&
        #declare -p match_vars &&
        match_info --steps_info match_steps_info --steps_id $_steps_id &&
        execute_query \
            --name STANDARDIZE_REQUEST \
            --query "CALL set_match_standardize(
                id => ${match_request[MATCH_REQUEST_ID]},
                mapping => '${match_vars[FORMAT_SQL]}'::HSTORE,
                force => ('${match_vars[FORCE]}' = 'yes'),
                raise_notice => ('${match_vars[VERBOSE]}' = 'yes')
            )"
    } || true
} &&
{
    in_array match_steps MATCH_CODE _steps_id && {
        match_info --steps_info match_steps_info --steps_id $_steps_id &&
        execute_query \
            --name MATCH_CODE_REQUEST \
            --query "CALL fr.set_match_code(
                id => ${match_request[$MATCH_REQUEST_ID]},
                force => ('${match_vars[FORCE]}' = 'yes')
            )"
    } || true
} &&
{
    in_array match_steps MATCH_ELEMENT _steps_id && {
        match_info --steps_info match_steps_info --steps_id $_steps_id &&
        execute_query \
            --name MATCH_ELEMENT_REQUEST \
            --query "CALL fr.set_match_element(
                id => ${match_request[$MATCH_REQUEST_ID]},
                force => ('${match_vars[FORCE]}' = 'yes'),
                raise_notice => ('${match_vars[VERBOSE]}' = 'yes')
            )"
    } || true
} || exit $ERROR_CODE

exit $SUCCESS_CODE
