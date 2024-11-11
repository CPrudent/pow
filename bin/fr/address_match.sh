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

# set defintion (SQL) of property (format or parameters) w/ OS file
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

    # defined property ?
    [ -n "${_vars_ref[${_property}]}" ] && {
        # default path
        _vars_ref[${_property}_PATH]="${POW_DIR_BIN}/${_vars_ref[${_property}]}_${get_arg_property}.sql"
        [ -f "${_vars_ref[${_property}_PATH]}" ] &&
        _vars_ref[${_property}_SQL]=$(cat "${_vars_ref[${_property}_PATH]}") || {
            # specific path
            [ -f "${_vars_ref[${_property}]}" ] &&
            _vars_ref[${_property}_SQL]=$(cat "${_vars_ref[${_property}]}") || {
                log_error "Le fichier ${get_arg_property^} ${_vars_ref[${_property}]} n'existe pas"
                return $ERROR_CODE
            }
        }
    }

    return $SUCCESS_CODE
}

bash_args \
    --args_p "
        file_path:Fichier des Adresses à rapprocher;
        force:Forcer le traitement même si celui-ci a déjà été fait;
        suffix:Entité SQL des Adresses avec ce suffixe particulier;
        format:Définition du Format des Adresses (ou fichier du Format);
        parameters:Définition des Paramètres du Rapprochement (ou fichier des Paramètres);
        import_options:Options import (du fichier) spécifiques à son type;
        import_limit:Limiter à n enregistrements;
        steps:Ensemble des étapes à réaliser (séparées par une virgule, si plusieurs);
        verbose:Ajouter des détails sur les traitements
    " \
    --args_o '
        file_path
    ' \
    --args_v '
        force:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        force:no;
        verbose:no;
        steps:ALL
    ' \
    "$@" || exit $ERROR_CODE

declare -A match_vars=(
    [FILE_PATH]="$get_arg_file_path"
    [FORCE]=$get_arg_force
    [SUFFIX]="$get_arg_suffix"
    [FORMAT]="$get_arg_format"
    [PARAMETERS]="$get_arg_parameters"
    [IMPORT_OPTIONS]="$get_arg_import_options"
    [IMPORT_LIMIT]=$get_arg_import_limit
    [TABLE_NAME]=''
    [FORMAT_PATH]=''
    [FORMAT_SQL]=''
    [PARAMETERS_PATH]=''
    [PARAMETERS_SQL]=''
    [STEPS]=${get_arg_steps// /}
    [VERBOSE]=$get_arg_verbose
)
_k=0
MATCH_REQUEST_ID=$((_k++))
MATCH_REQUEST_SUFFIX=$((_k++))
MATCH_REQUEST_NEW=$((_k++))
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

expect file "${match_vars[FILE_PATH]}" &&
get_definition --property parameters --vars match_vars &&
set_env --schema_name fr &&

execute_query \
    --name MATCH_REQUEST \
    --query "
        SELECT CONCAT_WS(' ', id, suffix, new_request)
        FROM fr.add_match_request(
            file_path => '${match_vars[FILE_PATH]}'
            $([ -n "${match_vars[SUFFIX]}" ] && echo ", suffix => '${match_vars[SUFFIX]}'")
            $([ -n "${match_vars[PARAMETERS_SQL]}" ] && echo ", parameters => '${match_vars[PARAMETERS_SQL]}'::HSTORE")
        )
    " \
    --psql_arguments 'tuples-only:pset=format=unaligned' \
    --return _request &&
match_request=($_request) &&
[ ${#match_request[*]} -eq $MATCH_REQUEST_ITEMS ] || {
    log_error "demande Rapprochement fichier '${match_vars[FILE_PATH]}' en erreur"
    exit $ERROR_CODE
} &&

match_vars[TABLE_NAME]=address_match_${match_request[$MATCH_REQUEST_SUFFIX]} &&

{
    in_array match_steps IMPORT _steps_id && {
        ([ match_vars[FORCE] = no ] && table_exists --schema_name fr --table_name ${match_vars[TABLE_NAME]}) || {
            match_info --steps_info match_steps_info --steps_id $_steps_id &&
            import_file \
                --file_path "${match_vars[FILE_PATH]}" \
                --schema_name fr \
                --table_name ${match_vars[TABLE_NAME]} \
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
        match_info --steps_info match_steps_info --steps_id $_steps_id &&
        execute_query \
            --name STANDARDIZE_REQUEST \
            --query "CALL set_match_standardize(
                file_path => '${match_vars[FILE_PATH]}',
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
