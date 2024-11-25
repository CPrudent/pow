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
            step:Entrée dans cette table
        " \
        --args_o '
            steps_info;
            step
        ' \
        "$@" || return $ERROR_CODE

    local -n _steps_ref=$get_arg_steps_info

    log_info "demande de Rapprochement (étape '${_steps_ref[$get_arg_step]}')"
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

output_columns() {
    bash_args \
        --args_p "
            columns_set:Ensemble des colonnes;
            columns_user:Liste des colonnes à traiter;
            columns_default:Ensemble des colonnes disponibles;
            columns_todo:Résultat
        " \
        --args_o '
            columns_set;
            columns_user;
            columns_default;
            columns_todo
        ' \
        --args_v '
            columns_set:IN|MORE
        ' \
        "$@" || return $ERROR_CODE

    local -n _user_ref=$get_arg_columns_user
    local -n _todo_ref=$get_arg_columns_todo
    local _item _minus _tmp _i _rc

    # clone default list
    _tmp=$(declare -p ${get_arg_columns_default}) &&
    #echo "$_tmp" &&
    eval "${_tmp/${get_arg_columns_default}=/_array_clone=}" &&
    #declare -p _array_clone

    for _item in ${_user_ref[@]}; do
        #echo "item=$_item (#${#_array_clone[@]})"
        _minus=${_item:0:1}
        if [ "$_minus" = - ]; then
            #_array_clone=( "${_array_clone[@]/${_item:1}}" ) &&
            for _i in "${!_array_clone[@]}"; do
                [[ $_i = ${_item:1} ]] && {
                    unset '_array_clone[$_i]'
                    #echo "(#${#_array_clone[@]})"
                    break
                }
            done
            continue
        fi

        in_array --array _array_clone --item "$_item"
        _rc=$?
        [ "$get_arg_columns_set" = IN ] && _item="i.${_item,,}"
        # column has to NOT exist if IN set (and vice versa if MORE)
        (([ "$get_arg_columns_set" = IN ] && [ $_rc -eq 1 ]) ||
        ([ "$get_arg_columns_set" = MORE ] && [ $_rc -eq 0 ])) &&
        _todo_ref+=( "$_item" )
    done
    _todo_ref+=( "${_array_clone[@]}" )

    return $SUCCESS_CODE
}

bash_args \
    --args_p "
        source_name:Source des Adresses à rapprocher;
        source_filter:Filtre à appliquer sur les données entrantes;
        source_query:Requête à appliquer pour obtenir les données entrantes;
        steps:Ensemble des étapes à réaliser (séparées par une virgule, si plusieurs);
        format:Définition du Format des Adresses (ou fichier du Format);
        parameters:Définition des Paramètres du Rapprochement (ou fichier des Paramètres);
        import_options:Options import (du fichier) spécifiques à son type;
        import_limit:Limiter à n enregistrements;
        output_in_columns:Liste des données entrantes à inclure dans le rapport;
        output_more_columns:Liste des données supplémentaires à inclure dans le rapport;
        output_srid:code SRID des géométries dans le rapport;
        force:Forcer le traitement même si celui-ci a déjà été fait;
        only_info:Afficher les informations de la demande;
        verbose:Ajouter des détails sur les traitements
    " \
    --args_o '
        source_name
    ' \
    --args_v '
        force:yes|no;
        only_info:NO|ID|ALL;
        verbose:yes|no
    ' \
    --args_d '
        steps:ALL;
        force:no;
        only_info:NO;
        output_srid:4326;
        verbose:no
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
    [COLUMNS_IN]="${get_arg_output_in_columns^^}"
    [COLUMNS_MORE]="${get_arg_output_more_columns^^}"
)

_k=0
MATCH_REQUEST_ID=$((_k++))                  # ID de la demande
MATCH_REQUEST_IMPORT=$((_k++))              # table d'import des données
MATCH_REQUEST_ITEMS=$_k
declare -a match_request

MATCH_STEPS=IMPORT,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT,REPORT,STATS
declare -a match_steps
[ "${match_vars[STEPS]}" = ALL ] && match_vars[STEPS]=$MATCH_STEPS
match_steps=( ${match_vars[STEPS]//,/ } )
declare -A match_steps_info=(
    [IMPORT]=Chargement
    [STANDARDIZE]=Standardisation
    [MATCH_CODE]='Calcul MATCH CODE'
    [MATCH_ELEMENT]='Rapprochement ELEMENT'
    [REPORT]=Rapport
    [STATS]=Statistiques
)

# columns to report
# w/ predefined aliases: i for input, a for address and t for territory
declare -A match_in_columns=(
    [ROWID]=i.rowid
)
declare -A match_more_columns=(
    # ADDRESS
    [COMPLEMENT]='COALESCE(a.lb_ligne3_normalise, a.lb_ligne3)'
    [HOUSENUMBER]=a.no_numero
    [EXTENSION]=a.lb_extension_numero
    [STREET]='COALESCE(a.lb_voie_normalise, a.lb_voie)'
    [OLD_MUNICIPALITY]=a.lb_ligne5
    [POSTCODE]=a.co_postal
    [MUNICIPALITY]=a.lb_acheminement
    # LA POSTE
    [QL]=a.rao_co_tournee
    [ROC]=a.co_roc_site
    [REGATE]=a.rao_co_regate
    # XY
    [LOCALISATION]=a.no_type_localisation_coord
    [GEOMETRY_X]='ST_X(ST_Transform(a.gm_coord, 4326))'
    [GEOMETRY_Y]='ST_Y(ST_Transform(a.gm_coord, 4326))'
    # HIERARCHY
    [COM]=t.codgeo_com_parent
    [CV]=t.codgeo_cv_parent
    [ARR]=t.codgeo_arr_parent
    [EPCI]=t.codgeo_epci_parent
    [DEP]=t.codgeo_dep_parent
    [REG]=t.codgeo_reg_parent
    [PDC]=t.codgeo_pdc_ppdc_parent
    [PPDC]=t.codgeo_ppdc_pdc_parent
    [DEX]=t.codgeo_dex_parent
)

set_env --schema_name fr &&
# determine kind of source
{
    [ -f "${match_vars[SOURCE_NAME]}" ] && {
        match_vars[SOURCE_KIND]=FILE
    } || {
        table_exists --schema_name fr --table_name "${match_vars[SOURCE_NAME]}" && match_vars[SOURCE_KIND]=TABLE || {
            [ -n "${match_vars[SOURCE_QUERY]}" ] && match_vars[SOURCE_KIND]=QUERY || {
                log_error 'type source non déterminé!'
                false
            }
        }
    }
} &&

{
    [ "${match_vars[VERBOSE]}" = yes ] && log_info "type source: ${match_vars[SOURCE_KIND]}" || true
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

{
    [ "${match_vars[VERBOSE]}" = yes ] && {
        log_info "$(declare -p match_request)"
        log_info "$(declare -p match_vars)"
    } || true
} &&

# ERROR if TABLE|QUERY as 2nd result (import_name) is null, and so array has only 1 element!
# declare -p match_request &&
# [ ${#match_request[*]} -eq $MATCH_REQUEST_ITEMS ] || {
#     log_error "demande Rapprochement données '${match_vars[SOURCE_NAME]}' en erreur"
#     exit $ERROR_CODE
# } &&

# only info
{
    [ "$get_arg_only_info" != NO ] && {
        case "$get_arg_only_info" in
        ID)
            echo ${match_request[MATCH_REQUEST_ID]}
            ;;
        ALL)
            declare -p match_request match_vars
            ;;
        esac
        exit $SUCCESS_CODE
    } || true
} &&

# import todo? only for FILE input
{
    ([ "${match_vars[SOURCE_KIND]}" = FILE ] && in_array --array match_steps --item IMPORT) && {
        ([ ${match_vars[FORCE]} = no ] && table_exists --schema_name fr --table_name ${match_request[MATCH_REQUEST_IMPORT]}) || {
            match_info --steps_info match_steps_info --step IMPORT &&
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
    in_array --array match_steps --item STANDARDIZE && {
        get_definition --property format --vars match_vars && {
            [ -n "${match_vars[FORMAT_SQL]}" ] && true || {
                log_error "manque définition du format (option --format)"
                exit $ERROR_CODE
            }
        } &&
        match_info --steps_info match_steps_info --step STANDARDIZE &&
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
    in_array --array match_steps --item MATCH_CODE && {
        match_info --steps_info match_steps_info --step MATCH_CODE &&
        execute_query \
            --name MATCH_CODE_REQUEST \
            --query "CALL fr.set_match_code(
                id => ${match_request[$MATCH_REQUEST_ID]},
                force => ('${match_vars[FORCE]}' = 'yes')
            )"
    } || true
} &&

{
    in_array --array match_steps --item MATCH_ELEMENT && {
        match_info --steps_info match_steps_info --step MATCH_ELEMENT &&
        execute_query \
            --name MATCH_ELEMENT_REQUEST \
            --query "CALL fr.set_match_element(
                id => ${match_request[$MATCH_REQUEST_ID]},
                force => ('${match_vars[FORCE]}' = 'yes'),
                raise_notice => ('${match_vars[VERBOSE]}' = 'yes')
            )"
    } || true
} &&

{
    # FIX-ME: bash_args don't accept argument beginning w/ a dash (-) !
    in_array --array match_steps --item REPORT && {
        match_info --steps_info match_steps_info --step REPORT &&
        declare -a _list_in _list_more &&
        declare -a match_columns_in=( ${match_vars[COLUMNS_IN]//,/ } ) &&
        echo "IN: (${get_arg_output_in_columns}) $(declare -p match_columns_in)" &&
        output_columns \
            --columns_set IN \
            --columns_user match_columns_in \
            --columns_default match_in_columns \
            --columns_todo _list_in &&
        declare -a match_columns_more=( ${match_vars[COLUMNS_MORE]//,/ } ) &&
        echo "MORE: (${get_arg_output_more_columns}) $(declare -p match_columns_more)" &&
        output_columns \
            --columns_set MORE \
            --columns_user match_columns_more \
            --columns_default match_more_columns \
            --columns_todo _list_more &&
        echo RESULT &&
        declare -p _list_in _list_more || {
            log_error 'gestion des colonnes du rapport en erreur!'
            false
        }
    } || true
} &&

{
    [ "${match_vars[VERBOSE]}" = yes ] && log_info "archive: ${POW_DIR_ARCHIVE}" || true
} || exit $ERROR_CODE

exit $SUCCESS_CODE
