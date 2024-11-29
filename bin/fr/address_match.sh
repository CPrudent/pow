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

export_get_columns() {
    bash_args \
        --args_p "
            columns_set:Ensemble des colonnes;
            columns_user:Liste des colonnes mentionnées;
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
    local _item _1st _tmp _pos _i

    # clone default list
    _tmp=$(declare -p ${get_arg_columns_default}) &&
    #echo "$_tmp" &&
    eval "${_tmp/${get_arg_columns_default}=/_array_clone=}" &&
    #declare -p _array_clone

    for _item in ${_user_ref[@]}; do
        #echo "item=$_item clone(#${#_array_clone[@]})"
        _1st=${_item:0:1}
        [ "$_1st" = - ] && _item=${_item:1}
        in_array --array _array_clone --item "$_item" --position _pos --search KEY
        #echo "pos=$_pos"
        ([ "$_1st" = - ] && [[ $_pos -ge 0 ]]) && {
            unset '_array_clone[$_item]'
            #echo "clone(#${#_array_clone[@]})"
            continue
        }

        # adding new column if NOT exists (only for IN set)
        ([ "$get_arg_columns_set" = IN ] && [[ $_pos -eq -1 ]]) &&
        # SQL as syntax : item matchs the name of column (w/ alias as uppercase)
        _todo_ref+=( "i.${_item,,} AS "'"'"$_item"'"'"" )
    done
    # add remaining columns
    [[ ${#_array_clone[@]} -gt 0 ]] && {
        for ((_i=0; _i<${#match_columns_order[@]}; _i++)); do
            # if exists key
            [ -v _array_clone[${match_columns_order[$_i]}] ] &&
            _todo_ref+=( "${_array_clone[${match_columns_order[$_i]}]} AS "'"'"${match_columns_order[$_i]}"'"'"" )
        done
    }

    return $SUCCESS_CODE
}

export_get_entry() {
    bash_args \
        --args_p "
            request:Entité de la demande;
            vars:Entité des variables globales;
            entry:Entité des données entrantes
        " \
        --args_o '
            request;
            vars;
            entry
        ' \
        "$@" || return $ERROR_CODE

    local -n _request_ref=$get_arg_request
    local -n _vars_ref=$get_arg_vars
    local -n _entry_ref=$get_arg_entry

    case ${_vars_ref[SOURCE_KIND]} in
    FILE)       _entry_ref=fr.${_request_ref[MATCH_REQUEST_IMPORT]}        ;;
    TABLE)      _entry_ref=fr.${_vars_ref[SOURCE_NAME]}                    ;;
    QUERY)      _entry_ref=${_vars_ref[SOURCE_NAME]}                       ;;
    esac

    return $SUCCESS_CODE
}

export_build() {
    bash_args \
        --args_p "
            columns_in:Ensemble des colonnes entrantes (Ensemble noté IN);
            columns_more:Ensemble des colonnes supplémentaires (Ensemble noté MORE);
            table_name:Table des données entrantes;
            request:Entité de la demande;
            vars:Entité des variables globales
        " \
        --args_o '
            columns_in;
            columns_more;
            table_name;
            request;
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _in_ref=$get_arg_columns_in
    local -n _more_ref=$get_arg_columns_more
    local -n _request_ref=$get_arg_request
    local -n _vars_ref=$get_arg_vars
    local _sql_file

    get_tmp_file --tmpext sql --tmpfile _sql_file --create yes &&
    log_info "SQL rapport: $_sql_file" &&
    {
        [ "${_vars_ref[SOURCE_KIND]}" = QUERY ] && {
            [ -z "${_vars_ref[SOURCE_QUERY]}" ] && {
                execute_query \
                    --name SOURCE_QUERY \
                    --query "
                        SELECT source_query
                        FROM fr.address_match_request
                        WHERE id_request = ${_request_ref[MATCH_REQUEST_ID]}
                    " \
                    --psql_arguments 'tuples-only:pset=format=unaligned' \
                    --return _vars_ref[SOURCE_QUERY]
            }
            echo "WITH ${_vars_ref[SOURCE_NAME]} AS (${_vars_ref[SOURCE_QUERY]})" >> $_sql_file
        } || true
    } &&
    echo 'SELECT' >> $_sql_file &&
    {
        [[ ${#_in_ref[@]} -gt 0 ]] && {
            printf '%s,\n' "${_in_ref[@]}" >> $_sql_file
        } || true
    } &&
    {
        [[ ${#_more_ref[@]} -gt 0 ]] && {
            printf '%s,\n' "${_more_ref[@]}" | sed --expression '$s/,$//' >> $_sql_file
        } || true
    } &&
    execute_query \
        --name OUTPUT_REPORT \
        --query "
            COPY (
                $(< $_sql_file)
                FROM
                    fr.address_match_result mr
                        JOIN fr.address_match_element me
                        ON (
                            ((mr.standardized_address).level = me.level)
                            AND (
                                ((mr.standardized_address).match_code_street = me.match_code)
                                OR
                                ((mr.standardized_address).match_code_housenumber = me.match_code)
                                OR
                                ((mr.standardized_address).match_code_complement = me.match_code)
                            )
                        )

                        JOIN fr.address_view a
                        ON (me.matched_element).codes_address[1] = a.co_adr

                        JOIN fr.territory t
                        ON (t.nivgeo, t.codgeo) = ('ZA', a.co_adr_za)

                        JOIN $get_arg_table_name i
                        ON mr.id_address = i.rowid
                WHERE
                    mr.id_request = ${_request_ref[MATCH_REQUEST_ID]}
                    AND
                    fr.is_match_element_ok(me.matched_element)
            ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
        " \
        --output ${_vars_ref[EXPORT_PATH]} &&
    mv $_sql_file $POW_DIR_ARCHIVE/export_${_request_ref[MATCH_REQUEST_ID]}.sql || return $ERROR_CODE

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
        export_in_columns:Liste des données entrantes à inclure dans la sortie;
        export_more_columns:Liste des données supplémentaires à inclure dans la sortie;
        export_path:Fichier de sortie;
        export_srid:code SRID des géométries dans la sortie;
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
        export_srid:4326;
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
    [COLUMNS_IN]="${get_arg_export_in_columns^^}"
    [COLUMNS_MORE]="${get_arg_export_more_columns^^}"
    [EXPORT_PATH]="$get_arg_export_path"
)

_k=0
MATCH_REQUEST_ID=$((_k++))                  # ID de la demande
MATCH_REQUEST_IMPORT=$((_k++))              # table d'import des données
MATCH_REQUEST_ITEMS=$_k
declare -a match_request

MATCH_STEPS=IMPORT,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT,EXPORT,REPORT
declare -a match_steps
[ "${match_vars[STEPS]}" = ALL ] && match_vars[STEPS]=$MATCH_STEPS
match_steps=( ${match_vars[STEPS]//,/ } )
declare -A match_steps_info=(
    [IMPORT]=Chargement
    [STANDARDIZE]=Standardisation
    [MATCH_CODE]='Calcul MATCH CODE'
    [MATCH_ELEMENT]='Rapprochement ELEMENT'
    [EXPORT]='Adresses rapprochées'
    [REPORT]=Rapport
)

# columns to report
# w/ predefined aliases: i for input, a for address and t for territory
declare -A match_in_columns=(
    [ROWID]=i.rowid
)
declare -A match_more_columns=(
    # ADDRESS
    [CEA]=a.co_adr
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
    [GEOMETRY_X]='ST_X(ST_Transform(a.gm_coord, '$get_arg_export_srid'))'
    [GEOMETRY_Y]='ST_Y(ST_Transform(a.gm_coord, '$get_arg_export_srid'))'
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

_k=0
declare -a match_columns_order=(
    # IN
    [$((_k++))]=ROWID
    # ADDRESS
    [$((_k++))]=CEA
    [$((_k++))]=COMPLEMENT
    [$((_k++))]=HOUSENUMBER
    [$((_k++))]=EXTENSION
    [$((_k++))]=STREET
    [$((_k++))]=OLD_MUNICIPALITY
    [$((_k++))]=POSTCODE
    [$((_k++))]=MUNICIPALITY
    # LA POSTE
    [$((_k++))]=QL
    [$((_k++))]=ROC
    [$((_k++))]=REGATE
    # XY
    [$((_k++))]=LOCALISATION
    [$((_k++))]=GEOMETRY_X
    [$((_k++))]=GEOMETRY_Y
    # HIERARCHY
    [$((_k++))]=COM
    [$((_k++))]=CV
    [$((_k++))]=ARR
    [$((_k++))]=EPCI
    [$((_k++))]=DEP
    [$((_k++))]=REG
    [$((_k++))]=PDC
    [$((_k++))]=PPDC
    [$((_k++))]=DEX
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
    [ -z "${match_vars[EXPORT_PATH]}" ] && match_vars[EXPORT_PATH]="$POW_DIR_ARCHIVE/export_${match_request[MATCH_REQUEST_ID]}.csv"

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
    in_array --array match_steps --item EXPORT && {
        match_info --steps_info match_steps_info --step EXPORT &&
        declare -a _list_in _list_more &&
        declare -a match_columns_in=( ${match_vars[COLUMNS_IN]//,/ } ) &&
        #echo "IN: (${get_arg_export_in_columns}) $(declare -p match_columns_in)" &&
        export_get_columns \
            --columns_set IN \
            --columns_user match_columns_in \
            --columns_default match_in_columns \
            --columns_todo _list_in &&
        declare -a match_columns_more=( ${match_vars[COLUMNS_MORE]//,/ } ) &&
        #echo "MORE: (${get_arg_export_more_columns}) $(declare -p match_columns_more)" &&
        export_get_columns \
            --columns_set MORE \
            --columns_user match_columns_more \
            --columns_default match_more_columns \
            --columns_todo _list_more &&
        export_get_entry \
            --request match_request \
            --vars match_vars \
            --entry _entry &&
        export_build \
            --columns_in _list_in \
            --columns_more _list_more \
            --table_name ${_entry} \
            --request match_request \
            --vars match_vars || {
            log_error 'gestion des colonnes du rapport en erreur!'
            false
        }
    } || true
} &&

{
    [ "${match_vars[VERBOSE]}" = yes ] && log_info "archive: ${POW_DIR_ARCHIVE}" || true
} || exit $ERROR_CODE

exit $SUCCESS_CODE
