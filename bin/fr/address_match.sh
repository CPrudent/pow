#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match FR addresses

    # DEBUG session
    # export POW_DEBUG_JSON='{"codes":[{"name":"address_match","steps":["export@break"]}]}'

    # retry match (only realized beginning parts)
    # address_match.sh \
    #   --request_id ID \
    #   --request_import BAL_<INSEE> \
    #   --request_kind QUERY \
    #   --steps MATCH_ELEMENT

# log info about matching step
match_info() {
    local -A _opts &&
    pow_argv \
        --args_n '
            steps_info:Table des libellés des étapes;
            step:Entrée dans cette table
        ' \
        --args_m '
            steps_info;step
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _steps_ref=${_opts[STEPS_INFO]}

    log_info "demande de Rapprochement (étape '${_steps_ref[${_opts[STEP]}]}')"
    return $SUCCESS_CODE
}

# get defintion of property (format or parameters) w/ OS file
get_definition() {
    local -A _opts &&
    pow_argv \
        --args_n '
            property:Propriété à définir;
            vars:Entité des variables globales
        ' \
        --args_m '
            property;vars
        ' \
        --args_v '
            property:format|parameters
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _vars_ref=${_opts[VARS]}
    local _property=${_opts[PROPERTY]^^}
    local _path=${_property}_PATH _sql=${_property}_SQL

    # defined property ?
    [ -n "${_vars_ref[${_property}]}" ] && {
        # default path
        _vars_ref[$_path]="${POW_DIR_BATCH}/${_opts[PROPERTY]}.sql"
        [ -f "${_vars_ref[$_path]}" ] &&
        _vars_ref[$_sql]=$(cat "${_vars_ref[_path]}") || {
            # specific path
            [ -f "${_vars_ref[${_property}]}" ] &&
            _vars_ref[$_path]="${_vars_ref[${_property}]}" &&
            _vars_ref[$_sql]=$(cat "${_vars_ref[${_property}]}") || {
                log_error "Le fichier ${_property} ${_vars_ref[${_property}]} n'existe pas"
                return $ERROR_CODE
            }
        }
    }

    return $SUCCESS_CODE
}

# eval requested columns (by set: IN|MORE)
export_get_columns() {
    local -A _opts &&
    pow_argv \
        --args_n '
            columns_set:Ensemble des colonnes;
            columns_user:Liste des colonnes mentionnées;
            columns_default:Ensemble des colonnes disponibles;
            columns_todo:Résultat
        ' \
        --args_m '
            columns_set;columns_user;columns_default;columns_todo
        ' \
        --args_v '
            columns_set:IN|MORE
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _user_ref=${_opts[COLUMNS_USER]}
    local -n _todo_ref=${_opts[COLUMNS_TODO]}
    local _item _1st _tmp _found _i
    local -A _array_clone

    # DEBUG steps
    declare -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'func argv clone build todo'

    [[ ${_debug_steps[func]:-1} -eq 0 ]] && {
        echo ${FUNCNAME[0]}
        [[ ${_debug_bps[func]} -eq 0 ]] && read
    }
    [[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
        declare -p _opts
        [[ ${_debug_bps[argv]} -eq 0 ]] && read
    }

    # clone default list
    _tmp=$(declare -p ${_opts[COLUMNS_DEFAULT]}) &&
    {
        [[ ${_debug_steps[clone]:-1} -ne 0 ]] || {
            echo "clone=($_tmp)"
            [[ ${_debug_bps[clone]} -ne 0 ]] || read
        }
    } &&
    eval "${_tmp/${_opts[COLUMNS_DEFAULT]}=/_array_clone=}" &&
    {
        [[ ${_debug_steps[clone]:-1} -ne 0 ]] || {
            declare -p _array_clone
            [[ ${_debug_bps[clone]} -ne 0 ]] || read
        }
    }

    for _item in ${_user_ref[@]}; do
        {
            [[ ${_debug_steps[build]:-1} -ne 0 ]] || {
                echo "item=$_item clone(#${#_array_clone[@]})"
                [[ ${_debug_bps[build]} -ne 0 ]] || read
            }
        }

        _1st=${_item:0:1}
        [ "$_1st" = - ] && _item=${_item:1}
        in_array --array _array_clone --item "$_item" --search KEY
        _found=$?
        {
            [[ ${_debug_steps[build]:-1} -ne 0 ]] || {
                echo "found=($_found)"
                [[ ${_debug_bps[build]} -ne 0 ]] || read
            }
        }

        # delete column
        [ "$_1st" = - ] &&
            [[ $_found -eq 0 ]] && {
                unset '_array_clone[$_item]'
                continue
            }

        # add new column if NOT exists (only for IN set)
        [ "${_opts[COLUMNS_SET]}" = IN ] &&
            [[ $_found -eq 1 ]] &&
            # SQL as syntax : item matchs the name of column (w/ alias as uppercase)
            _todo_ref+=( "i.${_item,,} AS "'"'"${_item,,}"'"'"" )
    done
    {
        [[ ${_debug_steps[todo]:-1} -ne 0 ]] || {
            echo "${#_todo_ref[@]}: [${_todo_ref[@]}]"
            [[ ${_debug_bps[todo]} -ne 0 ]] || read
        }
    }
    # add remaining columns
    [[ ${#_array_clone[@]} -gt 0 ]] && {
        for ((_i=0; _i<${#match_columns_order[@]}; _i++)); do
            # if exists key
            [ -v _array_clone[${match_columns_order[$_i]}] ] &&
            _todo_ref+=( "${_array_clone[${match_columns_order[$_i]}]} AS "'"'"${match_columns_order[$_i],,}"'"'"" )
        done
    }
    {
        [[ ${_debug_steps[todo]:-1} -ne 0 ]] || {
            echo "${#_todo_ref[@]}: [${_todo_ref[@]}]"
            [[ ${_debug_bps[todo]} -ne 0 ]] || read
        }
    }

    return $SUCCESS_CODE
}

# eval entry (table name)
export_get_entry() {
    local -A _opts &&
    pow_argv \
        --args_n '
            request:Entité de la demande;
            vars:Entité des variables globales;
            entry:Entité des données entrantes
        ' \
        --args_m '
            request;vars;entry
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _request_ref=${_opts[REQUEST]}
    local -n _vars_ref=${_opts[VARS]}
    local -n _entry_ref=${_opts[ENTRY]}

    case ${_vars_ref[SOURCE_KIND]} in
    FILE)       _entry_ref=fr.${_request_ref[MATCH_REQUEST_IMPORT]}        ;;
    TABLE)      _entry_ref=fr.${_vars_ref[SOURCE_NAME]}                    ;;
    QUERY)      _entry_ref=${_vars_ref[SOURCE_NAME]}                       ;;
    esac

    return $SUCCESS_CODE
}

# output matched addresses
export_build() {
    local -A _opts &&
    pow_argv \
        --args_n '
            columns_in:Ensemble des colonnes entrantes (Ensemble noté IN);
            columns_more:Ensemble des colonnes supplémentaires (Ensemble noté MORE);
            table_name:Table des données entrantes;
            request:Entité de la demande;
            vars:Entité des variables globales
        ' \
        --args_m '
            columns_in;columns_more;table_name;request;vars
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _in_ref=${_opts[COLUMNS_IN]}
    local -n _more_ref=${_opts[COLUMNS_MORE]}
    local -n _request_ref=${_opts[REQUEST]}
    local -n _vars_ref=${_opts[VARS]}
    local -a _export_set=(${_vars_ref[EXPORT_SET]//,/ })
    local -a _columns_array _columns_source _columns_laposte
    local _sql_file _error _i _output _columns_list

    {
        ([[ ${#_export_set[@]} -eq 1 ]] &&
        [ "${_export_set[0]}" = SOURCE ]) || {
            if [ -z "${_vars_ref[EXPORT_MUNICIPALITY]}" ]; then
                _error="Code INSEE commune à renseigner, option --export_municipality"
                false
            else
                execute_query \
                    --name SET_CROSS_REFERENCE \
                    --query "
                        SELECT table_name
                        FROM fr.set_match_cross_reference(
                            id => ${_request_ref[MATCH_REQUEST_ID]},
                            municipality_code => '${_vars_ref[EXPORT_MUNICIPALITY]}'
                        )
                    " \
                    --temporary ${_vars_ref[TEMPORARY]} \
                    --return _vars_ref[EXPORT_TABLE] &&
                {
                    ((! in_array --array _export_set --item ONLY_SOURCE) &&
                    (! in_array --array _export_set --item ONLY_LAPOSTE)) || {
                        execute_query \
                            --name GET_CROSS_REFERENCE_COLUMNS \
                            --query "
                                SELECT get_table_columns(
                                    schema_name => 'fr',
                                    table_name => '${_vars_ref[EXPORT_TABLE]}'
                                )
                            " \
                            --temporary ${_vars_ref[TEMPORARY]} \
                            --return _columns_list &&
                        array_sql_to_bash \
                            --array_sql "$_columns_list" \
                            --array_bash _columns_array &&
                        for ((_i=0; _i<${#_columns_array[@]}; _i++)); do
                            case "${_columns_array[$_i]}" in
                            ref_*)  _columns_laposte+=(${_columns_array[$_i]})  ;;
                            *)      _columns_source+=(${_columns_array[$_i]})   ;;
                            esac
                        done
                    }
                }
            fi
        }
    } &&
    {
        [ -n "${_vars_ref[EXPORT_FOLDER]}" ] || _vars_ref[EXPORT_FOLDER]=$POW_DIR_ARCHIVE
        #/export_${match_request[MATCH_REQUEST_ID]}.csv
    } &&
    {
        for ((_i=0; _i<${#_export_set[@]}; _i++)); do
            _output=${_vars_ref[EXPORT_FOLDER]}/export_${_vars_ref[SOURCE_NAME]}_${_export_set[$_i],,}.csv
            case ${_export_set[$_i]} in
            SOURCE)
                get_tmp_file --tmpext sql --tmpfile _sql_file --create yes &&
                #log_info "SQL export: $_sql_file" &&
                {
                    [ "${_vars_ref[SOURCE_KIND]}" != QUERY ] || {
                        [ -z "${_vars_ref[SOURCE_QUERY]}" ] && {
                            execute_query \
                                --name SOURCE_QUERY \
                                --query "
                                    SELECT source_query
                                    FROM fr.address_match_request
                                    WHERE id = ${_request_ref[MATCH_REQUEST_ID]}
                                " \
                                --temporary ${_vars_ref[TEMPORARY]} \
                                --return _vars_ref[SOURCE_QUERY]
                        }
                        echo "WITH ${_vars_ref[SOURCE_NAME]} AS (${_vars_ref[SOURCE_QUERY]})" >> $_sql_file
                    }
                } &&
                echo 'SELECT' >> $_sql_file &&
                {
                    [[ ${#_in_ref[@]} -eq 0 ]] || {
                        printf '%s,\n' "${_in_ref[@]}" >> $_sql_file
                    }
                } &&
                {
                    [[ ${#_more_ref[@]} -eq 0 ]] || {
                        printf '%s,\n' "${_more_ref[@]}" | sed --expression '$s/,$//' >> $_sql_file
                    }
                } &&
                execute_query \
                    --name MATCH_EXPORT_SOURCE \
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

                                    JOIN ${_opts[TABLE_NAME]} i
                                    ON mr.id_address = i.rowid
                            WHERE
                                mr.id_request = ${_request_ref[MATCH_REQUEST_ID]}
                                AND
                                fr.is_match_element_ok(me.matched_element)
                        ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
                    " \
                    --temporary ${_vars_ref[TEMPORARY]} \
                    --output "${_output}"
                #mv $_sql_file $POW_DIR_ARCHIVE/export_${_request_ref[MATCH_REQUEST_ID]}.sql
                ;;
            ONLY_SOURCE)
                # join array items
                # https://stackoverflow.com/questions/1527049/how-can-i-join-elements-of-a-bash-array-into-a-delimited-string
                execute_query \
                    --name MATCH_EXPORT_ONLY_SOURCE \
                    --query "
                        COPY (
                            SELECT $(IFS=, ; echo "${_columns_source[*]}")
                            FROM fr.${_vars_ref[EXPORT_TABLE]}
                            WHERE ref_code_address IS NULL
                        ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
                    " \
                    --temporary ${_vars_ref[TEMPORARY]} \
                    --output "${_output}"
                ;;
            COMMON)
                set -o noglob &&
                execute_query \
                    --name MATCH_EXPORT_COMMON \
                    --query "
                        COPY (
                            SELECT *
                            FROM fr.${_vars_ref[EXPORT_TABLE]}
                            WHERE
                                ref_code_address IS NOT NULL
                                AND
                                rowid IS NOT NULL
                        ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
                    " \
                    --temporary ${_vars_ref[TEMPORARY]} \
                    --output "${_output}" &&
                set +o noglob
                ;;
            ONLY_LAPOSTE)
                execute_query \
                    --name MATCH_EXPORT_ONLY_LAPOSTE \
                    --query "
                        COPY (
                            SELECT $(IFS=, ; echo "${_columns_laposte[*]}")
                            FROM fr.${_vars_ref[EXPORT_TABLE]}
                            WHERE rowid IS NULL
                        ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
                    " \
                    --temporary ${_vars_ref[TEMPORARY]} \
                    --output "${_output}"
                ;;
            esac
        done
    } || {
        [ -n "$_error" ] && log_error "$_error"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# get counters
report_get_result() {
    local -A _opts &&
    pow_argv \
        --args_n '
            request:Entité de la demande;
            vars:Entité des variables globales;
            result:Entité du résultat
        ' \
        --args_m '
            request;vars;result
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _request_ref=${_opts[REQUEST]}
    local -n _vars_ref=${_opts[VARS]}
    local -n _result_ref=${_opts[RESULT]}
    local _counters _len _tmp

    # get counters as {#,#, ...,#} array format
    execute_query \
        --name MATCH_REPORT \
        --query "
            SELECT counters FROM fr.set_match_result(id => ${_request_ref[MATCH_REQUEST_ID]})
            " \
        --temporary ${_vars_ref[TEMPORARY]} \
        --return _counters &&
    array_sql_to_bash --array_sql "$_counters" --array_bash _result_ref || return $ERROR_CODE

    return $SUCCESS_CODE
}

# build report, printing result counters
report_build() {
    local -A _opts &&
    pow_argv \
        --args_n '
            request:Entité de la demande;
            vars:Entité des variables globales;
            result:Entité du résultat
        ' \
        --args_m '
            request;vars;result
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _request_ref=${_opts[REQUEST]}
    local -n _vars_ref=${_opts[VARS]}
    local -n _result_ref=${_opts[RESULT]}
    local _source

    printf '\n%s\n' "Demande Rapprochement (ID): ${_request_ref[MATCH_REQUEST_ID]}"
    case ${_vars_ref[SOURCE_KIND]} in
    FILE)       _source="du fichier (${_vars_ref[SOURCE_NAME]})"        ;;
    TABLE)      _source="de la table (${_vars_ref[SOURCE_NAME]})"       ;;
    QUERY)      _source="de la requête (${_vars_ref[SOURCE_QUERY]})"    ;;
    esac
    echo 'Données issues '$_source
    [ -n "${_vars_ref[SOURCE_FILTER]}" ] && {
        echo 'Filtre: '${_vars_ref[SOURCE_FILTER]}
    }
    echo
    printf '%-10s  %15s  %4s  %15s  %4s  %15s  %4s\n' \
        'Total' \
        'OK (Strict)' \
        '%' \
        'OK (Approchant)' \
        '%' \
        'KO' \
        '%'
    printf '%-10d  %15d  %4s  %15d  %4s  %15d  %4s\n\n' \
        ${_result_ref[MATCH_RESULT_TOTAL]} \
        ${_result_ref[MATCH_RESULT_OK_STRICT]/./,} \
        ${_result_ref[MATCH_RESULT_PERCENT_STRICT]/./,} \
        ${_result_ref[MATCH_RESULT_OK_NEAR]/./,} \
        ${_result_ref[MATCH_RESULT_PERCENT_NEAR]/./,} \
        ${_result_ref[MATCH_RESULT_KO]/./,} \
        ${_result_ref[MATCH_RESULT_PERCENT_KO]/./,}

    return $SUCCESS_CODE
}

declare -A match_vars=(
    [SOURCE_KIND]=
    [FORMAT_PATH]=''
    [FORMAT_SQL]=''
    [PARAMETERS_PATH]=''
    [PARAMETERS_SQL]=''
    [COLUMNS_IN]=
    [COLUMNS_MORE]=
    [TEMPORARY]=USER
) &&
pow_argv \
    --args_n '
        source_name:Nommage des Adresses à rapprocher;
        source_filter:Filtre à appliquer sur les données entrantes;
        source_kind:Source des Adresses à rapprocher;
        source_query:Requête à appliquer pour obtenir les données entrantes;
        steps:Ensemble des étapes à réaliser (séparées par une virgule, si plusieurs);
        request_id:Passer ID du Rapprochement;
        request_import:Passer nom de la table du Rapprochement;
        request_kind:Passer type du Rapprochement;
        request_path:Exporter ID et nom de la table du Rapprochement;
        format:Définition du Format des Adresses (ou fichier du Format);
        parameters:Définition des Paramètres du Rapprochement (ou fichier des Paramètres);
        import_options:Options import (du fichier) spécifiques à son type;
        import_limit:Limiter à n enregistrements;
        export_set:Ensemble des données à exporter;
        export_table:Table des données à exporter;
        export_municipality:Code INSEE commune;
        export_in_columns:Liste des données entrantes à inclure dans la sortie;
        export_more_columns:Liste des données supplémentaires à inclure dans la sortie;
        export_folder:Dossier de sortie;
        export_srid:code SRID des géométries dans la sortie;
        parallel:Effectuer les traitements en parallèle;
        argv_exit:Afficher les arguments et Quitter;
        force:Forcer le traitement même si celui-ci a déjà été fait;
        verbose:Ajouter des détails sur les traitements
    ' \
    --args_m '
        source_name | request_id
    ' \
    --args_v '
        source_kind:FILE|TABLE|QUERY;
        export_set:SOURCE|ONLY_SOURCE|COMMON|ONLY_LAPOSTE;
        force:yes|no;
        parallel:yes|no;
        argv_exit:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        steps:ALL;
        export_set:SOURCE;
        force:no;
        request_import:NOT_DEFINED;
        request_kind:NOT_DEFINED;
        export_srid:4326;
        parallel:no;
        argv_exit:no;
        verbose:no
    ' \
    --args_p '
        reset:no;
        tag:source_kind@0N,export_set@XN,parallel@bool,argv_exit@bool,force@bool,verbose@bool
    ' \
    --pow_argv match_vars "$@" || exit $?

_k=0
MATCH_REQUEST_ID=$((_k++))                  # ID de la demande
MATCH_REQUEST_IMPORT=$((_k++))              # table d'import des données
MATCH_REQUEST_ITEMS=$_k
declare -a match_request

[ "${match_vars[PARALLEL]}" = yes ] && match_vars[TEMPORARY]=UNIQ
[ -f "${match_vars[SOURCE_QUERY]}" ] && match_vars[SOURCE_QUERY]=$(< "${match_vars[SOURCE_QUERY]}")
match_vars[STEPS]=${match_vars[STEPS]// /}
match_vars[COLUMNS_IN]=${match_vars[EXPORT_IN_COLUMNS]^^}
match_vars[COLUMNS_MORE]=${match_vars[EXPORT_MORE_COLUMNS]^^}
MATCH_STEPS=REQUEST,IMPORT,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT,EXPORT,REPORT
declare -a match_steps
[ "${match_vars[STEPS]}" = ALL ] && match_vars[STEPS]=$MATCH_STEPS
match_steps=( ${match_vars[STEPS]//,/ } )

declare -A match_steps_info=(
    [REQUEST]=Création
    [IMPORT]=Chargement
    [STANDARDIZE]=Standardisation
    [MATCH_CODE]='Calcul MATCH CODE'
    [MATCH_ELEMENT]='Rapprochement par Niveau'
    [EXPORT]='Export des Adresses rapprochées'
    [REPORT]=Rapport
)

# columns to report
# w/ predefined aliases: i for input, a for address and t for territory
declare -A match_in_columns=(
    [ROWID]=i.rowid
)
declare -A match_more_columns=(
    # ADDRESS
    [CODE_ADDRESS]=a.co_adr
    [COMPLEMENT]='COALESCE(a.lb_ligne3_normalise, a.lb_ligne3)'
    [HOUSENUMBER]=a.no_numero
    [EXTENSION]=a.lb_extension_numero
    [STREET]='COALESCE(a.lb_voie_normalise, a.lb_voie)'
    [AREA]=a.lb_ligne5
    [POSTCODE]=a.co_postal
    [MUNICIPALITY]=a.lb_acheminement
    # LA POSTE
    [QL]=a.rao_co_tournee
    [ROC]=a.co_roc_site
    [REGATE]=a.rao_co_regate
    # XY
    [LOCATION]=a.no_type_localisation_coord
    [GEOMETRY]='ST_Transform(a.gm_coord, '${match_vars[EXPORT_SRID]}')'
    [GEOMETRY_X]='ST_X(ST_Transform(a.gm_coord, '${match_vars[EXPORT_SRID]}'))'
    [GEOMETRY_Y]='ST_Y(ST_Transform(a.gm_coord, '${match_vars[EXPORT_SRID]}'))'
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
    [$((_k++))]=CODE_ADDRESS
    [$((_k++))]=COMPLEMENT
    [$((_k++))]=HOUSENUMBER
    [$((_k++))]=EXTENSION
    [$((_k++))]=STREET
    [$((_k++))]=AREA
    [$((_k++))]=POSTCODE
    [$((_k++))]=MUNICIPALITY
    # LA POSTE
    [$((_k++))]=QL
    [$((_k++))]=ROC
    [$((_k++))]=REGATE
    # XY
    [$((_k++))]=LOCATION
    [$((_k++))]=GEOMETRY
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

_k=0
MATCH_RESULT_TOTAL=$((_k++))
MATCH_RESULT_OK_STRICT=$((_k++))
MATCH_RESULT_PERCENT_STRICT=$((_k++))
MATCH_RESULT_OK_NEAR=$((_k++))
MATCH_RESULT_PERCENT_NEAR=$((_k++))
MATCH_RESULT_KO=$((_k++))
MATCH_RESULT_PERCENT_KO=$((_k++))
declare -a match_result

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv step request standardize export archive'

[[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
    declare -p match_vars
    [[ ${_debug_bps[argv]} -eq 0 ]] && read
}

set_env --schema_name fr &&

{
    [ "${match_vars[ARGV_EXIT]}" = no ] || {
        get_definition --property parameters --vars match_vars &&
        get_definition --property format --vars match_vars &&
        declare -p match_vars | tr -s ' ' '\n' | tail -n +3

        exit $SUCCESS_CODE
    }
} &&

{
    if in_array --array match_steps --item REQUEST; then
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(REQUEST)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        } &&

        {
            # determine kind of source (if unknown)
            [ -n "${match_vars[SOURCE_KIND]}" ] || {
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
            }
        } &&

        {
            [[ ${_debug_steps[request]:-1} -ne 0 ]] || {
                echo "type source=(${match_vars[SOURCE_KIND]})"
                [[ ${_debug_bps[request]} -ne 0 ]] || read
            }
        } &&

        get_definition --property parameters --vars match_vars &&

        {
            [[ ${_debug_steps[request]:-1} -ne 0 ]] || {
                echo parameters ; declare -p match_vars
                [[ ${_debug_bps[request]} -ne 0 ]] || read
            }
        } &&

        # create or get request informations (ID, import_name)
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
            --temporary ${match_vars[TEMPORARY]} \
            --return _request &&

        {
            [[ ${_debug_steps[request]:-1} -ne 0 ]] || {
                echo "request=($_request)"
                [[ ${_debug_bps[request]} -ne 0 ]] || read
            }
        } &&

        match_request=($_request) &&
        {
            # output request informations
            [ -z "${match_vars[REQUEST_PATH]}" ] || {
                echo -e "${match_request[MATCH_REQUEST_ID]}\n${match_request[MATCH_REQUEST_IMPORT]}\n${match_vars[SOURCE_KIND]}" > "${match_vars[REQUEST_PATH]}"
            }
        }
    else
        [ -n "${match_vars[REQUEST_ID]}" ] &&
        [ "${match_vars[REQUEST_IMPORT]}" != NOT_DEFINED ] &&
        [ "${match_vars[REQUEST_KIND]}" != NOT_DEFINED ] && {
            match_request[MATCH_REQUEST_ID]=${match_vars[REQUEST_ID]}
            match_request[MATCH_REQUEST_IMPORT]=${match_vars[REQUEST_IMPORT]}
            match_vars[SOURCE_KIND]=${match_vars[REQUEST_KIND]}
        } || {
            log_error 'manque informations Rapprochement (options --request*)'
            false
        }
    fi
} &&

# import todo? only for FILE input
{
    ([ "${match_vars[SOURCE_KIND]}" != FILE ] || \
    (! in_array --array match_steps --item IMPORT)) || {
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(IMPORT)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        }

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
    }
} &&

{
    (! in_array --array match_steps --item STANDARDIZE) || {
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(STANDARDIZE)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        } &&
        get_definition --property format --vars match_vars &&
        {
            [ -n "${match_vars[FORMAT_SQL]}" ] || {
                log_error 'manque définition du format (option --format)'
                false
            }
        } &&
        {
            [[ ${_debug_steps[standardize]:-1} -ne 0 ]] || {
                echo format ; declare -p match_vars
                [[ ${_debug_bps[standardize]} -ne 0 ]] || read
            }
        } &&
        match_info --steps_info match_steps_info --step STANDARDIZE &&
        execute_query \
            --name STANDARDIZE_REQUEST \
            --query "CALL fr.set_match_standardize(
                id => ${match_request[MATCH_REQUEST_ID]},
                mapping => '${match_vars[FORMAT_SQL]}'::HSTORE,
                force => ('${match_vars[FORCE]}' = 'yes'),
                raise_notice => ('${match_vars[VERBOSE]}' = 'yes')
            )" \
            --temporary ${match_vars[TEMPORARY]}
    }
} &&

{
    (! in_array --array match_steps --item MATCH_CODE) || {
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(MATCH_CODE)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        } &&
        match_info --steps_info match_steps_info --step MATCH_CODE &&
        execute_query \
            --name MATCH_CODE_REQUEST \
            --query "CALL fr.set_match_code(
                id => ${match_request[$MATCH_REQUEST_ID]},
                force => ('${match_vars[FORCE]}' = 'yes')
            )" \
            --temporary ${match_vars[TEMPORARY]}
    }
} &&

{
    (! in_array --array match_steps --item MATCH_ELEMENT) || {
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(MATCH_ELEMENT)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        } &&
        match_info --steps_info match_steps_info --step MATCH_ELEMENT &&
        execute_query \
            --name MATCH_ELEMENT_REQUEST \
            --query "CALL fr.set_match_element(
                id => ${match_request[$MATCH_REQUEST_ID]},
                force => ('${match_vars[FORCE]}' = 'yes'),
                raise_notice => ('${match_vars[VERBOSE]}' = 'yes')
            )" \
            --temporary ${match_vars[TEMPORARY]}
    }
} &&

{
    (! in_array --array match_steps --item EXPORT) || {
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(EXPORT)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        } &&
        match_info --steps_info match_steps_info --step EXPORT &&
        declare -a _list_in _list_more &&
        declare -a match_columns_in=( ${match_vars[COLUMNS_IN]//,/ } ) &&
        export_get_columns \
            --columns_set IN \
            --columns_user match_columns_in \
            --columns_default match_in_columns \
            --columns_todo _list_in &&
        {
            [[ ${_debug_steps[export]:-1} -ne 0 ]] || {
                echo IN ; declare -p match_vars match_columns_in _list_in
                [[ ${_debug_bps[export]} -ne 0 ]] || read
            }
        } &&
        declare -a match_columns_more=( ${match_vars[COLUMNS_MORE]//,/ } ) &&
        export_get_columns \
            --columns_set MORE \
            --columns_user match_columns_more \
            --columns_default match_more_columns \
            --columns_todo _list_more &&
        {
            [[ ${_debug_steps[export]:-1} -ne 0 ]] || {
                echo MORE ; declare -p match_columns_more _list_more
                [[ ${_debug_bps[export]} -ne 0 ]] || read
            }
        } &&
        export_get_entry \
            --request match_request \
            --vars match_vars \
            --entry _entry &&
        {
            [[ ${_debug_steps[export]:-1} -ne 0 ]] || {
                echo "entry=($_entry)"
                [[ ${_debug_bps[export]} -ne 0 ]] || read
            }
        } &&
        export_build \
            --columns_in _list_in \
            --columns_more _list_more \
            --table_name ${_entry} \
            --request match_request \
            --vars match_vars || {
            log_error 'export Rapprochement en erreur!'
            false
        }
    }
} &&

{
    (! in_array --array match_steps --item REPORT) || {
        {
            [[ ${_debug_steps[step]:-1} -ne 0 ]] || {
                echo "step=(REPORT)" ; declare -p match_steps
                [[ ${_debug_bps[step]} -ne 0 ]] || read
            }
        } &&
        match_info --steps_info match_steps_info --step REPORT &&
        report_get_result \
            --request match_request \
            --vars match_vars \
            --result match_result &&
        report_build \
            --request match_request \
            --vars match_vars \
            --result match_result || {
            log_error 'rapport Rapprochement en erreur!'
            false
        }
    }
} &&

{
    {
        [[ ${_debug_steps[archive]:-1} -ne 0 ]] || {
            echo "archive=(${POW_DIR_ARCHIVE})"
            [[ ${_debug_bps[archive]} -ne 0 ]] || read
        }
    }
} || exit $ERROR_CODE

exit $SUCCESS_CODE
