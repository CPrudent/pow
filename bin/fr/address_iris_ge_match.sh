#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match LAPOSTE addresses w/ IRIS-GE

    # DEBUG session
    # export POW_DEBUG_JSON='{"codes":[{"name":"iris_list_municipalities","steps":["func","sql@break","list@break"]},{"name":"address_iris_ge_match","steps":["count@break","io_begin@break","match@break","error@break","io_end@break"]}]}'

# print progress as (ratio, percent)
# $1= begin
    # $2= label
    # $3= size of (number of digits)
    # $4= subscript
    # $5= total
    # $6= end of line
# $1= end
    # $2= elapsed time
    # $3= more information
iris_print_progress() {
    case "${1^^}" in
    BEGIN)
        # if main display (municipality level) and only one then reduce informations
        ([[ "${2:0:5}" =~ INSEE|Commu|Temps ]] && [[ $5 -eq 1 ]]) && {
            printf '%-15s%b' "$2" $6
        } || {
            printf '%-15s\t%*d/%*d (%3d%%)%b' "$2" $3 $4 $3 $5 $((($4*100)/$5)) $6
        }
        ;;
    END)
        printf "\t\t\t\t\t%s\t\t%s\n" "$2" "$3"
        ;;
    esac

    return $SUCCESS_CODE
}

iris_set_progress() {
    local _elapsed

    get_elapsed_time --start ${global_vars[PROGRESS_START]} --result _elapsed &&
    iris_print_progress END "${_elapsed}" &&
    global_vars[PROGRESS_START]=$(date '+%s')

    return $SUCCESS_CODE
}

iris_context_init() {
    local _error

    execute_query \
        --name IRIS_MATCH_VERSION \
        --query "SELECT fr.get_match_iris_ge_version()" \
        --return global_vars[IRIS_MATCH_VERSION] &&
    execute_query \
        --name IRIS_ID \
        --query "SELECT (get_last_io('FR-TERRITORY-IGN-IRIS-GE')).id" \
        --return global_vars[IRIS_ID] &&
    {
        [ -n "${global_vars[IRIS_ID]}" ] || {
            _error="Vous devez d'abord générer le référentiel IRIS-GE"
            false
        }
    } &&
    execute_query \
        --name IRIS_DATE \
        --query "SELECT (get_last_io('FR-TERRITORY-IGN-IRIS-GE')).date_data_end" \
        --return global_vars[IRIS_DATE] || {
        [ -n "$_error" ] && log_error "$_error"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# select municipalities (w/ criteria & order) to match
iris_list_municipalities() {
    local -A _opts &&
    pow_argv \
        --args_n '
            list:Liste résultat
        ' \
        --args_m '
            list
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _list_ref=${_opts[LIST]}
    local _query _list

    # DEBUG steps
    declare -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'func argv sql list'

    [[ ${_debug_steps[func]:-1} -eq 0 ]] && {
        echo ${FUNCNAME[0]}
        [[ ${_debug_bps[func]} -eq 0 ]] && read
    }
    [[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
        declare -p _opts
        [[ ${_debug_bps[argv]} -eq 0 ]] && read
    }

    case "${global_vars[SELECT_CRITERIA]}" in
    POPULATION)
        _query="
            SELECT
                codgeo municipality,
                population criteria
            FROM
                fr.territory
            WHERE
                nivgeo = 'COM'
        "
        ;;
    esac &&
    _query="
        WITH
        criteria AS (
            $_query
        ),
        todo AS (
            SELECT
                c.municipality,
                m.mode
            FROM
                criteria c
                    CROSS JOIN fr.get_match_iris_ge_mode(
                        municipality => c.municipality
                    ) m
        )
        SELECT ARRAY(
            SELECT
                c.municipality
            FROM
                criteria c
                    JOIN todo d ON c.municipality = d.municipality
            WHERE
                d.mode = '${global_vars[IRIS_MODE]}'
    " &&
    _query+="
            ORDER BY
                c.criteria ${global_vars[SELECT_ORDER]}
    " &&
    {
        [[ ${global_vars[LIMIT]} -eq 0 ]] || {
            _query+="
                LIMIT
                    ${global_vars[LIMIT]}
            "
        }
    } &&
    _query+=")" &&
    {
        [[ ${_debug_steps[sql]:-1} -ne 0 ]] || {
            echo "query=[$_query]"
            [[ ${_debug_bps[sql]} -ne 0 ]] || read
        }
    } &&
    execute_query \
        --name LAPOSTE_MUNICIPALITIES \
        --query "$_query" \
        --return _list &&
    {
        [[ ${_debug_steps[list]:-1} -ne 0 ]] || {
            echo "list=($_list)"
            [[ ${_debug_bps[list]} -ne 0 ]] || read
        }
    } &&
    array_sql_to_bash --array_sql "$_list" --array_bash _list_ref || return $ERROR_CODE

    return $SUCCESS_CODE
}

iris_check_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune
        ' \
        --args_m '
            code
        ' \
        --pow_argv _opts "$@" || return $?

    local _valid _error _info

    execute_query \
        --name "LAPOSTE_MUNICIPALITY_${_opts[CODE]}" \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.laposte_address_area
                WHERE co_insee_commune = '${_opts[CODE]}' AND fl_active
            )" \
        --return _valid &&
    {
        [ "$_valid" = t ] || {
            _error="code Commune '${_opts[CODE]}' non valide!"
            false
        }
    } &&
    execute_query \
        --name "IRIS_MUNICIPALITY_${_opts[CODE]}" \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.laposte_municipality_vs_iris_ge
                WHERE iris = '${_opts[CODE]}'
            )" \
        --return _valid &&
    {
        [ "$_valid" = t ] || {
            _info="code Commune '${_opts[CODE]}' absent des données IRIS"
            log_info "$_info"
        }
    } || {
        [ -n "$_error" ] && log_error "$_error"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# prepare history context (IO name, begin/end dates)
iris_history_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune;
            name:Nom IO historique;
            date_begin:Date Début historique;
            date_end:Date Fin historique
        ' \
        --args_m '
            code;name;date_begin;date_end
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _name_ref=${_opts[NAME]}
    local -n _date_begin_ref=${_opts[DATE_BEGIN]}
    local -n _date_end_ref=${_opts[DATE_END]}
    local _date_begin _date_end _error

    _name_ref=LAPOSTE_${_opts[CODE]}_IRIS_GE &&
    case ${global_vars[IRIS_MODE]} in
    DELTA)
        execute_query \
            --name "BEGIN_MUNICIPALITY_${_opts[CODE]}" \
            --query "
                SELECT (get_last_io('$_name_ref')).date_data_end
            " \
            --return _date_begin &&
        {
            [ -n "$_date_begin" ] || {
                _error="Début historique '${_opts[CODE]}' vide!"
                false
            }
        }
        ;;
    esac &&
    _date_begin_ref=${_date_begin:-1970-01-01} &&
    execute_query \
        --name "END_MUNICIPALITY_${_opts[CODE]}" \
        --query "
            SELECT MAX(dt_reference)
            FROM fr.laposte_address_xy
            WHERE co_insee = '${_opts[CODE]}'
        " \
        --return _date_end &&
    _date_end_ref=${_date_end:-${global_vars[IRIS_DATE]}} || {
        [ -n "$_error" ] && log_error "$_error"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

declare -A global_vars=(
    [STOP_TIME]=
    [PROGRESS_START]=
    [PROGRESS_CURRENT]=1
    [PROGRESS_TOTAL]=1
    [PROGRESS_SIZE]=5
    [IRIS_MATCH_VERSION]=
    [IRIS_ID]=
    [IRIS_DATE]=
    [IRIS_MODE]=
) &&
pow_argv \
    --args_n '
        municipality:Code Commune INSEE à traiter (ou ALL pour traiter la liste complète);
        select_criteria:Sélection des Communes;
        select_order:Ordre de sélection des Communes;
        limit:Limiter à n communes (0 sans limite);
        stop_time:Temps d arrêt du traitement (format: MM-jj-hh:mm:ss);
        force_init:Forcer le traitement en mode INIT;
        dry_run:Simuler le traitement;
        progress:Afficher le ratio de progression;
        parallel:Effectuer les traitements en parallèle;
        parallel_chunk:Quantité de partage des données à traiter;
        parallel_jobs:Nombre de traitements en parallèle;
        clean:Effectuer la purge des fichiers temporaires;
        verbose:Ajouter des détails sur les traitements
    ' \
    --args_m '
        municipality
    ' \
    --args_v '
        select_criteria:POPULATION;
        select_order:ASC|DESC;
        force_init:yes|no;
        dry_run:yes|no;
        progress:yes|no;
        parallel:yes|no;
        clean:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        select_criteria:POPULATION;
        select_order:DESC;
        force_init:no;
        dry_run:no;
        limit:3;
        stop_time:0;
        progress:yes;
        parallel:yes;
        parallel_chunk:5;
        parallel_jobs:5;
        clean:yes;
        verbose:no
    ' \
    --args_p '
        reset:no;
        tag:select_criteria@1N,select_order:1N,force_init@bool,dry_run@bool,progress@bool,parallel@bool,clean@bool,verbose@bool,limit@int,parallel_chunk@int,parallel_jobs@int
    ' \
    --pow_argv global_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv count history limit io_begin match error nrows io_end'

[[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
    declare -p global_vars
    [[ ${_debug_bps[argv]} -eq 0 ]] && read
}

global_vars[MUNICIPALITY_CODE]=${global_vars[MUNICIPALITY]^^}
declare -a laposte_codes=()
declare -a laposte_codes2=()
declare -A laposte_histories=()
# reset LIMIT if STOP_TIME
[ "${global_vars[STOP_TIME]}" != 0 ] && [ ${global_vars[LIMIT]} -gt 0 ] && global_vars[LIMIT]=0
set_env --schema_name fr &&
{
    [ "${global_vars[PROGRESS]}" = no ] || set_log_echo no
} &&
{
    [ "${global_vars[STOP_TIME]}" = 0 ] || {
        log_info "Durée de traitement allouée jusqu'à ${global_vars[STOP_TIME]}"
    }
} &&
{
    iris_context_init &&
    case "${global_vars[MUNICIPALITY_CODE]}" in
    ALL)
        declare -a iris_modes=('INIT' 'DELTA')
        for ((laposte_i=0; laposte_i<${#iris_modes[@]}; laposte_i++)); do
            global_vars[IRIS_MODE]=${iris_modes[$laposte_i]}
            iris_list_municipalities --list laposte_codes || exit $ERROR_CODE

            [[ ${_debug_steps[count]:-1} -eq 0 ]] && {
                echo "${global_vars[IRIS_MODE]}=(${#laposte_codes[@]})"
                declare -p laposte_codes
                [[ ${_debug_bps[count]} -eq 0 ]] && read
            }
            # first run all municipalities as INIT, then as DELTA if needed
            [ ${#laposte_codes[@]} -gt 0 ] && break
            [ "${global_vars[FORCE_INIT]}" = yes ] && break
        done
        # finally nothing todo ?
        [ ${#laposte_codes[@]} -gt 0 ] || {
            [ "${global_vars[PROGRESS]}" = no ] || set_log_echo yes
            case ${global_vars[FORCE_INIT]} in
            yes)    _info='IRISation en mode INIT est déjà complète!'   ;;
            no)     _info='IRISation déjà à jour!'                      ;;
            esac
            log_info "$_info"
            exit $SUCCESS_CODE
        }
        ;;
    *)
        iris_check_municipality --code "${global_vars[MUNICIPALITY_CODE]}" &&
        laposte_codes[0]=${global_vars[MUNICIPALITY_CODE]}
        ;;
    esac
} || exit $ERROR_CODE

laposte_error=0
global_vars[PROGRESS_TOTAL]=${#laposte_codes[@]}

if [ "${global_vars[PARALLEL]}" = no ]; then
    for ((laposte_i=0; laposte_i<${#laposte_codes[@]}; laposte_i++)); do
        [ "${global_vars[STOP_TIME]}" != 0 ] && {
            # stop loop if allowed time is expired
            [[ "$(date +'%m-%d-%T')" > "${global_vars[STOP_TIME]}" ]] && break
        }

        [ "${global_vars[PROGRESS]}" = no ] || {
            global_vars[PROGRESS_START]=$(date '+%s') &&
            echo "INSEE ${laposte_codes[$laposte_i]}"
        }

        global_vars[PROGRESS_CURRENT]=$((laposte_i +1))
        global_vars[MUNICIPALITY_CODE]=${laposte_codes[$laposte_i]}

        [ "${global_vars[DRY_RUN]}" = yes ] || {
            iris_history_municipality \
                --code ${global_vars[MUNICIPALITY_CODE]} \
                --name laposte_io_name \
                --date_begin laposte_date_begin \
                --date_end laposte_date_end &&
            {
                [[ ${_debug_steps[history]:-1} -ne 0 ]] || {
                    echo "name=($laposte_io_name)"
                    echo "begin=($laposte_date_begin)"
                    echo "end=($laposte_date_end)"
                    [[ ${_debug_bps[history]} -ne 0 ]] || read
                }
            } &&
            io_history_begin \
                --io "$laposte_io_name" \
                --date_begin "$laposte_date_begin" \
                --date_end "$laposte_date_end" \
                --nrows_todo 1 \
                --id laposte_id &&
            execute_query \
                --name "IRIS_MATCH_${global_vars[MUNICIPALITY_CODE]}" \
                --query "
                    SELECT fr.set_laposte_address_match_iris_ge(
                        municipality => '${global_vars[MUNICIPALITY_CODE]}',
                        mode => '${global_vars[IRIS_MODE]}',
                        version => '${global_vars[IRIS_MATCH_VERSION]}',
                        iris_id => ${global_vars[IRIS_ID]}
                    )
                " \
                --return laposte_nrows &&
            io_history_end_ok \
                --nrows_processed $laposte_nrows \
                --infos '{"version":"'${global_vars[IRIS_MATCH_VERSION]}'","iris_id":'${global_vars[IRIS_ID]}'}' \
                --id ${laposte_id} &&
            io_history_update \
                --nrows_todo $laposte_nrows \
                --id ${laposte_id} || exit $ERROR_CODE

            [ "${global_vars[PROGRESS]}" = no ] || iris_set_progress
        }
    done
else
    laposte_tmpdir="$POW_DIR_TMP/$$"
    [ ! -d "$laposte_tmpdir" ] && mkdir "$laposte_tmpdir"
    laposte_limit=$(( ${#laposte_codes[@]} / global_vars[PARALLEL_CHUNK] ))
    [[ $(( ${#laposte_codes[@]} % global_vars[PARALLEL_CHUNK] )) -eq 0 ]] || ((laposte_limit++))
    laposte_serie=0
    [[ ${_debug_steps[limit]:-1} -eq 0 ]] && {
        echo "limit=($laposte_limit)"
        [[ ${_debug_bps[limit]} -eq 0 ]] && read
    }
    for ((laposte_j=0; laposte_j<$laposte_limit; laposte_j++)); do
        [ "${global_vars[STOP_TIME]}" != 0 ] && {
            # stop loop if allowed time is expired
            [[ "$(date +'%m-%d-%T')" > "${global_vars[STOP_TIME]}" ]] && break
        }

        laposte_codes2=( $(printf '%s ' ${laposte_codes[@]:((laposte_j*global_vars[PARALLEL_CHUNK])):${global_vars[PARALLEL_CHUNK]}}) )
        [ "${global_vars[PROGRESS]}" = no ] || {
            global_vars[PROGRESS_START]=$(date '+%s') &&
            echo "INSEE ${laposte_codes2[@]}"
        }

        [ "${global_vars[DRY_RUN]}" = yes ] || {
            for laposte_code in ${laposte_codes2[@]}; do
                iris_history_municipality \
                    --code $laposte_code \
                    --name laposte_io_name \
                    --date_begin laposte_date_begin \
                    --date_end laposte_date_end &&
                io_history_begin \
                    --io "$laposte_io_name" \
                    --date_begin "$laposte_date_begin" \
                    --date_end "$laposte_date_end" \
                    --nrows_todo 1 \
                    --id laposte_histories[$laposte_code] || {
                        laposte_error=1
                        log_error "Début Historique ($laposte_code)"
                        # exit if start of process (max 1 municipality done)
                        [ ${#laposte_histories[@]} -lt 2 ] && exit $ERROR_CODE
                    }
            done

            [[ ${_debug_steps[io_begin]:-1} -eq 0 ]] && {
                echo "io_begin"
                [[ ${_debug_bps[io_begin]} -eq 0 ]] && read
            }

            laposte_serie=$((laposte_serie +1))
            parallel \
                --jobs ${global_vars[PARALLEL_JOBS]} \
                --joblog $POW_DIR_ARCHIVE/parallel_${laposte_serie}_iris.log \
                $POW_DIR_BATCH/iris_match.sh \
                    --municipality {} \
                    --mode ${global_vars[IRIS_MODE]} \
                    --tmpdir "$laposte_tmpdir" \
                    --version "${global_vars[IRIS_MATCH_VERSION]}" \
                    --iris_id ${global_vars[IRIS_ID]} \
                ::: "${laposte_codes2[@]}"

            [[ ${_debug_steps[match]:-1} -eq 0 ]] && {
                echo "match"
                [[ ${_debug_bps[match]} -eq 0 ]] && read
            }

            # search for error (7: exit status)
            tail --lines +2 $POW_DIR_ARCHIVE/parallel_${laposte_serie}_iris.log | cut --fields 7 | grep --silent ^[^0]
            [ $? -eq 0 ] && {
                laposte_error=1
                [[ ${_debug_steps[error]:-1} -eq 0 ]] && {
                    tail --lines +2 $POW_DIR_ARCHIVE/parallel_${laposte_serie}_iris.log | cut --fields 7,9
                    [[ ${_debug_bps[error]} -eq 0 ]] && read
                }
            }
        }
        [ "${global_vars[PROGRESS]}" = no ] || iris_set_progress
    done

    [ "${global_vars[DRY_RUN]}" = yes ] || {
        for ((laposte_i=0; laposte_i<${#laposte_codes[@]}; laposte_i++)); do
            # equiv: [ ${laposte_histories[${laposte_codes[$laposte_i]}]+_} ]
            [[ -v laposte_histories[${laposte_codes[$laposte_i]}] ]] && {
                laposte_file="$laposte_tmpdir/IRIS_${laposte_codes[$laposte_i]}.dat"
                if [ -f "$laposte_file" ]; then
                    laposte_nrows=$(sed --silent --expression '1p' < "$laposte_file") &&
                    {
                        [[ ${_debug_steps[nrows]:-1} -ne 0 ]] || {
                            echo "nrows=($laposte_nrows)"
                            [[ ${_debug_bps[nrows]} -ne 0 ]] || read
                        }
                    } &&
                    io_history_end_ok \
                        --nrows_processed $laposte_nrows \
                        --infos '{"version":"'${global_vars[IRIS_MATCH_VERSION]}'","iris_id":'${global_vars[IRIS_ID]}'}' \
                        --id ${laposte_histories[${laposte_codes[$laposte_i]}]} &&
                    {
                        [[ ${_debug_steps[io_end]:-1} -ne 0 ]] || {
                            echo "io_end"
                            [[ ${_debug_bps[io_end]} -ne 0 ]] || read
                        }
                    } &&
                    io_history_update \
                        --nrows_todo $laposte_nrows \
                        --id ${laposte_histories[${laposte_codes[$laposte_i]}]} &&
                    {
                        [[ ${_debug_steps[io_end]:-1} -ne 0 ]] || {
                            echo "io_update"
                            [[ ${_debug_bps[io_end]} -ne 0 ]] || read
                        }
                    } || laposte_error=1
                else
                    io_history_end_ko \
                        --id ${laposte_histories[${laposte_codes[$laposte_i]}]}
                fi
            }
        done
        [ "${global_vars[CLEAN]}" = no ] || rm -rf "$laposte_tmpdir"
    }
fi

[ "${global_vars[DRY_RUN]}" = no ] &&
[ "${global_vars[PROGRESS_CURRENT]}" -gt 10 ] && {
    vacuum \
        --schema_name fr \
        --table_name laposte_address_match_iris_ge \
        --mode ANALYZE || laposte_error=1
}

_rc=$(( laposte_error != 0 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
