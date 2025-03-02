#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match BAL addresses w/ LAPOSTE ones

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
bal_print_progress() {
    case "${1^^}" in
    BEGIN)
        #expect argc bal_print_progress $# 6 || return $ERROR_CODE
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

bal_set_progress() {
    local _elapsed

    get_elapsed_time --start ${bal_vars[PROGRESS_START]} --result _elapsed &&
    bal_print_progress END "${_elapsed}" &&
    bal_vars[PROGRESS_START]=$(date '+%s')

    return $SUCCESS_CODE
}

bal_check_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune
        ' \
        --args_m '
            code
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _valid _error

    execute_query \
        --name "BAL_MUNICIPALITY_${_opts[CODE]}" \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.bal_municipality
                WHERE code = '${_opts[CODE]}'
            )" \
        --return _valid &&
    {
        [ "$_valid" = t ] || {
            execute_query \
                --name "LAPOSTE_MUNICIPALITY_${_opts[CODE]}" \
                --query "
                    SELECT EXISTS(
                        SELECT 1 FROM fr.laposte_address_area
                        WHERE co_insee_commune = '${_opts[CODE]}' AND fl_active
                    )" \
                --return _valid &&
            {
                case "$_valid" in
                f)  _error="code Commune '${_opts[CODE]}' non valide!"                          ;;
                t)  _error="Import préalable de l'ensemble des Communes (--municipality ALL)"   ;;
                esac
                log_error "$_error"
                false
            }
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# prepare municipality
bal_set_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune
        ' \
        --args_m '
            code
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _tmp _info
    local -a _array

    {
        case "${bal_vars[USECASE]}" in
        IMPORT)
            _info=Import
            ;;
        MATCH)
            _info=Rapprochement
            # reset
            bal_vars[STREETS]=-1
            bal_vars[HOUSENUMBERS]=-1
            bal_vars[IO_LAST_ID]=
            bal_vars[IO_LAST_END]=
            bal_vars[IO_LAST_ATTRIBUTES]=
            ;;
        esac
        log_info "$_info BAL (${_opts[CODE]})" &&
        {
            [ "${bal_vars[PROGRESS]}" = no ] || {
                bal_vars[PROGRESS_START]=$(date '+%s') &&
                bal_print_progress \
                    BEGIN \
                    "INSEE ${_opts[CODE]}" \
                    ${bal_vars[PROGRESS_SIZE]} \
                    ${bal_vars[PROGRESS_CURRENT]} \
                    ${bal_vars[PROGRESS_TOTAL]} \
                    '\r'
            }
        }
    } &&
    {
        [ "${bal_vars[USECASE]}" = MATCH ] || {
            # count areas (w/ old municipality owning at least one address)
            execute_query \
                --name "LAPOSTE_MUNICIPALITY_${_opts[CODE]}_AREAS" \
                --query "
                    SELECT
                        COUNT(1)
                    FROM
                        fr.laposte_address_area a
                    WHERE
                        fl_active
                        AND
                        co_insee_commune = '${_opts[CODE]}'
                        AND
                        lb_l5_nn IS NOT NULL
                        AND
                        EXISTS(
                            SELECT 1
                            FROM
                                fr.laposte_address r
                            WHERE
                                r.co_cea_za = a.co_cea
                                AND
                                r.fl_active
                                AND
                                r.co_cea_voie IS NOT NULL
                        )
                " \
                --return bal_vars[AREAS_OLD_MUNICIPALITY]
        }
    } &&
    {
        execute_query \
            --name "BAL_MUNICIPALITY_${_opts[CODE]}_LAST_IO" \
            --query "
                SELECT id, date_data_end, attributes
                FROM get_last_io('BAL_${_opts[CODE]}')
            " \
            --return _tmp &&
        {
            [ -z "$_tmp" ] || {
                IFS='|' read -a _array <<< "$_tmp"

                bal_vars[IO_LAST_ID]=${_array[0]}
                bal_vars[IO_LAST_END]=${_array[1]}
                bal_vars[IO_LAST_ATTRIBUTES]=${_array[2]}
            }
        }
    } &&
    {
        [ "${bal_vars[USECASE]}" = MATCH ] || {
            # check levels
            (is_yes --var bal_vars[LEVEL_MUNICIPALITY]) || {
                # last IO w/ municipality level ?
                _tmp=$(jq --raw-output '.integration.levels // empty' <<< "${bal_vars[IO_LAST_ATTRIBUTES]}")
                [[ "$(expr index "${_tmp}" M)" -gt 0 ]] || {
                    log_error "étape Commune ${_opts[CODE]} (--levels MSN|MS) est nécessaire!"
                    false
                }
            }
        }
    } &&
    {
        [ "${bal_vars[USECASE]}" = MATCH ] || {
            execute_query \
                --name "BAL_MUNICIPALITY_${_opts[CODE]}_ROWS" \
                --query "
                    SELECT
                        (areas + streets) streets,
                        housenumbers_auth
                    FROM fr.bal_municipality
                    WHERE code = '${_opts[CODE]}'
            " \
            --return _tmp &&
            {
                IFS='|' read -a _array <<< "$_tmp"

                bal_set_rows \
                    --streets ${_array[0]} \
                    --housenumbers ${_array[1]} \
                    --total bal_vars[IO_ROWS]
            }
        }
    }
    {
        [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
    } || return $ERROR_CODE

#     [ "${bal_vars[VERBOSE]}" = yes ] && {
#         echo '###Contexte'
#         declare -p bal_vars
#         echo
#     }

    return $SUCCESS_CODE
}

# select municipalities (w/ criteria & order) from summary
bal_list_municipalities() {
    bash_args \
        --args_p '
            list:Liste résultat
        ' \
        --args_o '
            list
        ' \
        "$@" || return $ERROR_CODE

    local -n _list_ref=$get_arg_list
    local _query _list _date_before_fix

    case "${bal_vars[FIX]:-${bal_vars[SELECT_CRITERIA]}}" in
    POPULATION)
        _query="
            SELECT
                codgeo municipality,
                population criteria
            FROM
                fr.territory
            WHERE
                nivgeo = 'COM'
                AND
                population IS NOT NULL
        "
        ;;
    STREETS)
        _query="
            SELECT
                co_insee_commune municipality,
                COUNT(DISTINCT co_voie) criteria
            FROM
                fr.laposte_address_street
            WHERE
                fl_active
            GROUP BY
                co_insee_commune
        "
        ;;
    REVISION)
        _query="
            SELECT
                code municipality,
                last_update criteria
            FROM
                fr.bal_municipality
        "
        ;;
    SPACE_IN_CODE)
        _date_before_fix='2025-01-01'
        _query="
            SELECT
                m.code municipality,
                m.code criteria
            FROM
                fr.bal_municipality m
            WHERE
                EXISTS(
                    SELECT 1
                    FROM
                        fr.bal_street s
                            JOIN fr.bal_housenumber n ON n.id_street = s.id
                    WHERE
                        s.id_municipality = m.id
                        AND
                        POSITION(' ' IN n.code) > 0
                )
        "
        ;;
    CONVERT_ATTRIBUTES)
        _date_before_fix='2025-01-01'
        _query="
            SELECT
                SUBSTR(l.name, 5) municipality,
                SUBSTR(l.name, 5) criteria
            FROM
                io_history io
                    JOIN get_last_io(io.name) l ON io.id = l.id
            WHERE
                io.name ~ '^BAL_[0-9]'
                AND
                l.attributes ~ '"'"'"STREETS"'"'" => [0-9]*, "'"'"HOUSENUMBERS_AUTH"'"'" => [0-9]*'
        "
        ;;
    MORE_ATTRIBUTES)
        _date_before_fix='2025-02-08'
        _query="
            SELECT
                SUBSTR(l.name, 5) municipality,
                SUBSTR(l.name, 5) criteria
            FROM
                io_history io
                    JOIN get_last_io(io.name) l ON io.id = l.id
            WHERE
                io.name ~ '^BAL_[0-9]'
                AND
                io.attributes IS JSON OBJECT
                AND
                (io.attributes::JSONB)->'integration'->>'levels' IS NULL
        "
        ;;
    esac &&
    _query="
        WITH
        history AS (
            SELECT
                SUBSTR(l.name, 5) municipality,
                l.date_data_end,
                l.attributes
            FROM
                io_history io
                    JOIN get_last_io(io.name) l ON io.id = l.id
            WHERE
                io.name ~ '^BAL_[0-9]'
        )
        , criteria AS (
            $_query
        )
        SELECT ARRAY(
            SELECT
                c.municipality
            FROM
                criteria c
                    JOIN fr.bal_municipality m ON c.municipality = m.code
                    LEFT OUTER JOIN history h ON h.municipality = c.municipality
            WHERE
    "
    [ -n "${bal_vars[FIX]}" ] && {
        _query+="
                h.date_data_end IS NOT NULL
                AND
                h.date_data_end < '$_date_before_fix'::DATE
                AND
                POSITION('${bal_vars[FIX]}' IN h.attributes) = 0
        "
    } || {
        case "${bal_vars[USECASE]}" in
        # only not already downloaded or newer import available
        IMPORT)
            _query+="
                    h.date_data_end IS NULL
                    OR
                    m.last_update > h.date_data_end
            "
            ;;
        # only already downloaded, but not matched yet (w/ at least 1 street)
        MATCH)
            _query+="
                    h.date_data_end IS NOT NULL
                    AND
                    h.attributes IS JSON OBJECT
                    AND
                    'match' NOT IN (
                        SELECT (JSON_ARRAY_ELEMENTS((h.attributes::JSON)->'usecases'))->>'name'
                    )
                    AND
                    ((h.attributes::JSON)->'integration'->>'streets')::INT > 0
            "
            ;;
        esac
    }
    _query+="
            ORDER BY
                c.criteria ${bal_vars[SELECT_ORDER]}
    " &&
    {
        [[ ${bal_vars[LIMIT]} -eq 0 ]] || {
            _query+="
                LIMIT
                    ${bal_vars[LIMIT]}
            "
        }
    } &&
    _query+=")" &&
    execute_query \
        --name BAL_MUNICIPALITIES \
        --query "$_query" \
        --return _list &&
    array_sql_to_bash --array_sql "$_list" --array_bash _list_ref || return $ERROR_CODE

    return $SUCCESS_CODE
}

bal_match_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune;
            io_id:ID dernier historique
        ' \
        --args_m '
            code;io_id
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _query=${bal_vars[QUERY_ADDRESSES]/XXXXX/${_opts[CODE]}} _request_id

    echo "INSEE ${_opts[CODE]}" &&
    # get request-ID
    set_log_echo no &&
    _request_id=$($POW_DIR_BATCH/address_match.sh \
        --source_name BAL_${_opts[CODE]} \
        --source_query "$_query" \
        --only_info ID) &&
    set_log_echo yes &&
    # match addresses
    $POW_DIR_BATCH/address_match.sh \
        --source_name BAL_${_opts[CODE]} \
        --source_query "$_query" \
        --steps STANDARDIZE,MATCH_CODE,MATCH_ELEMENT \
        --format "$POW_DIR_BATCH/bal/format.sql" \
        --force ${bal_vars[FORCE]} &&
    # update history
    io_history_update \
        --infos '{"usecases":[{"name":"match","id":'${_request_id}'}]}' \
        --id ${_opts[IO_ID]} || return $ERROR_CODE

    return $SUCCESS_CODE
}

declare -A bal_vars=(
    [USECASE]=MATCH
    [FIX]=
    [IO_NAME]=
    [IO_ID]=
    [IO_BEGIN]=
    [IO_END]="$(date +%F)"
    [IO_END_EPOCH]=
    [IO_ROWS]=0
    [IO_LAST_ID]=
    [IO_LAST_END]=
    [IO_LAST_ATTRIBUTES]=
    [FILE_NAME]=
    [TABLE_NAME]=
    [STOP_TIME]=
    [AREAS_OLD_MUNICIPALITY]=0
    [STREETS]=-1
    [HOUSENUMBERS]=-1
    [PROGRESS_START]=
    [PROGRESS_CURRENT]=1
    [PROGRESS_TOTAL]=1
    [PROGRESS_SIZE]=5
    [QUERY_ADDRESSES]=
) &&
pow_argv \
    --args_n '
        municipality:Code Commune INSEE à traiter (ou ALL pour traiter la liste complète);
        select_criteria:Sélection des Communes;
        select_order:Ordre de sélection des Communes;
        limit:Limiter à n communes (0 sans limite);
        stop_time:Temps d arrêt du traitement (format: MM-jj-hh:mm:ss);
        force:Forcer le traitement même si celui-ci a déjà été fait;
        dry_run:Simuler le traitement;
        progress:Afficher le ratio de progression;
        parallel:Obtenir les addresses en parallèle;
        clean:Effectuer la purge des fichiers temporaires;
        verbose:Ajouter des détails sur les traitements
    ' \
    --args_m '
        municipality
    ' \
    --args_v '
        select_criteria:REVISION|POPULATION|STREETS;
        select_order:ASC|DESC;
        force:yes|no;
        dry_run:yes|no;
        progress:yes|no;
        parallel:yes|no;
        clean:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        select_criteria:REVISION;
        select_order:DESC;
        force:no;
        dry_run:no;
        limit:3;
        stop_time:0;
        progress:no;
        parallel:yes;
        clean:yes;
        verbose:no
    ' \
    --args_p '
        RESET:no
    ' \
    --pow_argv bal_vars "$@" || exit $ERROR_CODE

export -f bal_match_municipality
bal_vars[MUNICIPALITY_CODE]=${bal_vars[MUNICIPALITY]^^}
declare -a bal_codes=()
declare -A bal_ids=()
bal_start=$(date '+%s')
# reset LIMIT if STOP_TIME
[ "${bal_vars[STOP_TIME]}" != 0 ] && [ ${bal_vars[LIMIT]} -gt 0 ] && bal_vars[LIMIT]=0
set_env --schema_name fr &&
{
    [ "${bal_vars[PROGRESS]}" = no ] || set_log_echo no
} &&
{
    execute_query \
        --name BAL_ADDRESSES \
        --query "
            SELECT q FROM fr.bal_municipality_addresses(code => 'XXXXX')
        " \
        --return bal_vars[QUERY_ADDRESSES] &&
    case "${bal_vars[MUNICIPALITY_CODE]}" in
    ALL)
        bal_list_municipalities --list bal_codes
        ;;
    *)
        bal_check_municipality --code "${bal_vars[MUNICIPALITY_CODE]}" &&
        bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
        ;;
    esac
} || exit $ERROR_CODE

bal_error=0
bal_vars[PROGRESS_TOTAL]=${#bal_codes[@]}
for ((bal_i=0; bal_i<${#bal_codes[@]}; bal_i++)); do
    [ "${bal_vars[STOP_TIME]}" != 0 ] && {
        # stop loop if allowed time is expired
        [[ "$(date +'%m-%d-%T')" > "${bal_vars[STOP_TIME]}" ]] && break
    }

    bal_vars[PROGRESS_CURRENT]=$((bal_i +1))
    bal_set_municipality --code "${bal_codes[$bal_i]}" || {
        bal_error=1
        continue
    }

    bal_vars[MUNICIPALITY_CODE]=${bal_codes[$bal_i]}
    [ "${bal_vars[DRY_RUN]}" = yes ] || {
#         # get request-ID
#         set_log_echo no &&
#         bal_ids[${bal_vars[MUNICIPALITY_CODE]}]=$($POW_DIR_BATCH/address_match.sh \
#             --source_name BAL_${bal_vars[MUNICIPALITY_CODE]} \
#             --source_query "${bal_vars[QUERY_ADDRESSES]/XXXXX/${bal_vars[MUNICIPALITY_CODE]}}" \
#             --only_info ID)
#         _rc=$?
#         set_log_echo yes
#         [[ $_rc -ne 0 ]] && {
#             ((bal_error++))
#             continue
#         }

        # match BAL by block of 3 municipalities
        #sem --jobs 1 --id bal_match
        bal_match_municipality \
            --code ${bal_vars[MUNICIPALITY_CODE]} \
            --io_id ${bal_vars[IO_LAST_ID]} || ((bal_error++))
    }
done
#[ "${bal_vars[DRY_RUN]}" = yes ] || sem --wait

[ "${bal_vars[DRY_RUN]}" = no ] &&
[ "${bal_vars[PROGRESS_CURRENT]}" -gt 3 ] && {
    vacuum \
        --schema_name fr \
        --table_name address_match_request,address_match_code,address_match_element,address_match_result \
        --mode ANALYZE || bal_error=1
}

_rc=$(( bal_error != 0 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
