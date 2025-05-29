#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match BAL addresses w/ LAPOSTE ones

    # DEBUG session
    # export POW_DEBUG_JSON='{"codes":[{"name":"address_bal_match","steps":["argv","chunk","query@break","before@break"]}]}'

source $POW_DIR_ROOT/lib/libbal.sh || exit $ERROR_CODE

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
        --pow_argv _opts "$@" || return $?

    local _query=${bal_vars[QUERY_ADDRESSES]//XXXXX/${_opts[CODE]}} _request_id

    # update request (query), if fix MATCH_AGAIN_ROWID
    {
        [ "${bal_vars[FIX]}" != MATCH_AGAIN_ROWID ] || {
            execute_query \
                --name REQUEST_UPDATE_${_opts[CODE]} \
                --query "UPDATE fr.address_match_request SET
                    source_query = '$_query'
                    WHERE source_name = CONCAT('BAL_', '${_opts[CODE]}')
                "
        }
    } &&
    # match addresses
    $POW_DIR_BATCH/address_match.sh \
        --source_name BAL_${_opts[CODE]} \
        --source_query "$_query" \
        --request_path $POW_DIR_TMP/BAL_${_opts[CODE]}.dat \
        --steps REQUEST,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT \
        --format "$POW_DIR_BATCH/bal/format.sql" \
        --force ${bal_vars[FORCE]} &&
    _request_id=$(sed --silent --expression '1p' < $POW_DIR_TMP/BAL_${_opts[CODE]}.dat) &&
    # update history
    io_history_update \
        --infos '{"usecases":[{"name":"match","id":'${_request_id}'}]}' \
        --id ${_opts[IO_ID]} &&
    {
        [ "${bal_vars[CLEAN]}" = no ] || rm $POW_DIR_TMP/BAL_${_opts[CODE]}.dat
    } || return $ERROR_CODE

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
    [PROGRESS_GROUPS]=INSEE
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
        fix:Corriger une erreur;
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
        select_criteria:REVISION|POPULATION|STREETS;
        select_order:ASC|DESC;
        force:yes|no;
        fix:MATCH_AGAIN_ROWID;
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
        parallel:no;
        parallel_chunk:5;
        parallel_jobs:5;
        clean:yes;
        verbose:no
    ' \
    --args_p '
        reset:no;
        tag:select_criteria@1N,select_order:1N,fix@0N,levels@1N,force@bool,dry_run@bool,progress@bool,parallel@bool,clean@bool,verbose@bool,limit@int,parallel_chunk@int,parallel_jobs@int,fix@0N
    ' \
    --pow_argv bal_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv init chunk query before error'

[[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
    declare -p bal_vars
    [[ ${_debug_bps[argv]} -eq 0 ]] && read
}

bal_vars[MUNICIPALITY_CODE]=${bal_vars[MUNICIPALITY]^^}
declare -a bal_codes=()
declare -a bal_codes2=()
bal_start=$(date '+%s')
# reset LIMIT if STOP_TIME
[ "${bal_vars[STOP_TIME]}" != 0 ] && [ ${bal_vars[LIMIT]} -gt 0 ] && bal_vars[LIMIT]=0
set_env --schema_name fr &&
{
    [ "${bal_vars[PROGRESS]}" = no ] || set_log_echo no
} &&
{
    [ "${bal_vars[STOP_TIME]}" = 0 ] || {
        log_info "Durée de traitement allouée jusqu'à ${bal_vars[STOP_TIME]}"
    }
} &&
{
    execute_query \
        --name BAL_ADDRESSES \
        --query "SELECT q FROM fr.bal_municipality_addresses(code => 'XXXXX')" \
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
[ "${bal_vars[FIX]}" = MATCH_AGAIN_ROWID ] && bal_vars[FORCE]=yes

[[ ${_debug_steps[init]:-1} -eq 0 ]] && {
    declare -p bal_vars bal_codes
    [[ ${_debug_bps[init]} -eq 0 ]] && read
}

if [ "${bal_vars[PARALLEL]}" = no ]; then
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
            bal_match_municipality \
                --code ${bal_vars[MUNICIPALITY_CODE]} \
                --io_id ${bal_vars[IO_LAST_ID]} || ((bal_error++))

            [ "${bal_vars[PROGRESS]}" = no ] || set_progress --start bal_vars[PROGRESS_START]
        }
    done
else
    bal_tmpdir="$POW_DIR_TMP/$$"
    [ ! -d "$bal_tmpdir" ] && mkdir "$bal_tmpdir"
    bal_limit=$(( ${#bal_codes[@]} / bal_vars[PARALLEL_CHUNK] ))
    [[ $(( ${#bal_codes[@]} % bal_vars[PARALLEL_CHUNK] )) -eq 0 ]] || ((bal_limit++))
    bal_serie=0
    for ((bal_j=0; bal_j<$bal_limit; bal_j++)); do
        [ "${bal_vars[STOP_TIME]}" != 0 ] && {
            # stop loop if allowed time is expired
            [[ "$(date +'%m-%d-%T')" > "${bal_vars[STOP_TIME]}" ]] && break
        }

        bal_codes2=( $(printf '%s ' ${bal_codes[@]:((bal_j*bal_vars[PARALLEL_CHUNK])):${bal_vars[PARALLEL_CHUNK]}}) )

        [[ ${_debug_steps[chunk]:-1} -eq 0 ]] && {
            declare -p bal_codes2
            [[ ${_debug_bps[chunk]} -eq 0 ]] && read
        }

        [ "${bal_vars[PROGRESS]}" = no ] || {
            bal_vars[PROGRESS_START]=$(date '+%s') &&
            echo "INSEE ${bal_codes2[@]}"
        }

        [ "${bal_vars[DRY_RUN]}" = yes ] || {
            set -o noglob
            for bal_item in ${bal_codes2[@]}; do
                bal_insee=${bal_item%%:*}
                bal_query=${bal_vars[QUERY_ADDRESSES]//XXXXX/${bal_insee}}

                {
                    [[ ${_debug_steps[query]:-1} -ne 0 ]] || {
                        echo "tmpdir=($bal_tmpdir)"
                        echo "query=[$bal_query]"
                        [[ ${_debug_bps[query]} -ne 0 ]] || read
                    }
                } &&
                echo "$bal_query" > "$bal_tmpdir/BAL_${bal_insee}.sql" &&
                # update request (query), if force
                {
                    [ "${bal_vars[FIX]}" != MATCH_AGAIN_ROWID ] || {
                        execute_query \
                            --name REQUEST_UPDATE_$bal_insee \
                            --query "UPDATE fr.address_match_request SET
                                source_query = \$\$$bal_query\$\$
                                WHERE source_name = CONCAT('BAL_', '$bal_insee')
                            "
                    }
                } || exit $ERROR_CODE
            done
            set +o noglob

            [[ ${_debug_steps[before]:-1} -eq 0 ]] && {
                echo 'before parallel...'
                [[ ${_debug_bps[before]} -eq 0 ]] && read
            }

            bal_serie=$((bal_serie +1))
            # item composed as INSEE:IO_ID (INSEE only wanted here)
            #+ can use --tag to print each item
            parallel \
                --jobs ${bal_vars[PARALLEL_JOBS]} \
                --joblog $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log \
                --rpl '{..} s/:[^:]*$//;' \
                $POW_DIR_BATCH/address_match.sh \
                    --source_name "BAL_{..}" \
                    --source_query "$bal_tmpdir/BAL_{..}.sql" \
                    --request_path "$bal_tmpdir/BAL_{..}.dat" \
                    --steps REQUEST,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT \
                    --format "$POW_DIR_BATCH/bal/format.sql" \
                    --parallel \
                    --force ${bal_vars[FORCE]} \
                ::: "${bal_codes2[@]}"

            # search for error (column 7: exit status)
            tail --lines +2 $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log | cut --fields 7 | grep --silent ^[^0]
            [ $? -eq 0 ] && {
                bal_error=1
                [[ ${_debug_steps[error]:-1} -eq 0 ]] && {
                    tail --lines +2 $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log | cut --fields 7,9
                    [[ ${_debug_bps[error]} -eq 0 ]] && read
                }
            }

            # update BAL history w/ match request
            for ((bal_i=0; bal_i<${#bal_codes2[@]}; bal_i++)); do
                bal_insee=${bal_codes2[$bal_i]%%:*}
                bal_io_id=${bal_codes2[$bal_i]#*:}
                bal_file="$bal_tmpdir/BAL_${bal_insee}.dat"

                [ -s "$bal_file" ] && {
                    bal_req_id=$(sed --silent --expression '1p' < "$bal_file") &&
                    io_history_update \
                        --infos '{"usecases":[{"name":"match","id":'${bal_req_id}'}]}' \
                        --id ${bal_io_id}
                }
            done
        }
        bal_vars[PROGRESS_CURRENT]=$((bal_vars[PROGRESS_CURRENT] + ${#bal_codes2[@]}))
        [ "${bal_vars[PROGRESS]}" = no ] ||
            set_progress --start bal_vars[PROGRESS_START]
    done
    [ "${bal_vars[DRY_RUN]}" = yes ] || {
        [[ $bal_error -ne 0 ]] || {
            [ "${bal_vars[CLEAN]}" = no ] || rm -rf "$bal_tmpdir"
        }
    }
fi

[ "${bal_vars[DRY_RUN]}" = no ] &&
[ "${bal_vars[PROGRESS_CURRENT]}" -gt 3 ] && {
    vacuum \
        --schema_name fr \
        --table_name address_match_request,address_match_code,address_match_element,address_match_result \
        --mode ANALYZE || bal_error=1
}

_rc=$(( bal_error != 0 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
