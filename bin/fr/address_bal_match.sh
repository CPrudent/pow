#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match BAL addresses w/ LAPOSTE ones

    # DEBUG session
    # export POW_DEBUG_JSON='{"codes":[{"name":"address_bal_match","steps":["argv","chunk","query@break","before@break"]}]}'

    # NOTE
    # can have many aliases
    # parallel --rpl '#1 s/:[^:]*$//;' --rpl '#2 s/^[^:]*://;' echo '1=#1 2=#2' ::: A:1 BB:22 CCC:333

    # TEST
    # MATCH + MATCH_CLEAN, new municipalities
    # 1- no parallel w/ 1 municipality
    # 2- parallel w/ 2 municipalities (--limit 2)
    # fix MATCH_AGAIN_ROWID
    # 3- no parallel w/ 1 municipality
    # 4- parallel w/ 2 municipalities
    # fix MATCH_CLEAN
    # 5- no parallel w/ 1 municipality
    # 6- parallel w/ 2 municipalities

source $POW_DIR_ROOT/lib/libbal.sh || exit $ERROR_CODE

bal_update_query() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune;
            history_id:ID dernier historique;
            query:Requête extraction données BAL
        ' \
        --args_m '
            code;history_id;query
        ' \
        --pow_argv _opts "$@" || return $?

    # update query (request linked to last history)
    # https://stackoverflow.com/questions/22736742/query-for-array-elements-inside-json-type
    # protect single quotes (into query) by using $$ syntax, and \$ because shell!
    execute_query \
        --name REQUEST_UPDATE_${_opts[CODE]} \
        --query "
            WITH
            match_case AS (
                SELECT uc
                FROM   io_history io,
                        json_array_elements((io.attributes::JSON)->'usecases') uc
                WHERE  io.id = ${_opts[HISTORY_ID]}
                        AND io.attributes IS JSON OBJECT
                        AND uc->>'name' = 'match'
            )
            UPDATE fr.address_match_request SET
            source_query = \$\$${_opts[QUERY]}\$\$
            FROM match_case
            WHERE
                id = (uc->>'id')::INT
        "

    return $?
}

# clean BAL match(s) of given municipality
bal_match_clean() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune
        ' \
        --args_m '
            code
        ' \
        --pow_argv _opts "$@" || return $?

    local _result _counters

    execute_query \
        --name MATCH_CLEAN_${_opts[CODE]} \
        --query "
            SELECT
                counters
            FROM
                fr.bal_match_clean(
                    code => '${_opts[CODE]}',
                    todo => 3
                )
        " \
        --return _result &&
    array_sql_to_bash \
        --array_sql "$_result" \
        --array_bash _counters &&
    bal_clean["old_${_opts[CODE]}"]=${_counters[0]} &&
    bal_clean["upd_${_opts[CODE]}"]=${_counters[1]}

    return $?
}

# set history of given municipality
bal_match_history() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune;
            history_id:ID dernier historique;
            request_id:ID traitement Rapprochement
        ' \
        --args_m '
            code;history_id
        ' \
        --pow_argv _opts "$@" || return $?

    local _infos

    # update history
    case "${bal_vars[FIX]}" in
    MATCH_CLEAN)
        _infos='"name":"MATCH_CLEAN"'
        ;;
    *)
        _infos='"name":"match","id":'${_opts[REQUEST_ID]}
        ;;
    esac
    # only if cleaned addresses
    [[ ${bal_clean["old_${_opts[CODE]}"]} -gt 0 || ${bal_clean["upd_${_opts[CODE]}"]} -gt 0 ]] && {
        _infos+=',"clean":{"old":'${bal_clean["old_${_opts[CODE]}"]}',"upd":'${bal_clean["upd_${_opts[CODE]}"]}'}'
    }
    io_history_update \
        --infos '{"usecases":[{'$_infos'}]}' \
        --id ${_opts[HISTORY_ID]}

    return $?
}

bal_match_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune;
            history_id:ID dernier historique
        ' \
        --args_m '
            code;history_id
        ' \
        --pow_argv _opts "$@" || return $?

    local _query=${bal_vars[QUERY_ADDRESSES]//XXXXX/${_opts[CODE]}}
    local _request_id=0

    # update request (query), if fix MATCH_AGAIN_ROWID
    {
        [ "${bal_vars[FIX]}" != MATCH_AGAIN_ROWID ] || {
            bal_update_query \
                --code ${_opts[CODE]} \
                --history_id ${_opts[HISTORY_ID]} \
                --query "$_query"
        }
    } &&
    {
        [ "${bal_vars[FIX]}" = MATCH_CLEAN ] || {
            # match addresses
            $POW_DIR_BATCH/address_match.sh \
                --source_name BAL_${_opts[CODE]} \
                --source_query "$_query" \
                --request_path $POW_DIR_TMP/BAL_${_opts[CODE]}.dat \
                --steps REQUEST,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT,MATCH_ADDRESS \
                --format "$POW_DIR_BATCH/bal/format.sql" \
                --force ${bal_vars[FORCE]} \
                --request_new ${bal_vars[REQUEST_NEW]} &&
            _request_id=$(sed --silent --expression '1p' < $POW_DIR_TMP/BAL_${_opts[CODE]}.dat)
        }
    } &&
    {
        bal_match_clean --code ${_opts[CODE]}
    } &&
    {
        bal_match_history \
            --code ${_opts[CODE]} \
            --request_id $_request_id \
            --history_id ${_opts[HISTORY_ID]}
    } &&
    {
        [ "${bal_vars[CLEAN]}" = no ] || rm --force $POW_DIR_TMP/BAL_${_opts[CODE]}.dat
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
        progress:Afficher le ratio de progression;
        parallel:Effectuer les traitements en parallèle;
        parallel_chunk:Quantité de partage des données à traiter;
        parallel_jobs:Nombre de traitements en parallèle;
        auth_only:Extraire seulement les adresses certifiées;
        dry_run:Simuler le traitement;
        print_only:Pas de traitement, mais affichage des prochaines communes à traiter;
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
        fix:MATCH_AGAIN_ROWID|MATCH_CLEAN;
        progress:yes|no;
        parallel:yes|no;
        auth_only:yes|no;
        dry_run:yes|no;
        print_only:yes|no;
        clean:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        select_criteria:REVISION;
        select_order:DESC;
        force:no;
        limit:3;
        stop_time:0;
        progress:no;
        parallel:no;
        parallel_chunk:5;
        parallel_jobs:5;
        auth_only:yes;
        dry_run:no;
        print_only:no;
        clean:yes;
        verbose:no
    ' \
    --args_p '
        reset:no;
        tag:select_criteria@1N,select_order:1N,fix@0N,levels@1N,force@bool,dry_run@bool,auth_only@bool,print_only@bool,progress@bool,parallel@bool,clean@bool,verbose@bool,limit@int,parallel_chunk@int,parallel_jobs@int,fix@0N
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

# pas de mode parallèle
[ "${bal_vars[PARALLEL]}" = yes ] &&
[ "${bal_vars[FIX]}" = MATCH_CLEAN ] && {
    echo 'Mode parallel désactivé pour ce correctif'
    bal_vars[PARALLEL]=no
    bal_vars[PROGRESS]=no
}

bal_vars[MUNICIPALITY_CODE]=${bal_vars[MUNICIPALITY]^^}
declare -a bal_codes
declare -a bal_codes2
declare -a bal_errors
# reset LIMIT if STOP_TIME
[ "${bal_vars[STOP_TIME]}" != 0 ] && [ ${bal_vars[LIMIT]} -gt 0 ] && bal_vars[LIMIT]=0
# MATCH_CLEAN results
declare -A bal_clean
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
        --query "
            SELECT q FROM fr.bal_municipality_addresses(
                code => 'XXXXX',
                force => ('${bal_vars[FORCE]}' = 'yes'),
                auth_only => ('${bal_vars[AUTH_ONLY]}' = 'yes')
            )
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

[ ${#bal_codes[@]} -eq 0 ] && {
    log_info "Rapprochement BAL déjà à jour!"
    exit $SUCCESS_CODE
}

[ "${bal_vars[PRINT_ONLY]}" = yes ] && {
    case "${bal_vars[FIX]}" in
    MATCH_AGAIN_ROWID)
        # last codes
        _from=$((${#bal_codes[@]} -5))
        ;;
    *)
        # first codes
        _from=0
        ;;
    esac
    echo "#${#bal_codes[@]} à traiter (${bal_codes[@]:${_from}:5})"
    exit $SUCCESS_CODE
}

bal_error=0
bal_vars[PROGRESS_TOTAL]=${#bal_codes[@]}
[ "${bal_vars[FIX]}" = MATCH_AGAIN_ROWID ] && bal_vars[FORCE]=yes
# needing to run again same request
[ "${bal_vars[FORCE]}" = yes ] && bal_vars[REQUEST_NEW]=no

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
                --history_id ${bal_vars[IO_LAST_ID]} || ((bal_error++))

            [ "${bal_vars[PROGRESS]}" = no ] ||
                set_progress --start bal_vars[PROGRESS_START]
        }
    done
else
    bal_tmpdir="$POW_DIR_TMP/$$"
    [ ! -d "$bal_tmpdir" ] && mkdir "$bal_tmpdir"
    bal_limit=$(( ${#bal_codes[@]} / bal_vars[PARALLEL_CHUNK] ))
    [[ $(( ${#bal_codes[@]} % bal_vars[PARALLEL_CHUNK] )) -eq 0 ]] || ((bal_limit++))
    bal_serie=0
    # break indicator (to exit from loop)
    _break=0
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
                bal_io_id=${bal_item#*:}
                bal_query=${bal_vars[QUERY_ADDRESSES]//XXXXX/${bal_insee}}

                {
                    [[ ${_debug_steps[query]:-1} -ne 0 ]] || {
                        echo "tmpdir=($bal_tmpdir)"
                        echo "query=[$bal_query]"
                        [[ ${_debug_bps[query]} -ne 0 ]] || read
                    }
                } &&
                echo "$bal_query" > "$bal_tmpdir/BAL_${bal_insee}.sql" &&
                # update request (query), if fix MATCH_AGAIN
                {
                    [ "${bal_vars[FIX]}" != MATCH_AGAIN_ROWID ] || {
                        bal_update_query \
                            --code $bal_insee \
                            --history_id $bal_io_id \
                            --query "$bal_query"

                    }
                } || exit $ERROR_CODE
            done
            set +o noglob

            [[ ${_debug_steps[before]:-1} -eq 0 ]] && {
                echo 'before parallel...'
                [[ ${_debug_bps[before]} -eq 0 ]] && read
            }

            bal_serie=$((bal_serie +1))
            {
                [ "${bal_vars[FIX]}" = MATCH_CLEAN ] || {
                    # item composed as INSEE:HISTORY_ID (INSEE only wanted here)
                    #+ can use --tag to print each item
                    parallel \
                        --jobs ${bal_vars[PARALLEL_JOBS]} \
                        --joblog $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log \
                        --rpl '{..} s/:[^:]*$//;' \
                        $POW_DIR_BATCH/address_match.sh \
                            --source_name "BAL_{..}" \
                            --source_query "$bal_tmpdir/BAL_{..}.sql" \
                            --request_path "$bal_tmpdir/BAL_{..}.dat" \
                            --steps REQUEST,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT,MATCH_ADDRESS \
                            --format "$POW_DIR_BATCH/bal/format.sql" \
                            --parallel \
                            --force ${bal_vars[FORCE]} \
                            --request_new ${bal_vars[REQUEST_NEW]} \
                        ::: "${bal_codes2[@]}"
                }
            } &&

# FIXME
# bal_clean empty after!
#             {
#                 # break by user, as CTRL-C (rc=-1)
#                 [ $? -eq 255 ] && _break=1 || {
#                     parallel \
#                         --jobs ${bal_vars[PARALLEL_JOBS]} \
#                         --joblog $POW_DIR_ARCHIVE/parallel_${bal_serie}_clean.log \
#                         --rpl '{..} s/:[^:]*$//;' \
#                         bal_match_clean \
#                             --code "{..}" \
#                         ::: "${bal_codes2[@]}"
#                 }
#             } &&

            {
#                 # break by user, as CTRL-C (rc=-1)
#                 _rc=$?
#                 [ $_break -eq 0 ] || {
#                     [ $_rc -eq 255 ] && _break=1
#                 }

                # search for error (column 7: exit status)
                tail --lines +2 $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log | cut --fields 7 | grep --silent ^[^0]
                [ $? -ne 0 ] || {
                    bal_errors+=($POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log)
                    [[ ${_debug_steps[error]:-1} -eq 0 ]] && {
                        tail --lines +2 $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log | cut --fields 7,9
                        [[ ${_debug_bps[error]} -eq 0 ]] && read
                    }
                    bal_error=1
                }
            } &&

            {
                # clean previous match(s) and update BAL history for all successfull
                for ((bal_i=0; bal_i<${#bal_codes2[@]}; bal_i++)); do
                    bal_insee=${bal_codes2[$bal_i]%%:*}
                    bal_io_id=${bal_codes2[$bal_i]#*:}
                    bal_req_id=0
                    bal_file="$bal_tmpdir/BAL_${bal_insee}.dat"
                    # ok match ?
                    _matched=$(grep BAL_${bal_insee} $POW_DIR_ARCHIVE/parallel_${bal_serie}_match.log | cut --fields 7)
                    [ "$_matched" = 0 ] && {
                        {
                            [ "${bal_vars[FIX]}" = MATCH_CLEAN ] || {
                                bal_req_id=$(sed --silent --expression '1p' < "$bal_file")
                            }
                        } &&
                        bal_match_clean --code $bal_insee &&
                        bal_match_history \
                            --code $bal_insee \
                            --request_id $bal_req_id \
                            --history_id $bal_io_id
                    } || {
                        bal_error=1
                    }
                done
            }

            [[ $_break -eq 1 || $bal_error -gt 0 ]] && break
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
    echo 'VACUUM Match'
    vacuum \
        --schema_name fr \
        --table_name address_match_request,address_match_code,address_match_element,address_match_result \
        --mode ANALYZE || bal_error=1
}

[ "${bal_vars[PARALLEL]}" = yes ] &&
[[ ${#bal_errors[@]} -gt 0 ]] && {
    bal_error=1
    echo "Série(s) en erreur: "
    echo ${bal_errors[@]} | tr ' ' '\n'
}

_rc=$(( bal_error != 0 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
