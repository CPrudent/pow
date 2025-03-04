#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match BAL addresses w/ LAPOSTE ones

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
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _query=${bal_vars[QUERY_ADDRESSES]//XXXXX/${_opts[CODE]}} _request_id

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
        parallel:Effectuer les traitements en parallèle;
        parallel_chunk:Quantité de partage des données à traiter;
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
        parallel:no;
        parallel_chunk:5;
        clean:yes;
        verbose:no
    ' \
    --args_p '
        RESET:no
    ' \
    --pow_argv bal_vars "$@" || exit $ERROR_CODE

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

            [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
        }
    done
else
    bal_tmpdir="$POW_DIR_TMP/$$"
    [ ! -d "$bal_tmpdir" ] && mkdir "$bal_tmpdir"
    bal_limit=$(( ${#bal_codes[@]} / bal_vars[PARALLEL_CHUNK] ))
    [[ $(( ${#bal_codes[@]} % bal_vars[PARALLEL_CHUNK] )) -eq 0 ]] || ((bal_limit++))
    for ((bal_j=0; bal_j<$bal_limit; bal_j++)); do
        [ "${bal_vars[STOP_TIME]}" != 0 ] && {
            # stop loop if allowed time is expired
            [[ "$(date +'%m-%d-%T')" > "${bal_vars[STOP_TIME]}" ]] && break
        }

        bal_codes2=( $(printf '%s ' ${bal_codes[@]:((bal_j*bal_vars[PARALLEL_CHUNK])):${bal_vars[PARALLEL_CHUNK]}}) )
        [ "${bal_vars[PROGRESS]}" = no ] || {
            bal_vars[PROGRESS_START]=$(date '+%s') &&
            echo "INSEE ${bal_codes2[@]}"
        }

        [ "${bal_vars[DRY_RUN]}" = yes ] || {
            parallel --jobs 3 --rpl '{..} s/:[^:]*$//;' \
                $POW_DIR_BATCH/address_match.sh \
                    --source_name "BAL_{..}" \
                    --source_query "${bal_vars[QUERY_ADDRESSES]//XXXXX/{..}}" \
                    --request_path "$bal_tmpdir/BAL_{..}.dat" \
                    --steps REQUEST,STANDARDIZE,MATCH_CODE,MATCH_ELEMENT \
                    --format "$POW_DIR_BATCH/bal/format.sql" \
                    --parallel \
                    --force ${bal_vars[FORCE]} \
                ::: "${bal_codes2[@]}"
        }
        [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
    done
    [ "${bal_vars[DRY_RUN]}" = yes ] || {
        for ((bal_i=0; bal_i<${#bal_codes[@]}; bal_i++)); do
            bal_insee=${bal_codes[$bal_i]%%:*}
            bal_io_id=${bal_codes[$bal_i]#*:}
            bal_file="$bal_tmpdir/BAL_${bal_insee}.dat"

            [ -f "$bal_file" ] && {
                bal_req_id=$(sed --silent --expression '1p' < "$bal_file") &&
                io_history_update \
                    --infos '{"usecases":[{"name":"match","id":'${bal_req_id}'}]}' \
                    --id ${bal_io_id}
            }
        done
        [ "${bal_vars[CLEAN]}" = no ] || rm -rf "$bal_tmpdir"
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
