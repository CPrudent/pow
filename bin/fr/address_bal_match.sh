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

    local _query=${bal_vars[QUERY_ADDRESSES]/XXXXX/${_opts[CODE]}} _request_id

    #echo "INSEE ${_opts[CODE]}" &&
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

# parallelism needing
export -f bal_match_municipality

bal_vars[MUNICIPALITY_CODE]=${bal_vars[MUNICIPALITY]^^}
declare -a bal_codes=()
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
        # parallelism mode: match BAL by block of 3 municipalities
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
