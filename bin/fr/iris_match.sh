#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match LAPOSTE municipality addresses w/ IRIS-GE
    #
    # NOTE script called by address_iris_ge_match.sh (w/ parallel)

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
    [TEMPORARY]=USER
) &&
pow_argv \
    --args_n '
        municipality:Code Commune INSEE à traiter;
        version:Version algorithme de Rapprochement IRIS-GE;
        iris_mode:Mode de traitement;
        iris_id:ID historique du référentiel IRIS-GE;
        iris_date:Date du référentiel IRIS-GE;
        parallel:Indicateur traitement en parallèle
    ' \
    --args_m '
        municipality;version;iris_mode;iris_id;iris_date
    ' \
    --args_v '
        iris_mode:INIT|DELTA;
        parallel:yes|no;
    ' \
    --args_d '
        iris_mode:INIT;
        parallel:no
    ' \
    --args_p '
        reset:no;
        tag:iris_mode@0N,iris_id@int,parallel@bool
    ' \
    --pow_argv global_vars "$@" || exit $?

[ "${global_vars[PARALLEL]}" = yes ] && global_vars[TEMPORARY]=UNIQ
iris_history_municipality \
    --code ${global_vars[MUNICIPALITY]} \
    --name laposte_io_name \
    --date_begin laposte_date_begin \
    --date_end laposte_date_end &&
io_history_begin \
    --io "$laposte_io_name" \
    --date_begin "$laposte_date_begin" \
    --date_end "$laposte_date_end" \
    --nrows_todo 1 \
    --id laposte_id &&
execute_query \
    --name "IRIS_MATCH_${global_vars[MUNICIPALITY]}" \
    --query "
        SELECT nrows FROM fr.set_laposte_address_match_iris_ge(
            municipality => '${global_vars[MUNICIPALITY]}',
            mode => '${global_vars[IRIS_MODE]}',
            version => '${global_vars[VERSION]}',
            iris_id => ${global_vars[IRIS_ID]}
        )
    " \
    --return laposte_nrows \
    --temporary ${global_vars[TEMPORARY]} &&
io_history_end_ok \
    --nrows_processed ${laposte_nrows:-0} \
    --infos '{"version":"'${global_vars[VERSION]}'","iris_id":'${global_vars[IRIS_ID]}'}' \
    --id ${laposte_id} &&
io_history_update \
    --nrows_todo ${laposte_nrows:-0} \
    --id ${laposte_id}

exit $?
