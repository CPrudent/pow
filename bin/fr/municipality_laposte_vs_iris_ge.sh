#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR correlations between LAPOSTE municipalities and IRIS-GE ones

on_integration_error() {
    local -A _opts &&
    pow_argv \
        --args_n '
            id:ID historique en cours
        ' \
        --args_m '
            id
        ' \
        --pow_argv _opts "$@" || return $?

    # history created?
    [ -n "${_opts[ID]}" ] && io_history_end_ko --id ${_opts[ID]}

    return $ERROR_CODE
}

declare -A io_vars=(
    [NAME]=FR-LAPOSTE-MUNICIPALITY-VS-IRIS-GE
    [DATE]=$(date '+%F')
    [TODO]=no
    [ID_IO_MAIN]=
    [ID_IO_STEP]=
) &&
pow_argv \
    --args_n "
        force:Forcer le traitement même si celui-ci a déjà été fait;
        depends:Mettre à jour les dépendances (si nécessaire);
        ressources:Mettre à jour les ressources (si nécessaire)
    " \
    --args_v '
        force:yes|no;
        depends:yes|no;
        ressources:yes|no
    ' \
    --args_d '
        force:no;
        depends:yes;
        ressources:yes
    ' \
    --args_p '
        reset:no;
        tag:force@bool
    ' \
    --pow_argv io_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv todo io_begin'

[[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
    declare -p io_vars
    [[ ${_debug_bps[argv]} -eq 0 ]] && read
}

declare -A io_hash &&
set_env --schema_name fr &&
log_info 'Calcul des correspondances (niveau Commune) entre LA POSTE/IRIS-GE' &&
io_get_info_integration \
    --io ${io_vars[NAME]} \
    --to_hash io_hash \
    --to_string io_string || {
    log_error "IO '${io_vars[NAME]}' en erreur!"
    exit $ERROR_CODE
}

([ "${io_vars[FORCE]}" = no ] && (! is_yes --var io_hash[TODO])) && {
    log_info "IO '${io_vars[NAME]}' déjà à jour (dépendances)"
} || {
    # already done or in progress ?
    io_todo_import \
        --force ${io_vars[FORCE]} \
        --io ${io_vars[NAME]} \
        --date_end "${io_vars[DATE]}"
    case $? in
    $POW_IO_TODO)
        io_vars[TODO]=yes
        ;;
    $POW_IO_SUCCESSFUL)
        log_info "IO '${io_vars[NAME]}' déjà à jour!"
        ;;
    $POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
        log_error "IO '${io_vars[NAME]}' en erreur!"
        exit $ERROR_CODE
        ;;
    esac
}

[ "${io_vars[TODO]}" = yes ] && {
    log_info "IO '${io_vars[NAME]}' mise à jour (dépendances)"
    [[ ${_debug_steps[todo]:-1} -eq 0 ]] && {
        echo $io_string | tr ',' '\n'
        [[ ${_debug_bps[todo]} -eq 0 ]] && read
    }
    io_history_begin \
        --io ${io_vars[NAME]} \
        --date_begin "${io_vars[DATE]}" \
        --date_end "${io_vars[DATE]}" \
        --id io_vars[ID_IO_MAIN] &&
    {
        [[ ${_debug_steps[io_begin]:-1} -ne 0 ]] || {
            echo "id_main=(${io_vars[ID_IO_MAIN]})"
            [[ ${_debug_bps[io_begin]} -ne 0 ]] || read
        }
    } &&
    {
        declare -a io_steps=(${io_hash[DEPENDS]//:/ })
        declare -a io_ids=()
        # default counts
        declare -a io_counts=()
        io_error=0

        # process FR territories, all depended IO already done
        execute_query \
            --name FR_MUNICIPALITY_LAPOSTE_VS_IRIS_GE \
            --query "CALL fr.set_laposte_municipality_vs_iris_ge()" &&
        # retrieve ID of depended IO
        for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
            io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
        done || {
            io_error=1
        }
    } &&
    [ $io_error -eq 0 ] && {
        io_get_ids_integration \
            --from ARRAY \
            --hash io_hash \
            --array io_ids \
            --ids _ids
    } &&
    io_history_end_ok \
        --nrows_processed "
            (SELECT COUNT(1) FROM fr.laposte_municipality_vs_iris_ge)
        " \
        --infos "$_ids" \
        --id ${io_vars[ID_IO_MAIN]} &&
    vacuum \
        --schema_name fr \
        --table_name laposte_municipality_vs_iris_ge \
        --mode ANALYZE || {
        on_integration_error --id ${io_vars[ID_IO_MAIN]}
        exit $ERROR_CODE
    }
}

exit $SUCCESS_CODE
