#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR postal territories (LAPOSTE)

on_integration_error() {
    local -A _opts &&
    pow_argv \
        --args_n '
            id:ID historique en cours
        ' \
        --args_o '
            id
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    # history created?
    [ "$POW_DEBUG" = yes ] && { echo "id=${_opts[ID]}"; }
    [ -n "${_opts[ID]}" ] && io_history_end_ko --id ${_opts[ID]}

    return $ERROR_CODE
}

declare -A io_vars=(
    [NAME]=FR-TERRITORY-LAPOSTE
    [DATE]=$(date '+%F')
    [TODO]=no
    [ID_IO_MAIN]=
    [ID_IO_STEP]=
) &&
pow_argv \
    --args_n '
        force:Forcer le traitement même si celui-ci a déjà été fait
    ' \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    --args_p '
        reset:no
    ' \
    --pow_argv io_vars "$@" || exit $ERROR_CODE

# to declare on command line before calling function (else array)
declare -A io_hash &&
set_env --schema_name fr &&
log_info 'Calcul des territoires postaux français' &&
io_get_info_integration --io ${io_vars[NAME]} --to_hash io_hash || {
    log_error "IO '${io_vars[NAME]}' en erreur!"
    exit $ERROR_CODE
}

([ "${io_vars[FORCE]}" = no ] && (! is_yes --var io_hash[TODO])) && {
    log_info "IO '${io_vars[NAME]}' déjà à jour!"
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
    io_history_begin \
        --io ${io_vars[NAME]} \
        --date_begin "${io_vars[DATE]}" \
        --date_end "${io_vars[DATE]}" \
        --id io_vars[ID_IO_MAIN] &&
    {
        declare -a io_steps=(${io_hash[DEPENDS]//:/ })
        declare -a io_ids=()
        # default counts
        declare -A io_counts=(
            [FR-TERRITORY-LAPOSTE-EVENT]=39192      # infra (ZA or COM_CP)
            [FR-TERRITORY-LAPOSTE-SUPRA]=8671       # supra
        )
        io_error=0

        for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
            # last id
            io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
            # step todo or force it ?
            ([ "${io_vars[FORCE]}" = no ] && (! is_yes --var io_hash[${io_steps[$io_step]}_t])) || {
                io_history_begin \
                    --io ${io_steps[$io_step]} \
                    --date_begin "${io_vars[DATE]}" \
                    --date_end "${io_vars[DATE]}" \
                    --nrows_todo ${io_counts[${io_steps[$io_step]}]:-1} \
                    --id io_vars[ID_IO_STEP] && {
                    case ${io_steps[$io_step]} in
                    FR-TERRITORY-LAPOSTE-EVENT)
                        io_count="
                            SELECT COUNT(1) FROM fr.laposte_address_history
                            WHERE change = 'MUNICIPALITY_EVENT' AND date_change = NOW()::DATE
                            " &&
                        execute_query \
                            --name FR_TERRITORY_LAPOSTE_EVENT \
                            --query "SELECT fr.set_laposte_area_to_now()"
                        ;;
                    FR-TERRITORY-LAPOSTE-SUPRA)
                        io_count="
                            SELECT COUNT(1) FROM fr.territory_laposte_supra
                            " &&
                        execute_query \
                            --name FR_TERRITORY_LAPOSTE_SUPRA \
                            --query "SELECT fr.set_territory_laposte_supra()"
                        ;;
                    esac
                } &&
                io_get_ids_integration \
                    --from HASH \
                    --name ${io_steps[$io_step]} \
                    --hash io_hash \
                    --ids _ids &&
                io_history_end_ok \
                    --nrows_processed "($io_count)" \
                    --infos "$_ids" \
                    --id ${io_vars[ID_IO_STEP]} &&
                io_ids[$io_step]=${io_vars[ID_IO_STEP]} || {
                    on_integration_error --id ${io_vars[ID_IO_STEP]}
                    io_error=1
                    break
                }
            }
        done
    } &&
    [ $io_error -eq 0 ] && {
        io_get_ids_integration \
            --from ARRAY \
            --hash io_hash \
            --array io_ids \
            --ids _ids &&
        io_history_end_ok \
            --nrows_processed 1 \
            --infos "$_ids" \
            --id ${io_vars[ID_IO_MAIN]}
    } || {
        on_integration_error --id ${io_vars[ID_IO_MAIN]}
        exit $ERROR_CODE
    }
}

exit $SUCCESS_CODE
