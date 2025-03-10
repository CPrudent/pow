#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories (LAPOSTE)

on_integration_error() {
    bash_args \
        --args_p '
            id:ID historique en cours
        ' \
        --args_o '
            id
        ' \
        "$@" || return $ERROR_CODE

    # history created?
    [ "$POW_DEBUG" = yes ] && { echo "id=$get_arg_id"; }
    [ -n "$get_arg_id" ] && io_history_end_ko --id $get_arg_id

    return $ERROR_CODE
}

bash_args \
    --args_p '
        force:Forcer le traitement même si celui-ci a déjà été fait
    ' \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

io_name=FR-TERRITORY-LAPOSTE
io_date=$(date '+%F')
io_force=$get_arg_force

# to declare on command line before calling function (else array)
declare -A io_hash &&
set_env --schema_name fr &&
io_get_info_integration --io $io_name --to_hash io_hash || exit $ERROR_CODE

([ "$io_force" = no ] && (! is_yes --var io_hash[TODO])) && {
    log_info "IO '$io_name' déjà à jour!"
    exit $SUCCESS_CODE
} || {
    # already done or in progress ?
    io_todo_import \
        --force $io_force \
        --io $io_name \
        --date_end "$io_date"
    case $? in
    $POW_IO_SUCCESSFUL)
        exit $SUCCESS_CODE
        ;;
    $POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
        exit $ERROR_CODE
        ;;
    esac
}

log_info "Calcul des territoires postaux français" &&
io_history_begin \
    --io $io_name \
    --date_begin "$io_date" \
    --date_end "$io_date" \
    --id io_main_id && {

    io_steps=(${io_hash[DEPENDS]//:/ })
    io_ids=()
    # default counts
    io_counts=(39192 8671)
    io_error=0

    for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
        # last id
        io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
        case ${io_steps[$io_step]} in
        FR-TERRITORY-LAPOSTE-AREA)
            # only event is to do (others influence FR-TERRITORY or here FR_TERRITORY_LAPOSTE_SUPRA)
            _step=${io_steps[$io_step]}-EVENT
            ;;
        *)
            _step=${io_steps[$io_step]}
            ;;
        esac
        # step todo or force it ?
        ([ "$io_force" = no ] && (! is_yes --var io_hash[${_step}_t])) || {
            io_history_begin \
                --io ${io_steps[$io_step]} \
                --date_begin "$io_date" \
                --date_end "$io_date" \
                --nrows_todo ${io_counts[$io_step]:-1} \
                --id io_step_id && {
                case ${io_steps[$io_step]} in
                FR-TERRITORY-LAPOSTE-AREA)
                    io_count="
                        SELECT COUNT(1) FROM fr.laposte_address_history
                        WHERE change = 'MUNICIPALITY_EVENT' AND date_change = NOW()::DATE
                        " &&
                    execute_query \
                        --name FR_TERRITORY_LAPOSTE_AREA \
                        --query "SELECT fr.set_address_area_to_now()"
                    ;;
                FR-TERRITORY-LAPOSTE-SUPRA)
                    io_count="
                        SELECT COUNT(1) FROM fr.territory_laposte
                        " &&
                    execute_query \
                        --name FR_TERRITORY_LAPOSTE_SUPRA \
                        --query "SELECT fr.set_territory_laposte()"
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
                --id $io_step_id &&
            io_ids[$io_step]=$io_step_id || {
                on_integration_error --id $io_step_id
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
        --id $io_main_id
} || {
    on_integration_error --id $io_main_id
    exit $ERROR_CODE
}

exit $SUCCESS_CODE
