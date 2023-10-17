#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories (LAPOSTE)

on_integration_error() {
    # history created?
    [ "$POW_DEBUG" = yes ] && { echo "io_history_id=$io_history_id"; }
    [ -n "$io_history_id" ] && io_history_end_ko --id $io_history_id

    exit $ERROR_CODE
}

bash_args \
    --args_p "
        force:Forcer le traitement même si celui-ci a déjà été fait
    " \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

force="$get_arg_force"
set_env --schema_name fr &&
declare -A ios &&
io_get_info_integration --name FR-TERRITORY-LAPOSTE --hash ios || exit $ERROR_CODE

io_date=$(date +%F)
io_steps=(${ios[DEPENDS]//:/ })
io_ids=()
# default counts
io_counts=(39192 8671)
for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
    # already done or in progress ?
    io_todo_import \
        --force $force \
        --type ${io_steps[$io_step]} \
        --date_end "$io_date"
    case $? in
    $POW_IO_SUCCESSFUL)
        continue
        ;;
    $POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
        on_integration_error
        ;;
    esac

    # last id
    io_ids[$io_step]=${ios[${io_steps[$io_step]}_i]}
    # step todo or force it ?
    ([ "$force" = no ] && (! is_yes --var ios[${io_steps[$io_step]}_t])) || {
        io_history_begin \
            --type ${io_steps[$io_step]} \
            --date_begin "$io_date" \
            --date_end "$io_date" \
            --nrows_todo ${io_counts[$io_step]:-1} \
            --id io_history_id && {
            case ${io_steps[$io_step]} in
            FR-TERRITORY-LAPOSTE-AREA)
                io_count="
                    SELECT COUNT(1) FROM public.territory WHERE country = 'FR' AND level = 'ZA'
                    " &&
                execute_query \
                    --name SET_TERRITORY_LAPOSTE_AREA \
                    --query "SELECT fr.set_zone_address_to_now()"
                ;;
            FR-TERRITORY-LAPOSTE-SUPRA)
                ;;
            esac
        } &&
        io_get_ids_integration \
            --name ${io_steps[$io_step]} \
            --hash ios \
            --ids ids &&
        io_history_end_ok \
            --nrows_processed "($io_count)" \
            --infos "$ids" \
            --id $io_history_id &&
        io_ids[$io_step]=$io_history_id || on_integration_error
    }
done


exit $SUCCESS_CODE
