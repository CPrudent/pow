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
io_todo_integration --name FR-TERRITORY-LAPOSTE --hash ios || exit $ERROR_CODE

io_date=$(date +%F)
io_steps=(${ios[DEPENDS]//,/ })
for io_step in "${io_steps[@]}"; do
    ([ "$force" = no ] && (! is_yes --var ios[${io_step}_t])) || {
        # nothing todo here
        [ "$io_step" = FR-TERRITORY-LAPOSTE-GEOMETRY ] && continue

        io_history_begin \
            --type $io_step \
            --date_begin "$io_date" \
            --date_end "$io_date" \
            --nrows_todo 1 \
            --id io_history_id && {
            case $io_step in
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
        io_history_end_ok \
            --nrows_processed "($io_count)" \
            --id $io_history_id || on_integration_error
    }
done


exit $SUCCESS_CODE
