#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories (LAPOSTE)

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
declare -A territory_laposte &&
io_todo_integration --name FR-TERRITORY-LAPOSTE --hash territory_laposte || exit $ERROR_CODE

territory_steps=(${territory_laposte[DEPENDS]//,/ })
for territory_step in "${territory_steps[@]}"; do
    ([ "$force" = no ] && (! is_yes --var territory_laposte[${territory_step}_t])) || {
        case $territory_step in
        FR-TERRITORY-LAPOSTE-AREA)
            ;;
        FR-TERRITORY-LAPOSTE-SUPRA)
            ;;
        FR-TERRITORY-LAPOSTE-GEOMETRY)
            ;;
        esac
    }
done


    io_history_begin \
        --type $co_type_import \
        --date_begin "${years[$year_id]}" \
        --date_end "${years[$year_id]}" \
        --nrows_todo 35000 \
        --id year_history_id
    execute_query \
        --name SET_TERRITORY_LAPOSTE_AREA \
        --query "SELECT fr.set_zone_address_to_now()" &&

}

exit $SUCCESS_CODE
