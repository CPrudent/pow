#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR's constants

on_integration_error() {
    bash_args \
        --args_p "
            id:ID historique en cours
        " \
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

io_name=FR-CONSTANT
io_info='Mise à jour des constantes (FR)'
io_date=$(date +%F)
io_force=$get_arg_force
declare -A io_hash

set_env --schema_name fr &&
io_get_info_integration --name $io_name --to_hash io_hash || exit $ERROR_CODE

([ "$io_force" = no ] && (! is_yes --var io_hash[TODO])) && {
    log_info "IO '$io_name' déjà à jour!"
    exit $SUCCESS_CODE
} || {
    # already done or in progress ?
    io_todo_import \
        --force $io_force \
        --name $io_name \
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

log_info "$io_info" &&
io_history_begin \
    --name $io_name \
    --date_begin "$io_date" \
    --date_end "$io_date" \
    --id io_main_id && {

    io_steps=(${io_hash[DEPENDS]//:/ })
    io_ids=()
    # default counts
    io_counts=()
    io_error=0

    for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
        # last id
        io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
        # step todo or force it ?
        ([ "$io_force" = no ] && (! is_yes --var io_hash[${_step}_t])) || {
            #breakpoint "${io_steps[$io_step]}: io begin"
            io_history_begin \
                --name ${io_steps[$io_step]} \
                --date_begin "$io_date" \
                --date_end "$io_date" \
                --nrows_todo ${io_counts[$io_step]:-1} \
                --id io_step_id && {
                case ${io_steps[$io_step]} in
                FR-CONSTANT-ADDRESS)
                    io_count="
                        (SELECT COUNT(1) FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME')
                        +
                        (SELECT COUNT(1) FROM fr.laposte_address_street_keyword)
                        " &&
                    #breakpoint "${io_steps[$io_step]}: query" &&
                    execute_query \
                        --name FR_CONSTANT_ADDRESS \
                        --query "
                            SELECT public.drop_table_indexes('fr', 'constant');
                            CALL fr.set_laposte_address_street_uniq();
                            CALL fr.set_laposte_address_street_word();
                            CALL fr.set_laposte_address_street_type();
                            CALL fr.set_laposte_address_street_ext();
                            CALL fr.set_laposte_address_street_title();
                            CALL fr.set_laposte_address_street_firstname();
                            CALL fr.set_laposte_address_street_kw_exception();
                            CALL fr.set_laposte_municipality_normalized_label_exception();
                            CALL fr.set_territory_overseas();
                            CALL fr.set_laposte_address_correction_list();
                            CALL fr.set_constant_index();
                        "
                    ;;
                esac
            } &&
            #breakpoint "${io_steps[$io_step]}: ids" &&
            io_get_ids_integration \
                --from HASH \
                --name ${io_steps[$io_step]} \
                --hash io_hash \
                --ids _ids &&
            #breakpoint "${io_steps[$io_step]}: io end" &&
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
} &&
vacuum \
    --schema_name fr \
    --table_name constant \
    --mode ANALYZE &&
vacuum \
    --schema_name fr \
    --table_name laposte_address_street_keyword \
    --mode ANALYZE &&
vacuum \
    --schema_name fr \
    --table_name laposte_address_street_kw_exception \
    --mode ANALYZE || {
    on_integration_error --id $io_main_id
    exit $ERROR_CODE
}

log_info "$io_info avec succès"
exit $SUCCESS_CODE
