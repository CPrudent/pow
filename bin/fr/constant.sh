#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR's constants
    #
    # TODO FR-CONSTANT-ADDRESS could be splitted as
    #       FR-CONSTANT-ADDRESS-INIT
    #       FR-CONSTANT-ADDRESS-FAULT

on_integration_error() {
    local -A _opts &&
    pow_argv \
        --args_n "
            id:ID historique en cours
        " \
        --args_m '
            id
        ' \
        --pow_argv _opts "$@" || return $?

    # history created?
    [ "$POW_DEBUG" = yes ] && { echo "id=${_opts[ID]}"; }
    [ -n "${_opts[ID]}" ] && io_history_end_ko --id ${_opts[ID]}

    return $ERROR_CODE
}

declare -A io_vars=(
    [NAME]=FR-CONSTANT
    [INFO]='Mise à jour des constantes (FR)'
    [DATE]=$(date '+%F')
    [ID_MAIN]=
    [ID_STEP]=
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
        reset:no;
        tag:force@bool
    ' \
    --pow_argv io_vars "$@" || exit $?

declare -A io_hash &&
set_env --schema_name fr &&
io_get_info_integration --io ${io_vars[NAME]} --to_hash io_hash || exit $ERROR_CODE

([ "${io_vars[FORCE]}" = no ] && (! is_yes --var io_hash[TODO])) && {
    log_info "IO '${io_vars[NAME]}' déjà à jour!"
    exit $SUCCESS_CODE
} || {
    # already done or in progress ?
    io_todo_import \
        --force ${io_vars[FORCE]} \
        --io ${io_vars[NAME]} \
        --date_end "${io_vars[DATE]}"
    case $? in
    $POW_IO_SUCCESSFUL)
        exit $SUCCESS_CODE
        ;;
    $POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
        exit $ERROR_CODE
        ;;
    esac
}

log_info "${io_vars[INFO]}" &&
io_history_begin \
    --io ${io_vars[NAME]} \
    --date_begin "${io_vars[DATE]}" \
    --date_end "${io_vars[DATE]}" \
    --id io_vars[ID_MAIN] && {

    io_steps=(${io_hash[DEPENDS]//:/ })
    io_ids=()
    # default counts
    io_counts=()
    io_error=0

    for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
        # last id
        io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
        # step todo or force it ?
        ([ "${io_vars[FORCE]}" = no ] && (! is_yes --var io_hash[${_step}_t])) || {
            #breakpoint "${io_steps[$io_step]}: io begin"
            io_history_begin \
                --io ${io_steps[$io_step]} \
                --date_begin "${io_vars[DATE]}" \
                --date_end "${io_vars[DATE]}" \
                --nrows_todo ${io_counts[$io_step]:-1} \
                --id io_vars[ID_STEP] && {
                case ${io_steps[$io_step]} in
                FR-CONSTANT-ADDRESS)
                    io_count="
                        (SELECT COUNT(1) FROM fr.laposte_address_street_uniq)
                        " &&
                    #breakpoint "${io_steps[$io_step]}: query" &&
                    import_file \
                        --file_path "$POW_DIR_BATCH/db.objects.d/data/address_faults_manual_correction.csv" \
                        --schema_name fr \
                        --table_name laposte_address_fault_correction \
                        --rowid no \
                        --load_mode OVERWRITE_TABLE &&
                    execute_query \
                        --name FR_CONSTANT_ADDRESS \
                        --query "
                            CALL fr.set_constant_address();
                        "
                    ;;
                esac
            } &&
            #breakpoint "${io_steps[$io_step]}: ids" &&
            io_get_ids_integration \
                --from HASH \
                --group ${io_steps[$io_step]} \
                --hash io_hash \
                --ids _ids &&
            #breakpoint "${io_steps[$io_step]}: io end" &&
            io_history_end_ok \
                --nrows_processed "($io_count)" \
                --infos "$_ids" \
                --id ${io_vars[ID_STEP]} &&
            io_ids[$io_step]=${io_vars[ID_STEP]} || {
                on_integration_error --id ${io_vars[ID_STEP]}
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
        --id ${io_vars[ID_MAIN]}
} &&
vacuum \
    --schema_name fr \
    --table_name constant,laposte_address_street_uniq,laposte_address_street_word_descriptor,laposte_address_keyword,laposte_address_street_kw_exception,laposte_address_fault_street \
    --mode ANALYZE || {
    on_integration_error --id ${io_vars[ID_MAIN]}
    exit $ERROR_CODE
}

log_info "${io_vars[INFO]} avec succès"
exit $SUCCESS_CODE
