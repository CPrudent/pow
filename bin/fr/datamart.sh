#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR's constants
    #

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
    [ -n "${_opts[ID]}" ] && io_history_end_ko --id ${_opts[ID]}

    return $ERROR_CODE
}

declare -A io_vars=(
    [NAME]=FR-DATAMART
    [DATE]=$(date '+%F')
    [TODO]=no
    [ID_IO_MAIN]=
    [ID_IO_STEP]=
) &&
pow_argv \
    --args_n '
        force:Forcer le traitement même si celui-ci a déjà été fait;
        depends:Mettre à jour les dépendances (si nécessaire);
        do_datamart:Indicateur de génération des Données;
        do_vacuum:Indicateur de réorganisation des Données
    ' \
    --args_v '
        force:yes|no;
        depends:yes|no;
        do_datamart:yes|no;
        do_vacuum:yes|no
    ' \
    --args_d '
        force:no;
        depends:yes;
        do_datamart:yes;
        do_vacuum:yes
    ' \
    --args_p '
        reset:no;
        tag:force@bool,depends@bool,do_datamart@bool,do_vacuum@bool
    ' \
    --pow_argv io_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv todo steps io_begin ids'

[[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
    declare -p io_vars
    [[ ${_debug_bps[argv]} -eq 0 ]] && read
}

declare -A io_hash &&
set_env --schema_name fr &&
log_info 'Mise à jour des Métriques (FR)' &&
io_get_info_integration \
    --io ${io_vars[NAME]} \
    --to_hash io_hash \
    --to_string io_string || {
    log_error "IO '${io_vars[NAME]}' en erreur!"
    exit $ERROR_CODE
}

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
        declare -p io_hash ; echo $io_string | tr ',' '\n'
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
        io_steps=(${io_hash[DEPENDS]//:/ })
        [[ ${_debug_steps[steps]:-1} -eq 0 ]] && {
            declare -p io_steps
            [[ ${_debug_bps[steps]} -eq 0 ]] && read
        }

        io_ids=()
        # default counts
        io_counts=()
        io_error=0

        for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
            # last id
            io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
            [[ ${_debug_steps[ids]:-1} -eq 0 ]] && {
                declare -p io_ids ; echo "step=($io_step)"
                [[ ${_debug_bps[ids]} -eq 0 ]] && read
            }

            # step todo or force it ?
            ([ "${io_vars[FORCE]}" = no ] && (! is_yes --var io_hash[${io_steps[$io_step]}_t])) || {
                io_history_begin \
                    --io ${io_steps[$io_step]} \
                    --date_begin "${io_vars[DATE]}" \
                    --date_end "${io_vars[DATE]}" \
                    --nrows_todo ${io_counts[$io_step]:-1} \
                    --id io_vars[ID_IO_STEP] &&
                {
                    [[ ${_debug_steps[io_begin]:-1} -ne 0 ]] || {
                        echo "id_step=(${io_vars[ID_IO_STEP]})"
                        [[ ${_debug_bps[io_begin]} -ne 0 ]] || read
                    }
                } &&
                {
                    case ${io_steps[$io_step]} in
                    FR-DATAMART-ADDRESS)
                        io_count="
                            (SELECT COUNT(1) FROM fr.laposte_address_street_uniq)
                            " &&
                        execute_query \
                            --name FR_DATAMART_ADDRESS \
                            --query "
                                DO \$DATAMART\$
                                BEGIN
                                    IF '${io_vars[DO_DATAMART]}' = 'yes' THEN
                                        CALL fr.set_datamart_address();
                                    END IF;
                                END \$DATAMART\$;
                            "
                        ;;
                    esac
                } &&
                # retrieve each ID of depends (of group), or none (if any)
                io_get_ids_integration \
                    --from HASH \
                    --group ${io_steps[$io_step]} \
                    --hash io_hash \
                    --ids _ids &&
                {
                    [[ ${_debug_steps[ids]:-1} -ne 0 ]] || {
                        echo "${io_steps[$io_step]}=($_ids)"
                        [[ ${_debug_bps[ids]} -ne 0 ]] || read
                    }
                } &&
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
        {
            [[ ${_debug_steps[ids]:-1} -ne 0 ]] || {
                echo "${io_vars[NAME]}=($_ids)" ; declare -p io_ids
                [[ ${_debug_bps[ids]} -ne 0 ]] || read
            }
        } &&
        io_history_end_ok \
            --nrows_processed 1 \
            --infos "$_ids" \
            --id ${io_vars[ID_IO_MAIN]}
    } &&
    {
        [ "${io_vars[DO_VACUUM]}" = no ] || {
            vacuum \
                --schema_name fr \
                --table_name laposte_address_keyword,laposte_address_street_uniq,laposte_address_street_membership,laposte_address_street_word_descriptor,laposte_address_street_word_level,laposte_address_street_kw_exception,laposte_address_housenumber_uniq,laposte_address_complement_uniq,laposte_address_complement_membership,laposte_address_complement_word_descriptor,laposte_address_complement_word_level,laposte_address_fault \
                --mode ANALYZE
        }
    } || {
        on_integration_error --id ${io_vars[ID_IO_MAIN]}
        exit $ERROR_CODE
    }
}

log_info "IO ${io_vars[NAME]} avec succès"
exit $SUCCESS_CODE
