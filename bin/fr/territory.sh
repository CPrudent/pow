#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories

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
    [NAME]=FR-TERRITORY
    [DATE]=$(date '+%F')
    [TODO]=no
    [ID_IO_MAIN]=
    [ID_IO_STEP]=
) &&
pow_argv \
    --args_n "
        force:Forcer le traitement même si celui-ci a déjà été fait;
        depends:Mettre à jour les dépendances (si nécessaire);
        ressources:Mettre à jour les ressources (si nécessaire);
        mode:Indiquer le mode de calcul
    " \
    --args_v '
        force:yes|no;
        depends:yes|no;
        ressources:yes|no;
        mode:AUTO|CREATE|UPDATE
    ' \
    --args_d '
        force:no;
        depends:yes;
        ressources:yes;
        mode:AUTO
    ' \
    --args_p '
        reset:no
    ' \
    --pow_argv io_vars "$@" || exit $ERROR_CODE

declare -A io_hash &&
set_env --schema_name fr &&
log_info 'Calcul des territoires français' && {
    [ "${io_vars[DEPENDS]}" = no ] || {
        $POW_DIR_BATCH/territory_insee.sh --force ${io_vars[FORCE]} &&
        $POW_DIR_BATCH/territory_ign.sh --force ${io_vars[FORCE]} &&
        $POW_DIR_BATCH/territory_banatic.sh --force ${io_vars[FORCE]} &&
        $POW_DIR_BATCH/territory_laposte.sh --force ${io_vars[FORCE]}
    }
} &&
io_get_info_integration \
    --io ${io_vars[NAME]} \
    --to_hash io_hash \
    --to_string io_string || {
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
    [ "$POW_DEBUG" = yes ] && { echo $io_string | tr ',' '\n'; }
    _not_ok=''
    # check up-to-date dependences (w/ municipality events)
    for _io in INSEE IGN LAPOSTE; do
        [ -n "$_not_ok" ] && _not_ok+=", "
        is_yes --var io_hash[FR-TERRITORY-${_io}-EVENT_t] && _not_ok+=$_io
    done
    [ -n "$_not_ok" ] && {
        log_error "IO $_not_ok non à jour des évènements Commune!"
        exit $ERROR_CODE
    }
    io_history_begin \
        --io ${io_vars[NAME]} \
        --date_begin "${io_vars[DATE]}" \
        --date_end "${io_vars[DATE]}" \
        --id io_vars[ID_IO_MAIN] && {

        declare -a io_steps=(${io_hash[DEPENDS]//:/ })
        declare -a io_ids=()
        # default counts
        declare -a io_counts=()
        io_error=0

        # process FR territories, all depended IO already done
        execute_query \
            --name FR_TERRITORY \
            --query "SELECT fr.set_territory(
                io_infos => '$io_string'::HSTORE,
                mode => '${io_vars[MODE]}',
                force => ('${io_vars[FORCE]}' = 'yes')
            )" &&
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
        --nrows_processed "(SELECT COUNT(1) FROM fr.territory)" \
        --infos "$_ids" \
        --id ${io_vars[ID_IO_MAIN]} &&
    vacuum \
        --schema_name fr \
        --table_name territory \
        --mode ANALYZE || {
        on_integration_error --id ${io_vars[ID_IO_MAIN]}
        exit $ERROR_CODE
    }
}

io_steps=(${io_hash[RESSOURCES]//:/ })
([ "${io_vars[RESSOURCES]}" = yes ] && [[ ${#io_steps[@]} -gt 0 ]]) && {
    log_info "IO '${io_vars[NAME]}' mise à jour (ressources)"
    {
        [ -n "${io_vars[ID_IO_MAIN]}" ] || {
            execute_query \
                --name FR_TERRITORY_ID \
                --query "
                    SELECT (public.get_last_io(name => '${io_vars[NAME]}')).id
                " \
                --return io_vars[ID_IO_MAIN]
        }
    } &&
    io_ids=() &&
    for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
        io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}
        if ([ "${io_vars[FORCE]}" = yes ] || (is_yes --var io_hash[${io_steps[$io_step]}_t])); then
            io_history_begin \
                --io ${io_steps[$io_step]} \
                --date_begin "${io_vars[DATE]}" \
                --date_end "${io_vars[DATE]}" \
                --nrows_todo ${io_counts[$io_step]:-1} \
                --id io_vars[ID_IO_STEP] && {
                case ${io_steps[$io_step]} in
                # build geometry on low level (ZA as default), then set supra
                FR-TERRITORY-GEOMETRY)
                    io_count="
                        SELECT COUNT(1) FROM fr.territory WHERE nivgeo = 'ZA'
                        " &&
                    execute_query \
                        --name FR_TERRITORY_GEOMETRY \
                        --query "CALL fr.set_territory_geometry()" && {
                            _error=$(grep '^ERREUR' $POW_DIR_ARCHIVE/FR_TERRITORY_GEOMETRY.notice.log)
                            [ -z "$_error" ] || {
                                log_error "calcul des géométries : $_error"
                                false
                            }
                        }
                    ;;
                FR-TERRITORY-NEXT)
                    io_count="
                        SELECT COUNT(1) FROM fr.territory WHERE gm_contour IS NOT NULL
                        " &&
                    execute_query \
                        --name FR_TERRITORY_NEXT \
                        --query "CALL fr.set_territory_next()"
                    ;;
                FR-MUNICIPALITY-ALTITUDE)
                    io_count="
                        SELECT COUNT(1)
                        FROM fr.municipality_altitude
                        WHERE z_min IS NOT NULL AND z_max IS NOT NULL
                        " &&
                        $POW_DIR_BATCH/territory_altitude.sh --reset_municipality yes
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
        fi
    done &&
    [ $io_error -eq 0 ] && {
        io_get_ids_integration \
            --from ARRAY \
            --hash io_hash \
            --array io_ids \
            --ids _ids
    } &&
    io_history_update \
        --infos "$_ids" \
        --id ${io_vars[ID_IO_MAIN]}
}

exit $SUCCESS_CODE
