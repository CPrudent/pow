#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories

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
    --args_p "
        force:Forcer le traitement même si celui-ci a déjà été fait;
        depends:Mettre à jour les dépendances (si nécessaire)
    " \
    --args_v '
        force:yes|no;
        depends:yes|no
    ' \
    --args_d '
        force:no;
        depends:yes
    ' \
    "$@" || exit $ERROR_CODE

io_name=FR-TERRITORY
io_date=$(date +%F)
io_force=$get_arg_force
declare -A io_hash

set_env --schema_name fr && {
    [ "$get_arg_depends" = yes ] && {
        $POW_DIR_BATCH/territory_insee.sh --force $io_force &&
        $POW_DIR_BATCH/territory_ign.sh --force $io_force &&
        $POW_DIR_BATCH/territory_banatic.sh --force $io_force &&
        $POW_DIR_BATCH/territory_laposte.sh --force $io_force
    } || true
} &&
io_get_info_integration --name $io_name --to_hash io_hash --to_string io_str || exit $ERROR_CODE

([ "$io_force" = no ] && (! is_yes --var io_hash[TODO])) && {
    log_info "IO '$io_name' déjà à jour!"
    exit $SUCCESS_CODE
} || {
    # already done or in progress ?
    io_todo_import \
        --force $io_force \
        --type $io_name \
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

[ "$POW_DEBUG" = yes ] && { echo $io_str | tr ',' '\n'; }
_not_ok=''
# check up-to-date dependences
for _io in INSEE IGN LAPOSTE-AREA; do
    [ -n "$_not_ok" ] && _not_ok+=", "
    is_yes --var io_hash[FR-TERRITORY-${_io}_t] && _not_ok+=$_io
done
[ -n "$_not_ok" ] && {
    log_error "IO $_not_ok non à jour des évènements Commune!"
    exit $ERROR_CODE
}
log_info "Calcul des territoires français" &&
io_history_begin \
    --type $io_name \
    --date_begin "$io_date" \
    --date_end "$io_date" \
    --nrows_todo 1 \
    --id io_main_id && {

    io_steps=(${io_hash[DEPENDS]//:/ })
    io_ids=()
    # default counts
    io_counts=()
    io_error=0

    # process FR territories
    #  if FR-TERRITORY-GEOMETRY todo then rebuild based level (ZA) else update them
    #  build supra territories
    execute_query \
        --name FR_TERRITORY \
        --query "SELECT fr.set_territory('$io_str'::HSTORE)" &&

    for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
        # last id
        io_ids[$io_step]=${io_hash[${io_steps[$io_step]}_i]}

        case ${io_steps[$io_step]} in
        # only IOs not already done (above depends)
        FR-TERRITORY-GEOMETRY)
            # step todo or force it ?
            if ([ "$io_force" = yes ] || (is_yes --var io_hash[${io_steps[$io_step]}_t])); then
                io_history_begin \
                    --type ${io_steps[$io_step]} \
                    --date_begin "$io_date" \
                    --date_end "$io_date" \
                    --nrows_todo ${io_counts[$io_step]:-1} \
                    --id io_step_id && {
                    case ${io_steps[$io_step]} in
                    # build geometry on low level, then set supra
                    FR-TERRITORY-GEOMETRY)
                        io_count="
                            SELECT COUNT(1) FROM fr.territory WHERE nivgeo = 'ZA'
                            " &&
                        execute_query \
                            --name FR_AREA_GEOMETRY \
                            --query "CALL fr.set_territory_geometry()" && {
                                _error=$(grep '^ERREUR' $POW_DIR_ARCHIVE/FR_AREA_GEOMETRY.notice.log)
                                [ -n "$_error" ] && {
                                    log_error "calcul des géométries : $_error"
                                    false
                                } || true
                            }
                        ;;
                    esac
                } &&
                io_get_ids_integration \
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
            # not todo? only necessary to propagate (area, simplified geometry) on SUPRA
            elif (! is_yes --var io_hash[${io_steps[$io_step]}_t]); then
                execute_query \
                    --name FR_AREA_GEOMETRY \
                    --query "CALL fr.set_territory_geometry(part_todo => 16)" && {
                        _error=$(grep '^ERREUR' $POW_DIR_ARCHIVE/FR_AREA_GEOMETRY.notice.log)
                        [ -n "$_error" ] && {
                            log_error "calcul des géométries : $_error"
                            io_error=1
                            false
                        } || true
                    }
            fi
            ;;
        # nothing todo for IOs already done
        *)
            continue
            ;;
        esac
    done
} &&

[ $io_error -eq 0 ] && {
    io_info=''
    for (( io_step=0; io_step<${#io_steps[@]}; io_step++ )); do
        [ ${io_ids[${io_step}]} -eq 0 ] && continue
        [ -n "$io_info" ] && io_info+=,
        io_info+=$(printf '"%s":%d' ${io_steps[$io_step]} ${io_ids[${io_step}]})
    done
    [ -n "$io_info" ] && io_info="{${io_info}}"
} &&

# build adjoining territories
execute_query \
    --name FR_TERRITORY_NEAR \
    --query "SELECT fr.set_territory_next()" &&

# update altitude if needed
$POW_DIR_BATCH/territory_altitude.sh --reset_territory yes &&

io_history_end_ok \
    --nrows_processed "(SELECT COUNT(1) FROM fr.territory)" \
    --infos "$io_info" \
    --id $io_main_id &&
vacuum \
    --schema_name fr \
    --table_name territory \
    --mode ANALYZE || {
    on_integration_error --id $io_main_id
    exit $ERROR_CODE
}

exit $SUCCESS_CODE
