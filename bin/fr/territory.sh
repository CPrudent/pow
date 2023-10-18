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
        force:Forcer le traitement même si celui-ci a déjà été fait
    " \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

io_name=FR-TERRITORY
io_date=$(date +%F)
io_force=$get_arg_force
declare -A io_data

set_env --schema_name fr &&
$POW_DIR_BATCH/territory_insee.sh --force $io_force &&
$POW_DIR_BATCH/territory_ign.sh --force $io_force &&
$POW_DIR_BATCH/territory_banatic.sh --force $io_force &&
$POW_DIR_BATCH/territory_laposte.sh --force $io_force &&
io_get_info_integration --name $io_name --hash io_data || exit $ERROR_CODE

([ "$io_force" = no ] && (! is_yes --var io_data[TODO])) && {
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

log_info "Calcul des territoires français" &&
io_history_begin \
    --type $io_name \
    --date_begin "$io_date" \
    --date_end "$io_date" \
    --nrows_todo 1 \
    --id io_main_id && {

    {
        [ -v "io_data[FR-TERRITORY-LAPOSTE-AREA]" ] &&
        [ -v "io_data[FR-TERRITORY-IGN]" ] &&
        [ -v "io_data[FR-ADDRESS-LAPOSTE-DELIVERY-POINT]" ] && {
            io_geometry=1
            [ "$io_force" = no ] && {
                (! is_yes --var io_data[FR-TERRITORY-LAPOSTE-AREA]) &&
                (! is_yes --var io_data[FR-TERRITORY-IGN]) &&
                (! is_yes --var io_data[FR-ADDRESS-LAPOSTE-DELIVERY-POINT]) && {
                    io_geometry=0
                    table_exists --schema_name fr --table_name territory &&
                    execute_query \
                        --name BACKUP_AREA_GEOMETRY \
                        --query "
                            DROP TABLE IF EXISTS fr.territory_za;
                            CREATE TABLE fr.territory_za AS (
                                SELECT codgeo, superficie, gm_contour
                                FROM fr.territory
                                WHERE nivgeo = public.get_bigger_sublevel('fr', 'CP')
                            );
                        "
                } || true
            } || true
        } || {
            log_error "IO '$io_name' incomplet!"
            false
        }
    } &&

    # build low level (ZA or COM_CP), and propagate on supra territories
    execute_query \
        --name SET_TERRITORY \
        --query "SELECT fr.set_territory()" && {

        # build or restore geometry
        [ $io_geometry -eq 1 ] && {
            execute_query \
                --name SET_AREA_GEOMETRY \
                --query "CALL fr.set_territory_geometry()" && {
                _error=$(grep '^ERREUR' $POW_DIR_ARCHIVE/SET_AREA_GEOMETRY.notice.log)
                [ -n "$_error" ] && {
                    log_error "calcul des géométries : $_error"
                    false
                } || true
            }
        } || {
            table_exists --schema_name fr --table_name territory_za &&
            execute_query \
                --name RESTORE_AREA_GEOMETRY \
                --query "
                    UPDATE fr.territory
                    SET gm_contour = territory_za.gm_contour
                        , superficie = territory_za.superficie
                    FROM fr.territory_za
                    WHERE territory.codgeo = territory_za.codgeo
                    AND territory.nivgeo = public.get_bigger_sublevel('fr', 'CP');

                    DROP INDEX IF EXISTS fr.ix_territory_gm_contour;
                    SELECT fr.set_territory_supra(
                        schema_name => 'fr'
                        , table_name => 'territory'
                        , base_level => public.get_bigger_sublevel('fr', 'CP')
                        , columns_agg => ARRAY['gm_contour', 'superficie']
                        , update_mode => TRUE
                    );
                    CREATE INDEX IF NOT EXISTS ix_territory_gm_contour ON fr.territory USING GIST(nivgeo, gm_contour);
                "
        }
    } &&

    # build adjoining territories
    execute_query \
        --name SET_TERRITORY_NEAR \
        --query "SELECT fr.set_territory_next()" &&

    # update altitude if needed
    $POW_DIR_BATCH/territory_altitude.sh --reset yes &&

    io_get_ids_integration \
        --name $io_name \
        --hash io_data \
        --ids _ids &&
    io_history_end_ok \
        --nrows_processed 1 \
        --infos "$_ids" \
        --id $io_main_id &&
    vacuum \
        --schema_name fr \
        --table_name territory \
        --mode ANALYZE
} || {
    on_integration_error --id $io_main_id
    exit $ERROR_CODE
}

exit $SUCCESS_CODE
