#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories

bash_args \
    --args_p "
        force:Forcer le traitement même si celui-ci a déjà été fait;
        postal_geom:Recalculer les contours de base (ZA) de la hiérarchie postale
    " \
    --args_v '
        force:yes|no;
        postal_geom:yes|no
    ' \
    --args_d '
        force:no;
        postal_geom:yes
    ' \
    "$@" || exit $ERROR_CODE

# TODO
# replace postal_geom w/ deduce of: RAN?|PDI|IGN date greatest than TERRITORY date
# and use force to bypass this rule

force="$get_arg_force"
set_env --schema_name fr &&
log_info "Définition des territoires français" &&
#$POW_DIR_BATCH/territory_insee.sh --force $force &&
$POW_DIR_BATCH/insee_administrative_cutting.sh --force $force &&
#$POW_DIR_BATCH/territory_ign.sh --force $force &&
$POW_DIR_BATCH/ign_geometry_territories.sh --force $force &&
#$POW_DIR_BATCH/territory_laposte.sh --force $force &&
execute_query \
    --name SET_TERRITORY_LAPOSTE \
    --query "SELECT fr.set_territory_laposte()" &&
$POW_DIR_BATCH/banatic_setof_municipalities.sh --force $force && {
    if [ "$get_arg_postal_geom" = no ]; then
        table_exists --schema_name fr --table_name territory &&
        execute_query \
            --name BACKUP_TERRITORY_ZA_GEOM \
            --query "
                DROP TABLE IF EXISTS fr.territory_za;
                CREATE TABLE fr.territory_za AS (
                    SELECT codgeo, superficie, gm_contour
                    FROM fr.territory WHERE nivgeo = fr.get_bigger_sublevel('CP')
                );
            "
    fi
} &&
execute_query \
    --name SET_TERRITORY \
    --query "SELECT fr.set_territory()" && {
    if [ "$get_arg_postal_geom" = yes ]; then
        execute_query \
            --name SET_TERRITORY_GEOMETRY \
            --query "CALL fr.set_territory_geometry(
            )" && {
            _error=$(grep '^ERREUR' $POW_DIR_ARCHIVE/SET_TERRITORY_GEOMETRY.notice.log)
            [ -n "$_error" ] && {
                log_error "calcul des géométries : $_error"
                false
            } || true
        }
    else
        table_exists --schema_name fr --table_name territory_za &&
        execute_query \
            --name RESTORE_TERRITORY_ZA_GEOM \
            --query "
                UPDATE fr.territory
                SET gm_contour = territory_za.gm_contour
                    , superficie = territory_za.superficie
                FROM fr.territory_za
                WHERE territory.codgeo = territory_za.codgeo
                AND territory.nivgeo = fr.get_bigger_sublevel('CP');

                DROP INDEX IF EXISTS fr.ix_territory_gm_contour;
                SELECT fr.set_territory_supra(
                    schema_name => 'fr'
                    , table_name => 'territory'
                    , base_level => fr.get_bigger_sublevel('CP')
                    , columns_agg => ARRAY['gm_contour', 'superficie']
                    , update_mode => TRUE
                );
                CREATE INDEX IF NOT EXISTS ix_territory_gm_contour ON fr.territory USING GIST(nivgeo, gm_contour);
            "
    fi
} &&
execute_query \
    --name SET_TERRITORY_NEAR \
    --query "SELECT fr.set_territory_next()" &&
vacuum \
    --schema_name fr \
    --table_name territory \
    --mode ANALYZE || exit $ERROR_CODE

exit $SUCCESS_CODE
