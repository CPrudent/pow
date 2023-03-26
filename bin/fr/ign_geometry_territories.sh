#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import IGN geometry of territories (into FR schema)

# CHANGELOG (ADMIN-EXPRESS)
# 2.5 :
#   ENTITE_RATTACHEE existe toujours mais n'inclut plus les arrondissements municipaux (entite_rattachee.type = 'ARM')
#	qui sont dans un nouveau fichier ARRONDISSEMENT_MUNICIPAL(colonnes indentiques à entite_rattachee)
# 3.0 :
#   ENTITE_RATTACHEE est remplacée par COMMUNE_ASSOCIEE_OU_DELEGUEE
#   les colonnes suivantes sont renommées :
#       commune.nom_com/nom_com_m -> nom/nom_m
#       commune.code_epci -> siren_epci
#       departement.nom_dep/nom_dep_m -> nom/nom_m
#       region.nom_reg/nom_reg_m -> nom/nom_m
#       epci.code_epci -> epci.code_siren
#       epci.nom_epci -> epci.nom
#       epci.type_epci -> epci.nature
#       arrondissement_municipal.insee_com -> insee_arm
#       arrondissement_municipal.insee_ratt -> insee_com
#       arrondissement_municipal.nom_com/nom_m -> nom/nom_m
#   une archive contenant tous les territoires, mais avec 6 fichiers pour chaque élément (COMMUNE, ...)
# 3.1 :
#   archive du millésime est décomposée en plusieurs : FR métropolitaine, GLP, MTQ, GUF, REU et MYT
#   chacune des 6 archives contient un fichier par élément

# NOTE: les types d'élements correspondent aux différents fichiers shapefile existants dans l'archive ADMIN EXPRESS
declare -a _AVAILABLE_ITEMS=(
    ARRONDISSEMENT
    ARRONDISSEMENT_MUNICIPAL
    CANTON
    CHFLIEU_ARRONDISSEMENT_MUNICIPAL
    CHFLIEU_COMMUNE
    CHFLIEU_COMMUNE_ASSOCIEE_OU_DELEGUEE
    COLLECTIVITE_TERRITORIALE
    COMMUNE
    COMMUNE_ASSOCIEE_OU_DELEGUEE
    DEPARTEMENT
    EPCI
    REGION
)
_AVAILABLE_ITEMS_JOIN_PIPE=${_AVAILABLE_ITEMS[@]}
_AVAILABLE_ITEMS_JOIN_PIPE=${_AVAILABLE_ITEMS_JOIN_PIPE// /|}

bash_args \
    --args_p '
        force:Forcer le traitement même si celui-ci a déjà été fait;
        item:Type d élément a importer;
        year:Importer un millésime spécifique (au format YYYY-MM-DD) au lieu du dernier millésime disponible
    ' \
    --args_v '
        force:yes|no;
        item:'$_AVAILABLE_ITEMS_JOIN_PIPE \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

co_type_import=IGN_ADMINEXPRESS
force="$get_arg_force_import"

on_import_error() {
    # import created?
    [ "$POW_DEBUG" = yes ] && { echo "year_history_id=$year_history_id"; }
    [ -n "$year_history_id" ] && io_history_end_ko --id $year_history_id

    # not requested, so more recent
#     if [ -z "$get_arg_year" ]; then
#         # import OK w/ another (than last one) : reimport it else reimport previous
#         if [ -n "$_last_io" ] && [ "$_last_io" != "${years[0]}" ]; then
#             $POW_DIR_BATCH/ign_geometry_territories.sh \
#                 --force $force \
#                 --item $get_arg_item \
#                 --year "$_last_io" &&
#             exit $SUCCESS_CODE ||
#             exit $ERROR_CODE
#         else
#             $POW_DIR_BATCH/ign_geometry_territories.sh \
#                 --force $force \
#                 --item $get_arg_item \
#                 --year "${years[1]}" &&
#             exit $SUCCESS_CODE ||
#             exit $ERROR_CODE
#         fi
#     else
        exit $ERROR_CODE
#    fi
}

# according command line
if [ -z "$get_arg_item" ]; then
    # NOTE: ARRONDISSEMENT_DEPARTEMENTAL and CHEF_LIEU seem not useful
    declare -a ITEMS=(COMMUNE ARRONDISSEMENT_MUNICIPAL EPCI DEPARTEMENT REGION)
else
	co_type_import=IGN_ADMINEXPRESS_$get_arg_item
	declare -a ITEMS=($get_arg_item)
fi

set_env --schema_name fr &&
# get last import
execute_query \
    --name "LAST_IO_${co_type_import}" \
    --query "
        SELECT TO_CHAR(dt_data_end, 'YYYY-MM-DD')
        FROM get_last_io('${co_type_import}')" \
    --psql_arguments 'tuples-only:pset=format=unaligned' \
    --return _last_io &&
# get years
io_get_list_online_available \
    --type_import $co_type_import \
    --details_file years_list_path \
    --dates_list years || {
    [ -n "$_last_io" ] && _rc=$SUCCESS_CODE || _rc=$ERROR_CODE
    exit $_rc
}
[ "$POW_DEBUG" = yes ] && { declare -p years; declare -p years_list_path; }

# get year (w/ format YYYY)
if [ -z "$get_arg_year" ]; then
    # get more recent
    year_id=0
else
    in_array years "${get_arg_year}" year_id || {
        log_error "Impossible de trouver le millésime $get_arg_year de $co_type_import, les millésimes disponibles sont ${years[@]}"
        on_import_error
    }
fi
year=${years[$year_id]}
[ -z "$year" ] && {
    log_error "Impossible de trouver le millésime de $co_type_import"
    on_import_error
}
[ "$POW_DEBUG" = yes ] && { echo "year=$year (${years[$year_id]})"; }

io_todo \
    --force $get_arg_force \
    --type $co_type_import \
    --date_end "${years[$year_id]}"
case $? in
$POW_IO_SUCCESSFUL)
    exit $SUCCESS_CODE
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
    on_import_error
    ;;
esac

log_info "Import du millésime $year de $co_type_import" &&
# no history (think about requested item, so REGEX)
execute_query \
    --name "DELETE_IO_${co_type_import}" \
    --query "DELETE FROM io_history WHERE co_type ~ '^${co_type_import}'" &&
io_history_begin \
    --type $co_type_import \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 35000 \
    --id year_history_id &&
{
    # search for ADMIN-EXPRESS_XXX to avoid ADMIN-EXPRESS-COG
    # exclude WGS84 full (from v3.1)
    url_data_all=($(grep --only-matching --perl-regexp 'href="(http|ftp)[^"]+ADMIN-EXPRESS_(?(?!WM)[^"])+'$year'\.7z[^"]*' "$years_list_path" | grep --only-matching --perl-regexp '(http|ftp).*' | grep --invert-match WGS84))
    [ "$POW_DEBUG" = yes ] && { declare -p url_data_all; }

    for ((_i=0; _i<${#url_data_all[*]}; _i++)); do
        url_data_one=${url_data_all[$_i]}
        year_data=$(basename $url_data_one)
        # remove optionnal .001
        year_data=${year_data/.001/}
        [ "$POW_DEBUG" = yes ] && echo "data=${year_data}"

        io_download_file \
            --url "${url_data_one}" \
            --output_directory "${POW_DIR_IMPORT}" \
            --output_file "${year_data}" \
            --overwrite no &&
        mkdir --parent "$POW_DIR_TMP/$year_data" &&
        extract_archive \
            --archive_path "$POW_DIR_IMPORT/$year_data" \
            --extract_path "$POW_DIR_TMP/$year_data" && {

            _query_count=''
            for _item in ${ITEMS[@]}; do
                _query_union=
                _query_drop=
                _file_count=1
                _table_name=$(echo 'admin_express_'$_item | tr '[:upper:]' '[:lower:]')
                [ "$POW_DEBUG" = yes ] && echo "table=${_table_name}"
                for _shapefile_full_path in $(find "$POW_DIR_TMP/$year_data" -type f -iname ${_item}.shp); do
                    # NOTE: no spatial index (not slow down)
                    # NOTE: more temporary tables to insert different SRID
                    _table_name_tmp="tmp_${_table_name}_${_file_count}"
                    # NOTE: no projection, -r 'EPSG:3857'
                    import_geo_file \
                        --file_path "$_shapefile_full_path" \
                        --table_name "$_table_name_tmp" \
                        --geometry_type PROMOTE_TO_MULTI \
                        --load_mode OVERWRITE_TABLE \
                        --spatial_index no || on_import_error
                    [ -n "$_query_union" ] && _query_union="${_query_union} UNION ALL "
                    _query_union+="(SELECT * FROM fr.${_table_name_tmp})"
                    _query_drop+="DROP TABLE fr.${_table_name_tmp};"
                    _file_count=$((_file_count+1))
                done

                [ "$POW_DEBUG" = yes ] && {
                    echo -e "item=${_item}\nload=${_query_union}\ndrop=${_query_drop}"
                    #exprDebug "vérification en base ... <ENTREE>"
                }

                {
                    # data to import ?
                    if [ -n "$_query_union" ]; then
                        # NOTE: no interpretation of * (as list of files!)
                        set -o noglob
                        if [ $_i -eq 0 ]; then
                            # 1st loop, create table and alter it to accept multi-SRID
                            execute_query \
                                --name "IMPORT_FIRST_${_table_name}" \
                                --query "
                                    DROP TABLE IF EXISTS fr.$_table_name CASCADE;
                                    CREATE TABLE fr.$_table_name AS ($_query_union);
                                    ALTER TABLE fr.$_table_name
                                        ALTER COLUMN geom TYPE geometry(MultiPolygon);
                                    "
                        else
                            # other loop, append
                            execute_query \
                                --name "IMPORT_MORE_${_table_name}" \
                                --query "
                                    INSERT INTO fr.$_table_name $_query_union;
                                    "
                        fi
                        set +o noglob
                    fi
                } &&
                execute_query \
                    --name "DROP_TMP_${_table_name}" \
                    --query "$_query_drop" || on_import_error
            done
        } &&
        rm --recursive "$POW_DIR_TMP/$year_data" &&
        rm "$POW_DIR_IMPORT/$year_data"
    done
} && {
    _query_count=''
    for _item in ${ITEMS[@]}; do
        _query_union=
        _query_drop=
        _file_count=1
        _table_name=$(echo 'admin_express_'$_item | tr '[:upper:]' '[:lower:]')
        # NOTE: some geometry are invalid, correct them
        # TODO: give some examples
        execute_query \
            --name UPDATE_INVALID_GEOM \
            --query "
                UPDATE fr.${_table_name} SET geom = ST_MakeValid2(geom)
                WHERE ST_IsValid(geom) = FALSE" &&
        # TODO: label updates, always useful?
        execute_query \
            --name UPDATE_LABEL \
            --query "
                UPDATE fr.${_table_name}
                SET nom = REGEXP_REPLACE(REGEXP_REPLACE(nom, '^(¼|½)', 'Oe'), '(¼|½)', 'oe') WHERE nom ~ '.*(¼|½).*'" && {
            _key_idx=''
            case "$_item" in
            COMMUNE)
                _key_idx=insee_com
                # update hierarchy for EPT (as EPCI MGP/EPT w/ MGP=200054781)
                # MGP=Métropole du Grand Paris (w/ 131 municipalities)
                execute_query \
                    --name UPDATE_LINK_EPCI \
                    --query "
                        UPDATE fr.${_table_name}
                        SET siren_epci = '200054781'
                        WHERE POSITION('200054781/' IN siren_epci) > 0" || on_import_error
                ;;
            COMMUNE_ASSOCIEE_OU_DELEGUEE)
                _key_idx=insee_cad
                ;;
            ARRONDISSEMENT_MUNICIPAL)
                _key_idx=insee_arm
                ;;
            EPCI)
                _key_idx=code_siren
                ;;
            DEPARTEMENT)
                _key_idx=insee_dep
                ;;
            REGION)
                _key_idx=insee_reg
                ;;

            # TODO: if needed
            #CANTON) ;;
            #ARRONDISSEMENT_DEPARTEMENTAL) ;;
            #CHEF_LIEU) ;;
            esac

            if [ -n "$_key_idx" ]; then
                execute_query \
                    --name CREATE_UNIQUE_INDEX \
                    --query "
                        CREATE UNIQUE INDEX ON fr.${_table_name}($_key_idx)
                        " || on_import_error
            fi
        } &&
        vacuum \
            --schema_name fr \
            --table_name $_table_name \
            --mode ANALYZE &&
        {
            [ -n "$_query_count" ] && _query_count+='+'
            _query_count+="(SELECT COUNT(*) FROM fr.$_table_name)"
        }
    done
} &&
rm --force "$years_list_path" &&
set -o noglob &&
io_history_end_ok \
    --nrows_processed "($_query_count)" \
    --id $year_history_id || on_import_error

log_info "Import du millésime $year de $co_type_import avec succès"
exit $SUCCESS_CODE
