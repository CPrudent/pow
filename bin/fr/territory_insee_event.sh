#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import INSEE events of municipality updates (into FR schema)

bash_args \
    --args_p "
        force:Forcer l'import même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique (au format YYYY) au lieu du dernier millésime disponible
    " \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

io_name=FR-MUNICIPALITY-EVENT-INSEE
io_force="$get_arg_force"
# year of municipality events (w/ YYYY format)
year=

on_import_error() {
    # import created?
    [ "$POW_DEBUG" = yes ] && { echo "year_history_id=$year_history_id"; }
    [ -n "$year_history_id" ] && io_history_end_ko --id $year_history_id

    # ignoring error if last year already exists
    if io_history_exists --io $io_name --date_end "${years[$year_id]}"; then
        if [ -z "$get_arg_year" ]; then
            log_info "Erreur ignorée car le millésime de l'année courante (${year}) a déjà été importé avec succès"
        else
            log_info "Erreur ignorée car le millésime demandé (${year}) a déjà été importé avec succès"
        fi
        exit $SUCCESS_CODE
    fi

    log_error "Erreur import du millésime (${year})"
    exit $ERROR_CODE
}

# get years
io_get_list_online_available \
    --name $io_name \
    --details_file years_list_path \
    --dates_list years || exit $ERROR_CODE
[ "$POW_DEBUG" = yes ] && { declare -p years; declare -p years_list_path; }

# fix INSEE change (only last year available)
year=$(date +%Y)
year_id=0
[ "$POW_DEBUG" = yes ] && { echo "year=$year (${years[$year_id]})"; }

# example: https://www.insee.fr/fr/statistiques/fichier/3720946/mvtcommune-01012019-csv.zip
# example: https://www.insee.fr/fr/statistiques/fichier/4316069/mvtcommune2020-csv.zip
# example: https://www.insee.fr/fr/statistiques/fichier/6051727/mvtcommune_2022.csv

# NOTE change 2023 (v_)mvtcommune_2023.csv

# NOTE change 2024
# href="fr/information/7766585/v_mvt_commune_2024.csv"

# NOTE change 2025 (archive)
# search for: Code officiel géographique au 1er janvier 2025
# href="fr/information/8377162/cog_ensemble_2025_csv.zip"
#+ v_arrondissement_2025.csv
#+ v_canton_2025.csv
#+ v_codes_extension_2025.csv
#+ v_comer_2025.csv
#+ v_commune_2025.csv
#+ v_commune_comer_2025.csv
#+ v_commune_depuis_1943.csv
#+ v_commune_outremer_depuis_1943.csv
#+ v_ctcd_2025.csv
#+ v_departement_2025.csv
#+ v_mvt_commune_2025.csv
#+ v_pays_et_territoire_depuis_1943.csv
#+ v_pays_territoire_2025.csv
#+ v_region_2025.csv
#+ v_tom_depuis_1943.csv

url_data=$(grep --only-matching --perl-regexp "/fr/statistiques/fichier/8377162/cog_ensemble_${year}_csv.zip" "$years_list_path")
[ "$POW_DEBUG" = yes ] && { echo "url=$url_data"; }
[ -z "$url_data" ] && {
    log_error "Impossible de trouver URL de $io_name"
    on_import_error
}

url_data="https://www.insee.fr/${url_data}"
year_data=$(basename "$url_data")
[ "$POW_DEBUG" = yes ] && { echo "year_data=$year_data"; }
rm --force "$years_list_path"

set_env --schema_name fr &&
io_todo_import \
    --force $io_force \
    --io $io_name \
    --date_end "${years[$year_id]}"
case $? in
$POW_IO_SUCCESSFUL)
    exit $SUCCESS_CODE
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
    on_import_error
    ;;
esac

# estimate to ~35000 municipalities
log_info "Import du millésime $year de $io_name" &&
# execute_query \
#     --name "DELETE_IO_${io_name}" \
#     --query "DELETE FROM io_history WHERE name = '${io_name}'" &&
io_history_begin \
    --io $io_name \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 35000 \
    --id year_history_id &&
io_download_file \
    --url "$url_data" \
    --overwrite_mode no \
    --output_directory "$POW_DIR_IMPORT" &&
year_ressource="$POW_DIR_TMP/$year_data/v_mvt_commune_${year}.csv" &&
{
    [ "$POW_DEBUG" = yes ] && { echo "year_ressource=$year_ressource"; } || true
} &&
extract_archive \
    --archive_path "$POW_DIR_IMPORT/$year_data" \
    --extract_path "$POW_DIR_TMP/$year_data" &&
import_file \
    --file_path "$year_ressource" \
    --table_name insee_municipality_event_tmp \
    --load_mode OVERWRITE_DATA \
    --import_options 'table_columns:HEADER_TO_LOWER_CODE' &&
execute_query \
    --name MUNICIPALITY_EVENT \
    --query "$POW_DIR_BATCH/territory_insee_event.sql" &&
io_history_end_ok \
    --nrows_processed '(SELECT COUNT(*) FROM fr.insee_municipality_event)' \
    --id $year_history_id &&
vacuum \
    --schema_name fr \
    --table_name insee_municipality_event \
    --mode ANALYZE &&
rm --force "$POW_DIR_IMPORT/$year_data" &&
rm --force --recursive "$POW_DIR_TMP/$year_data" || on_import_error

log_info "Import du millésime $year de $io_name avec succès"
exit $SUCCESS_CODE
