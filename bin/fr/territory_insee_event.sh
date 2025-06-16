#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import INSEE events of municipality updates (into FR schema)

# CHANGELOG
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

on_import_error() {
    local -A _opts &&
    pow_argv \
        --args_n '
            id:ID historique en cours
        ' \
        --args_m '
            id
        ' \
        --pow_argv _opts "$@" || return $?

    # history created?
    #echo "id=${_opts[ID]}"
    [ -n "${_opts[ID]}" ] && io_history_end_ko --id ${_opts[ID]}

    # ignoring error if last year already exists
    if io_history_exists --io ${io_vars[NAME]} --date_end "${years[$year_id]}"; then
        if [ -z "${io_vars[YEAR]}" ]; then
            log_info "Erreur ignorée car le millésime de l'année courante (${year}) a déjà été importé avec succès"
        else
            log_info "Erreur ignorée car le millésime demandé (${year}) a déjà été importé avec succès"
        fi
        exit $SUCCESS_CODE
    fi

    log_error "Erreur import du millésime (${year})"
    exit $ERROR_CODE
}

# deal w/ interrupt signal (CTRL-C, kill)
on_break() {
    log_error 'arrêt utilisateur' &&
    rm --force "$source_page_path" &&
    on_import_error --id ${io_vars[ID]}
}
trap on_break SIGINT

declare -A io_vars=(
    [NAME]=FR-MUNICIPALITY-EVENT-INSEE
    [ID]=
    [URL_BASE]=
    [RE_SEARCH]=
) &&
pow_argv \
    --args_n "
        force:Forcer l'import même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique (au format YYYY) au lieu du dernier millésime disponible
    " \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    --args_p '
        reset:no;
        tag:force@bool,year@int
    ' \
    --pow_argv io_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv years year url url_data io_begin ressource' &&
{
    [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
        declare -p io_vars
        [[ ${_debug_bps[argv]} -ne 0 ]] || read
    }
} &&
set_env --schema_name fr &&
# year of municipality events (w/ YYYY format)
io_get_years_online_available \
    --name ${io_vars[NAME]} \
    --source_page source_page_path \
    --years years &&
{
    [[ ${_debug_steps[years]:-1} -ne 0 ]] || {
        declare -p years source_page_path
        [[ ${_debug_bps[years]} -ne 0 ]] || read
    }
} &&
# fix INSEE change (only last year available)
year_id=0 &&
year=$(date --date ${years[$year_id]} +%Y) &&
{
    [[ ${_debug_steps[year]:-1} -ne 0 ]] || {
        echo "year=$year"
        [[ ${_debug_bps[year]} -ne 0 ]] || read
    }
} &&
io_get_property_online_available    \
    --name ${io_vars[NAME]}         \
    --key URL_BASE                  \
    --value io_vars[URL_BASE]       &&
io_get_property_online_available    \
    --name ${io_vars[NAME]}         \
    --key REGEXP_SEARCH             \
    --value io_vars[RE_SEARCH]      &&
{
    url_data=$(grep --only-matching --perl-regexp ${io_vars[RE_SEARCH]//\#DATE/$year} "$source_page_path")
    [ -n "$url_data" ] || {
        log_error "Impossible d'extraire les URL des éléments de ${io_vars[NAME]}"
        false
    }
} &&
{
    [[ ${_debug_steps[url]:-1} -ne 0 ]] || {
        echo "url=$url_data"
        [[ ${_debug_bps[url]} -ne 0 ]] || read
    }
} &&
url_data="${io_vars[URL_BASE]}/${url_data}" &&
year_data=$(basename "$url_data") &&
{
    [[ ${_debug_steps[url_data]:-1} -ne 0 ]] || {
        echo "year_data=$year_data"
        [[ ${_debug_bps[url_data]} -ne 0 ]] || read
    }
} &&
rm --force "$source_page_path" &&
{
    io_todo_import \
        --force ${io_vars[FORCE]} \
        --io ${io_vars[NAME]} \
        --date_end "${years[$year_id]}"
    case $? in
    $POW_IO_SUCCESSFUL)
        exit $SUCCESS_CODE
        ;;
    $POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
        false
        ;;
    esac
} &&
# estimate to ~35000 municipalities
log_info "Import du millésime $year de ${io_vars[NAME]}" &&
io_history_begin \
    --io ${io_vars[NAME]} \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 35000 \
    --id io_vars[ID] &&
{
    [[ ${_debug_steps[io_begin]:-1} -ne 0 ]] || {
        echo "id=(${io_vars[ID]})"
        [[ ${_debug_bps[io_begin]} -ne 0 ]] || read
    }
} &&
{
    io_download_file \
        --url "$url_data" \
        --overwrite_mode no \
        --output_directory "$POW_DIR_IMPORT"
    [ $? -lt $POW_DOWNLOAD_ERROR ] || false
} &&
year_ressource="$POW_DIR_TMP/$year_data/v_mvt_commune_${year}.csv" &&
{
    [[ ${_debug_steps[ressource]:-1} -ne 0 ]] || {
        echo "year_ressource=($year_ressource)"
        [[ ${_debug_bps[ressource]} -ne 0 ]] || read
    }
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
    --id ${io_vars[ID]} &&
vacuum \
    --schema_name fr \
    --table_name insee_municipality_event \
    --mode ANALYZE &&
rm --force "$POW_DIR_IMPORT/$year_data" &&
rm --force --recursive "$POW_DIR_TMP/$year_data" ||
{
    on_import_error --id ${io_vars[ID]}
}

io_purge_common --name ${io_vars[NAME]}
log_info "Import du millésime $year de ${io_vars[NAME]} avec succès"

exit $SUCCESS_CODE
