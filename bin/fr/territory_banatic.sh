#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BANATIC setof municipalities, as EPCI (into FR schema)

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

    exit $ERROR_CODE
}

declare -A io_vars=(
    [NAME]=FR-TERRITORY-GOUV-EPCI
    [ID]=
) &&
pow_argv \
    --args_n '
        force:Forcer l import même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique (au format YYYY) au lieu du dernier millésime disponible' \
    --args_v '
        force:yes|no' \
    --args_d '
        force:no' \
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
    'argv years year io_begin ressource'

[[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
    declare -p io_vars
    [[ ${_debug_bps[argv]} -eq 0 ]] && read
}

# get years
set_env --schema_name fr &&
io_get_list_online_available \
    --name ${io_vars[NAME]} \
    --details_file years_list_path \
    --dates_list years || exit $ERROR_CODE
[[ ${_debug_steps[years]:-1} -eq 0 ]] && {
    declare -p years years_list_path
    [[ ${_debug_bps[years]} -eq 0 ]] && read
}

# get year (w/ format YYYY)
if [ -z "${io_vars[YEAR]}" ]; then
    # get more recent
    year_id=0
else
    in_array --array years --item "${io_vars[YEAR]}-01-01" --position year_id || {
        log_error "Impossible de trouver le millésime ${io_vars[YEAR]} de ${io_vars[NAME]}, les millésimes disponibles sont ${years[@]}"
        on_import_error --id ${io_vars[ID]}
    }
fi
year=$(date -d "${years[$year_id]}" '+%Y')
[[ ${_debug_steps[year]:-1} -eq 0 ]] && {
    echo "year=$year (${years[$year_id]})"
    [[ ${_debug_bps[year]} -eq 0 ]] && read
}
[ -z "$year" ] && {
    log_error "Impossible de trouver le millésime de ${io_vars[NAME]} (${years[@]})"
    exit $ERROR_CODE
}
# not useful here
rm "$years_list_path"

io_todo_import \
    --force ${io_vars[FORCE]} \
    --io ${io_vars[NAME]} \
    --date_end "${years[$year_id]}"
case $? in
$POW_IO_SUCCESSFUL)
    exit $SUCCESS_CODE
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
    on_import_error --id ${io_vars[ID]}
    ;;
esac

io_get_property_online_available    \
    --name ${io_vars[NAME]}         \
    --key URL_BASE                  \
    --value url_base                &&
url_list=/files/Accueil/DESL/${year}/epcisanscom${year}.xlsx    &&
url_compose=/files/Accueil/DESL/${year}/epcicom${year}.xlsx     &&
url_list=${url_base}/${url_list}                                &&
url_compose=${url_base}/${url_compose}                          &&
{
    [[ ${_debug_steps[ressource]:-1} -ne 0 ]] || {
        echo "url_list=($url_list)"
        echo "url_compose=($url_compose)"
        [[ ${_debug_bps[ressource]} -ne 0 ]] || read
    }
} &&
log_info "Import du millésime $year de ${io_vars[NAME]}" &&
io_history_begin \
    --io ${io_vars[NAME]} \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 1250 \
    --id io_vars[ID] &&
{
    [[ ${_debug_steps[io_begin]:-1} -ne 0 ]] || {
        echo "id=(${io_vars[ID]})"
        [[ ${_debug_bps[io_begin]} -ne 0 ]] || read
    }
} &&
{
    io_download_file \
        --url "${url_list}" \
        --overwrite_mode no \
        --output_directory "$POW_DIR_IMPORT" \
        --output_file gouv_epci_${year}.xlsx
    [ $? -lt $POW_DOWNLOAD_ERROR ] || false
} &&
{
    io_download_file \
        --url "${url_compose}" \
        --overwrite_mode no \
        --output_directory "$POW_DIR_IMPORT" \
        --output_file gouv_epci_municipality_${year}.xlsx
    [ $? -lt $POW_DOWNLOAD_ERROR ] || false
} &&
import_file \
    --file_path "$POW_DIR_IMPORT/gouv_epci_${year}.xlsx" \
    --import_options 'table_columns:HEADER_TO_LOWER_CODE' \
    --rowid no \
    --table_name gouv_epci \
    --load_mode OVERWRITE_TABLE &&
import_file \
    --file_path "$POW_DIR_IMPORT/gouv_epci_municipality_${year}.xlsx" \
    --import_options 'table_columns:HEADER_TO_LOWER_CODE' \
    --rowid no \
    --table_name gouv_epci_municipality \
    --load_mode OVERWRITE_TABLE &&
execute_query \
    --name EPCI_KIND \
    --query "
        SELECT value FROM fr.constant WHERE usecase = 'FR_ADDRESS' AND key = 'EPCI_KIND'
    " \
    --return _epci_kind &&
execute_query \
    --name CREATE_INDEX \
    --query "
        CREATE UNIQUE INDEX iux_gouv_epci_siren
            ON fr.gouv_epci(siren_epci);
        CREATE UNIQUE INDEX iux_gouv_epci_municipality_insee
            ON fr.gouv_epci_municipality(insee)
            WHERE nature_juridique ~ '^($_epci_kind)$';
        CREATE UNIQUE INDEX iux_gouv_epci_municipality_siren
            ON fr.gouv_epci_municipality(siren_membre)
            WHERE nature_juridique ~ '^($_epci_kind)$';
        " &&
execute_query \
    --name RENAME_COLUMNS \
    --query "
        ALTER TABLE fr.gouv_epci RENAME nj_epci${year} TO nature_juridique;
        ALTER TABLE fr.gouv_epci RENAME fisc_epci${year} TO fisc;
        ALTER TABLE fr.gouv_epci RENAME nb_com_${year} TO nb_communes;
        " &&
rm \
    "$POW_DIR_IMPORT/gouv_epci_${year}.xlsx" \
    "$POW_DIR_IMPORT/gouv_epci_municipality_${year}.xlsx" &&
io_history_end_ok \
    --nrows_processed '
        (SELECT COUNT(*) FROM gouv_epci)
        ' \
    --id ${io_vars[ID]} &&
vacuum \
    --schema_name fr \
    --table_name gouv_epci,gouv_epci_municipality \
    --mode ANALYZE || on_import_error --id ${io_vars[ID]}

log_info "Import du millésime $year de ${io_vars[NAME]} avec succès"
exit $SUCCESS_CODE
