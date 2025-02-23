#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BANATIC setof municipalities, as EPCI (into FR schema)

bash_args \
    --args_p '
        force:Forcer l import même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique (au format YYYY) au lieu du dernier millésime disponible' \
    --args_v '
        force:yes|no' \
    --args_d '
        force:no' \
    "$@" || exit $ERROR_CODE

io_name=FR-TERRITORY-BANATIC
io_force="$get_arg_force"

on_import_error() {
    # import created?
    [ "$POW_DEBUG" = yes ] && { echo "year_history_id=$year_history_id"; }
    [ -n "$year_history_id" ] && io_history_end_ko --id $year_history_id

    exit $ERROR_CODE
}

# get years
io_get_list_online_available \
    --name $io_name \
    --details_file years_list_path \
    --dates_list years || exit $ERROR_CODE
[ "$POW_DEBUG" = yes ] && { declare -p years years_list_path; }

# not useful here
rm "$years_list_path"

# get year (w/ format YYYY)
if [ -z "$get_arg_year" ]; then
    # get more recent
    year_id=0
else
    in_array --array years --item "${get_arg_year}-01-01" --position year_id || {
        log_error "Impossible de trouver le millésime $get_arg_year de $io_name, les millésimes disponibles sont ${years[@]}"
        on_import_error
    }
fi
year=$(date -d ${years[$year_id]} +%Y)
[ -z "$year" ] && {
    log_error "Impossible de trouver le millésime de $io_name"
    exit $ERROR_CODE
}
[ "$POW_DEBUG" = yes ] && { echo "year=$year (${years[$year_id]})"; }

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

io_get_property_online_available    \
    --name $io_name                 \
    --key URL_BASE                  \
    --value url_base                &&
url_list=/files/Accueil/DESL/${year}/epcisanscom${year}.xlsx    &&
url_compose=/files/Accueil/DESL/${year}/epcicom${year}.xlsx     &&
url_list=${url_base}/${url_list}            &&
url_compose=${url_base}/${url_compose}      &&
{
    [ "$POW_DEBUG" = yes ] && { declare -p url_list url_compose; } || true
} &&
log_info "Import du millésime $year de $io_name" &&
io_history_begin \
    --io $io_name \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 1250 \
    --id year_history_id &&
io_download_file \
    --url "${url_list}" \
    --output_directory "$POW_DIR_IMPORT" \
    --output_file gouv_epci_${year}.xlsx &&
io_download_file \
    --url "${url_compose}" \
    --output_directory "$POW_DIR_IMPORT" \
    --output_file gouv_epci_municipality_${year}.xlsx &&
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
    --name CREATE_INDEX \
    --query "
        CREATE UNIQUE INDEX iux_gouv_epci_siren
            ON fr.gouv_epci(siren_epci);
        CREATE UNIQUE INDEX iux_gouv_epci_municipality_insee
            ON fr.gouv_epci_municipality(insee)
            WHERE nature_juridique ~
                '^(' ||
                (SELECT value FROM fr.constant WHERE usecase = 'FR_ADDRESS' AND key = 'EPCI_KIND')
                || ')$';
        CREATE UNIQUE INDEX iux_gouv_epci_municipality_siren
            ON fr.gouv_epci_municipality(siren_membre)
            WHERE nature_juridique ~
                '^(' ||
                (SELECT value FROM fr.constant WHERE usecase = 'FR_ADDRESS' AND key = 'EPCI_KIND')
                || ')$';
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
    --id $year_history_id &&
vacuum \
    --schema_name fr \
    --table_name gouv_epci,gouv_epci_municipality \
    --mode ANALYZE || on_import_error

log_info "Import du millésime $year de $io_name avec succès"
exit $SUCCESS_CODE
