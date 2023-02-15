#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import events of district updates

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

force="$get_arg_force_import"
year=
co_type_import=INSEE_EVENEMENT_COMMUNE

on_import_error() {
    # import created?
    [ -n "$year_history_id" ] && io_end_ko --id $year_history_id

    #On ignore l'erreur si le millésime demandé / ou de l'année courante a déjà été importé avec succès
    if [ -z "$get_arg_year" ]; then
        year=$(date +%Y)
        date_millesime='01/01/'$year
    fi
    if io_exists --type $co_type_import --date_end "${years[$year_id]}"; then
        if [ -z "$get_arg_year" ]; then
            log_info "Erreur ignorée car le millésime de l'année courante (${year}) a déjà été importé avec succès"
        else
            log_info "Erreur ignorée car le millésime demandé (${year}) a déjà été importé avec succès"
        fi
        exit $SUCCESS_CODE
    fi

    exit $ERROR_CODE
}

# get years
io_get_list_online_available --type_import $co_type_import --details_file years_list_path --dates_list years || exit $ERROR_CODE

# get year (w/ format YYYY)
if [ -z "$get_arg_year" ]; then
    year_id=0
else
    in_array years "${get_arg_year}-01-01" year_id || {
        log_error "Impossible de trouver le millésime $millesime_arg de $co_type_import, les millésimes disponibles sont ${years[@]}"
        on_import_error
    }
fi
year=$(date -d ${years[$year_id]} +%Y)
[ -z "$year" ] && {
    log_error "Impossible de trouver le millésime $year de $co_type_import"
    on_import_error
}

# URL to year information
url_information='https://www.insee.fr'$(grep --only-matching --perl-regexp 'Millésime '$year'&nbsp;: <a class="renvoi" href="[^"]*"' $years_list_path | grep --only-matching --perl-regexp '/fr/information/[^"]*') &&
io_download_file --url "$url_information" --output_directory $POW_DIR_TMP &&
year_information=$(basename $url_information) || on_import_error

# example: https://www.insee.fr/fr/statistiques/fichier/3720946/mvtcommune-01012019-csv.zip
# example: https://www.insee.fr/fr/statistiques/fichier/4316069/mvtcommune2020-csv.zip
# example: https://www.insee.fr/fr/statistiques/fichier/6051727/mvtcommune_2022.csv

# URL to data (take last one, thinking it's the more recent)
url_data=$(grep --only-matching --perl-regexp "/fr/statistiques/fichier/[0-9]*/mvtcommune[0-9-_]*$year(-csv\.zip|\.csv)" "$POW_DIR_TMP/$year_information" | tail -1)
[ -z "$url_data" ] && {
	log_error "Impossible de trouver le fichier evenement commune du millésime $year sur la page $url_information"
	on_import_error
}

url_data="https://www.insee.fr/$url_data"
year_data=$(basename $url_data)
rm --force $years_list_path $POW_DIR_TMP/$year_information

io_todo \
    --force $get_arg_force \
    --type $co_type_import \
    --date_end "${years[$year_id]}"
case $? in
$POW_IO_SUCCESSFUL)
    exit $SUCCESS_CODE
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR)
    exit $ERROR_CODE
    ;;
esac

log_info "Import du millésime $year de $co_type_import" &&
io_download_file --url $url_data --output_directory $POW_DIR_IMPORT &&
year_ressource="$POW_DIR_IMPORT/$year_data" &&
import_file \
    --file_path "$year_ressource" \
    --table_name 'evenement_commune_tmp' \
    --load_mode OVERWRITE_DATA \
    --import_options 'table_columns:HEADER_TO_LOWER_CODE' &&
execute_query \
    --name "DELETE_IO_${co_type_import}" \
    --query "DELETE FROM io_history WHERE co_type = '${co_type_import}'" &&
io_begin \
    --type $co_type_import \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 35000 \
    --id year_history_id &&
execute_query \
    --query "$POW_DIR_BATCH/import_district_event.sql" &&
io_end_ok \
    --type $co_type_import \
    --nrows_processed '(SELECT COUNT(*) FROM insee.district_event)' \
    --id $year_history_id &&
vacuum \
    --schema_name insee \
    --table_name district_event \
    --mode ANALYSE || on_import_error

exit $SUCCESS_CODE
