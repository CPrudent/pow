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

# THANKS to Guillaume for interesting comment!

#automatiser le chargement de la page d'accueil (actuellement en V5 mais pourrait changer)
#https://www.banatic.interieur.gouv.fr/ -> https://www.banatic.interieur.gouv.fr/V5/accueil/index.php
#puis de la page de téléchargement (lien "Télécharger un fichier")
#https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/fichiers-telech.php
#puis des fichiers au niveau national ("France") dans sa dernier version (actuellement indiqué par "Données mises à jour le : 01/04/2021")
#lien "Liste des groupements"
#https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=N&date=01/04/2021&format=A
#lien "Périmètre des EPCI à fiscalité propre"
#https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=N&date=01/04/2021&format=E
#lien "Table de correspondance code SIREN / Code Insee des communes"
#https://www.banatic.interieur.gouv.fr/V5/ressources/documents/document_reference/TableCorrespondanceSirenInsee.zip

# get years
io_get_list_online_available \
    --name $io_name \
    --details_file years_list_path \
    --dates_list years || exit $ERROR_CODE
[ "$POW_DEBUG" = yes ] && { declare -p years; declare -p years_list_path; }

# not useful here
rm "$years_list_path"
# only one version available
year_id=0
year=$(date -d ${years[$year_id]} +%Y)
[ -n "$get_arg_year" ] && [ "${get_arg_year}" != "year" ] && {
    log_info "seule la dernière version ($io_name) est en ligne!"
}
[ -z "$year" ] && {
    log_error "Impossible de trouver le millésime de $io_name"
    exit $ERROR_CODE
}
[ "$POW_DEBUG" = yes ] && { echo "year=$year (${years[$year_id]})"; }

set_env --schema_name fr &&
io_todo_import \
    --force $io_force \
    --name $io_name \
    --date_end "${years[$year_id]}"
case $? in
$POW_IO_SUCCESSFUL)
    exit $SUCCESS_CODE
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
    on_import_error
    ;;
esac

url_data='https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=N&date='$(date -d ${years[$year_id]} +%d/%m/%Y)'&format=' &&
{
    [ "$POW_DEBUG" = yes ] && { echo "url_data=$url_data"; } || true
} &&
log_info "Import du millésime $year de $io_name" &&
execute_query \
    --name "DELETE_IO_${io_name}" \
    --query "DELETE FROM io_history WHERE co_type = '${io_name}'" &&
io_history_begin \
    --name $io_name \
    --date_begin "${years[$year_id]}" \
    --date_end "${years[$year_id]}" \
    --nrows_todo 1250 \
    --id year_history_id &&
io_download_file \
    --url "${url_data}A" \
    --output_directory "$POW_DIR_IMPORT" \
    --output_file banatic_listof_epci_${years[$year_id]}.txt &&
io_download_file \
    --url "${url_data}E" \
    --output_directory "$POW_DIR_IMPORT" \
    --output_file banatic_setof_epci_${years[$year_id]}.txt &&
# bypass TAB at end of line (in data and sometimes header too) by dummy column
sed \
    --in-place \
    '1s/\t\?\(DUMMY\)\?$/\tDUMMY/g' \
    "$POW_DIR_IMPORT/banatic_listof_epci_${years[$year_id]}.txt" &&
sed \
    --in-place \
    '1s/\t\?\(DUMMY\)\?$/\tDUMMY/g' \
    "$POW_DIR_IMPORT/banatic_setof_epci_${years[$year_id]}.txt" &&
# delete double quote (to avoid conflict w/ CSV data)
sed \
    --in-place \
    's/"//g' \
    "$POW_DIR_IMPORT/banatic_listof_epci_${years[$year_id]}.txt" &&
sed \
    --in-place \
    's/"//g' \
    "$POW_DIR_IMPORT/banatic_setof_epci_${years[$year_id]}.txt" &&
import_file \
    --file_path "$POW_DIR_IMPORT/banatic_listof_epci_${years[$year_id]}.txt" \
    --import_options 'table_columns:HEADER_TO_LOWER_CODE' \
    --rowid no \
    --table_name banatic_listof_epci \
    --load_mode OVERWRITE_TABLE &&
import_file \
    --file_path "$POW_DIR_IMPORT/banatic_setof_epci_${years[$year_id]}.txt" \
    --import_options 'table_columns:HEADER_TO_LOWER_CODE' \
    --rowid no \
    --table_name banatic_setof_epci \
    --load_mode OVERWRITE_TABLE &&
io_download_file \
    --url 'https://www.banatic.interieur.gouv.fr/V5/ressources/documents/document_reference/TableCorrespondanceSirenInsee.zip' \
    --output_directory "$POW_DIR_IMPORT" \
    --output_file banatic_siren_insee_${years[$year_id]}.zip &&
extract_archive \
    --archive_path "$POW_DIR_IMPORT/banatic_siren_insee_${years[$year_id]}.zip" \
    --extract_path "$POW_DIR_TMP/banatic_siren_insee_${years[$year_id]}" &&
import_file \
    --file_path "$POW_DIR_TMP/banatic_siren_insee_${years[$year_id]}/Banatic_SirenInsee${year}.xlsx" \
    --rowid no \
    --table_name banatic_siren_insee \
    --load_mode OVERWRITE_TABLE &&
execute_query \
    --name CREATE_INDEX \
    --query "
        CREATE UNIQUE INDEX iux_banatic_listof_epci_n_siren
            ON fr.banatic_listof_epci(n_siren);
        CREATE UNIQUE INDEX iux_banatic_setof_epci_siren_membre
            ON fr.banatic_setof_epci(siren_membre)
            WHERE nature_juridique IN ('MET69','CC','CA','METRO','CU');
        CREATE UNIQUE INDEX iux_banatic_siren_insee_siren
            ON fr.banatic_siren_insee(siren);
        CREATE UNIQUE INDEX iux_banatic_siren_insee_insee
            ON fr.banatic_siren_insee(insee);
        " &&
rm \
    "$POW_DIR_IMPORT/banatic_listof_epci_${years[$year_id]}.txt" \
    "$POW_DIR_IMPORT/banatic_setof_epci_${years[$year_id]}.txt" \
    "$POW_DIR_IMPORT/banatic_siren_insee_${years[$year_id]}.zip" &&
rm --recursive "$POW_DIR_TMP/banatic_siren_insee_${years[$year_id]}" &&
io_history_end_ok \
    --nrows_processed '
        (SELECT COUNT(*) FROM banatic_listof_epci)
        ' \
    --id $year_history_id &&
vacuum \
    --schema_name fr \
    --table_name banatic_listof_epci \
    --mode ANALYZE &&
vacuum \
    --schema_name fr \
    --table_name banatic_setof_epci \
    --mode ANALYZE &&
vacuum \
    --schema_name fr \
    --table_name banatic_siren_insee \
    --mode ANALYZE || on_import_error

log_info "Import du millésime $year de $io_name avec succès"
exit $SUCCESS_CODE
