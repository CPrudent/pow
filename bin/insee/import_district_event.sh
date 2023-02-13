#!/usr/bin/env bash

bash_args \
    --args_p '
        force:Forcer l import même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique au format YYYY au lieu du dernier millésime disponible
    ' \
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
id_import=

on_import_error() {
    # import created?
    [ -n "$id_import" ] && io_end_ko --id $id_import

    #On ignore l'erreur si le millésime demandé / ou de l'année courante a déjà été importé avec succès
    if [ -z "$get_arg_millesime" ]; then
        year=$(date +%Y)
        date_millesime='01/01/'$year
    fi
    if io_exists --type $co_type_import --date_end $date_millesime; then
        if [ -z "$get_arg_millesime" ]; then
            log_info "Erreur ignorée car le millésime de l'année courante (${year}) a déjà été importé avec succès"
        else
            log_info "Erreur ignorée car le millésime demandé (20${year}) a déjà été importé avec succès"
        fi
        exit $SUCCESS_CODE
    fi

    exit $ERROR_CODE
}

# year(s) w/ format YYYY
io_get_list_online_available --type_import $co_type_import --details_file years_list_path --dates_list years || exit $ERROR_CODE

# no requested specific year
if [ -z "$get_arg_millesime" ]; then
    year=${years[0]}
    if [ -z "$year" ]; then
        log_error "Impossible de trouver le dernier millésime de $co_type_import"
        on_import_error
    fi
else
    year=$(echo ${years[@]} | grep --only-matching --perl-regexp "$get_arg_millesime" | head -1) || {
        log_error "Impossible de trouver le millésime $millesime_arg de $co_type_import, les millésimes disponibles sont ${years[@]}"
        on_import_error
    }
fi

lien_page_millesime='https://www.insee.fr'$(grep --only-matching --perl-regexp 'Millésime '$year'&nbsp;: <a class="renvoi" href="[^"]*"' $dir_tmp/$download_liste_file_name | grep --only-matching --perl-regexp '/fr/information/[^"]*')
#fichier dans la page du millésime du COG
download_page_file_name=$(basename $lien_page_millesime)
download_file -u $lien_page_millesime -d $dir_tmp'/'

#exemple : https://www.insee.fr/fr/statistiques/fichier/3720946/mvtcommune-01012019-csv.zip
#exemple : https://www.insee.fr/fr/statistiques/fichier/4316069/mvtcommune2020-csv.zip
#exemple : https://www.insee.fr/fr/statistiques/fichier/6051727/mvtcommune_2022.csv
#note : on prend le dernier de la liste, supposé être le plus récent, car il arrive parfois qu'il y ait plusieurs fichiers disponibles
lien_fichier_millesime=$(grep --only-matching --perl-regexp "/fr/statistiques/fichier/[0-9]*/mvtcommune[0-9-_]*$year(-csv\.zip|\.csv)" $dir_tmp'/'$download_page_file_name | tail -1)

if [ -z "$lien_fichier_millesime" ]; then
	log_error "Impossible de trouver le fichier evenement commune du millésime $year sur la page $lien_page_millesime"
	on_import_error
fi

lien_fichier_millesime="https://www.insee.fr/$lien_fichier_millesime"
nom_fichier_millesime=$(basename $lien_fichier_millesime)
date_millesime=$(echo $nom_fichier_millesime | grep -o '[0-9]\+')
if [ "${#date_millesime}" = '4' ] && [ "$date_millesime" = "$year" ]; then
	#date en 4 chiffres = année uniquement : on transforme au 1er janvier au format YYYY-MM-DD
	date_millesime="$date_millesime-01-01"
elif [ "${#date_millesime}" = '8' ] && [ "${date_millesime:4:4}" = "$year" ]; then
	#date en 8 chiffres, année en dernier : on transforme au format YYYY-MM-DD
	date_millesime=$(echo $date_millesime | sed 's/\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)/\3-\2-\1/')
else
	log_error "Impossible de déterminer le format de la date $date_millesime dans le nom du fichier $nom_fichier_millesime"
	exit $ERROR_CODE
fi

rm -f $dir_tmp'/'$download_liste_file_name
rm -f $dir_tmp'/'$download_page_file_name

if [ "$force" = 'no' ] && est_import_avec_succes -t $co_type_import -f $date_millesime; then
	log_info "Le millésime $year de $co_type_import a déjà été importé"
	exit $SUCCESS_CODE
fi

if est_import_en_cours -t $co_type_import -f $date_millesime; then
	log_error "Le millésime $year de $co_type_import est en cours d'import"
	exit $ERROR_CODE
fi


log_info "Import du millésime $year de $co_type_import" &&


download_file -u $lien_fichier_millesime -d $dir_import'/' &&
chemin_archive=$dir_import'/'$(basename $lien_fichier_millesime) &&
import_file --file_path $chemin_archive --table_name 'evenement_commune_tmp' --load_mode 'OVERWRITE_DATA' --import_options 'table_columns:HEADER_TO_LOWER_CODE' &&
execute_sql_command "DELETE_HISTORIQUE_IMPORT_${co_type_import}" "DELETE FROM public.historique_import WHERE co_type = '${co_type_import}'" &&
debut_import_en_cours -t $co_type_import -d $date_millesime -f $date_millesime -n '36000' &&
execute_sql_file $dir_batch/evenement_commune.sql &&
execute_sql_command 'DROP_TMP' "DROP TABLE insee.evenement_commune_tmp" &&
fin_import_avec_succes -i $id_import -n '(SELECT COUNT(*) FROM insee.evenement_commune)' &&
vacuum -s 'insee' -t 'evenement_commune' -m 'ANALYSE' || on_import_error

exit $SUCCESS_CODE
