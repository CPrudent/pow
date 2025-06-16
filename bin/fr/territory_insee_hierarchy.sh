#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import INSEE administrative cuttings (into FR schema)

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
    [NAME]=FR-TERRITORY-INSEE
    [ID]=
    [URL_BASE]=
    [RE_SEARCH]=
) &&
pow_argv \
    --args_n "
        force:Forcer l'import même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique (au format YY ou ALL pour tous) au lieu du dernier millésime disponible;
        load_mode:Mode de chargement des données
    " \
    --args_v '
        force:yes|no;
        load_mode:OVERWRITE|APPEND
    ' \
    --args_d '
        force:no;
        load_mode:APPEND
    ' \
    --args_p '
        reset:no;
        tag:force@bool,load_mode@1N
    ' \
    --pow_argv io_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv years year url_data context io_begin ressource' &&
{
    [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
        declare -p io_vars
        [[ ${_debug_bps[argv]} -ne 0 ]] || read
    }
} &&
set_env --schema_name fr &&
# year of administrative cutting (w/ YY format)
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
# get year (w/ format YY)
{
    if [ -z "${io_vars[YEAR]}" ]; then
        # get more recent
        year_id=0
    elif [ "${io_vars[YEAR]}" = ALL ]; then
        # get all available
        load_mode_all=${io_vars[LOAD_MODE]}
        for _year in ${years[@]}; do
            # _year w/ format YYYY-01-01
            _yy=$(date --date $_year +%y)
            $POW_DIR_BATCH/territory_insee_hierarchy.sh \
                --year $_yy \
                --load_mode $load_mode_all \
                --force ${io_vars[FORCE]} || exit $ERROR_CODE
            load_mode_all=APPEND
        done
        exit $SUCCESS_CODE
    else
        in_array --array years --item "$(date +%C)${io_vars[YEAR]}-01-01" --position year_id || {
            log_error "Impossible de trouver le millésime ${io_vars[YEAR]} de ${io_vars[NAME]}, les millésimes disponibles sont ${years[@]}"
            false
        }
    fi
} &&
year=$(date --date ${years[$year_id]} +%y) &&
{
    [[ ${_debug_steps[year]:-1} -ne 0 ]] || {
        echo "year=$year (${years[$year_id]})"
        [[ ${_debug_bps[year]} -ne 0 ]] || read
    }
} &&
{
    [ -n "$year" ] || {
        log_error "Impossible de trouver le millésime de ${io_vars[NAME]}"
        false
    }
} &&
# up to 2024, year coded on 4 digits
{
    [ $year -lt 24 ] || year="$(date +%C)$year"
} &&
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
    url_data=$(grep --only-matching --perl-regexp ${io_vars[RE_SEARCH]//\#DATE/$year} "$source_page_path" | \
    head --lines 1)
    [ -n "$url_data" ] || {
        log_error "Impossible d'extraire les URL des éléments de ${io_vars[NAME]}"
        false
    }
} &&
{
    [[ ${_debug_steps[url_data]:-1} -ne 0 ]] || {
        declare -p url_data
        [[ ${_debug_bps[url_data]} -ne 0 ]] || read
    }
} &&
url_data="${io_vars[URL_BASE]}/${url_data}" &&
year_data=$(basename "$url_data") &&
rm --force "$source_page_path" &&
# fix current year w/ century!
{
    [ ${#year} -eq 4 ] || year="$(date +%C)$year"
} &&
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
# from 2014 to now
name_worksheet_municipality=COM &&
name_worksheet_district=ARM &&
name_worksheet_supra=Zones_supra_communales &&
line_number_supra=6 &&
{
    # before 2011 (included)
    if [ $year -le 2011 ]; then
        name_worksheet_municipality=Liste_COM
        name_worksheet_district=
        name_worksheet_supra=Niv_supracom
        line_number_supra=5
    # 2012 and 2013
    elif [ $year -le 2013 ]; then
        name_worksheet_municipality='Emboîtements communaux'
        name_worksheet_district=
        name_worksheet_supra='Zones supra-communales'
    fi
} &&
{
    [[ ${_debug_steps[context]:-1} -ne 0 ]] || {
        echo "name_worksheet_municipality=$name_worksheet_municipality"
        echo "name_worksheet_district=$name_worksheet_district"
        echo "name_worksheet_supra=$name_worksheet_supra"
        echo "line_number_supra=$line_number_supra"
        [[ ${_debug_bps[context]} -ne 0 ]] || read
    }
} &&
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
        --output_directory "$POW_DIR_IMPORT" \
        --overwrite_mode no
    [ $? -lt $POW_DOWNLOAD_ERROR ] || false
} &&
year_ressource="$POW_DIR_IMPORT/$year_data" &&
{
    [[ ${_debug_steps[ressource]:-1} -ne 0 ]] || {
        echo "year_ressource=($year_ressource)"
        [[ ${_debug_bps[ressource]} -ne 0 ]] || read
    }
} &&
import_file \
    --file_path "$year_ressource" \
    --import_options "worksheet_name:${name_worksheet_municipality};from_line_number:6" \
    --table_name tmp_insee_municipality \
    --load_mode OVERWRITE_TABLE &&
execute_query \
    --name MUNICIPALITY_ADD_COLUMNS_EPCI \
    --query '
        ALTER TABLE fr.tmp_insee_municipality
            ADD COLUMN IF NOT EXISTS "EPCI" VARCHAR;
        ALTER TABLE fr.tmp_insee_municipality
            ADD COLUMN IF NOT EXISTS "NATURE_EPCI" VARCHAR;
            ' &&
import_file \
    --file_path "$year_ressource" \
    --import_options "worksheet_name:${name_worksheet_supra};from_line_number:${line_number_supra}" \
    --table_name tmp_insee_supra \
    --load_mode OVERWRITE_TABLE &&
{
    {
        # before 2011 (included)
        if [ $year -le 2011 ]; then
            execute_query \
                --name SUPRA_RENAME_COLUMNS \
                --query '
                    ALTER TABLE fr.tmp_insee_supra
                        RENAME COLUMN "Code géographique" TO "CODGEO";
                    ALTER TABLE fr.tmp_insee_supra
                        RENAME COLUMN "Niveau géographique" TO "NIVGEO";
                    ALTER TABLE fr.tmp_insee_supra
                        RENAME COLUMN "Libellé géographique" TO "LIBGEO";
                    '
        fi
    } &&
    {
        if [ -n "$name_worksheet_district" ]; then
            import_file \
                --file_path "$year_ressource" \
                --import_options "worksheet_name:${name_worksheet_district};from_line_number:6" \
                --table_name tmp_insee_municipal_district \
                --load_mode OVERWRITE_TABLE &&
            execute_query \
                --name DISTRICT_ADD_COLUMNS_EPCI \
                --query '
                    ALTER TABLE fr.tmp_insee_municipal_district
                        ADD COLUMN IF NOT EXISTS "EPCI" VARCHAR;
                    ALTER TABLE fr.tmp_insee_municipal_district
                        ADD COLUMN IF NOT EXISTS "NATURE_EPCI" VARCHAR;
                    '
        else
            # before 2013 (included) : no district (included into supra)
            execute_query \
                --name DISTRICT_CREATE_TMP \
                --query '
                    DROP TABLE IF EXISTS fr.tmp_insee_municipal_district;
                    CREATE TABLE fr.tmp_insee_municipal_district AS (
                        SELECT
                            arm."CODGEO"
                            ,arm."LIBGEO"
                            ,com_globale_arm."CODGEO" AS "COM"
                            ,com_globale_arm."DEP"
                            ,com_globale_arm."REG"
                            ,com_globale_arm."EPCI"
                            ,com_globale_arm."NATURE_EPCI"
                            ,com_globale_arm."ARR"
                            ,com_globale_arm."CV"
                        FROM fr.tmp_insee_supra AS arm
                        INNER JOIN fr.tmp_insee_municipality AS com_globale_arm
                            ON com_globale_arm."CODGEO" = (
                                CASE LEFT(arm."CODGEO",3)
                                WHEN '"'"'132'"'"' THEN '"'"'13055'"'"' /*Marseille*/
                                WHEN '"'"'693'"'"' THEN '"'"'69123'"'"' /*Lyon*/
                                WHEN '"'"'751'"'"' THEN '"'"'75056'"'"' /*Paris*/
                                END
                            )
                        WHERE arm."NIVGEO" = '"'"'ARM'"'"'
                    );
                    DELETE FROM fr.tmp_insee_supra
                        WHERE "NIVGEO" = '"'"'ARM'"'"';
                    '
        fi
    }
} &&
execute_query \
    --name TRUNCATE_DATA \
    --query '
        TRUNCATE TABLE fr.insee_municipality;
        TRUNCATE TABLE fr.insee_supra;
        ' &&
execute_query \
    --name ADMINISTRATIVE_CUTTING \
    --query "$POW_DIR_BATCH/territory_insee_hierarchy.sql" &&
execute_query \
    --name DROP_TMP \
    --query '
        DROP TABLE fr.tmp_insee_municipality;
        DROP TABLE fr.tmp_insee_municipal_district;
        DROP TABLE fr.tmp_insee_supra;
        ' &&
io_history_end_ok \
    --nrows_processed '
        (
            (SELECT COUNT(*) FROM fr.insee_municipality)+
            (SELECT COUNT(*) FROM fr.insee_supra)
        )
        ' \
    --id ${io_vars[ID]} &&
vacuum \
    --schema_name fr \
    --table_name insee_municipality,insee_supra \
    --mode ANALYZE &&
rm --force "$year_ressource" ||
{
    on_import_error --id ${io_vars[ID]}
}

io_purge_common --name ${io_vars[NAME]}
log_info "Import du millésime $year de ${io_vars[NAME]} avec succès"

exit $SUCCESS_CODE
