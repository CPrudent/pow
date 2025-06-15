#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import IGN geometry of IRIS Grande Echelle (GE), into FR schema

# CHANGELOG (IRIS_GE)
# 3.0
#  many archives:
#   - FXX (FR métropolitaine)
#   - GLP (Guadeloupe)
#   - MTQ (Martinique)
#   - GUF (Guyane)
#   - REU (La Réunion)
#   - SPM (Saint-Pierre-et-Miquelon)
#   - MYT (Mayotte)
#   - BLM (Saint-Barthélémy)
#   - MAF (Saint-Martin)


# NOTE to debug:
# export POW_DEBUG_JSON='{"codes":[{"name":"territory_ign_iris_ge","steps":["argv","url@break","create@break","copy@break"]}]}'

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

# deal w/ interrupt signal (CTRL-C, kill)
on_break() {
    log_error 'arrêt utilisateur' &&
    rm --force "$years_list_path" &&
    on_import_error --id ${io_vars[ID]}
}
trap on_break SIGINT

declare -A io_vars=(
    [NAME]=FR-TERRITORY-IGN-IRIS_GE
    [TODO]=no
    [ID]=
    [PASSWD]=
    [RE_SEARCH]=
    [TABLE_NAME]=ign_iris_ge
) &&
pow_argv \
    --args_n '
        force:Forcer le traitement même si celui-ci a déjà été fait;
        year:Importer un millésime spécifique (au format YYYY-MM-DD) au lieu du dernier millésime disponible;
        clean:Effacer les résultats intermédiaires
    ' \
    --args_v '
        force:yes|no;
        clean:yes|no
    ' \
    --args_d '
        force:no;
        clean:yes
    ' \
    --args_p '
        reset:no;
        tag:force@bool,clean@bool
    ' \
    --pow_argv io_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps &&
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv years year io_begin url shp create copy' &&
io_get_property_online_available    \
    --name ${io_vars[NAME]}         \
    --key REGEXP_SEARCH             \
    --value io_vars[RE_SEARCH]      &&
{
    [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
        declare -p io_vars
        [[ ${_debug_bps[argv]} -ne 0 ]] || read
    }
} &&
set_env --schema_name fr &&
# get years
io_get_years_online_available \
    --name ${io_vars[NAME]} \
    --details_file years_list_path \
    --dates_list years &&
{
    [[ ${_debug_steps[years]:-1} -ne 0 ]] || {
        declare -p years years_list_path
        [[ ${_debug_bps[years]} -ne 0 ]] || read
    }
} &&
# get year (w/ format YYYY)
{
    if [ -z "${io_vars[YEAR]}" ]; then
        # get more recent
        year_id=0
    else
        in_array --array years --item "${io_vars[YEAR]}" --position year_id || {
            log_error "Impossible de trouver le millésime ${io_vars[YEAR]} de ${io_vars[NAME]}, les millésimes disponibles sont ${years[@]}"
            on_import_error --id ${io_vars[ID]}
        }
    fi
} &&
year=${years[$year_id]} &&
{
    [ -n "$year" ] || {
        log_error "Impossible de trouver le millésime de ${io_vars[NAME]}"
        false
    }
} &&
{
    [[ ${_debug_steps[year]:-1} -ne 0 ]] || {
        echo "year=$year (${years[$year_id]})"
        [[ ${_debug_bps[year]} -ne 0 ]] || read
    }
} &&
# already done or in progress ?
io_todo_import \
    --force ${io_vars[FORCE]} \
    --io ${io_vars[NAME]} \
    --date_end "${years[$year_id]}"
case $? in
$POW_IO_TODO)
    io_vars[TODO]=yes
    ;;
$POW_IO_SUCCESSFUL)
    log_info "IO '${io_vars[NAME]}' déjà à jour!"
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
    log_error "IO '${io_vars[NAME]}' en erreur!"
    false
    ;;
esac &&
{
    [ "${io_vars[TODO]}" = no ] || {
        log_info "Import du millésime $year de ${io_vars[NAME]}" &&
        {
            get_pg_passwd --user_name $POW_PG_USERNAME --password io_vars[PASSWD] || {
                log_error "Erreur de récupération du mot de passe (user=$POW_PG_USERNAME)"
                false
            }
        } &&
        [ -n "${io_vars[PASSWD]}" ] &&
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
            # search for IRIS_GE (of year)
            url_data_all=($(grep --only-matching --perl-regexp "${io_vars[RE_SEARCH]/\#DATE/$year}" "$years_list_path" | grep --only-matching --perl-regexp '(http|ftp).*')) &&
            {
                [[ ${_debug_steps[url]:-1} -ne 0 ]] || {
                    declare -p url_data_all
                    [[ ${_debug_bps[url]} -ne 0 ]] || read
                }
            } &&
            for ((_i=0; _i<${#url_data_all[*]}; _i++)); do
                url_data_one=${url_data_all[$_i]} &&
                year_data=$(basename $url_data_one) &&
                {
                    [[ ${_debug_steps[url]:-1} -ne 0 ]] || {
                        echo "year_data=${year_data}"
                        [[ ${_debug_bps[url]} -ne 0 ]] || read
                    }
                } &&
                {
                    io_download_file \
                        --url "${url_data_one}" \
                        --output_directory "${POW_DIR_IMPORT}" \
                        --overwrite_mode no
                    [[ $? -lt $POW_DOWNLOAD_ERROR ]]
                } &&
                mkdir --parent "$POW_DIR_TMP/IRIS_GE-$year" &&
                extract_archive \
                    --archive_path "$POW_DIR_IMPORT/$year_data" \
                    --extract_path "$POW_DIR_TMP/IRIS_GE-$year" || {
                    log_error "abandon téléchargement IRIS_GE-$year"
                    on_import_error --id ${io_vars[ID]}
                }
            done &&
            _first_file=yes &&
            for _shp_full_path in $(find $POW_DIR_TMP/IRIS_GE-$year -type f -iname IRIS*.shp); do
                _shp_file=$(basename $_shp_full_path) &&
                # NOTE on ne crée pas d'index géographique pour éviter de ralentir les imports successifs
                #      de plus non exploité
                # NOTE on importe dans un table temporaire, qu'on recopie dans la table commune, afin de
                #      stocker les SRID d'origines
                #      en mode overwrite + append on ne peut pas le faire
                {
                    [[ ${_debug_steps[shp]:-1} -ne 0 ]] || {
                        echo "shp_file=($_shp_file)"
                        [[ ${_debug_bps[shp]} -ne 0 ]] || read
                    }
                } &&
                import_geo_file \
                    --file_path "$_shp_full_path" \
                    --table_name "tmp_${io_vars[TABLE_NAME]}" \
                    --password "${io_vars[PASSWD]}" \
                    --geometry_type PROMOTE_TO_MULTI \
                    --load_mode OVERWRITE_DATA \
                    --spatial_index no &&
                {
                    [ "$_first_file" = no ] || {
                        execute_query \
                            --name CREATE_TABLE \
                            --query "
                                CREATE TABLE IF NOT EXISTS fr.${io_vars[TABLE_NAME]} AS TABLE fr.tmp_${io_vars[TABLE_NAME]};
                                TRUNCATE TABLE fr.${io_vars[TABLE_NAME]};
                                SELECT public.drop_table_indexes('fr', '${io_vars[TABLE_NAME]}');
                                ALTER TABLE fr.${io_vars[TABLE_NAME]} ALTER COLUMN geom TYPE GEOMETRY;
                            "  &&
                        _first_file=no &&
                        {
                            [[ ${_debug_steps[create]:-1} -ne 0 ]] || {
                                echo "CREATE fr.${io_vars[TABLE_NAME]}"
                                [[ ${_debug_bps[create]} -ne 0 ]] || read
                            }
                        }
                    }
                } &&
                execute_query \
                    --name COPY_TMP_TO_TABLE \
                    --query "
                        INSERT INTO fr.${io_vars[TABLE_NAME]}
                            SELECT * FROM fr.tmp_${io_vars[TABLE_NAME]};
                    " &&
                {
                    [[ ${_debug_steps[copy]:-1} -ne 0 ]] || {
                        echo "COPY fr.tmp_${io_vars[TABLE_NAME]}"
                        [[ ${_debug_bps[copy]} -ne 0 ]] || read
                    }
                } || {
                    log_error "abandon chargement IRIS_GE-$year ($_shp_file)"
                    on_import_error --id ${io_vars[ID]}
                }
            done &&
            # applying buffer can enable geometry
            execute_query \
                --name UPDATE_INVALID_GEOM \
                --query "
                    UPDATE fr.${io_vars[TABLE_NAME]} SET geom = ST_MakeValid2(geom)
                    WHERE NOT ST_IsValid(geom)
                " &&
            execute_query \
                --name CREATE_INDEX \
                --query "
                    CREATE UNIQUE INDEX ON fr.${io_vars[TABLE_NAME]}(code_iris)
                "
        } &&
        vacuum \
            --schema_name fr \
            --table_name ${io_vars[TABLE_NAME]} \
            --mode ANALYZE &&
        _query_count="(SELECT COUNT(*) FROM fr.${io_vars[TABLE_NAME]})" &&
        {
            rm --force "$years_list_path" &&
            rm --force --recursive "$POW_DIR_TMP/IRIS_GE-$year" &&
            execute_query \
                --name DROP_TMP_TABLE \
                --query "
                    DROP TABLE fr.tmp_${io_vars[TABLE_NAME]};
                "
        } &&
        io_history_end_ok \
            --nrows_processed "($_query_count)" \
            --id ${io_vars[ID]} ||
        {
            on_import_error --id ${io_vars[ID]}
        }

        io_purge_common --name ${io_vars[NAME]}
        log_info "Import du millésime $year de ${io_vars[NAME]} avec succès"
    }
}

$POW_DIR_BATCH/municipality_laposte_vs_iris-ge.sh --force ${io_vars[FORCE]}

exit $SUCCESS_CODE
