#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import IGN geometry of territories (into FR schema)

    # NOTE to debug,
    # export POW_DEBUG_JSON='{"codes":[{"name":"territory_ign_admin-express","steps":["argv","items","years","year@break","url_all","url@break","item","table@break"]}]}'

    # TODO
    # add function purge_common --name <IO>

# CHANGELOG (ADMIN-EXPRESS)
# 2.5
#   ENTITE_RATTACHEE existe toujours mais n'inclut plus les arrondissements municipaux (entite_rattachee.type = 'ARM')
#	qui sont dans un nouveau fichier ARRONDISSEMENT_MUNICIPAL(colonnes indentiques à entite_rattachee)
# 3.0
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
# 3.1
#   archive du millésime est décomposée en plusieurs : FR métropolitaine, GLP, MTQ, GUF, REU et MYT
#   chacune des 6 archives contient un fichier par élément
# 3.2
#
# 4.0
#   https://geoservices.ign.fr/sites/default/files/2025-06/SE_ADMIN_EXPRESS_depuis_v4-0.pdf
#   no SHP but GPKG !
#   https://gis.stackexchange.com/questions/290582/uploading-geopackage-contents-to-postgresql
#   ajout (et renommage) classes d'objets
#   ajout (et renommage) attributs (noms des colonnes)

# deal w/ interrupt signal (CTRL-C, kill)
on_break() {
    log_error 'arrêt utilisateur' &&
    on_import_error --id ${io_vars[ID]}
}
trap on_break SIGINT

# NOTE: les types d'élements correspondent aux différentes couches existantes
declare -a _AVAILABLE_LAYERS=(
    arrondissement
    arrondissement_municipal
    canton
    chef_lieu_d_arrondissement
    chef_lieu_d_arrondissement_municipal
    chef_lieu_commune
    chef_lieu_commune_associee_ou_deleguee
    chef_lieu_de_canton
    chef_lieu_de_collectivite_territoriale
    chef_lieu_de_departement
    chef_lieu_d_epci
    chef_lieu_de_region
    collectivite_territoriale
    commune
    commune_associee_ou_deleguee
    departement
    epci
    region
)
_AVAILABLE_LAYERS_JOIN_PIPE=${_AVAILABLE_LAYERS[@]}
_AVAILABLE_LAYERS_JOIN_PIPE=${_AVAILABLE_LAYERS_JOIN_PIPE// /|}

declare -A _TABLES=(
    [arrondissement]=district
    [arrondissement_municipal]=municipal_district
    [canton]=canton
    [chef_lieu_d_arrondissement]=district_capital
    [chef_lieu_d_arrondissement_municipal]=municipal_district_capital
    [chef_lieu_commune]=municipality_capital
    [chef_lieu_commune_associee_ou_deleguee]=municipality_old_capital
    [chef_lieu_de_canton]=canton_capital
    [chef_lieu_de_collectivite_territoriale]=local_authority_capital
    [chef_lieu_de_departement]=departement_capital
    [chef_lieu_d_epci]=epci_capital
    [chef_lieu_de_region]=region_capital
    [collectivite_territoriale]=local_authority
    [commune]=municipality
    [commune_associee_ou_deleguee]=municipality_old
    [departement]=department
    [epci]=epci
    [region]=region
)

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
    [NAME]=FR-TERRITORY-IGN
    [ID]=
    [PASSWD]=
    [RE_SEARCH]=
    [RE_FILE]=
    [LOAD_MODE]=OVERWRITE_DATA
) &&
pow_argv \
    --args_n '
        force:Forcer le traitement même si celui-ci a déjà été fait;
        layer:Elément à importer (couche de données);
        year:Importer un millésime spécifique (au format YYYY-MM-DD) au lieu du dernier millésime disponible;
        clean:Effacer les résultats intermédiaires
    ' \
    --args_v '
        force:yes|no;
        clean:yes|no;
        layer:'$_AVAILABLE_LAYERS_JOIN_PIPE \
    --args_d '
        force:no;
        clean:yes
    ' \
    --args_p '
        reset:no;
        tag:force@bool,clean@bool,layer@0N
    ' \
    --pow_argv io_vars "$@" || exit $?

# DEBUG steps
declare -A _debug_steps _debug_bps &&
get_env_debug \
    "$(basename $0 .sh)" \
    _debug_steps \
    _debug_bps \
    'argv items io_last years year io_begin url_all url item table' &&
{
    io_get_property_online_available    \
        --name ${io_vars[NAME]}         \
        --key REGEXP_SEARCH             \
        --value io_vars[RE_SEARCH]      &&
    io_get_property_online_available    \
        --name ${io_vars[NAME]}         \
        --key REGEXP_FILE               \
        --value io_vars[RE_FILE]
} &&
{
    [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
        declare -p io_vars
        [[ ${_debug_bps[argv]} -ne 0 ]] || read
    }
} &&
{
    # according command line
    if [ -z "${io_vars[LAYER]}" ]; then
        # NOTE: some items seem not useful
        declare -a LAYERS=(commune arrondissement_municipal departement epci region)
    else
        io_name=FR-TERRITORY-IGN-${io_vars[LAYER]}
        declare -a LAYERS=(${io_vars[LAYER]})
    fi
} &&
{
    [[ ${_debug_steps[items]:-1} -ne 0 ]] || {
        declare -p LAYERS
        [[ ${_debug_bps[items]} -ne 0 ]] || read
    }
} &&
set_env --schema_name fr &&
# no error if already downloaded (w/ last IO)
execute_query \
    --name "LAST_IO_${io_vars[NAME]}" \
    --query "
        SELECT TO_CHAR(date_data_end, 'YYYY-MM-DD')
        FROM get_last_io('${io_vars[NAME]}')
    " \
    --return _last_io &&
{
    [[ ${_debug_steps[io_last]:-1} -ne 0 ]] || {
        echo "last_io=($_last_io)"
        [[ ${_debug_bps[io_last]} -ne 0 ]] || read
    }
} &&
# get years
io_get_years_online_available \
    --name ${io_vars[NAME]} \
    --details_file years_list_path \
    --dates_list years || {
    [ -n "$_last_io" ] && _rc=$SUCCESS_CODE || _rc=$ERROR_CODE
    exit $_rc
} &&
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
            false
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
esac &&
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
# search for requested ADMIN-EXPRESS (avoiding all items as WGS84)
url_data_all=($(grep --only-matching --perl-regexp ${io_vars[RE_SEARCH]/\#DATE/$year} "$years_list_path" | grep --only-matching --perl-regexp '(http|ftp).*' | grep --invert-match WGS84)) &&
{
    [[ ${_debug_steps[url_all]:-1} -ne 0 ]] || {
        declare -p url_data_all
        [[ ${_debug_bps[url_all]} -ne 0 ]] || read
    }
} &&
[[ ${#url_data_all[@]} -gt 0 ]] &&
for ((_url_i=0; _url_i<${#url_data_all[*]}; _url_i++)); do
    url_data_one=${url_data_all[$_url_i]} &&
    year_data=$(basename $url_data_one) &&
    {
        [[ ${_debug_steps[url]:-1} -ne 0 ]] || {
            echo "data=${year_data}"
            [[ ${_debug_bps[url]} -ne 0 ]] || read
        }
    } &&
    {
        io_download_file \
            --url "${url_data_one}" \
            --output_directory "${POW_DIR_IMPORT}" \
            --output_file "${year_data}" \
            --overwrite_mode no
        [ $? -lt $POW_DOWNLOAD_ERROR ] || false
    } &&
    mkdir --parent "$POW_DIR_TMP/$year_data" &&
    extract_archive \
        --archive_path "$POW_DIR_IMPORT/$year_data" \
        --extract_path "$POW_DIR_TMP/$year_data" &&
    {
        [[ $_url_i -eq 0 ]] || {
            [ "${io_vars[LOAD_MODE]}" = APPEND ] || {
                io_vars[LOAD_MODE]=APPEND
            }
        }
    } &&
    {
        # match item
        [[ $year_data =~ ${io_vars[RE_FILE]}_(...) ]] && {
            # don't forget REMATCH[1] points to format(SHP|GPKG)
            _item=${BASH_REMATCH[2]}
        }
    } &&
    {
        [[ ${_debug_steps[item]:-1} -ne 0 ]] || {
            echo "item=${_item}"
            echo "match=${io_vars[RE_FILE]}_(...)"
            [[ ${_debug_bps[item]} -ne 0 ]] || read
        }
    } &&
    for _layer in ${LAYERS[@]}; do
        {
            [ "$_layer" != arrondissement_municipal ] || {
                # no municipal district ?
                [ "$_item" = FXX ] || continue
            }
        } &&
        _table_name=tmp_ign_${_layer} &&
        {
            [[ ${_debug_steps[table]:-1} -ne 0 ]] || {
                echo "table=(${_table_name})"
                echo "mode=(${io_vars[LOAD_MODE]})"
                [[ ${_debug_bps[table]} -ne 0 ]] || read
            }
        } &&
        _gpkg_full_path=$(find "$POW_DIR_TMP/$year_data" -type f -iname '*.gpkg') &&
        [ -n "$_gpkg_full_path" ] &&
        # NOTE: no spatial index (not slow down)
        import_geo_file \
            --file_path "$_gpkg_full_path" \
            --table_name ${_table_name} \
            --layers ${_layer} \
            --password "${io_vars[PASSWD]}" \
            --geometry_type PROMOTE_TO_MULTI \
            --load_mode ${io_vars[LOAD_MODE]} \
            --spatial_index no || {
            log_error "chargement '$year_data' en erreur!"
            on_import_error --id ${io_vars[ID]}
        }
    done &&
    rm --recursive "$POW_DIR_TMP/$year_data" &&
    rm "$POW_DIR_IMPORT/$year_data"
done &&
for _layer in ${LAYERS[@]}; do
    _tmp_table_name=tmp_ign_${_layer} &&
    _ign_table_name=ign_${_TABLES[${_layer}]} &&
    {
        [[ ${_debug_steps[table]:-1} -ne 0 ]] || {
            echo "tmp=(${_tmp_table_name})"
            echo "ign=(${_ign_table_name})"
            [[ ${_debug_bps[table]} -ne 0 ]] || read
        }
    } &&
    # NOTE: some geometry are invalid, correct them
    # TODO: give some examples
    execute_query \
        --name UPDATE_INVALID_GEOM \
        --query "
            UPDATE fr.${_tmp_table_name} SET geom = ST_MakeValid2(geom)
            WHERE NOT ST_IsValid(geom)
        " &&
    # TODO: label updates, always useful?
    execute_query \
        --name UPDATE_LABEL \
        --query "
            UPDATE fr.${_tmp_table_name}
            SET nom_officiel =
                REGEXP_REPLACE(REGEXP_REPLACE(nom_officiel, '^(¼|½)', 'Oe'), '(¼|½)', 'oe'),
                nom_officiel_en_majuscules =
                REGEXP_REPLACE(REGEXP_REPLACE(nom_officiel_en_majuscules, '^(¼|½)', 'OE'), '(¼|½)', 'OE')
            WHERE
                nom_officiel ~ '.*(¼|½).*'
                OR
                nom_officiel_en_majuscules ~ '.*(¼|½).*'
        " &&
    execute_query \
        --name UPDATE_DATA \
        --query "
            DROP TABLE IF EXISTS fr.${_ign_table_name};
            ALTER TABLE fr.${_tmp_table_name} RENAME TO ${_ign_table_name};
        " &&
    {
        _key_idx=

#                 # update hierarchy for EPT (as EPCI MGP/EPT w/ MGP=200054781)
#                 # MGP=Métropole du Grand Paris (w/ 131 municipalities)
#                 execute_query \
#                     --name UPDATE_LINK_EPCI \
#                     --query "
#                         UPDATE fr.${_table_name}
#                         SET siren_epci = '200054781'
#                         WHERE POSITION('200054781/' IN siren_epci) > 0" || on_import_error --id ${io_vars[ID]}
        case "$_layer" in
        epci)
            _key_idx=code_siren
            ;;
        *)
            _key_idx=code_insee
            ;;
        esac

        if [ -n "$_key_idx" ]; then
            execute_query \
                --name CREATE_UNIQUE_INDEX \
                --query "
                    CREATE UNIQUE INDEX ON fr.${_ign_table_name}($_key_idx)
                "
        fi
    } &&
    vacuum \
        --schema_name fr \
        --table_name ${_ign_table_name} \
        --mode ANALYZE &&
    {
        [ -n "$_query_count" ] && _query_count+='+'
        _query_count+="(SELECT COUNT(*) FROM fr.${_ign_table_name})"
    } || {
        log_error "mise à jour '$_layer' en erreur!"
        on_import_error --id ${io_vars[ID]}
    }
done &&
rm --force "$years_list_path" &&
io_history_end_ok \
    --nrows_processed "($_query_count)" \
    --id ${io_vars[ID]} ||
{
    on_import_error --id ${io_vars[ID]}
}

io_purge_common --name ${io_vars[NAME]}
log_info "Import du millésime $year de ${io_vars[NAME]} avec succès"

exit $SUCCESS_CODE
