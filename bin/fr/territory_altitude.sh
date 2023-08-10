#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # update territories w/ altitude (if available) from multiples sources

# many sources (not one complete, have to mix them)
ALTITUDE_SOURCE_WIKIPEDIA=1
ALTITUDE_SOURCE_LALTITUDE=2
ALTITUDE_SOURCE_CARTESFRANCE=3

declare -a altitude_sources_order=(
    $ALTITUDE_SOURCE_WIKIPEDIA
    $ALTITUDE_SOURCE_CARTESFRANCE
)

altitude_log_info() {
    bash_args \
        --args_p '
            step:Etape du traitement (0 étant la première itération);
            source:Source des Données
        ' \
        --args_o '
            step;
            source
        ' \
        "$@" || return $ERROR_CODE

    local _info='Téléchargement '
    [ $get_arg_step -eq 0 ] && _info+='de base' || _info='en complément'
    _info+=' (à partir de '
    case $get_arg_source in
    $ALTITUDE_SOURCE_WIKIPEDIA)
        _info+='WIKIPEDIA'
        ;;
    $ALTITUDE_SOURCE_LALTITUDE)
        _info+='LALTITUDE'
        ;;
    $ALTITUDE_SOURCE_CARTESFRANCE)
        _info+='CARTESFRANCE'
        ;;
    *)
        return $ERROR_CODE
        ;;
    esac
    _info+=')'
    log_info "$_info"

    return $SUCCESS_CODE
}

# initiate list of municipalities (according w/ step) : 1st=all, 2nd=only missing (or error)
altitude_set_list() {
    bash_args \
        --args_p '
            step:Etape du traitement (0 étant la première itération);
            list:Ensemble des communes à traiter
        ' \
        --args_o '
            step;
            list
        ' \
        "$@" || return $ERROR_CODE

    local _where
    [ $get_arg_step -eq 0 ] && _where='NOT done' || _where='z_min IS NULL OR z_max IS NULL OR z_max < z_min'

    execute_query \
        --name TODO_TERRITORY_ALTITUDE \
        --query "
            COPY (
                SELECT
                    code
                    , municipality
                    , department
                    , district
                FROM
                    fr.municipality_altitude
                WHERE
                    $_where
            ) TO STDOUT WITH (DELIMITER E':', FORMAT CSV, HEADER FALSE, ENCODING UTF8)
        " \
        --output $get_arg_list || return $ERROR_CODE

    return $SUCCESS_CODE
}

altitude_set_cache() {
    bash_args \
        --args_p '
            source:Source des Données;
            cache:Dossier du cache;
            tmpfile:Fichier temporaire de travail
        ' \
        --args_o '
            source;
            cache
        ' \
        "$@" || return $ERROR_CODE

    local -n _dir_cache_ref=$get_arg_cache
    local -n _file_tr_ref=$get_arg_tmpfile

    case $get_arg_source in
    $ALTITUDE_SOURCE_WIKIPEDIA)
        # temporary transformed Wikipedia downloaded file
        get_tmp_file --tmpext html --tmpfile _file_tr_ref
        _dir_cache_ref=wikipedia
        ;;
    $ALTITUDE_SOURCE_LALTITUDE)
        _dir_cache_ref=laltitude
        ;;
    $ALTITUDE_SOURCE_CARTESFRANCE)
        _dir_cache_ref=cartesfrance
        ;;
    *)
        return $ERROR_CODE
        ;;
    esac
    _dir_cache_ref="$POW_DIR_COMMON_GLOBAL_SCHEMA/$_dir_cache_ref"
    mkdir -p "$_dir_cache_ref"
    return $?
}

altitude_set_url() {
    bash_args \
        --args_p '
            source:Source des Données;
            code:Code INSEE de la commune;
            municipality:Nom de la commune;
            department:Nom du département de la commune;
            district:Nom Arrondissement communal;
            url:URL à interroger
        ' \
        --args_o '
            source;
            url
        ' \
        "$@" || return $ERROR_CODE

    local -n _url_ref=$get_arg_url
    local _url_site _url_page

    # exceptions :
        # no altitude for Polynésie française (987*)
    case $get_arg_source in
    $ALTITUDE_SOURCE_WIKIPEDIA)
        # exceptions :
            # namesake! ex: Devoluy, need suffix _(commune) to access it
            # ...
        [ -n "$get_arg_district" ] && _url_page=$get_arg_district || _url_page=$get_arg_municipality
        [ -n "$get_arg_department" ] && _url_page+="_($get_arg_department)"
        _url_site='https://fr.wikipedia.org/wiki'
        # replace space by underscore
        _url_page=${_url_page// /_}
        ;;
    $ALTITUDE_SOURCE_LALTITUDE)
        log_error 'non implémenté!'
        return $ERROR_CODE
        ;;
    $ALTITUDE_SOURCE_CARTESFRANCE)
        # exceptions :
            # municipality event
            # 05139 Dévoluy
            # 06107 La Roque-en-Provence
            # 15268 Le Rouget-Pers
            # 16052 Bors (Canton de Tude-et-Lavalette)
            # 16053 Bors (Canton de Charente-Sud)
            # 21195 Cormot-Vauchignon
            # 22046 Le Mené
            # 22158 Guerlédan
            # 24035 Pays de Belvès
            # 24142 Coux et Bigaroque-Mouzens
            # 27022 Le Val d'Hazey
            # 27032 Chambois
            # 27198 Mesnils-sur-Iton
            # 27412 Terres de Bord
            # 27693 Sylvains-Lès-Moulins
            # 28236 Arcisses
            # 28406 Éole-en-Beauce
            # 28422 Les Villages Vovéens
            # 37021 Beaumont-Louestault
            # 37232 Coteaux-sur-Loire
            # 38066 Chalon
            # 38253 Les Deux Alpes
            # 38456 Châtel-en-Trièves
            # 39368 Hauts de Bienne
            # 39510 Septmoncel les Molunes
            # 46138 Cœur de Causse
            # 46268 Saint Géry-Vers
            # 48094 Massegros Causses Gorges
            # 48152 Ventalon en Cévennes
            # 48166 Cans et Cévennes
            # 50535 Le Parc
            # 51075 Bourgogne-Fresne
            # 51457 Cœur-de-la-Vallée : ex- Reuil (01/01/2023)
            # 52033 Avrecourt
            # 52266 Laneuville-à-Rémy
            # 52278 Lavilleneuve-au-Roi
            # 52405 Le Montsaugeonnais
            # 55138 Culey
            # 55298 Loisey
            # 56102 Forges de Lanouée
            # 61211 Juvigny Val d'Andaine
            # 61474 Gouffern en Auge
            # 65192 Gavarnie-Gèdre
            # 67004 Sommerau
            # 68320 Spechbach
            # 69066 Cours
            # 70418 La Romaine
            # 73010 Entrelacs
            # 73150 La Plagne Tarentaise
            # 73227 Courchevel
            # 74282 Fillière
            # 76289 Saint Martin de l'If
            # 76601 Saint-Lucien
            # 79251 Marcillé
            # 85001 L'Aiguillon-la-Presqu'île
            # 86053 Champigny en Rochereau
            # 89130 Deux Rivières
            # 89334 Le Val d'Ocre

            # municipality upcase!
            # 08165 Faux : FAUX
        # replace {space,'} by minus, waited: Arrondissement and translate accent
        [ -n "$get_arg_district" ] && {
            # need capitalize 'arrondissement'
            local _tmp=${get_arg_district/a/A}
            _url_page=${_tmp//_/-}
        } || _url_page=$get_arg_municipality
        _url_page=${_url_page/Œ/OE}
        _url_page=${_url_page/œ/oe}
        _url_page=${get_arg_code}_$(echo ${_url_page//[ \']/-} | sed 'y/àâçéèêëîïôöùûüÉÈÎ/aaceeeeiioouuuEEI/').html
        _url_site='https://www.cartesfrance.fr/carte-france-ville'
        ;;
    *)
        return $ERROR_CODE
        ;;
    esac

    # encode URL (see: https://stackoverflow.com/questions/296536/how-to-urlencode-data-for-curl-command)
    # don't work on CARTESFRANCE!
    #_url_ref=${_url_site}/$(python3 -c "import urllib.parse; print(urllib.parse.quote(input()))" <<< "$_url_page")

    _url_ref=${_url_site}/${_url_page}

    return $SUCCESS_CODE
}

# get (min, max) values from downloaded HTML
altitude_set_values() {
    bash_args \
        --args_p '
            source:Source des Données;
            file_path:Contenu de la commune;
            tmpfile:Fichier temporaire de transformation;
            min:Altitude minimum;
            max:Altitude maximum
        ' \
        --args_o '
            source;
            file_path;
            min;
            max
        ' \
        "$@" || return $ERROR_CODE

    local -n _min_ref=$get_arg_min
    local -n _max_ref=$get_arg_max

    # negative altitude (min) : re w/ minus
    case $get_arg_source in
    $ALTITUDE_SOURCE_WIKIPEDIA)
        # duplicate altitude, ie Condé-sur-Vire : max-count 1
        sed --expression 's/&#[0-9]*;//g' "$get_arg_file_path" > $get_arg_tmpfile
        _min_ref=$(grep --only-matching --perl-regexp 'Min\.[ ]*[0-9 -]*' --max-count 1 $get_arg_tmpfile | grep --only-matching --perl-regexp '[0-9 -]*')
        _max_ref=$(grep --only-matching --perl-regexp 'Max\.[ ]*[0-9 -]*' --max-count 1 $get_arg_tmpfile | grep --only-matching --perl-regexp '[0-9 -]*')
        ;;
    $ALTITUDE_SOURCE_LALTITUDE)
        log_error 'non implémenté!'
        return $ERROR_CODE
        ;;
    $ALTITUDE_SOURCE_CARTESFRANCE)
        _min_ref=$(sed --silent '/Altitude minimum/,/align/p' "$get_arg_file_path" | grep --only-matching --perl-regexp '<td>[0-9 -]*' | grep --only-matching --perl-regexp '[0-9 -]*')
        _max_ref=$(sed --silent '/Altitude maximum/,/align/p' "$get_arg_file_path" | grep --only-matching --perl-regexp '<td>[0-9 -]*' | grep --only-matching --perl-regexp '[0-9 -]*')
        ;;
    *)
        return $ERROR_CODE
        ;;
    esac
    # delete potential space
    _min_ref=${_min_ref// }
    _max_ref=${_max_ref// }

    return $SUCCESS_CODE
}

# main
bash_args \
    --args_p '
        force_list:Lister les communes même si elles possèdent déjà des altitudes;
        force_public:Forcer la mise à jour des altitudes, même si données incomplètes;
        use_cache:Utiliser les données présentes dans le cache;
        except_territory:RE pour écarter certaines communes;
        only_territory:RE pour traiter certaines communes
    ' \
    --args_v '
        force_list:yes|no;
        force_public:yes|no;
        use_cache:yes|no
    ' \
    --args_d '
        force_list:no;
        force_public:no;
        use_cache:yes
    ' \
    "$@" || exit $ERROR_CODE

[ "$get_arg_force_list" = no ] && _where='AND (t.z_min IS NULL OR t.z_max IS NULL OR t.z_max < t.z_min)' || _where=''
log_info 'Mise à jour des données Altitude (min, max) des Communes' &&
set_env --schema_name fr &&
execute_query \
    --name PREPARE_TERRITORY_ALTITUDE \
    --query "
        CREATE TABLE IF NOT EXISTS fr.municipality_altitude AS
            WITH
            municipality_namesake AS (
                SELECT
                    name
                FROM
                    public.territory
                WHERE
                    country = 'FR'
                    AND
                    level = 'COM'
                GROUP BY
                    name
                HAVING
                    COUNT(*) > 1
            )
            SELECT
                t.code, t.name municipality
                , CASE WHEN mns.name IS NULL THEN NULL::VARCHAR
                ELSE
                    d.name
                END department
                , CASE WHEN g.name IS NULL THEN NULL::VARCHAR
                ELSE
                    CONCAT(REGEXP_REPLACE(REGEXP_REPLACE(t.name, '^[^0-9]*', ''), ' A', '_a'), '_de_', g.name)
                END district
                , NULL::INT z_min, NULL::INT z_max
                , FALSE done
            FROM public.territory t
                LEFT OUTER JOIN municipality_namesake mns ON t.name = mns.name
                CROSS JOIN get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'COM', t.code), 'DEP')) d
                LEFT OUTER JOIN get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'COM', t.code), 'COM_GLOBALE_ARM')) g ON TRUE
            WHERE t.country = 'FR' AND t.level = 'COM' $_where
        " && {
execute_query \
    --name WITH_TERRITORY_ALTITUDE \
    --query 'SELECT COUNT(1) FROM fr.municipality_altitude' \
    --psql_arguments 'tuples-only:pset=format=unaligned' \
    --return _territory_count && {
        [ ${_territory_count:-0} -eq 0 ] && {
            log_info 'Mise à jour non nécessaire'
            exit $SUCCESS_CODE
        } || true
    }
} &&
_error=0 &&
_territory_list=$POW_DIR_TMP/territory_altitude.txt && {
    for ((_altitude_step=0; _altitude_step < ${#altitude_sources_order[@]}; _altitude_step++)); do
        altitude_log_info \
            --step $_altitude_step \
            --source ${altitude_sources_order[$_altitude_step]} &&
        altitude_set_list \
            --step $_altitude_step \
            --list $_territory_list &&
        get_file_nrows $_territory_list _rows &&
        log_info "A traiter: $_rows communes" &&
        altitude_set_cache \
            --source ${altitude_sources_order[$_altitude_step]} \
            --cache _territory_cache \
            --tmpfile _tmpfile && {
            while IFS=: read _code _name _department _district; do
                # only territory?
                [ -n "$get_arg_only_territory" ] && [[ ! $_code =~ $get_arg_only_territory ]] && continue
                # except territory?
                [ -n "$get_arg_except_territory" ] && [[ $_code =~ $get_arg_except_territory ]] && continue
                altitude_set_url \
                    --source ${altitude_sources_order[$_altitude_step]} \
                    --code $_code \
                    --municipality "$_name" \
                    --department "$_department" \
                    --district "$_district" \
                    --url _url &&
                _file=$(basename "$_url") || {
                    log_error "Obtention URL ($_file) en erreur"
                    continue
                }
                ([ "$get_arg_use_cache" = no ] || [ ! -s "$_territory_cache/$_file" ]) && {
                    curl --output "$_territory_cache/$_file" "$_url" || {
                        _rc=$?
                        log_error "Téléchargement ($_file) en erreur [$_rc] URL=$_url"
                        continue
                    }
                }
                [ ! -s "$_territory_cache/$_file" ] && {
                    echo "Téléchargement ($_file) URL=$_url"
                } || {
                    altitude_set_values \
                        --source ${altitude_sources_order[$_altitude_step]} \
                        --file_path "$_territory_cache/$_file" \
                        --tmpfile $_tmpfile \
                        --min _min \
                        --max _max
                    echo "$_name ($_code) min=$_min max=$_max"
                    execute_query \
                        --name UDPATE_TERRITORY_ALTITUDE \
                        --query "
                            UPDATE fr.municipality_altitude SET
                                z_min = ${_min:-NULL::INT}
                                , z_max = ${_max:-NULL::INT}
                                , done = TRUE
                            WHERE
                                code = '$_code'
                        " || {
                        log_error "Mise à jour $_code en erreur"
                    }
                }
            done < $_territory_list
        }
        # check for complete (or error)
        execute_query \
            --name IS_OK_TERRITORY_ALTITUDE \
            --query 'SELECT EXISTS(SELECT 1 FROM fr.municipality_altitude WHERE z_min IS NULL OR z_max IS NULL OR z_max < z_min)' \
            --psql_arguments 'tuples-only:pset=format=unaligned' \
            --return _territory_ko && {
            is_yes --var _territory_ko || break
        } || {
            # to raise error (after loop)
            _error=1
            break
        }
    done
} &&
[ $_error -eq 0 ] &&
# remove temporary worked file
rm --force $_tmpfile || {
    exit $ERROR_CODE
}

# update territory w/ altitude values (municipality then supra)
([ "$get_arg_force_public" = no ] && is_yes --var _territory_ko) || {
    execute_query \
        --name SET_TERRITORY_ALTITUDE \
        --query "
            UPDATE fr.territory t SET
                z_min = ma.z_min
                , z_max = ma.z_max
            FROM fr.municipality_altitude ma
            WHERE
                t.nivgeo = 'COM' AND t.codgeo = ma.code;
            -- set altitudes (SUPRA levels)
            PERFORM fr.set_territory_supra(
                table_name => 'territory'
                , schema_name => 'fr'
                , base_level => 'COM'
                , update_mode => TRUE
                , columns_agg => ARRAY['z_min', 'z_max']
                , columns_agg_func => '{\"z_min\":\"MIN\", \"z_max\":\"MAX\"}'::JSONB
            );
        " &&
    archive_file $_territory_list &&
    log_info 'Mise à jour avec succès'
} || {
    log_error 'Mise à jour Altitudes des communes non complète!'
    exit $ERROR_CODE
}

exit $SUCCESS_CODE
