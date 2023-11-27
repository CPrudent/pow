#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # update territories w/ altitude (if available) from multiples sources

# many sources (not one complete, have to mix them) : idea is to call one as base and another as complement
ALTITUDE_SOURCE_WIKIPEDIA=1
ALTITUDE_SOURCE_LALTITUDE=2
ALTITUDE_SOURCE_CARTESFRANCE=3
# base as WIKIPEDIA and complement as CARTESFRANCE
declare -a altitude_sources_order=(
    $ALTITUDE_SOURCE_WIKIPEDIA
    $ALTITUDE_SOURCE_CARTESFRANCE
)

# update usecases
_k=0
ALTITUDE_UPDATE_BASIC=$((_k++))
ALTITUDE_UPDATE_RENAME=$((_k++))
ALTITUDE_UPDATE_MERGE_ABORT=$((_k++))
ALTITUDE_UPDATE_MERGE=$((_k++))

# extracted data
_k=0
TERRITORY_CODE=$((_k++))
TERRITORY_NAME=$((_k++))
TERRITORY_DEPARTMENT=$((_k++))
TERRITORY_DISTRICT=$((_k++))
TERRITORY_EVENT_CODE=$((_k++))
TERRITORY_EVENT_DATE=$((_k++))
TERRITORY_EVENT_NAME_BEFORE=$((_k++))
TERRITORY_EVENT_CODE_AFTER=$((_k++))
TERRITORY_EVENT_NAME_AFTER=$((_k++))

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
    [ $get_arg_step -eq 0 ] && _where='NOT done' || _where='(z_min IS NULL OR z_max IS NULL OR z_max < z_min)'

    execute_query \
        --name TODO_TERRITORY_ALTITUDE \
        --query "
            COPY (
                SELECT
                    code
                    , municipality
                    , department
                    , district
                    , me.mod
                    , me.date_eff
                    , me.libelle_av
                    , me.com_ap
                    , me.libelle_ap
                FROM
                    fr.municipality_altitude ma
                        LEFT OUTER JOIN LATERAL (
                            SELECT
                                com_av
                                , com_ap
                                , date_eff
                                , mod
                                , libelle_av
                                , libelle_ap
                            FROM
                                fr.insee_municipality_event me
                            WHERE
                                me.com_av = ma.code
                                AND me.typecom_av = 'COM'
                                AND me.typecom_ap = 'COM'
                            ORDER BY
                                me.date_eff DESC
                            LIMIT
                                1
                        ) me ON ma.code = me.com_av
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
            territory_data:Tableau des données de la commune;
            url:URL à interroger
        ' \
        --args_o '
            source;
            url
        ' \
        "$@" || return $ERROR_CODE

    local -n _territory_data_ref=$get_arg_territory_data
    local -n _url_ref=$get_arg_url
    local _url_site _url_page

    # exceptions :
        # no altitude for Polynésie française (987*)
    case $get_arg_source in
    $ALTITUDE_SOURCE_WIKIPEDIA)
        # exceptions :
            # namesake! ex: Devoluy, need suffix _(commune) to access it
            # case sensitive! ex: Bors_(Canton_de_Tude-et-Lavalette) KO due to Canton (instead canton)
        [ -n "${_territory_data_ref[$TERRITORY_DISTRICT]}" ] && _url_page=${_territory_data_ref[$TERRITORY_DISTRICT]} || _url_page=${_territory_data_ref[$TERRITORY_NAME]}
        [ -n "${_territory_data_ref[$TERRITORY_DEPARTMENT]}" ] && _url_page+="_(${_territory_data_ref[$TERRITORY_DEPARTMENT]})"
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
            # municipality upcase! ex: 08165 Faux : FAUX
        # replace {space,'} by minus, waited: Arrondissement and translate accent
        [ -n "${_territory_data_ref[$TERRITORY_DISTRICT]}" ] && {
            # need capitalize 'arrondissement'
            local _tmp=${_territory_data_ref[$TERRITORY_DISTRICT]/a/A}
            _url_page=${_tmp//_/-}
        } || _url_page=${_territory_data_ref[$TERRITORY_NAME]}
        # replace œ
        _url_page=${_url_page/Œ/OE}
        _url_page=${_url_page/œ/oe}
        # translate accent
        _url_page=${_territory_data_ref[$TERRITORY_CODE]}_$(echo ${_url_page//[ \']/-} | sed --expression 'y/àâçéèêëîïôöùûüÉÈÎ/aaceeeeiioouuuEEI/' --expression 's/[()]//g').html
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

# set (min, max) values for exceptions (not found elsewhere)
altitude_set_exceptions() {
    execute_query \
        --name TODO_TERRITORY_ALTITUDE \
        --query "
            WITH
            missing_altitude AS (
                SELECT
                    codgeo
                    , z_min
                    , z_max
                FROM (
                    VALUES
                        ('97501', 'Miquelon-Langlade', 0, 240)
                        , ('97502', 'Saint-Pierre', 0, 207)
                        , ('97701', 'Saint-Barthélemy', 0, 286)
                        , ('97801', 'Saint-Martin', 0, 424)
                        , ('97607', 'Dembeni', 0, 651)
                ) AS t(codgeo, libgeo, z_min, z_max)
            )
            UPDATE fr.municipality_altitude SET
                z_min = m.z_min
                , z_max = m.z_max
                FROM missing_altitude m
                WHERE
                    code = m.codgeo
        " || return $ERROR_CODE

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
        reset_territory:Effacer la table de préparation;
        except_territory:RE pour écarter certaines communes;
        only_territory:RE pour traiter certaines communes;
        from_date:Prise en compte des fusions de communes à partir de cette date

    ' \
    --args_v '
        force_list:yes|no;
        force_public:yes|no;
        use_cache:yes|no;
        reset_territory:yes|no
    ' \
    --args_d '
        force_list:no;
        force_public:no;
        use_cache:yes;
        from_date:2009-01-01;
        reset_territory:no
    ' \
    "$@" || exit $ERROR_CODE

# TODO be careful w/ name of option, because general variable (get_arg_*) can be changed
#      by another call of bash_args !
#      get_arg_reset also used by set_env_pg()

[ "$get_arg_force_list" = no ] && _where='AND (t.z_min IS NULL OR t.z_max IS NULL OR t.z_max < t.z_min)' || _where=''
set_env --schema_name fr &&
log_info 'Mise à jour des données Altitude (min, max) des Communes' && {
    [ "$get_arg_reset_territory" = yes ] && {
        execute_query \
            --name RESET_TERRITORY_ALTITUDE \
            --query "DROP TABLE IF EXISTS fr.municipality_altitude"
    } || true
} &&
execute_query \
    --name PREPARE_TERRITORY_ALTITUDE \
    --query "
        CREATE TABLE IF NOT EXISTS fr.municipality_altitude AS
        WITH
        municipality_namesake AS (
            SELECT
                libgeo
            FROM
                fr.territory
            WHERE
                nivgeo = 'COM'
            GROUP BY
                libgeo
            HAVING
                COUNT(*) > 1
        )
        SELECT
            t.codgeo code
            , REGEXP_REPLACE(t.libgeo, '\(Canton ', '(canton ') municipality
            , CASE WHEN mns.libgeo IS NULL THEN NULL::VARCHAR
            ELSE
                d.libgeo
            END department
            , CASE WHEN t.codgeo_com_globale_arm_parent IS NULL THEN NULL::VARCHAR
            ELSE
                CONCAT(REGEXP_REPLACE(REGEXP_REPLACE(t.libgeo, '^[^0-9]*', ''), ' A', '_a'), '_de_', cg.libgeo)
            END district
            , NULL::INT z_min, NULL::INT z_max
            , FALSE done
        FROM fr.territory t
            LEFT OUTER JOIN municipality_namesake mns ON t.libgeo = mns.libgeo
            JOIN fr.territory d ON d.nivgeo = 'DEP' AND d.codgeo = t.codgeo_dep_parent
            LEFT OUTER JOIN fr.territory cg ON cg.nivgeo = 'COM_GLOBALE_ARM' AND cg.codgeo = t.codgeo_com_globale_arm_parent
        WHERE
            t.nivgeo = 'COM' AND t.codgeo !~ '^98' $_where
        " &&
altitude_set_exceptions && {
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
_error_complete=0 &&
_territory_list=$POW_DIR_TMP/territory_altitude.txt && {
    for ((_altitude_i=0; _altitude_i < ${#altitude_sources_order[@]}; _altitude_i++)); do
        altitude_log_info \
            --step $_altitude_i \
            --source ${altitude_sources_order[$_altitude_i]} &&
        altitude_set_list \
            --step $_altitude_i \
            --list $_territory_list &&
        get_file_nrows $_territory_list _rows && {
            _info="A traiter: $_rows commune"
            if [ $_rows -gt 1 ]; then
                _info+='s'
            fi
            log_info "$_info"
        } &&
        altitude_set_cache \
            --source ${altitude_sources_order[$_altitude_i]} \
            --cache _territory_cache \
            --tmpfile _tmpfile && {
            while IFS=: read -a _territory_data; do
                # only territory?
                [ -n "$get_arg_only_territory" ] && [[ ! ${_territory_data[$TERRITORY_CODE]} =~ $get_arg_only_territory ]] && continue
                # except territory?
                [ -n "$get_arg_except_territory" ] && [[ ${_territory_data[$TERRITORY_CODE]} =~ $get_arg_except_territory ]] && continue

                _first=1
                while true; do
                    _territory_skip=0 &&
                    altitude_set_url \
                        --source ${altitude_sources_order[$_altitude_i]} \
                        --territory_data _territory_data \
                        --url _url &&
                    _file=$(basename "$_url")
                    _rc=$?
                    [ "$POW_DEBUG" = yes ] && declare -p _territory_data _url
                    ([ "$get_arg_use_cache" = no ] || [ ! -s "$_territory_cache/$_file" ]) && {
                        curl --fail --output "$_territory_cache/$_file" "$_url"
                        _rc=$?
                    }

                    _altitude_update=$ALTITUDE_UPDATE_BASIC
                    if [ $_first -eq 1 ]; then
                        _first=0
                        _altitude_code=(${_territory_data[$TERRITORY_CODE]})
                        _altitude_name=("${_territory_data[$TERRITORY_NAME]}")
                        _altitude_file=($_file)
                        _territory_data_copy=("${_territory_data[@]}")

                        case ${altitude_sources_order[$_altitude_i]} in
                        $ALTITUDE_SOURCE_WIKIPEDIA)
                            [ -s "$_territory_cache/$_file" ] && [ $_rc -eq 0 ] && {
                                grep --silent 'homonymie de Wikipedia' "$_territory_cache/$_file"
                                if [ $? -eq 1 ]; then
                                    break
                                else
                                    _territory_data[$TERRITORY_NAME]+='_(commune)'
                                fi
                            } || {
                                # exceptions:
                                # Le Mené (22046), Guerlédan (22158) need department (Côtes-d'Armor)
                                log_error "$_file: téléchargement en erreur URL=$_url"$([ $_rc -ne 0 ] && echo " [$_rc]")
                                _territory_skip=1
                                break
                            }
                            ;;
                        $ALTITUDE_SOURCE_CARTESFRANCE)
                            [ -s "$_territory_cache/$_file" ] && [ $_rc -eq 0 ] && {
                                break
                            }
                            [ -z "${_territory_data[$TERRITORY_EVENT_CODE]}" ] && {
                                # exception (no code), case of: 08165_FAUX (upcase)
                                _altitude_code=(${_territory_data[$TERRITORY_CODE]})
                                _altitude_name=("${_territory_data[$TERRITORY_NAME]}")
                                _territory_data[$TERRITORY_NAME]=${_territory_data[$TERRITORY_NAME]^^}
                            } || {
                                # municipality events aren't applied!
                                case ${_territory_data[$TERRITORY_EVENT_CODE]} in
                                10|21) # rename, abort (merge)
                                    _altitude_code=(${_territory_data[$TERRITORY_CODE]})
                                    _altitude_name=("${_territory_data[$TERRITORY_NAME]}")
                                    _territory_data[$TERRITORY_NAME]=${_territory_data[$TERRITORY_EVENT_NAME_BEFORE]}
                                    ;;
                                3[1-4]) # merge
                                    _altitude_update=$ALTITUDE_UPDATE_MERGE
                                    [ "$POW_DEBUG" = yes ] && {
                                        echo 'Fusion de communes'
                                        declare -p _territory_data
                                        echo 'recherche séparation'
                                    }
                                    # find if eventually "separated", and final name of merged _municipality
                                    # SQL: exists abort (code 21)?, more recent merge ?
                                    execute_query \
                                        --name TERRITORY_MERGE_ABORT \
                                        --query "
                                            SELECT
                                                t.com_ap
                                                , CASE WHEN me.com_ap IS NULL THEN t.libelle_ap
                                                ELSE me.libelle_ap
                                                END
                                            FROM (
                                                SELECT
                                                    com_ap, libelle_ap, date_eff
                                                FROM fr.insee_municipality_event me
                                                WHERE
                                                    com_ap = '${_territory_data[$TERRITORY_EVENT_CODE_AFTER]}'
                                                    AND com_av = '${_territory_data[$TERRITORY_CODE]}'
                                                    AND typecom_av = 'COM'
                                                    AND typecom_ap = 'COM'
                                                    AND mod BETWEEN 31 AND 34
                                                    AND EXISTS(
                                                        SELECT 1
                                                        FROM fr.insee_municipality_event me2
                                                        WHERE
                                                            me2.mod = 21
                                                            AND
                                                            me2.com_av = me.com_ap
                                                            AND
                                                            me2.com_ap = me.com_av
                                                    )
                                            ) t
                                                LEFT OUTER JOIN fr.insee_municipality_event me ON
                                                    t.com_ap = me.com_ap
                                                    AND me.mod BETWEEN 31 AND 34
                                                    AND me.date_eff > t.date_eff
                                                    AND me.typecom_av = 'COM'
                                                    AND me.typecom_ap = 'COM'
                                        " \
                                        --psql_arguments 'tuples-only:pset=format=unaligned' \
                                        --output $_tmpfile && {
                                        [ -s $_tmpfile ] && {
                                            [ "$POW_DEBUG" = yes ] && echo 'avec séparation'
                                            _altitude_code=($(head -n 1 $_tmpfile | cut --delimiter \| --field 1))
                                            _altitude_name=("$(head -n 1 $_tmpfile | cut --delimiter \| --field 2)")
                                        } || {
                                            [ "$POW_DEBUG" = yes ] && echo 'ensemble des communes'
                                            # else, find old municipalities (before merge) starting at 2009/1/1 (web seems updated up to this date), not before!
                                            set -o noglob &&
                                            execute_query \
                                                --name TERRITORY_MERGE \
                                                --query "
                                                    SELECT * FROM fr.get_municipalities_of_merge(
                                                        municipality_code => '${_territory_data[$TERRITORY_CODE]}'
                                                        , from_date => '${get_arg_from_date}'
                                                    )
                                                " \
                                                --psql_arguments 'tuples-only:pset=format=unaligned' \
                                                --output $_tmpfile &&
                                            set +o noglob &&
                                            readarray -t _altitude_code < <(cut --delimiter \| --field 4 $_tmpfile) &&
                                            readarray -t _altitude_name < <(cut --delimiter \| --field 8 $_tmpfile)
                                        }
                                    } && {
                                        _altitude_j=0
                                        _altitude_file=()
                                        _territory_data[$TERRITORY_CODE]=${_altitude_code[0]}
                                        _territory_data[$TERRITORY_NAME]=${_altitude_name[0]}
                                        [ "$POW_DEBUG" = yes ] && declare -p _altitude_code _altitude_name
                                    }
                                    ;;
                                *)
                                    log_error "$_file: évènement (${_territory_data[$TERRITORY_EVENT_CODE]}) non géré!"
                                    _territory_skip=1
                                    break
                                    ;;
                                esac
                            }
                            ;;
                        *)
                            log_error "$_file: téléchargement en erreur URL=$_url"$([ $_rc -ne 0 ] && echo " [$_rc]")
                            _territory_skip=1
                            break
                            ;;
                        esac
                    else
                        ([ ! -s "$_territory_cache/$_file" ] || [ $_rc -ne 0 ]) && {
                            log_error "$_file: téléchargement en erreur URL=$_url"$([ $_rc -ne 0 ] && echo " [$_rc]")
                            _territory_skip=1
                            break
                        }

                        case ${altitude_sources_order[$_altitude_i]} in
                        $ALTITUDE_SOURCE_WIKIPEDIA)
                            _altitude_file=($_file)
                            break
                            ;;
                        $ALTITUDE_SOURCE_CARTESFRANCE)
                            [ -z "${_territory_data[$TERRITORY_EVENT_CODE]}" ] && {
                                _altitude_file=($_file)
                                break
                            } || {
                                case ${_territory_data_copy[$TERRITORY_EVENT_CODE]} in
                                10) # rename
                                    _altitude_update=$ALTITUDE_UPDATE_RENAME
                                    _altitude_file=($_file)
                                    break
                                    ;;
                                21) # abort (merge)
                                    _altitude_update=$ALTITUDE_UPDATE_MERGE_ABORT
                                    _altitude_file=($_file)
                                    break
                                    ;;
                                3[1-4]) # merge
                                    _altitude_file+=($_file)
                                    [ $((++_altitude_j)) -eq ${#_altitude_code[*]} ] && {
                                        break
                                    } || {
                                        _territory_data[$TERRITORY_CODE]=${_altitude_code[$_altitude_j]}
                                        _territory_data[$TERRITORY_NAME]=${_altitude_name[$_altitude_j]}
                                    }
                                    ;;
                                esac
                            }
                            ;;
                        esac
                    fi
                done

                [ $_territory_skip -eq 1 ] && continue

                declare -A _altitude_min _altitude_max
                [ "$POW_DEBUG" = yes ] && declare -p _altitude_code _altitude_name _altitude_file
                for ((_altitude_k=0; _altitude_k < ${#_altitude_code[*]}; _altitude_k++)); do
                    altitude_set_values \
                        --source ${altitude_sources_order[$_altitude_i]} \
                        --file_path "$_territory_cache/${_altitude_file[$_altitude_k]}" \
                        --tmpfile $_tmpfile \
                        --min _altitude_min[${_altitude_code[$_altitude_k]}] \
                        --max _altitude_max[${_altitude_code[$_altitude_k]}]
                    echo "${_altitude_name[$_altitude_k]} (${_altitude_code[$_altitude_k]}) min=${_altitude_min[${_altitude_code[$_altitude_k]}]} max=${_altitude_max[${_altitude_code[$_altitude_k]}]}"
                    [ $_altitude_k -eq 0 ] && {
                        _min=${_altitude_min[${_altitude_code[$_altitude_k]}]}
                        _max=${_altitude_max[${_altitude_code[$_altitude_k]}]}
                    } || {
                        _min=$((_altitude_min[${_altitude_code[$_altitude_k]}] < _min ? _altitude_min[${_altitude_code[$_altitude_k]}] : _min))
                        _max=$((_altitude_max[${_altitude_code[$_altitude_k]}] > _max ? _altitude_max[${_altitude_code[$_altitude_k]}] : _max))
                    }
                done

                execute_query \
                    --name UDPATE_TERRITORY_ALTITUDE \
                    --query "
                        UPDATE fr.municipality_altitude SET
                            z_min = ${_min:-NULL::INT}
                            , z_max = ${_max:-NULL::INT}
                            , done = TRUE
                        WHERE
                            code = '${_territory_data_copy[$TERRITORY_CODE]}'
                    " || {
                    log_error "Mise à jour ${_territory_data_copy[$TERRITORY_CODE]} en erreur"

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
            _error_complete=1
            break
        }
    done
} &&
[ $_error_complete -eq 0 ] &&
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
    exit $SUCCESS_CODE
}

log_error 'Mise à jour Altitudes des communes non complète!'
exit $ERROR_CODE


