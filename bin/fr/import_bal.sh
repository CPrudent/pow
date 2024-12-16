#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BAL addresses: summary (municipality), street and only certified housenumber)

    # NOTE
    # https://stackoverflow.com/questions/16908084/bash-script-to-calculate-time-elapsed
    # https://stackoverflow.com/questions/3953645/ternary-operator-in-bash
    # https://stackoverflow.com/questions/10586153/how-to-split-a-string-into-an-array-in-bash

    # TODO
    # assign PROGRESS_SIZE w/ max (municipalities, streets, housenumbers) of selection

on_import_error() {
    bash_args \
        --args_p '
            vars:Entité des variables globales
        ' \
        --args_o '
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars

    # import created?
    [ "$POW_DEBUG" = yes ] && { echo "io_history_id=${_vars_ref[IO_ID]}"; }
    [ -n "${_vars_ref[IO_ID]}" ] && io_history_end_ko --id ${_vars_ref[IO_ID]}

    log_error "Erreur import BAL (${_vars_ref[IO_NAME]#*_})"
    exit $ERROR_CODE
}

# print progress as (ratio, percent)
# $1= begin
    # $2= label
    # $3= size of (number of digits)
    # $4= subscript
    # $5= total
    # $6= end of line
# $1= end
    # $2= elapsed time
bal_progress_bar() {
    case "${1^^}" in
    BEGIN)
        expect argc bal_progress_bar $# 6 || exit $ERROR_CODE
        # if main display (municipality level) and only one then reduce informations
        ([ "${2:0:5}" = INSEE ] && [[ $5 -eq 1 ]]) && {
            printf '%-15s%b' "$2" $6
        } || {
            printf '%-15s\t%*d/%*d (%d%%)%b' "$2" $3 $4 $3 $5 $((($4*100)/$5)) $6
        }
        ;;
    END)
        printf "\t\t\t\t\t%s\n" "$2"
        ;;
    esac

    return $SUCCESS_CODE
}

# load BAL data (summary or one municipality)
bal_load() {
    bash_args \
        --args_p '
            vars:Entité des variables globales
        ' \
        --args_o '
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars
    local _query _import_options='' _overwrite_key _overwrite_value _info

    case "${_vars_ref[MUNICIPALITY_CODE]}" in
    ALL)
        _vars_ref[IO_NAME]=BAL_SUMMARY
        _vars_ref[URL_DATA]='api/communes-summary.csv'
        # number of row(s)
        _query="
            SELECT COUNT(DISTINCT co_insee_commune)
            FROM fr.laposte_address_area WHERE fl_active
        "
        # no download if present summary is max 2 days old
        _overwrite_key=TIME
        _overwrite_value=$((2*24*60*60))        # 2 days
        ;;
    *)
        _vars_ref[IO_NAME]=BAL_${_vars_ref[MUNICIPALITY_CODE]}
        _vars_ref[URL_DATA]='lookup/'${_vars_ref[MUNICIPALITY_CODE]}
        # number of row(s)
        _query="
            SELECT areas + streets + housenumbers_auth
            FROM fr.bal_municipality WHERE code='${_vars_ref[MUNICIPALITY_CODE]}'"
        # table w/ 1 column named 'data' to import JSON stream
        _import_options="--import_options column_name=data"
        execute_query \
            --name BAL_IO_END \
            --query "
                SELECT last_update
                FROM fr.bal_municipality WHERE code='${_vars_ref[MUNICIPALITY_CODE]}'
            " \
            --psql_arguments 'tuples-only:pset=format=unaligned' \
            --return _vars_ref[IO_END] || return $ERROR_CODE
        # no download if JSON file newer than municipality's last_update
        _overwrite_key=DATE
        _vars_ref[IO_END_EPOCH]=$(date '+%s' --date "${_vars_ref[IO_END]}")
        _overwrite_value=${_vars_ref[IO_END_EPOCH]}
        ;;
    esac
    _vars_ref[TABLE_NAME]=tmp_${_vars_ref[IO_NAME],,}

    io_todo_import \
        --force ${_vars_ref[FORCE]} \
        --io ${_vars_ref[IO_NAME]} \
        --date_end "${_vars_ref[IO_END]}"
    case $? in
    $POW_IO_SUCCESSFUL)                                 return $SUCCESS_CODE        ;;
    $POW_IO_IN_PROGRESS|$POW_IO_ERROR|$ERROR_CODE)      return $ERROR_CODE          ;;
    esac

    # take all following 'BAL_' as info
    _info=${_vars_ref[IO_NAME]#*_}
    log_info "Import BAL (${_info})" &&
    {
        (! is_yes --var _vars_ref[PROGRESS]) || {
            _vars_ref[PROGRESS_START]=$(date '+%s') &&
            bal_progress_bar \
                BEGIN \
                "INSEE ${_info}" \
                ${_vars_ref[PROGRESS_SIZE]} \
                ${_vars_ref[PROGRESS_CURRENT]} \
                ${_vars_ref[PROGRESS_TOTAL]} \
                '\r'
        }
    } &&
    {
        _vars_ref[FILE_NAME]=$(basename "${_vars_ref[URL]}/${_vars_ref[URL_DATA]}") &&
        {
            [ "${_vars_ref[MUNICIPALITY_CODE]}" != ALL ] && {
                _vars_ref[FILE_NAME]+=.json &&
                execute_query \
                    --name BAL_CREATE \
                    --query "
                        CREATE TABLE IF NOT EXISTS fr.${_vars_ref[TABLE_NAME]} (data JSON)
                    " &&
                execute_query \
                    --name BAL_IO_BEGIN \
                    --query "SELECT (get_last_io('${_vars_ref[IO_NAME]}')).date_data_end" \
                    --psql_arguments 'tuples-only:pset=format=unaligned' \
                    --return _vars_ref[IO_BEGIN]
            } || true
        }
    } &&
    {
        # reset BEGIN if equal (force running w/o change of END date)
        [ "${_vars_ref[IO_BEGIN]}" != "${_vars_ref[IO_END]}" ] || _vars_ref[IO_BEGIN]=
    } &&
    execute_query \
        --name BAL_IO_ROWS \
        --query "$_query" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _vars_ref[IO_ROWS] &&
    {
        [ -z "${_vars_ref[IO_ROWS]}" ] && {
            log_error "Import préalable de l'ensemble des Communes (--municipality ALL)"
            false
        } || true
    } &&
    io_history_begin \
        --io "${_vars_ref[IO_NAME]}" \
        --date_begin "${_vars_ref[IO_BEGIN]:-1970-01-01}" \
        --date_end "${_vars_ref[IO_END]}" \
        --nrows_todo ${_vars_ref[IO_ROWS]:-1} \
        --id _vars_ref[IO_ID] &&
    io_download_file \
        --url "${_vars_ref[URL]}/${_vars_ref[URL_DATA]}" \
        --overwrite_mode NEWER \
        --overwrite_key $_overwrite_key \
        --overwrite_value $_overwrite_value \
        --common_subdir bal \
        --output_directory "$POW_DIR_IMPORT" \
        --output_file "${_vars_ref[FILE_NAME]}" &&
    import_file \
        --file_path "$POW_DIR_IMPORT/${_vars_ref[FILE_NAME]}" \
        --table_name ${_vars_ref[TABLE_NAME]} \
        --load_mode OVERWRITE_DATA \
        $_import_options || return $ERROR_CODE

    return $SUCCESS_CODE
}

# get list of code(s), checking count
bal_get_list() {
    bash_args \
        --args_p '
            name:Nommage de la sélection;
            query:Requête de sélection;
            as_string:Retour en tant que chaîne;
            as_array:Retour en tant que tableau
        ' \
        --args_o '
            name;
            query
        ' \
        "$@" || return $ERROR_CODE

    local _result _return=0
    local -a _results

    execute_query \
        --name "$get_arg_name" \
        --query "$get_arg_query" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _result &&
    {
        IFS='|' read -ra _results <<< "$_result"
    } &&
    {
        [[ ${#_results[@]} -eq 2 ]] || {
            log_error 'liste attendue avec comptage pour contrôle!'
            false
        }
    } &&
    {
        [ -z "$get_arg_as_array" ] || {
            local -n _as_array_ref=$get_arg_as_array

            array_sql_to_bash \
                --array_sql "${_results[0]}" \
                --count ${_results[1]} \
                --array_bash _as_array_ref &&
            _return=$((_return +1))
        }
    } &&
    {
        [ -z "$get_arg_as_string" ] || {
            local -n _as_string_ref=$get_arg_as_string

            # TODO check count!
            [[ ${_results[1]} -gt 0 ]] && _as_string_ref="${_results[0]}" || _as_string_ref=''
            _return=$((_return +1))
        }
    } &&
    {
        [[ $_return -gt 0 ]] || {
            log_error 'pas de retour demandé?'
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# load BAL addresses (streets or housenumbers)
bal_load_addresses() {
    bash_args \
        --args_p '
            vars:Entité des variables globales;
            level:Niveau Adresses;
            count:Comptage des Adresses
        ' \
        --args_o '
            vars;
            level;
            count
        ' \
        --args_v '
            level:STREET|HOUSENUMBER
        ' \
        "$@" || return $ERROR_CODE

    # be careful at circular name reference, a trick is to use different name (_globals_ref) !
    # due to other call, as: --vars _vars_ref
    local -n _globals_ref=$get_arg_vars
    local -n _count_ref=$get_arg_count
    local _name _query _info
    local -a _addresses

    _name="BAL_SELECT_${_globals_ref[MUNICIPALITY_CODE]}" &&
    case "$get_arg_level" in
    STREET)
        _name+=_STREETS
        _info=voies
        _query="
            SELECT
                ARRAY_AGG(s.code),
                COUNT(1)
            FROM
                fr.bal_street s
                    JOIN fr.bal_municipality m ON s.id_municipality = m.id
            WHERE
                s.housenumbers_auth > 0
                AND
        "
        ;;
    HOUSENUMBER)
        _name+=_HOUSENUMBERS
        _info=numéros
        _query="
            SELECT
                ARRAY_AGG(hn.code),
                COUNT(1)
            FROM
                fr.bal_housenumber hn
                    JOIN fr.bal_street s ON hn.id_street = s.id
                    JOIN fr.bal_municipality m ON s.id_municipality = m.id
            WHERE
        "
        ;;
    esac &&
    _query+="
        m.code = '${_globals_ref[MUNICIPALITY_CODE]}'
    " &&
    # select streets|housenumbers w/ count (to check conversion)
    bal_get_list \
        --name "$_name" \
        --query "$_query" \
        --as_array _addresses &&
    _count_ref=${#_addresses[@]} &&
    {
        [[ $_count_ref -gt 0 ]] && {
            execute_query \
                --name BAL_TRUNCATE \
                --query "TRUNCATE TABLE fr.${_globals_ref[TABLE_NAME]}" &&
            # load addresses as JSON in table (has to be empty!)
            for ((_j=0; _j<${#_addresses[@]}; _j++)); do
                {
                    (! is_yes --var _globals_ref[PROGRESS]) || {
                        bal_progress_bar \
                            BEGIN \
                            "${_info}" \
                            ${_globals_ref[PROGRESS_SIZE]} \
                            $((_j +1)) \
                            ${_count_ref} \
                            '\r'
                    }
                } &&
                _globals_ref[URL_DATA]="lookup/${_addresses[$_j]}" &&
                _globals_ref[FILE_NAME]="${_addresses[$_j]}.json" &&
                io_download_file \
                    --url "${_globals_ref[URL]}/${_globals_ref[URL_DATA]}" \
                    --overwrite_mode NEWER \
                    --overwrite_key DATE \
                    --overwrite_value ${_globals_ref[IO_END_EPOCH]} \
                    --common_subdir bal \
                    --output_directory "$POW_DIR_IMPORT" \
                    --output_file "${_globals_ref[FILE_NAME]}" &&
                import_file \
                    --file_path "$POW_DIR_IMPORT/${_globals_ref[FILE_NAME]}" \
                    --table_name ${_globals_ref[TABLE_NAME]} \
                    --import_options column_name=data \
                    --load_mode APPEND
            done
        } || {
            (! is_yes --var _globals_ref[PROGRESS]) || {
                echo 'pas de numéros certifiés'
            }
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# integration BAL data (summary or one municipality)
bal_integration() {
    bash_args \
        --args_p '
            vars:Entité des variables globales
        ' \
        --args_o '
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars
    local _list _j _streets=0 _housenumbers=0 _elapsed _obsolete _counters

    table_exists --schema_name fr --table_name "${_vars_ref[TABLE_NAME]}" &&
    {
        case "${_vars_ref[MUNICIPALITY_CODE]}" in
        ALL)
            # manage municipality, deleting obsolete ones w/ {housenumbers, streets} dependences
            execute_query \
                --name "BAL_INTEGRATION_SUMMARY" \
                --query "
                    INSERT INTO fr.bal_municipality(
                        code,
                        name,
                        population,
                        areas,
                        streets,
                        housenumbers,
                        housenumbers_auth,
                        last_update
                    )
                    SELECT
                        code_commune,
                        nom_commune,
                        population::INT,
                        nb_lieux_dits::INT,
                        nb_voies::INT,
                        nb_numeros::INT,
                        nb_numeros_certifies::INT,
                        composed_at::TIMESTAMP WITHOUT TIME ZONE
                    FROM
                        fr.${_vars_ref[TABLE_NAME]}
                    ON CONFLICT(code) DO UPDATE SET
                        name = EXCLUDED.name,
                        population = EXCLUDED.population,
                        areas = EXCLUDED.areas,
                        streets = EXCLUDED.streets,
                        housenumbers = EXCLUDED.housenumbers,
                        housenumbers_auth = EXCLUDED.housenumbers_auth,
                        last_update = EXCLUDED.last_update
                " &&
            bal_get_list \
                --name "BAL_SELECT_OBSOLETE_SUMMARY" \
                --query "
                    SELECT
                        ARRAY_AGG(m.code),
                        COUNT(1)
                    FROM
                        fr.bal_municipality m
                    WHERE
                        NOT EXISTS(
                            SELECT 1
                            FROM fr.${_vars_ref[TABLE_NAME]} tm
                            WHERE m.code = tm.code_commune
                        )
                "
                --as_string _obsolete &&
            {
                [ -z "$_obsolete" ] || {
                    log_info "liste Communes obsolètes ($_obsolete)" &&
                    execute_query \
                        --name "BAL_DELETE_OBSOLETE_SUMMARY" \
                        --query "
                            SELECT counters FROM fr.bal_delete_obsolete_addresses(
                                list => '$_obsolete'
                            )
                        " \
                        --psql_arguments 'tuples-only:pset=format=unaligned' \
                        --return _counters &&
                    log_info "comptage: ${_counters}"
                }
            } &&
            execute_query \
                --name "BAL_DROP_SUMMARY" \
                --query "DROP TABLE fr.${_vars_ref[TABLE_NAME]}" &&
            {
                (! is_yes --var _vars_ref[PROGRESS]) || {
                    _elapsed=$(($(date '+%s') - ${_vars_ref[PROGRESS_START]})) &&
                    bal_progress_bar END "$(date --date @${_elapsed} --utc +%H:%M:%S)"
                }
            } &&
            io_history_end_ok \
                --nrows_processed '(SELECT COUNT(*) FROM fr.bal_municipality)' \
                --id ${_vars_ref[IO_ID]} &&
            vacuum \
                --schema_name fr \
                --table_name bal_municipality \
                --mode ANALYZE || return $ERROR_CODE
            ;;
        *)
            # insert/update streets, and delete old ones (obsolete)
            execute_query \
                --name "BAL_INTEGRATION_${_vars_ref[MUNICIPALITY_CODE]}_STREETS" \
                --query "
                    INSERT INTO fr.bal_street (
                        id_municipality,
                        code,
                        name,
                        kind,
                        sources,
                        housenumbers,
                        housenumbers_auth,
                        last_update
                    )
                    SELECT
                        m.id,
                        v->>'idVoie',
                        v->>'nomVoie',
                        v->>'type',
                        CASE
                            WHEN v->'sources' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(v->'sources'))::TEXT[]
                            ELSE ARRAY[v->>'sources']::TEXT[]
                        END,
                        (v->>'nbNumeros')::INT,
                        (v->>'nbNumerosCertifies')::INT,
                        TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE
                    FROM
                        fr.${_vars_ref[TABLE_NAME]}
                            CROSS JOIN JSON_ARRAY_ELEMENTS(data->'voies') v
                            JOIN fr.bal_municipality m ON m.code = data->>'codeCommune'
                    ON CONFLICT(code) DO UPDATE SET
                        id_municipality = EXCLUDED.id_municipality,
                        name = EXCLUDED.name,
                        kind = EXCLUDED.kind,
                        sources = EXCLUDED.sources,
                        housenumbers = EXCLUDED.housenumbers,
                        housenumbers_auth = EXCLUDED.housenumbers_auth,
                        last_update = EXCLUDED.last_update
                " &&
            bal_get_list \
                --name "BAL_SELECT_OBSOLETE_${_vars_ref[MUNICIPALITY_CODE]}_STREETS" \
                --query "
                    SELECT
                        ARRAY_AGG(s.code),
                        COUNT(1)
                    FROM
                        fr.bal_street s
                            JOIN fr.bal_municipality m ON m.id = s.id_municipality
                    WHERE
                        m.code = '${_vars_ref[MUNICIPALITY_CODE]}'
                        AND
                        NOT EXISTS(
                            SELECT 1
                            FROM fr.${_vars_ref[TABLE_NAME]}
                                CROSS JOIN JSON_ARRAY_ELEMENTS(data->'voies') s2
                            WHERE
                                s.code = s2->>'idVoie'
                        )
                "
                --as_string _obsolete &&
            {
                [ -z "$_obsolete" ] || {
                    log_info "liste Voies obsolètes ($_obsolete)" &&
                    execute_query \
                        --name "BAL_DELETE_OBSOLETE_${_vars_ref[MUNICIPALITY_CODE]}_STREETS" \
                        --query "
                            SELECT counters FROM fr.bal_delete_obsolete_addresses(
                                list => '$_obsolete'
                            )
                        " \
                        --psql_arguments 'tuples-only:pset=format=unaligned' \
                        --return _counters &&
                    log_info "comptage: ${_counters}"
                }
            } &&
            {
                (! is_yes --var _vars_ref[PROGRESS]) || {
                    _elapsed=$(($(date '+%s') - ${_vars_ref[PROGRESS_START]})) &&
                    bal_progress_bar END "$(date --date @${_elapsed} --utc +%H:%M:%S)" &&
                    _vars_ref[PROGRESS_START]=$(date '+%s')
                }
            } &&
            # count areas
            execute_query \
                --name "BAL_MUNICIPALITY_${_vars_ref[MUNICIPALITY_CODE]}_AREAS" \
                --query "
                    SELECT
                        COUNT(1)
                    FROM
                        fr.laposte_address_area
                    WHERE
                        fl_active
                        AND
                        co_insee_commune = '${_vars_ref[MUNICIPALITY_CODE]}'
                " \
                --psql_arguments 'tuples-only:pset=format=unaligned' \
                --return _vars_ref[MUNICIPALITY_AREAS] &&
            # select streets w/ certified housenumbers
            bal_load_addresses --vars _vars_ref --level STREET --count _streets &&
            {
                # w/ auth housenumbers ?
                [[ $_streets -eq 0 ]] || {
                    {
                        (! is_yes --var _vars_ref[PROGRESS]) || {
                            _elapsed=$(($(date '+%s') - ${_vars_ref[PROGRESS_START]})) &&
                            bal_progress_bar END "$(date --date @${_elapsed} --utc +%H:%M:%S)" &&
                            _vars_ref[PROGRESS_START]=$(date '+%s')
                        }
                    } &&
                    # insert/update housenumbers, and delete old ones (obsolete)
                    # update street's geometry
                    execute_query \
                        --name "BAL_INTEGRATION_${_vars_ref[MUNICIPALITY_CODE]}_HOUSENUMBERS" \
                        --query "
                            INSERT INTO fr.bal_housenumber (
                                id_street,
                                code,
                                number,
                                extension,
                                sources,
                                postcode,
                                parcels,
                                geom,
                                location,
                                last_update
                            )
                            SELECT
                                s.id,
                                n->>'id',
                                (n->>'numero')::INT,
                                n->>'suffixe',
                                CASE
                                    WHEN n->'sources' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->'sources'))::VARCHAR[]
                                    ELSE ARRAY[n->>'sources']::VARCHAR[]
                                END,
                                n->>'postcode',
                                CASE
                                    WHEN n->'parcelles' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->'parcelles'))::VARCHAR[]
                                    ELSE ARRAY[n->>'parcelles']::VARCHAR[]
                                END,
                                CASE
                                    WHEN n->'position'->'coordinates' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(n->'position'->'coordinates'))::TEXT[]::FLOAT[]
                                    ELSE NULL::FLOAT[]
                                END,
                                n->>'positionType',
                                TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE
                            FROM
                                fr.${_vars_ref[TABLE_NAME]}
                                    CROSS JOIN JSON_ARRAY_ELEMENTS(data->'numeros') n
                                    JOIN fr.bal_street s ON s.code = data->>'idVoie'
                            WHERE
                                UPPER(n->>'certifie') = 'TRUE'
                            ON CONFLICT(code) DO UPDATE SET
                                id_street = EXCLUDED.id_street,
                                number = EXCLUDED.number,
                                extension = EXCLUDED.extension,
                                sources = EXCLUDED.sources,
                                postcode = EXCLUDED.postcode,
                                parcels = EXCLUDED.parcels,
                                geom = EXCLUDED.geom,
                                location = EXCLUDED.location,
                                last_update = EXCLUDED.last_update
                            ;
                            WITH
                            street_geometry AS (
                                SELECT
                                    s.id,
                                    CASE
                                        WHEN data->'position'->'coordinates' IS JSON ARRAY THEN ARRAY(SELECT JSON_ARRAY_ELEMENTS(data->'position'->'coordinates'))::TEXT[]::FLOAT[]
                                        ELSE NULL::FLOAT[]
                                    END geom
                                FROM
                                    fr.${_vars_ref[TABLE_NAME]}
                                        JOIN fr.bal_street s ON s.code = data->>'idVoie'
                            )
                            UPDATE fr.bal_street s SET
                                geom = g.geom
                                FROM street_geometry g
                                WHERE
                                    s.id = g.id
                            ;
                        " &&
                        bal_get_list \
                            --name "BAL_SELECT_OBSOLETE_${_vars_ref[MUNICIPALITY_CODE]}_HOUSENUMBERS" \
                            --query "
                                SELECT
                                    ARRAY_AGG(n.code),
                                    COUNT(1)
                                FROM
                                    fr.bal_housenumber n
                                        JOIN fr.bal_street s ON s.id = n.id_street
                                        JOIN fr.bal_municipality m ON m.id = s.id_municipality
                                WHERE
                                    m.code = '${_vars_ref[MUNICIPALITY_CODE]}'
                                    AND
                                    NOT EXISTS(
                                        SELECT 1
                                        FROM fr.${_vars_ref[TABLE_NAME]}
                                            CROSS JOIN JSON_ARRAY_ELEMENTS(data->'numeros') n2
                                        WHERE
                                            n.code = n2->>'id'
                                    )
                            "
                            --as_string _obsolete &&
                        {
                            [ -z "$_obsolete" ] || {
                                log_info "liste Numéros obsolètes ($_obsolete)" &&
                                execute_query \
                                    --name "BAL_DELETE_OBSOLETE_${_vars_ref[MUNICIPALITY_CODE]}_HOUSENUMBERS" \
                                    --query "
                                        SELECT counters FROM fr.bal_delete_obsolete_addresses(
                                            list => '$_obsolete'
                                        )
                                    " \
                                    --psql_arguments 'tuples-only:pset=format=unaligned' \
                                    --return _counters &&
                                log_info "comptage: ${_counters}"
                            }
                        } &&
                        # need to request API on each housenumber, if many areas! to obtain old municipality
                        {
                            [[ ${_vars_ref[MUNICIPALITY_AREAS]} -eq 0 ]] || {
                                # select housenumbers
                                bal_load_addresses --vars _vars_ref --level HOUSENUMBER --count _housenumbers &&
                                execute_query \
                                    --name "BAL_INTEGRATION_${_vars_ref[MUNICIPALITY_CODE]}_HOUSENUMBERS_AREA" \
                                    --query "
                                        WITH
                                        old_municipality AS (
                                            SELECT
                                                data->>'id' id,
                                                CASE
                                                    WHEN data->'adressesOriginales' IS JSON ARRAY THEN
                                                        data->'adressesOriginales'->-1 #>> '{meta, bal, nomAncienneCommune}'
                                                END old_municipality
                                            FROM
                                                fr.${_vars_ref[TABLE_NAME]}
                                        )
                                        UPDATE fr.bal_housenumber hn SET
                                            area = om.old_municipality
                                            FROM old_municipality om
                                            WHERE
                                                hn.code = om.id
                                                AND
                                                om.old_municipality IS NOT NULL
                                    " &&
                                {
                                    (! is_yes --var _vars_ref[PROGRESS]) || {
                                        _elapsed=$(($(date '+%s') - ${_vars_ref[PROGRESS_START]})) &&
                                        bal_progress_bar END "$(date --date @${_elapsed} --utc +%H:%M:%S)"
                                    }
                                }
                            }
                        }
                    }
                } &&
                execute_query \
                    --name BAL_DROP \
                    --query "DROP TABLE IF EXISTS fr.${_vars_ref[TABLE_NAME]}" &&
                # total can be less than waited, only streets w/ certified housenumbers
                io_history_end_ok \
                    --nrows_processed $((_streets+_housenumbers)) \
                    --infos ""'"'"STREETS"'"'" => $_streets, "'"'"HOUSENUMBERS_AUTH"'"'" => $_housenumbers" \
                    --id ${_vars_ref[IO_ID]} &&
                vacuum \
                    --schema_name fr \
                    --table_name bal_street,bal_housenumber \
                    --mode ANALYZE || return $ERROR_CODE
        esac
    }

    return $SUCCESS_CODE
}

# select municipalities (w/ criteria & order) from summary
bal_list_municipalities() {
    bash_args \
        --args_p '
            vars:Entité des variables globales;
            list:Liste résultat
        ' \
        --args_o '
            vars;
            list
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars
    local -n _list_ref=$get_arg_list
    local _query _list

    case "${bal_vars[SELECT_CRITERIA]}" in
    POPULATION)
        _query="
            SELECT
                codgeo municipality,
                population criteria
            FROM
                fr.territory
            WHERE
                nivgeo = 'COM'
                AND
                population IS NOT NULL
        "
        ;;
    STREETS)
        _query="
            SELECT
                co_insee_commune municipality,
                COUNT(DISTINCT co_voie) criteria
            FROM
                fr.laposte_address_street
            WHERE
                fl_active
            GROUP BY
                co_insee_commune
        "
        ;;
    REVISION)
        _query="
            SELECT
                code municipality,
                last_update criteria
            FROM
                fr.bal_municipality
        "
        ;;
    esac &&
    _query="
        WITH
        history AS (
            SELECT
                SUBSTR(io.name, 5) municipality,
                MAX(l.date_data_end) date_date_end
            FROM
                io_history io
                    CROSS JOIN get_last_io(io.name) l
            WHERE
                io.name ~ '^BAL_[0-9]'
            GROUP BY
                SUBSTR(io.name, 5)
        )
        , criteria AS (
            $_query
        )
        SELECT ARRAY(
            SELECT
                c.municipality
            FROM
                criteria c
                    JOIN fr.bal_municipality m ON c.municipality = m.code
                    LEFT OUTER JOIN history h ON h.municipality = c.municipality
            WHERE
                h.date_date_end IS NULL
                OR
                m.last_update > h.date_date_end
            ORDER BY
                c.criteria ${bal_vars[SELECT_ORDER]}
    " &&
    {
        [[ ${bal_vars[LIMIT]} -gt 0 ]] && {
            _query+="
                LIMIT
                    ${bal_vars[LIMIT]}
            "
        } || true
    } &&
    _query+=")" &&
    execute_query \
        --name BAL_MUNICIPALITIES \
        --query "$_query" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _list &&
    array_sql_to_bash --array_sql "$_list" --array_bash _list_ref || return $ERROR_CODE

    return $SUCCESS_CODE
}

# is code OK ?
valid_municipality_code() {
    bash_args \
        --args_p '
            municipality:Code Commune
        ' \
        --args_o '
            municipality
        ' \
        "$@" || return $ERROR_CODE

    local _valid

    execute_query \
        --name "BAL_MUNICIPALITY_$get_arg_municipality" \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.laposte_address_area
                WHERE co_insee_commune = COALESCE('$get_arg_municipality', '99999') AND fl_active
            )" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _valid || return $ERROR_CODE

    [ "$_valid" = f ] && {
        log_error "code Commune '$get_arg_municipality' non valide!"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

bash_args \
    --args_p '
        municipality:Code Commune INSEE à traiter (ou ALL pour télécharger la liste complète);
        select_criteria:Sélection des Communes;
        select_order:Ordre de sélection des Communes;
        limit:Limiter à n communes (0 sans limite);
        stop_time:Heure d arrêt du traitement (format: hh:mm:ss);
        force:Forcer le traitement même si celui-ci a déjà été fait;
        dry_run:Simuler le traitement;
        progress:Afficher une jauge de progression;
        clean:Effectuer la purge des fichiers temporaires;
        verbose:Ajouter des détails sur les traitements
    ' \
    --args_o '
        municipality
    ' \
    --args_v '
        select_criteria:REVISION|POPULATION|STREETS;
        select_order:ASC|DESC;
        force:yes|no;
        dry_run:yes|no;
        progress:yes|no;
        clean:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        select_criteria:REVISION;
        select_order:DESC;
        force:no;
        dry_run:no;
        limit:30;
        stop_time:0;
        progress:no;
        clean:yes;
        verbose:no
    ' \
    "$@" || exit $ERROR_CODE

declare -A bal_vars=(
    [MUNICIPALITY_CODE]="${get_arg_municipality^^}"
    [MUNICIPALITY_AREAS]=0
    [URL]='https://plateforme.adresse.data.gouv.fr'
    [URL_DATA]=
    [IO_NAME]=
    [IO_ID]=
    [IO_BEGIN]=
    [IO_END]="$(date +%F)"
    [IO_END_EPOCH]=
    [IO_ROWS]=0
    [FILE_NAME]=
    [TABLE_NAME]=
    [SELECT_CRITERIA]=$get_arg_select_criteria
    [SELECT_ORDER]=$get_arg_select_order
    [LIMIT]=$get_arg_limit
    [STOP_TIME]=$get_arg_stop_time
    [FORCE]=$get_arg_force
    [DRY_RUN]=$get_arg_dry_run
    [CLEAN]=$get_arg_clean
    [PROGRESS]=$get_arg_progress
    [PROGRESS_START]=
    [PROGRESS_CURRENT]=1
    [PROGRESS_TOTAL]=1
    [PROGRESS_SIZE]=5
    [VERBOSE]=$get_arg_verbose
)
declare -a bal_codes=()

set_env --schema_name fr &&
{
    (! is_yes --var bal_vars[PROGRESS]) || set_log_echo no
} &&
{
    [ "${bal_vars[MUNICIPALITY_CODE]}" = ALL ] && {
        bal_load --vars bal_vars &&
        bal_integration --vars bal_vars &&
        bal_list_municipalities --vars bal_vars --list bal_codes || on_import_error --vars bal_vars
        is_yes --var bal_vars[CLEAN] && rm --force "$POW_DIR_IMPORT/${bal_vars[FILE_NAME]}"
    } || {
        bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
    }
} &&

bal_error=0
bal_vars[PROGRESS_TOTAL]=${#bal_codes[@]}
for ((bal_i=0; bal_i<${#bal_codes[@]}; bal_i++)); do
    # check municipality
    valid_municipality_code --municipality "${bal_codes[$bal_i]}" || {
        log_error "commune BAL '${bal_codes[$bal_i]}' non valide!"
        bal_error=1
        continue
    }
    bal_vars[MUNICIPALITY_CODE]=${bal_codes[$bal_i]}
    # progress bar
    bal_vars[PROGRESS_CURRENT]=$((bal_i +1))
    # do it ?
    is_yes --var bal_vars[DRY_RUN] || {
        bal_load --vars bal_vars &&
        bal_integration --vars bal_vars || on_import_error --vars bal_vars
    }
    # purge ?
    is_yes --var bal_vars[CLEAN] && rm --force $POW_DIR_IMPORT/${bal_vars[MUNICIPALITY_CODE]}*.json
done

_rc=$(( bal_error == 1 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
