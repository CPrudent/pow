#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BAL addresses: summary (municipality), street and only certified housenumber

    # NOTE
    # be careful at circular name reference (passing array to function)
    # due to other call, as: --vars _vars_ref
    # a trick is to use different name (_globals_ref)
    # but limited to low imbrication, so finally use global variable !

    # HELP
    # https://stackoverflow.com/questions/16908084/bash-script-to-calculate-time-elapsed
    # https://stackoverflow.com/questions/3953645/ternary-operator-in-bash
    # https://stackoverflow.com/questions/10586153/how-to-split-a-string-into-an-array-in-bash
    # https://linuxhint.com/bash_arithmetic_operations/

    # TODO
    # assign PROGRESS_SIZE w/ max (municipalities, streets, housenumbers) of selection

on_break() {
    log_error 'arrêt utilisateur' &&
    on_import_error
}

on_import_error() {
    local _info=$( [ -n "${bal_vars[FIX]}" ] && echo ${bal_vars[FIX]} || echo ${bal_vars[IO_NAME]#*_} )

    # import created?
    [ "$POW_DEBUG" = yes ] && { echo "io_history_id=${bal_vars[IO_ID]}"; }
    [ -n "${bal_vars[IO_ID]}" ] && io_history_end_ko --id ${bal_vars[IO_ID]}

    log_error "Erreur import BAL ($_info)"
    exit $ERROR_CODE
}

# deal w/ interrupt signal (CTRL-C, kill)
trap on_break SIGINT

# print progress as (ratio, percent)
# $1= begin
    # $2= label
    # $3= size of (number of digits)
    # $4= subscript
    # $5= total
    # $6= end of line
# $1= end
    # $2= elapsed time
    # $3= more information
bal_progress_bar() {
    case "${1^^}" in
    BEGIN)
        expect argc bal_progress_bar $# 6 || return $ERROR_CODE
        # if main display (municipality level) and only one then reduce informations
        ([[ "${2:0:5}" =~ INSEE|Commu ]] && [[ $5 -eq 1 ]]) && {
            printf '%-15s%b' "$2" $6
        } || {
            printf '%-15s\t%*d/%*d (%3d%%)%b' "$2" $3 $4 $3 $5 $((($4*100)/$5)) $6
        }
        ;;
    END)
        printf "\t\t\t\t\t%s\t\t%s\n" "$2" "$3"
        ;;
    esac

    return $SUCCESS_CODE
}

# is code OK ?
bal_check_municipality() {
    bash_args \
        --args_p '
            code:Code Commune
        ' \
        --args_o '
            code
        ' \
        "$@" || return $ERROR_CODE

    local _valid

    execute_query \
        --name "BAL_MUNICIPALITY_${get_arg_code}" \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.laposte_address_area
                WHERE co_insee_commune = COALESCE('${get_arg_code}', '99999') AND fl_active
            )" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _valid || return $ERROR_CODE

    [ "$_valid" = f ] && {
        log_error "code Commune '${get_arg_code}' non valide!"
        return $ERROR_CODE
    }

    # count areas (w/ old municipality owning at least one address)
    execute_query \
        --name "BAL_MUNICIPALITY_${get_arg_code}_AREAS" \
        --query "
            SELECT
                COUNT(1)
            FROM
                fr.laposte_address_area a
            WHERE
                fl_active
                AND
                co_insee_commune = '${get_arg_code}'
                AND
                lb_l5_nn IS NOT NULL
                AND
                EXISTS(
                    SELECT 1
                    FROM
                        fr.laposte_address r
                    WHERE
                        r.co_cea_za = a.co_cea
                        AND
                        r.fl_active
                        AND
                        r.co_cea_voie IS NOT NULL
                )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return bal_vars[WITH_OLD_AREA] &&
    execute_query \
        --name "BAL_MUNICIPALITY_${get_arg_code}_ROWS" \
        --query "
            SELECT
                CASE
                WHEN ${bal_vars[WITH_OLD_AREA]} > 0 THEN
                    areas + streets + housenumbers_auth
                ELSE
                    areas + streets
                END
            FROM fr.bal_municipality
            WHERE code = '${get_arg_code}'
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return bal_vars[IO_ROWS] &&
    {
        [ -n "${bal_vars[IO_ROWS]}" ] || {
            log_error "Import préalable de l'ensemble des Communes (--municipality ALL)"
            false
        }
    } &&
    {
        # reset counters
        bal_vars[STREETS]=-1
        bal_vars[HOUSENUMBERS]=-1
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# deal w/ counters (STREET & HOUSENUMBER)
bal_get_counters() {
    local -A _opts &&
    pow_argv \
        --args_n '
            usage:Cas usage;
            value:Résultat
        ' \
        --args_m '
            usage;value
        ' \
        --args_v '
            usage:NROWS|ATTRIBUTES|PROGRESS
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -n _value_ref=${_opts[VALUE]}

    case "${_opts[USAGE]}" in
    NROWS)
        _value_ref=${bal_vars[STREETS]}
        [[ ${bal_vars[WITH_OLD_AREA]} > 0 ]] && _value_ref=$((_value_ref + bal_vars[HOUSENUMBERS]))
        ;;
    ATTRIBUTES)
        _value_ref='{"integration":{"streets":'${bal_vars[STREETS]}',"housenumbers":'${bal_vars[HOUSENUMBERS]}'}}'
        ;;
    PROGRESS)
        _value_ref="#${bal_vars[STREETS]} voies, #${bal_vars[HOUSENUMBERS]} numéros"
        ;;
    esac

    return $SUCCESS_CODE
}

# get last update
bal_last_update_municipality() {
    execute_query \
        --name BAL_IO_END \
        --query "
            SELECT last_update
            FROM fr.bal_municipality
            WHERE code = '${bal_vars[MUNICIPALITY_CODE]}'
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return bal_vars[IO_END] &&
    bal_vars[IO_END_EPOCH]=$(date '+%s' --date "${bal_vars[IO_END]}") || return $ERROR_CODE

    return $SUCCESS_CODE
}

# get average time to download an address (street or housenumber) from BAL site
bal_average_time() {
    bash_args \
        --args_p '
            avg:Temps moyen nécessaire pour télécharger une adresse (voie ou numéro)
        ' \
        --args_o '
            avg
        ' \
        "$@" || return $ERROR_CODE

    local -n _avg_ref=$get_arg_avg

    execute_query \
        --name BAL_TIMEX \
        --query "
            SELECT AVG(timex) FROM (
                SELECT
                    (EXTRACT(EPOCH FROM h2.date_exec_end) - EXTRACT(EPOCH FROM h2.date_exec_begin)) / NULLIF(h2.nb_rows_processed, 0) timex
                FROM
                    io_history h1
                        JOIN get_last_io(h1.name) h2 ON h1.id = h2.id
                WHERE
                    h1.name ~ '^BAL_[0-9]'
                    AND
                    h2.nb_rows_processed > 0
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _avg_ref || return $ERROR_CODE

    return $SUCCESS_CODE
}

# deal w/ temporary import table
bal_import_table() {
    bash_args \
        --args_p '
            command:Action SQL à faire
        ' \
        --args_o '
            command
        ' \
        --args_v '
            command:CREATE|DROP
        ' \
        "$@" || return $ERROR_CODE

    local _query _ddl=1

    {
        [ -n "${bal_vars[IO_NAME]}" ] \
            && bal_vars[TABLE_NAME]=tmp_${bal_vars[IO_NAME],,} \
            || bal_vars[TABLE_NAME]=tmp_bal_${bal_vars[MUNICIPALITY_CODE]}
    } &&
    case "$get_arg_command" in
    CREATE)
        [ "${bal_vars[IO_NAME]}" = BAL_SUMMARY ] && _ddl=0
        _query="CREATE TABLE IF NOT EXISTS fr.${bal_vars[TABLE_NAME]} (data JSON)"
        ;;
    DROP)
        _query="DROP TABLE IF EXISTS fr.${bal_vars[TABLE_NAME]}"
        ;;
    esac &&
    {
        [ $_ddl -eq 0 ] || {
            execute_query \
                --name BAL_${get_arg_command} \
                --query "$_query"
        }
    } || return $ERROR_CODE

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

bal_count_addresses() {
    {
        [[ ${bal_vars[STREETS]} != -1 ]] || {
            execute_query \
                --name BAL_${bal_vars[MUNICIPALITY_CODE]}_NSTREETS \
                --query "
                    SELECT
                        COUNT(1)
                    FROM
                        fr.bal_street s
                            JOIN fr.bal_municipality m ON s.id_municipality = m.id
                    WHERE
                        s.housenumbers_auth > 0
                        AND
                        m.code = '${bal_vars[MUNICIPALITY_CODE]}'
                " \
                --psql_arguments 'tuples-only:pset=format=unaligned' \
                --return bal_vars[STREETS]
        }
    } &&
    {
        [[ ${bal_vars[HOUSENUMBERS]} != -1 ]] || {
            execute_query \
                --name BAL_${bal_vars[MUNICIPALITY_CODE]}_NHOUSENUMBERS \
                --query "
                    SELECT
                        COUNT(1)
                    FROM
                        fr.bal_housenumber n
                            JOIN fr.bal_street s ON n.id_street = s.id
                            JOIN fr.bal_municipality m ON s.id_municipality = m.id
                    WHERE
                        s.housenumbers_auth > 0
                        AND
                        m.code = '${bal_vars[MUNICIPALITY_CODE]}'
                " \
                --psql_arguments 'tuples-only:pset=format=unaligned' \
                --return bal_vars[HOUSENUMBERS]
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# load BAL addresses (streets or housenumbers)
bal_load_addresses() {
    bash_args \
        --args_p '
            level:Niveau Adresses
        ' \
        --args_o '
            level
        ' \
        --args_v '
            level:STREET|HOUSENUMBER
        ' \
        "$@" || return $ERROR_CODE

    local _name _query _info _j _rc _field=${get_arg_level}S _code _len _url _file
    local -a _addresses

    _name="BAL_SELECT_${bal_vars[MUNICIPALITY_CODE]}_${_field}" &&
    case "${get_arg_level}" in
    STREET)
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
        _info=numéros
        _query="
            SELECT
                ARRAY_AGG(n.code),
                COUNT(1)
            FROM
                fr.bal_housenumber n
                    JOIN fr.bal_street s ON n.id_street = s.id
                    JOIN fr.bal_municipality m ON s.id_municipality = m.id
            WHERE
                s.housenumbers_auth > 0
                AND
        "
        ;;
    esac &&
    _query+="
        m.code = '${bal_vars[MUNICIPALITY_CODE]}'
    " &&
    {
        [ -z "${bal_vars[FIX]}" ] || {
            case "${bal_vars[FIX]}" in
            SPACE_IN_CODE)
                _query+="
                    AND
                    POSITION(' ' IN n.code) > 0
                "
                ;;
            esac
        }
    } &&
    # select streets|housenumbers w/ count (to check conversion)
    bal_get_list \
        --name "$_name" \
        --query "$_query" \
        --as_array _addresses &&
    bal_vars[$_field]=${#_addresses[@]} &&
    {
        [[ ${bal_vars[$_field]} > 0 ]] && {
            execute_query \
                --name BAL_TRUNCATE \
                --query "TRUNCATE TABLE fr.${bal_vars[TABLE_NAME]}" &&
            # load addresses as JSON in table (has to be empty!)
            for ((_j=0; _j<${#_addresses[@]}; _j++)); do
                {
                    (! is_yes --var bal_vars[PROGRESS]) || {
                        bal_progress_bar \
                            BEGIN \
                            "${_info}" \
                            ${bal_vars[PROGRESS_SIZE]} \
                            $((_j +1)) \
                            ${bal_vars[$_field]} \
                            '\r'
                    }
                } &&
                # code between quotes (w/ space) ?
                {
                    _code=
                    [[ ${_addresses[$_j]:0:1} != '"' ]] || {
                        _len=$((${#_addresses[$_j]} -2)) &&
                        _code=${_addresses[$_j]:1:$_len}
                    }
                } &&
                _url=lookup/${_code:-${_addresses[$_j]}} &&
                bal_vars[FILE_NAME]=${_code:-${_addresses[$_j]}}.json &&
                {
                    io_download_file \
                        --url "${bal_vars[URL]}/$_url" \
                        --overwrite_mode NEWER \
                        --overwrite_key DATE \
                        --overwrite_value ${bal_vars[IO_END_EPOCH]} \
                        --common_subdir bal \
                        --output_directory "$POW_DIR_IMPORT" \
                        --output_file "${bal_vars[FILE_NAME]}" \
                        --verbose ${bal_vars[VERBOSE]}
                    _rc=$?
                    [[ $_rc -lt $POW_DOWNLOAD_ERROR ]] && {
                        # same data has to be loaded again ?
                        ([ "${bal_vars[FORCE_LOAD]}" = no ] && [[ $_rc -eq $POW_DOWNLOAD_ALREADY_AVAILABLE ]]) || {
                            import_file \
                                --file_path "$POW_DIR_IMPORT/${bal_vars[FILE_NAME]}" \
                                --table_name ${bal_vars[TABLE_NAME]} \
                                --import_options column_name=data \
                                --load_mode APPEND
                        }
                    }
                }
            done
        } || {
            (! is_yes --var bal_vars[PROGRESS]) || {
                echo 'pas de numéros certifiés'
            }
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# deal w/ obsolescence
bal_deal_obsolescence() {
    bash_args \
        --args_p '
            level:Niveau Adresses
        ' \
        --args_o '
            level
        ' \
        --args_v '
            level:MUNICIPALITY|STREET|HOUSENUMBER
        ' \
        "$@" || return $ERROR_CODE

    local _label1=SELECT _label2 _query _info _obsolete _counters

    case "$get_arg_level" in
    MUNICIPALITY)
        _label2=SUMMARY
        _info=Communes
        _query="
            SELECT
                ARRAY_AGG(m.code),
                COUNT(1)
            FROM
                fr.bal_municipality m
            WHERE
                NOT EXISTS(
                    SELECT 1
                    FROM fr.${bal_vars[TABLE_NAME]} tm
                    WHERE
                        m.code = tm.code_commune
                )
        "
        ;;
    STREET)
        _label2=${bal_vars[MUNICIPALITY_CODE]}_${get_arg_level}S
        _info=Voies
        _query="
            SELECT
                ARRAY_AGG(s.code),
                COUNT(1)
            FROM
                fr.bal_street s
                    JOIN fr.bal_municipality m ON m.id = s.id_municipality
            WHERE
                m.code = '${bal_vars[MUNICIPALITY_CODE]}'
                AND
                NOT EXISTS(
                    SELECT 1
                    FROM fr.${bal_vars[TABLE_NAME]}
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->'voies') s2
                    WHERE
                        s.code = s2->>'idVoie'
                )
        "
        ;;
    HOUSENUMBER)
        _label2=${bal_vars[MUNICIPALITY_CODE]}_${get_arg_level}S
        _info=Numéros
        _query="
            SELECT
                ARRAY_AGG(n.code),
                COUNT(1)
            FROM
                fr.bal_housenumber n
                    JOIN fr.bal_street s ON s.id = n.id_street
                    JOIN fr.bal_municipality m ON m.id = s.id_municipality
            WHERE
                m.code = '${bal_vars[MUNICIPALITY_CODE]}'
                AND
                NOT EXISTS(
                    SELECT 1
                    FROM fr.${bal_vars[TABLE_NAME]}
                        CROSS JOIN JSON_ARRAY_ELEMENTS(data->'numeros') n2
                    WHERE
                        n.code = n2->>'id'
                )
        "
        ;;
    esac &&
    bal_get_list \
        --name "BAL_${_label1}_OBSOLETE_${_label2}" \
        --query "$_query" \
        --as_string _obsolete &&
    {
        [ -z "$_obsolete" ] || {
            _label1=DELETE
            log_info "liste ${_info} obsolètes: ($_obsolete)" &&
            execute_query \
                --name "BAL_${_label1}_OBSOLETE_${_label2}" \
                --query "
                    SELECT counters FROM fr.bal_delete_obsolete_addresses(
                        list => '$_obsolete'
                    )
                " \
                --psql_arguments 'tuples-only:pset=format=unaligned' \
                --return _counters &&
            log_info "comptage: ${_counters}"
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# select municipalities (w/ criteria & order) from summary
bal_list_municipalities() {
    bash_args \
        --args_p '
            list:Liste résultat
        ' \
        --args_o '
            list
        ' \
        "$@" || return $ERROR_CODE

    local -n _list_ref=$get_arg_list
    local _query _list _date_before_fix

    case "${bal_vars[FIX]:-${bal_vars[SELECT_CRITERIA]}}" in
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
    SPACE_IN_CODE)
        _date_before_fix='2025-01-01'
        _query="
            SELECT
                m.code municipality,
                m.code criteria
            FROM
                fr.bal_municipality m
            WHERE
                EXISTS(
                    SELECT 1
                    FROM
                        fr.bal_street s
                            JOIN fr.bal_housenumber n ON n.id_street = s.id
                    WHERE
                        s.id_municipality = m.id
                        AND
                        POSITION(' ' IN n.code) > 0
                )
        "
        ;;
    CONVERT_ATTRIBUTES)
        _date_before_fix='2025-01-01'
        _query="
            SELECT
                SUBSTR(l.name, 5) municipality,
                SUBSTR(l.name, 5) criteria
            FROM
                io_history io
                    JOIN get_last_io(io.name) l ON io.id = l.id
            WHERE
                io.name ~ '^BAL_[0-9]'
                AND
                l.attributes ~ '"'"'"STREETS"'"'" => [0-9]*, "'"'"HOUSENUMBERS_AUTH"'"'" => [0-9]*'
        "
        ;;
    esac &&
    _query="
        WITH
        history AS (
            SELECT
                SUBSTR(l.name, 5) municipality,
                l.date_data_end,
                l.attributes
            FROM
                io_history io
                    JOIN get_last_io(io.name) l ON io.id = l.id
            WHERE
                io.name ~ '^BAL_[0-9]'
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
    "
    [ -n "${bal_vars[FIX]}" ] && {
        _query+="
                h.date_data_end IS NOT NULL
                AND
                h.date_data_end < '$_date_before_fix'::DATE
                AND
                POSITION('${bal_vars[FIX]}' IN h.attributes) = 0
        "
    } || {
        _query+="
                h.date_data_end IS NULL
                OR
                m.last_update > h.date_data_end
        "
    }
    _query+="
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

# context BAL data
bal_context() {
    local -A _opts &&
    pow_argv \
        --args_n '
            level:Niveau Adresses;
            vars:Ensemble des variables
        ' \
        --args_m '
            level;vars
        ' \
        --args_v '
            level:SUMMARY|MUNICIPALITY|STREET|HOUSENUMBER
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -n _vars_ref=${_opts[VARS]}

    case "${_opts[LEVEL]}" in
    SUMMARY)
        _vars_ref[URL_DATA]='api/communes-summary.csv' &&
        _vars_ref[NEXT_LEVEL]=MUNICIPALITY &&
        # no download if present summary is max 3 days old
        _vars_ref[OVERWRITE_KEY]=TIME &&
        _vars_ref[OVERWRITE_VALUE]=$((3*24*60*60)) &&
        # no option for CSV summary
        _vars_ref[IMPORT_OPTIONS]= &&
        # vacuum list
        _vars_ref[VACUUM]=bal_municipality
        ;;
    MUNICIPALITY)
        _vars_ref[URL_DATA]='lookup/'${bal_vars[MUNICIPALITY_CODE]} &&
        _vars_ref[NEXT_LEVEL]=STREET &&
        # no download if JSON file newer than municipality's last_update
        _vars_ref[OVERWRITE_KEY]=DATE &&
        # get last update (as epoch)
        bal_last_update_municipality &&
        _vars_ref[OVERWRITE_VALUE]=${bal_vars[IO_END_EPOCH]} &&
        # table w/ 1 column named 'data' to import JSON stream
        _vars_ref[IMPORT_OPTIONS]="--import_options column_name=data" &&
        # vacuum list
        _vars_ref[VACUUM]=bal_street,bal_housenumber
        ;;
    STREET)
        _vars_ref[NEXT_LEVEL]=HOUSENUMBER
        ;;
    HOUSENUMBER)
        _vars_ref[NEXT_LEVEL]=AREA
        ;;
    esac || return $ERROR_CODE

    return $SUCCESS_CODE
}

# integration BAL data
bal_integration() {
    bash_args \
        --args_p '
            level:Niveau Adresses
        ' \
        --args_o '
            level
        ' \
        --args_v '
            level:MUNICIPALITY|STREET|HOUSENUMBER|AREA
        ' \
        "$@" || return $ERROR_CODE

    local _elapsed

    case "${get_arg_level}" in
    MUNICIPALITY)
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
                    fr.${bal_vars[TABLE_NAME]}
                ON CONFLICT(code) DO UPDATE SET
                    name = EXCLUDED.name,
                    population = EXCLUDED.population,
                    areas = EXCLUDED.areas,
                    streets = EXCLUDED.streets,
                    housenumbers = EXCLUDED.housenumbers,
                    housenumbers_auth = EXCLUDED.housenumbers_auth,
                    last_update = EXCLUDED.last_update
            " &&
        bal_deal_obsolescence --level MUNICIPALITY
        ;;
    STREET)
        # insert/update streets, and delete old ones (obsolete)
        execute_query \
            --name "BAL_INTEGRATION_${bal_vars[MUNICIPALITY_CODE]}_STREETS" \
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
                    fr.${bal_vars[TABLE_NAME]}
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
        bal_deal_obsolescence --level STREET &&
        {
            (! is_yes --var bal_vars[PROGRESS]) || {
                get_elapsed_time --start ${bal_vars[PROGRESS_START]} --result _elapsed &&
                bal_progress_bar END "${_elapsed}" &&
                bal_vars[PROGRESS_START]=$(date '+%s')
            }
        } &&
        bal_load --level STREET
        ;;
    HOUSENUMBER)
        # insert/update housenumbers, and delete old ones (obsolete)
        # update street's geometry
        execute_query \
            --name "BAL_INTEGRATION_${bal_vars[MUNICIPALITY_CODE]}_HOUSENUMBERS" \
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
                    fr.${bal_vars[TABLE_NAME]}
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
                        fr.${bal_vars[TABLE_NAME]}
                            JOIN fr.bal_street s ON s.code = data->>'idVoie'
                )
                UPDATE fr.bal_street s SET
                    geom = g.geom
                    FROM street_geometry g
                    WHERE
                        s.id = g.id
                ;
            " &&
        bal_deal_obsolescence --level HOUSENUMBER &&
        {
            (! is_yes --var bal_vars[PROGRESS]) || {
                get_elapsed_time --start ${bal_vars[PROGRESS_START]} --result _elapsed &&
                bal_progress_bar END "${_elapsed}" &&
                bal_vars[PROGRESS_START]=$(date '+%s')
            }
        } &&
        # need to request API on each housenumber, if many areas! to obtain old municipality
        {
            [[ ${bal_vars[WITH_OLD_AREA]} == 0 ]] || {
                bal_load --level HOUSENUMBER
            }
        }
        ;;
    AREA)
        execute_query \
            --name "BAL_INTEGRATION_${bal_vars[MUNICIPALITY_CODE]}_HOUSENUMBERS_AREA" \
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
                        fr.${bal_vars[TABLE_NAME]}
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
            (! is_yes --var bal_vars[PROGRESS]) || {
                get_elapsed_time --start ${bal_vars[PROGRESS_START]} --result _elapsed &&
                bal_progress_bar END "${_elapsed}"
            }
        }
        ;;
    esac || return $ERROR_CODE

    return $SUCCESS_CODE
}

# load BAL data
bal_load() {
    bash_args \
        --args_p '
            level:Niveau Adresses
        ' \
        --args_o '
            level
        ' \
        --args_v '
            level:SUMMARY|MUNICIPALITY|STREET|HOUSENUMBER
        ' \
        "$@" || return $ERROR_CODE

    local _level=${get_arg_level} _elapsed _info _file _rc
    local -A _context

    case "$_level" in
    SUMMARY)
        bal_vars[IO_NAME]=BAL_SUMMARY
        ;;
    MUNICIPALITY)
        bal_vars[IO_NAME]=BAL_${bal_vars[MUNICIPALITY_CODE]}
        ;;
    esac
    # initialize context
    bal_context --level $_level --vars _context || return $ERROR_CODE

    case "$_level" in
    SUMMARY|MUNICIPALITY)
        io_todo_import \
            --force ${bal_vars[FORCE]} \
            --io ${bal_vars[IO_NAME]} \
            --date_end "${bal_vars[IO_END]}" \
            --id bal_vars[IO_ID]
        case $? in
        $POW_IO_SUCCESSFUL)                                 return $SUCCESS_CODE    ;;
        $POW_IO_IN_PROGRESS|$POW_IO_ERROR|$ERROR_CODE)      return $ERROR_CODE      ;;
        esac

        # take all following 'BAL_' as info (SUMMARY or municipality code)
        _info=${bal_vars[IO_NAME]#*_}
        log_info "Import BAL (${_info})" &&
        {
            (! is_yes --var bal_vars[PROGRESS]) || {
                bal_vars[PROGRESS_START]=$(date '+%s') &&
                bal_progress_bar \
                    BEGIN \
                    "INSEE ${_info}" \
                    ${bal_vars[PROGRESS_SIZE]} \
                    ${bal_vars[PROGRESS_CURRENT]} \
                    ${bal_vars[PROGRESS_TOTAL]} \
                    '\r'
            }
        } &&
        {
            bal_import_table --command CREATE &&
            _file=$(basename "${bal_vars[URL]}/${_context[URL_DATA]}") &&
            bal_vars[FILE_NAME]="$_file" &&
            {
                [ "${_level}" = SUMMARY ] || {
                    bal_vars[FILE_NAME]+=.json &&
                    execute_query \
                        --name BAL_IO_BEGIN \
                        --query "SELECT (get_last_io('${bal_vars[IO_NAME]}')).date_data_end" \
                        --psql_arguments 'tuples-only:pset=format=unaligned' \
                        --return bal_vars[IO_BEGIN]
                }
            }
        } &&
        {
            # reset BEGIN if equal (force running w/o change of END date)
            [ "${bal_vars[IO_BEGIN]}" != "${bal_vars[IO_END]}" ] || bal_vars[IO_BEGIN]=
        } &&
        {
            # rows already defined ?
            [ "${_level}" = MUNICIPALITY ] || {
                execute_query \
                    --name BAL_IO_ROWS \
                    --query "
                        SELECT COUNT(DISTINCT co_insee_commune)
                        FROM fr.laposte_address_area
                        WHERE fl_active
                    " \
                    --psql_arguments 'tuples-only:pset=format=unaligned' \
                    --return bal_vars[IO_ROWS]
            }
        } &&
        io_history_begin \
            --io "${bal_vars[IO_NAME]}" \
            --date_begin "${bal_vars[IO_BEGIN]:-1970-01-01}" \
            --date_end "${bal_vars[IO_END]}" \
            --nrows_todo ${bal_vars[IO_ROWS]:-0} \
            --id bal_vars[IO_ID] &&
        {
            io_download_file \
                --url "${bal_vars[URL]}/${_context[URL_DATA]}" \
                --overwrite_mode NEWER \
                --overwrite_key ${_context[OVERWRITE_KEY]} \
                --overwrite_value ${_context[OVERWRITE_VALUE]} \
                --common_subdir bal \
                --output_directory "$POW_DIR_IMPORT" \
                --output_file "${bal_vars[FILE_NAME]}"
            _rc=$?
            [[ $_rc -lt $POW_DOWNLOAD_ERROR ]] && {
                # same data has to be loaded again ?
                ([ "${bal_vars[FORCE_LOAD]}" = no ] && [[ $_rc -eq $POW_DOWNLOAD_ALREADY_AVAILABLE ]]) || {
                    import_file \
                        --file_path "$POW_DIR_IMPORT/${bal_vars[FILE_NAME]}" \
                        --table_name ${bal_vars[TABLE_NAME]} \
                        --load_mode OVERWRITE_DATA \
                        ${_context[IMPORT_OPTIONS]} &&
                    bal_integration --level ${_context[NEXT_LEVEL]}
                }
            } &&
            bal_import_table --command DROP &&
            case "${_level}" in
            SUMMARY)
                {
                    (! is_yes --var bal_vars[PROGRESS]) || {
                        get_elapsed_time --start ${bal_vars[PROGRESS_START]} --result _elapsed &&
                        bal_progress_bar END "${_elapsed}"
                    }
                } &&
                io_history_end_ok \
                    --nrows_processed '(SELECT COUNT(*) FROM fr.bal_municipality)' \
                    --id ${bal_vars[IO_ID]}
                ;;
            MUNICIPALITY)
                {
                    # case when counters aren't initialized (specially housenumbers)
                    bal_count_addresses &&
                    bal_get_counters --usage NROWS --value bal_vars[NROWS_PROCESSED] &&
                    bal_get_counters --usage ATTRIBUTES --value bal_vars[ATTRIBUTES]
                } &&
                # total can be less than waited (only streets w/ certified housenumbers)
                io_history_end_ok \
                    --nrows_processed ${bal_vars[NROWS_PROCESSED]} \
                    --infos "${bal_vars[ATTRIBUTES]}" \
                    --id ${bal_vars[IO_ID]}
                ;;
            esac &&
            vacuum \
                --schema_name fr \
                --table_name "${_context[VACUUM]}" \
                --mode ANALYZE
        }
        ;;
    STREET)
        # select streets w/ certified housenumbers
        bal_load_addresses --level STREET &&
        {
            # at least one street w/ auth housenumbers ?
            [[ ${bal_vars[STREETS]} == 0 ]] || {
                bal_integration --level HOUSENUMBER
            }
        }
        ;;
    HOUSENUMBER)
        # select housenumbers
        bal_load_addresses --level HOUSENUMBER &&
        bal_integration --level AREA
        ;;
    esac || return $ERROR_CODE

    return $SUCCESS_CODE
}

# fix already exists (not todo)
bal_fix_exists() {
    bash_args \
        --args_p '
            state:Correctif réalisé (o|n)
        ' \
        --args_o '
            state
        ' \
        "$@" || return $ERROR_CODE

    local _io=BAL_${bal_vars[MUNICIPALITY_CODE]} _fix _tmpfile
    local -n _state_ref=$get_arg_state

    execute_query \
        --name BAL_IO_ID \
        --query "SELECT (get_last_io('$_io')).id" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return bal_vars[IO_ID] &&
    # use a file as argument (because of inside *)
    # CONVERT_ATTRIBUTES will be always false, but selection of municipalities too !
    get_tmp_file --tmpfile _tmpfile &&
    cat <<EOC > $_tmpfile &&
    SELECT
        (JSONB_PATH_QUERY(
            io.attributes::JSONB,
            '$ ? (@.integration.fixes[*].name == "${bal_vars[FIX]}")'
        ))->'integration'->'fixes' ->> 0
    FROM
        get_last_io('$_io') io
    WHERE
        io.attributes IS JSON OBJECT
EOC
    execute_query \
        --name BAL_FIX_EXISTS \
        --query "$_tmpfile" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _fix &&
    {
        _state_ref=$( [ -n "$_fix" ] && echo 'yes' || echo 'no' )
    } || return $ERROR_CODE

    [ "$_state_ref" = yes ] &&
    [ "${bal_vars[FORCE]}" = no ] &&
    log_info "Le correctif ${bal_vars[FIX]} a déjà été appliqué avec succès"

    rm $_tmpfile
    return $SUCCESS_CODE
}

# fix problems
bal_fix_apply() {
    local _exists

    bal_fix_exists --state _exists &&
    {
        ([ "${bal_vars[FORCE]}" = no ] && is_yes --var _exists) || {
            {
                (! is_yes --var bal_vars[PROGRESS]) || {
                    bal_vars[PROGRESS_START]=$(date '+%s') &&
                    bal_progress_bar \
                        BEGIN \
                        "INSEE ${bal_vars[MUNICIPALITY_CODE]}" \
                        ${bal_vars[PROGRESS_SIZE]} \
                        ${bal_vars[PROGRESS_CURRENT]} \
                        ${bal_vars[PROGRESS_TOTAL]} \
                        '\n'
                }
            } &&
            case "${bal_vars[FIX]}" in
            # some housenumber's codes have space!
            # for these, list returned by PostgreSQL (bal_get_list) contains code between quotes
            # these quotes are now deleted, and don't worry download...
            SPACE_IN_CODE)
                bal_import_table --command CREATE &&
                bal_last_update_municipality &&
                bal_load --level HOUSENUMBER &&
                bal_import_table --command DROP &&
                io_history_update \
                    --id ${bal_vars[IO_ID]} \
                    --infos '{"integration":{"fixes":[{"name":"SPACE_IN_CODE", "housenumbers":'${bal_vars[HOUSENUMBERS]}'}]}}'
                ;;
            # no log into history, but no problem due to empty selection of municipalities
            CONVERT_ATTRIBUTES)
                execute_query \
                    --name BAL_FIX_ATTRIBUTES \
                    --query "
                        WITH
                        addresses AS (
                            SELECT
                                id,
                                (attributes::HSTORE->'STREETS')::INT streets,
                                (attributes::HSTORE->'HOUSENUMBERS_AUTH')::INT housenumbers
                            FROM
                                io_history
                            WHERE
                                id = ${bal_vars[IO_ID]}
                        )
                        UPDATE io_history io SET
                        attributes = CONCAT(
                            '{"'"'"integration"'"'":{"'"'"streets"'"'":',
                            a.streets,
                            ',"'"'"housenumbers"'"'":',
                            a.housenumbers,
                            '}}'
                        )
                        FROM addresses a
                        WHERE
                            io.id = a.id
                    "
                ;;
            esac
        }
    } || return $ERROR_CODE

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
        force_load:Forcer le chargement même si celui-ci a déjà été fait;
        fix:Corriger une erreur;
        levels:Ensemble des niveaux Adresse à traiter;
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
        force_load:yes|no;
        fix:NONE|SPACE_IN_CODE|CONVERT_ATTRIBUTES;
        dry_run:yes|no;
        progress:yes|no;
        clean:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        select_criteria:REVISION;
        select_order:DESC;
        force:no;
        force_load:yes;
        fix:NONE;
        levels:ALL;
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
    [WITH_OLD_AREA]=0
    [URL]='https://plateforme.adresse.data.gouv.fr'
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
    [STREETS]=-1
    [HOUSENUMBERS]=-1
    [STOP_TIME]=$get_arg_stop_time
    [FORCE]=$get_arg_force
        # same data (POW_DOWNLOAD_ALREADY_AVAILABLE)
        # nothing todo (already downloaded and so imported) ? but problem (if not) !
        #  obsolescence (diff between level-table and json-table) can wrongly delete elements
        #  if there are not loaded
    [FORCE_LOAD]=$get_arg_force_load
    [FIX]=$get_arg_fix
    [DRY_RUN]=$get_arg_dry_run
    [CLEAN]=$get_arg_clean
    [PROGRESS]=$get_arg_progress
    [PROGRESS_START]=
    [PROGRESS_CURRENT]=1
    [PROGRESS_TOTAL]=1
    [PROGRESS_SIZE]=5
    [VERBOSE]=$get_arg_verbose
    [LEVELS]=$get_arg_levels
)
declare -a bal_codes=()
[ "${bal_vars[FIX]}" = NONE ] && bal_vars[FIX]=
[ "${bal_vars[LEVELS]}" = ALL ] && bal_vars[LEVELS]=MUNICIPALITY,STREET,HOUSENUMBER

set_env --schema_name fr &&
{
    (! is_yes --var bal_vars[PROGRESS]) || set_log_echo no
} &&
{
    [ "${bal_vars[MUNICIPALITY_CODE]}" != ALL ] && {
        bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
    } || {
        bal_load --level SUMMARY &&
        bal_list_municipalities --list bal_codes &&
        {
            (! is_yes --var bal_vars[CLEAN]) || {
                [ -z "${bal_vars[FILE_NAME]}" ] || {
                    rm --force "$POW_DIR_IMPORT/${bal_vars[FILE_NAME]}"
                }
            }
        }
    }
} || on_import_error

bal_error=0
bal_vars[PROGRESS_TOTAL]=${#bal_codes[@]}
[[ ${bal_vars[PROGRESS_TOTAL]} > 0 ]] &&
is_yes --var bal_vars[DRY_RUN] && {
    bal_average_time --avg bal_average &&
    bal_progress_bar BEGIN Communes 0 0 1 '\r' &&
    bal_progress_bar END Estimation Adresses
}
for ((bal_i=0; bal_i<${#bal_codes[@]}; bal_i++)); do
    # check municipality code
    bal_check_municipality --code "${bal_codes[$bal_i]}" || {
        bal_error=1
        continue
    }
    bal_vars[MUNICIPALITY_CODE]=${bal_codes[$bal_i]}
    # progress bar
    bal_vars[PROGRESS_CURRENT]=$((bal_i +1))
    # do it ?
    is_yes --var bal_vars[DRY_RUN] && {
        {
            bal_progress_bar BEGIN "INSEE ${bal_vars[MUNICIPALITY_CODE]}" 0 0 1 '\r' &&
            ([ -n "$bal_average" ] && [[ $bal_average > 0 ]]) && {
                case "${bal_vars[FIX]}" in
                SPACE_IN_CODE)
                    bal_query="
                        SELECT
                            COUNT(1)
                        FROM
                            fr.bal_housenumber n
                                JOIN fr.bal_street s ON n.id_street = s.id
                                JOIN fr.bal_municipality m ON s.id_municipality = m.id
                        WHERE
                            POSITION(' ' IN n.code) > 0
                            AND
                            m.code = '${bal_vars[MUNICIPALITY_CODE]}'
                    "
                    ;;
                CONVERT_ATTRIBUTES)
                    bal_query="
                        SELECT
                            (io.attributes::HSTORE->'STREETS')::INT +
                            (io.attributes::HSTORE->'HOUSENUMBERS_AUTH')::INT
                        FROM
                            io_history io
                                JOIN get_last_io(io.name) l ON io.id = l.id
                        WHERE
                            io.name = CONCAT('BAL_', '${bal_vars[MUNICIPALITY_CODE]}')
                    "
                    ;;
                # no fix (next municipality todo) w/ housenumbers (if exists old municipality)
                *)
                    bal_query=
                    bal_rows=${bal_vars[IO_ROWS]}
                    ;;
                esac &&
                {
                    [ -z "$bal_query" ] || {
                        execute_query \
                            --name BAL_${bal_vars[MUNICIPALITY_CODE]}_ROWS \
                            --query "$bal_query" \
                            --psql_arguments 'tuples-only:pset=format=unaligned' \
                            --return bal_rows
                    }
                } &&
                _start=$(echo "$(date '+%s') - (${bal_rows}*${bal_average})" | bc -l) &&
                # remove decimal part
                get_elapsed_time --start ${_start%.*} --result _elapsed &&
                bal_progress_bar END "${_elapsed}" "#${bal_rows}"

            } || {
                bal_progress_bar END 'Non disponible'
            }
        } || true
    } || {
        {
            [ -n "${bal_vars[FIX]}" ] && {
                # don't raise error to not loss history (specially exec time, attributes)
                bal_fix_apply || {
                    log_error "consulter dossier $POW_DIR_ARCHIVE !"
                    true
                }
            } || bal_load --level MUNICIPALITY
        } || on_import_error
    }
    # purge ?
    is_yes --var bal_vars[CLEAN] && find $POW_DIR_IMPORT -name "${bal_vars[MUNICIPALITY_CODE]}*.json" -exec rm {} \;
done

_rc=$(( bal_error == 1 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
