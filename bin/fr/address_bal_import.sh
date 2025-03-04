#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BAL addresses: summary (municipality), street and only certified housenumber
    #  summary : all municipalities
    #  municipality : municipality and its streets
    #  street : street ans its housenumbers
    #  housenumber : housenumber details (some are not present into street stream!)

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

source $POW_DIR_ROOT/lib/libbal.sh || exit $ERROR_CODE

on_break() {
    log_error 'arrêt utilisateur' &&
    on_import_error
}

on_import_error() {
    local _info=$( [ -n "${bal_vars[FIX]}" ] && echo ${bal_vars[FIX]} || echo ${bal_vars[IO_NAME]#*_} )

    # IO created?
    [ "$POW_DEBUG" = yes ] && { echo "io_history_id=${bal_vars[IO_ID]}"; }

    [ "${bal_vars[DRY_RUN]}" = no ] &&
    [ -n "${bal_vars[IO_ID]}" ] &&
    io_history_end_ko --id ${bal_vars[IO_ID]}

    log_error "Erreur import BAL ($_info)"
    exit $ERROR_CODE
}

# deal w/ interrupt signal (CTRL-C, kill)
trap on_break SIGINT

# prepare levels
bal_set_levels() {
    local _level _tmp _key

    for _level in M S N; do
        case $_level in
        M)  _key=LEVEL_MUNICIPALITY     ;;
        S)  _key=LEVEL_STREET           ;;
        N)  _key=LEVEL_HOUSENUMBER      ;;
        esac
        _tmp=$(expr index "${bal_vars[LEVELS]}" $_level)
        [ $_tmp -eq 0 ] && bal_vars[$_key]=no || bal_vars[$_key]=yes
    done

    return $SUCCESS_CODE
}

# eval total of rows
bal_set_rows() {
    local -A _opts &&
    pow_argv \
        --args_n '
            streets:Nombre de voie(s);
            housenumbers:Nombre de numéro(s);
            total:Total
        ' \
        --args_m '
            streets;housenumbers;total
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -n _total_ref=${_opts[TOTAL]}

    _total_ref=0
    (is_yes --var bal_vars[LEVEL_STREET]) && {
        _total_ref=$((_total_ref + _opts[STREETS]))
    }
    (is_yes --var bal_vars[LEVEL_HOUSENUMBER]) &&
    [[ ${bal_vars[AREAS_OLD_MUNICIPALITY]} -gt 0 ]] && {
        _total_ref=$((_total_ref + _opts[HOUSENUMBERS]))
    }

    return $SUCCESS_CODE
}

# deal w/ counters (STREET & HOUSENUMBER) according usage
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
        bal_set_rows \
            --streets ${bal_vars[STREETS]} \
            --housenumbers ${bal_vars[HOUSENUMBERS]} \
            --total _value_ref
        ;;
    ATTRIBUTES)
        _value_ref='{"integration":{"areas":'${bal_vars[AREAS_OLD_MUNICIPALITY]}',"streets":'${bal_vars[STREETS]}',"housenumbers":'${bal_vars[HOUSENUMBERS]}',"levels":"'${bal_vars[LEVELS]}'"}}'
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
                --return bal_vars[HOUSENUMBERS]
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# import downloaded file (dealing w/ some error)
bal_import_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            option:Option de chargement du fichier;
            source:Dossier source du fichier;
            mode:Mode de chargement du fichier
        ' \
        --args_m '
            mode;source
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _try _ext _rc _file="${_opts[SOURCE]}/${bal_vars[FILE_NAME]}" _tmpfile

    for ((_try=0; _try<2; _try++)); do
        case $_try in
        0)
            # normal case (no error yet)
            # null command, to fill this case
            :
            ;;
        1)
            # error: double quote inside value ?
            _ext=$(get_file_extension --file_path "$_file")
            [ "$_ext" != json ] && return $ERROR_CODE
            get_tmp_file --tmpfile _tmpfile --tmpext json
            grep --perl-regexp ':"[^"]*"[^"]+"[^"]*",?' $_file > /dev/null
            # no : other error (not catched yet)
            [[ $? -eq 0 ]] || return $ERROR_CODE
            # need to protect \"
            # https://stackoverflow.com/questions/15637429/how-to-escape-double-quotes-in-json
            sed --expression 's/\\"/\\\\\\"/g' < $_file > $_tmpfile
            _file=$_tmpfile
            log_info "Chargement (${bal_vars[FILE_NAME]}) : double apostrophe"
            ;;
        *)
            # error: other, not catched
            [ -n "$_tmpfile" ] && rm $_tmpfile
            log_error "Chargement (${bal_vars[FILE_NAME]}) : erreur non gérée!"
            return $ERROR_CODE
            ;;
        esac

        import_file \
            --file_path "$_file" \
            --table_name ${bal_vars[TABLE_NAME]} \
            ${_opts[OPTION]} \
            --load_mode ${_opts[MODE]}
        [[ $? -eq 0 ]] && break
    done
    [ -n "$_tmpfile" ] && rm $_tmpfile

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
            # load addresses as JSON in table (has to be empty!)
            execute_query \
                --name BAL_TRUNCATE \
                --query "TRUNCATE TABLE fr.${bal_vars[TABLE_NAME]}" &&
            {
                if [ "${bal_vars[PARALLEL]}" = no ]; then
                    for ((_j=0; _j<${#_addresses[@]}; _j++)); do
                        {
                            [ "${bal_vars[PROGRESS]}" = no ] || {
                                bal_print_progress \
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
                                    bal_import_file \
                                        --mode APPEND \
                                        --source "$POW_DIR_IMPORT" \
                                        --option '\--import_options column_name=data'
                                }
                            }
                        }
                    done
                else
                    # download
                    #+ need to (1) eventually unquote code, and (2) replace space by %20
                    #+ so 2 linked inputs
                    parallel --jobs 5 \
                        wget --quiet --limit-rate=100k \
                        --output-document="$POW_DIR_COMMON_GLOBAL/fr/bal/{=1 uq() =}.json" \
                        https://plateforme.adresse.data.gouv.fr/lookup/'{=2 uq() ; s/ /%20/g =}' \
                        ::: "${_addresses[@]}" :::+ "${_addresses[@]}" &&
                    # load into db
                    #+ here unquote code only is needed
                    parallel --jobs 5 \
                        jq --raw-output --compact-output '.' \
                        "$POW_DIR_COMMON_GLOBAL/fr/bal/{=1 uq() =}.json" ::: "${_addresses[@]}" |
                        sed --expression 's/\\"/\\\\\\"/g' |
                        execute_query --name LOAD_JSON --query 'COPY fr.'${bal_vars[TABLE_NAME]}'(data) FROM STDIN'
                fi
            }
        } || {
            [ "${bal_vars[PROGRESS]}" = no ] || {
                echo 'aucune voie avec numéros certifiés'
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
    MORE_ATTRIBUTES)
        _date_before_fix='2025-02-08'
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
                io.attributes IS JSON OBJECT
                AND
                (io.attributes::JSONB)->'integration'->>'levels' IS NULL
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
        # protect (\) to be not interpret as an option, but a value
        _vars_ref[IMPORT_OPTIONS]='\--import_options column_name=data' &&
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
        {
            [ -z "${bal_vars[IO_LAST_ID]}" ] || bal_deal_obsolescence --level STREET
        } &&
        {
            [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
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
        {
            [ -z "${bal_vars[IO_LAST_ID]}" ] || bal_deal_obsolescence --level HOUSENUMBER
        } &&
        {
            [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
        } &&
        # need to request API on each housenumber, if many areas! to obtain old municipality
        {
            [[ ${bal_vars[AREAS_OLD_MUNICIPALITY]} == 0 ]] || {
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
            [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
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

    local _level=${get_arg_level} _file _rc _option _force="${bal_vars[FORCE_LOAD]}"
    local -A _context

    case "$_level" in
    SUMMARY)
        # don't need to load again
        _force=no
        bal_vars[IO_NAME]=BAL_SUMMARY
        [ "${bal_vars[STOP_TIME]}" = 0 ] || {
            log_info "Durée de traitement allouée jusqu'à ${bal_vars[STOP_TIME]}"
        }
        log_info "Import BAL (${_level})" &&
        {
            [ "${bal_vars[PROGRESS]}" = no ] || {
                bal_vars[PROGRESS_START]=$(date '+%s') &&
                bal_print_progress \
                    BEGIN \
                    "INSEE ${_level}" \
                    ${bal_vars[PROGRESS_SIZE]} \
                    ${bal_vars[PROGRESS_CURRENT]} \
                    ${bal_vars[PROGRESS_TOTAL]} \
                    '\r'
            }
        }
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
            --date_end "${bal_vars[IO_END]}"
        case $? in
        $POW_IO_SUCCESSFUL)                                 return $SUCCESS_CODE    ;;
        $POW_IO_IN_PROGRESS|$POW_IO_ERROR|$ERROR_CODE)      return $ERROR_CODE      ;;
        esac

        {
            bal_import_table --command CREATE &&
            _file=$(basename "${bal_vars[URL]}/${_context[URL_DATA]}") &&
            bal_vars[FILE_NAME]="$_file" &&
            {
                [ "${_level}" = SUMMARY ] || {
                    bal_vars[FILE_NAME]+=.json
                    bal_vars[IO_BEGIN]=${bal_vars[IO_LAST_END]}
                }
            }
        } &&
        {
            # reset BEGIN if equal (force running w/o change of END date)
            [ "${bal_vars[IO_BEGIN]}" != "${bal_vars[IO_END]}" ] || bal_vars[IO_BEGIN]=
        } &&
        {
            [ "${_level}" = MUNICIPALITY ] || {
                # summary
                execute_query \
                    --name BAL_IO_ROWS \
                    --query "
                        SELECT COUNT(DISTINCT co_insee_commune)
                        FROM fr.laposte_address_area
                        WHERE fl_active
                    " \
                    --return bal_vars[IO_ROWS]
            }
        } &&
        io_history_begin \
            --io "${bal_vars[IO_NAME]}" \
            --date_begin "${bal_vars[IO_BEGIN]:-1970-01-01}" \
            --date_end "${bal_vars[IO_END]}" \
            --nrows_todo ${bal_vars[IO_ROWS]} \
            --id bal_vars[IO_ID] &&
        {
            {
                if ([ "${_level}" = SUMMARY ] || (is_yes --var bal_vars[LEVEL_MUNICIPALITY])); then
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
                        ([ "$_force" = no ] && [[ $_rc -eq $POW_DOWNLOAD_ALREADY_AVAILABLE ]]) || {
                            [ -n "${_context[IMPORT_OPTIONS]}" ] && _option="--option ${_context[IMPORT_OPTIONS]}"
                            bal_import_file \
                                --mode OVERWRITE_DATA \
                                --source "$POW_DIR_IMPORT" \
                                $_option &&
                            bal_integration --level ${_context[NEXT_LEVEL]}
                        }
                    }
                else
                    bal_load --level ${_context[NEXT_LEVEL]}
                fi
            } &&
            bal_import_table --command DROP &&
            case "${_level}" in
            SUMMARY)
                {
                    [ "${bal_vars[PROGRESS]}" = no ] || bal_set_progress
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
            {
                # vacuum only for last municipality (or summary)
                ([ "${_level}" = MUNICIPALITY ] &&
                [[ ${bal_vars[PROGRESS_CURRENT]} -lt ${bal_vars[PROGRESS_TOTAL]} ]]) || {
                    vacuum \
                        --schema_name fr \
                        --table_name "${_context[VACUUM]}" \
                        --mode ANALYZE
                }
            }
        }
        ;;
    STREET)
        if (is_yes --var bal_vars[LEVEL_STREET]); then
            # select streets w/ certified housenumbers
            bal_load_addresses --level STREET &&
            {
                # at least one street w/ auth housenumbers ?
                [[ ${bal_vars[STREETS]} == 0 ]] || {
                    bal_integration --level ${_context[NEXT_LEVEL]}
                }
            }
        else
            bal_load --level ${_context[NEXT_LEVEL]}
        fi
        ;;
    HOUSENUMBER)
        if (is_yes --var bal_vars[LEVEL_HOUSENUMBER]); then
            # select housenumbers
            bal_load_addresses --level HOUSENUMBER &&
            bal_integration --level ${_context[NEXT_LEVEL]}
        fi
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
                [ "${bal_vars[PROGRESS]}" = no ] || {
                    bal_vars[PROGRESS_START]=$(date '+%s') &&
                    bal_print_progress \
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
                    --name BAL_${bal_vars[FIX]} \
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
            MORE_ATTRIBUTES)
                io_history_update \
                    --infos '{"integration":{"levels":"'${bal_vars[LEVELS]}'","areas":'${bal_vars[AREAS_OLD_MUNICIPALITY]}'}}' \
                    --id ${bal_vars[IO_ID]}
                ;;
            esac
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# allow runing 3 hours
# --stop_time "$(date -d 'today + 3 hours' +'%m-%d-%T')"

bash_args \
    --args_p '
        municipality:Code Commune INSEE à traiter (ou ALL pour télécharger la liste complète);
        select_criteria:Sélection des Communes;
        select_order:Ordre de sélection des Communes;
        limit:Limiter à n communes (0 sans limite);
        stop_time:Temps d arrêt du traitement (format: MM-jj-hh:mm:ss);
        force:Forcer le traitement même si celui-ci a déjà été fait;
        force_load:Forcer le chargement même si celui-ci a déjà été fait;
        fix:Corriger une erreur;
        levels:Ensemble des niveaux Adresse à traiter;
        dry_run:Simuler le traitement;
        progress:Afficher le ratio de progression;
        parallel:Obtenir les addresses en parallèle;
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
        fix:NONE|SPACE_IN_CODE|CONVERT_ATTRIBUTES|MORE_ATTRIBUTES;
        levels:MSN|MS|N;
        dry_run:yes|no;
        progress:yes|no;
        parallel:yes|no;
        clean:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        select_criteria:REVISION;
        select_order:DESC;
        force:no;
        force_load:yes;
        fix:NONE;
        levels:MS;
        dry_run:no;
        limit:3;
        stop_time:0;
        progress:no;
        parallel:yes;
        clean:yes;
        verbose:no
    ' \
    "$@" || exit $ERROR_CODE

declare -A bal_vars=(
    [USECASE]=IMPORT
    [MUNICIPALITY_CODE]="${get_arg_municipality^^}"
    [URL]='https://plateforme.adresse.data.gouv.fr'
    [IO_NAME]=
    [IO_ID]=
    [IO_BEGIN]=
    [IO_END]="$(date +%F)"
    [IO_END_EPOCH]=
    [IO_ROWS]=0
    [IO_LAST_ID]=
    [IO_LAST_END]=
    [IO_LAST_ATTRIBUTES]=
    [FILE_NAME]=
    [TABLE_NAME]=
    [SELECT_CRITERIA]=$get_arg_select_criteria
    [SELECT_ORDER]=$get_arg_select_order
    [LIMIT]=$get_arg_limit
    [STOP_TIME]=$get_arg_stop_time
    [AREAS_OLD_MUNICIPALITY]=0
    [STREETS]=-1
    [HOUSENUMBERS]=-1
    [FORCE]=$get_arg_force
        # same data (POW_DOWNLOAD_ALREADY_AVAILABLE)
        # nothing todo (already downloaded and so imported) ? but problem !
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
    [PARALLEL]=$get_arg_parallel
    [VERBOSE]=$get_arg_verbose
    [LEVELS]=$get_arg_levels
    [LEVEL_MUNICIPALITY]=
    [LEVEL_STREET]=
    [LEVEL_HOUSENUMBER]=
)
declare -a bal_codes=()
bal_start=$(date '+%s')
[ "${bal_vars[FIX]}" = NONE ] && bal_vars[FIX]=
# reset LIMIT if STOP_TIME
[ "${bal_vars[STOP_TIME]}" != 0 ] && [ ${bal_vars[LIMIT]} -gt 0 ] && bal_vars[LIMIT]=0
# with level(s)
bal_set_levels &&
set_env --schema_name fr &&
{
    [ "${bal_vars[PROGRESS]}" = no ] || set_log_echo no
} &&
{
    case "${bal_vars[MUNICIPALITY_CODE]}" in
    ALL)
        bal_load --level SUMMARY &&
        bal_list_municipalities --list bal_codes &&
        {
            [ "${bal_vars[CLEAN]}" = no ] || {
                [ -z "${bal_vars[FILE_NAME]}" ] || {
                    rm --force "$POW_DIR_IMPORT/${bal_vars[FILE_NAME]}"
                }
            }
        }
        ;;
    *)
        bal_check_municipality --code "${bal_vars[MUNICIPALITY_CODE]}" &&
        bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
        ;;
    esac
} || on_import_error

bal_error=0
bal_vars[PROGRESS_TOTAL]=${#bal_codes[@]}
[[ ${bal_vars[PROGRESS_TOTAL]} -gt 0 ]] &&
[ "${bal_vars[DRY_RUN]}" = yes ] && {
    bal_average_time --avg bal_average &&
    bal_print_progress BEGIN Communes 0 0 1 '\r' &&
    bal_print_progress END Estimation Adresses
}
for ((bal_i=0; bal_i<${#bal_codes[@]}; bal_i++)); do
    bal_vars[PROGRESS_CURRENT]=$((bal_i +1))
    # check municipality code, prepare properties
    bal_set_municipality --code "${bal_codes[$bal_i]}" || {
        bal_error=1
        continue
    }
    bal_vars[MUNICIPALITY_CODE]=${bal_codes[$bal_i]}
    # do it ?
    [ "${bal_vars[DRY_RUN]}" = yes ] && {
        {
            bal_print_progress BEGIN "INSEE ${bal_vars[MUNICIPALITY_CODE]}" 0 0 1 '\r' &&
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
                # no fix (next municipality todo) w/ housenumbers (if exists old municipality)
                *)
                    bal_query=
                    case "${bal_vars[FIX]}" in
                    *_ATTRIBUTES)
                        bal_rows=1
                        ;;
                    *)
                        bal_rows=${bal_vars[IO_ROWS]}
                        ;;
                    esac
                    ;;
                esac &&
                {
                    [ -z "$bal_query" ] || {
                        execute_query \
                            --name BAL_${bal_vars[MUNICIPALITY_CODE]}_ROWS \
                            --query "$bal_query" \
                            --return bal_rows
                    }
                } &&
                {
                    case "${bal_vars[FIX]}" in
                    *_ATTRIBUTES)
                        _elapsed=0h:0m:1s
                        ;;
                    *)
                        _start=$(echo "$(date '+%s') - (${bal_rows}*${bal_average})" | bc -l) &&
                        # remove decimal part
                        get_elapsed_time --start ${_start%.*} --result _elapsed
                        ;;
                    esac
                } &&
                bal_print_progress END "${_elapsed}" "#${bal_rows} (ANC=#${bal_vars[AREAS_OLD_MUNICIPALITY]})"

            } || {
                bal_print_progress END 'Non disponible'
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
    # case insensitive! (Corse 2A|2B) but codes are (2a|2b)*
    [ "${bal_vars[CLEAN]}" = yes ] && find $POW_DIR_IMPORT -iname "${bal_vars[MUNICIPALITY_CODE]}*.json" -exec rm {} \;
    # delete 0-sz
    find $POW_DIR_ARCHIVE -size 0 -exec rm {} \;

    [ "${bal_vars[STOP_TIME]}" != 0 ] && {
        # stop loop if allowed time is expired
        [[ "$(date +'%m-%d-%T')" > "${bal_vars[STOP_TIME]}" ]] && break
    }
done

[[ $bal_error -eq 0 ]] &&
[[ $bal_i -eq 0 ]] && {
    log_error "Import préalable de l'ensemble des Communes (--municipality ALL)"
    exit $ERROR_CODE
}

[ "${bal_vars[STOP_TIME]}" != 0 ] && {
    [[ ${bal_vars[PROGRESS_CURRENT]} -eq ${bal_vars[PROGRESS_TOTAL]} ]] || {
        vacuum \
            --schema_name fr \
            --table_name 'bal_street,bal_housenumber' \
            --mode ANALYZE
    }
}

[ "${bal_vars[PROGRESS_CURRENT]}" -gt 100 ] && {
    set_env --schema_name public &&
    vacuum \
        --schema_name public \
        --table_name io_history \
        --mode ANALYZE || bal_error=1
}

[ "${bal_vars[PROGRESS]}" = no ] || {
    # trick to print global timex
    bal_print_progress BEGIN "Temps Traitement" 0 0 1 '\r' &&
    get_elapsed_time --start ${bal_start} --result _elapsed &&
    bal_print_progress END "${_elapsed}"
}

_rc=$(( bal_error == 1 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
