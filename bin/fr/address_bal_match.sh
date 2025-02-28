#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match BAL addresses w/ LAPOSTE ones

bal_check_municipality() {
    local -A _opts &&
    pow_argv \
        --args_n '
            code:Code Commune
        ' \
        --args_m '
            code
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _valid _error

    execute_query \
        --name "BAL_MUNICIPALITY_${_opts[CODE]}" \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.bal_municipality
                WHERE code = '${_opts[CODE]}'
            )" \
        --return _valid &&
    {
        [ "$_valid" = t ] || {
            execute_query \
                --name "LAPOSTE_MUNICIPALITY_${_opts[CODE]}" \
                --query "
                    SELECT EXISTS(
                        SELECT 1 FROM fr.laposte_address_area
                        WHERE co_insee_commune = '${_opts[CODE]}' AND fl_active
                    )" \
                --return _valid &&
            {
                case "$_valid" in
                f)  _error="code Commune '${_opts[CODE]}' non valide!"                          ;;
                t)  _error="Import préalable de l'ensemble des Communes (--municipality ALL)"   ;;
                esac
                log_error "$_error"
                false
            }
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
        case "${bal_vars[USECASE]}" in
        # only not already downloaded or newer import available
        IMPORT)
            _query+="
                    h.date_data_end IS NULL
                    OR
                    m.last_update > h.date_data_end
            "
            ;;
        # only already downloaded
        MATCH)
            _query+="
                    h.date_data_end IS NOT NULL
            "
            ;;
        esac
    }
    _query+="
            ORDER BY
                c.criteria ${bal_vars[SELECT_ORDER]}
    " &&
    {
        [[ ${bal_vars[LIMIT]} -eq 0 ]] || {
            _query+="
                LIMIT
                    ${bal_vars[LIMIT]}
            "
        }
    } &&
    _query+=")" &&
    execute_query \
        --name BAL_MUNICIPALITIES \
        --query "$_query" \
        --return _list &&
    array_sql_to_bash --array_sql "$_list" --array_bash _list_ref || return $ERROR_CODE

    return $SUCCESS_CODE
}

declare -A bal_vars=(
    [USECASE]=MATCH
    [FIX]=
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
    [STOP_TIME]=
    [AREAS_OLD_MUNICIPALITY]=0
    [STREETS]=-1
    [HOUSENUMBERS]=-1
    [PROGRESS_START]=
    [PROGRESS_CURRENT]=1
    [PROGRESS_TOTAL]=1
    [PROGRESS_SIZE]=5
) &&
pow_argv \
    --args_n '
        municipality:Code Commune INSEE à traiter (ou ALL pour traiter la liste complète);
        select_criteria:Sélection des Communes;
        select_order:Ordre de sélection des Communes;
        limit:Limiter à n communes (0 sans limite);
        stop_time:Temps d arrêt du traitement (format: MM-jj-hh:mm:ss);
        force:Forcer le traitement même si celui-ci a déjà été fait;
        dry_run:Simuler le traitement;
        progress:Afficher le ratio de progression;
        parallel:Obtenir les addresses en parallèle;
        clean:Effectuer la purge des fichiers temporaires;
        verbose:Ajouter des détails sur les traitements
    ' \
    --args_m '
        municipality
    ' \
    --args_v '
        select_criteria:REVISION|POPULATION|STREETS;
        select_order:ASC|DESC;
        force:yes|no;
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
        dry_run:no;
        limit:3;
        stop_time:0;
        progress:no;
        parallel:yes;
        clean:yes;
        verbose:no
    ' \
    --args_p '
        RESET:no
    ' \
    --pow_argv bal_vars "$@" || exit $ERROR_CODE

bal_vars[MUNICIPALITY_CODE]=${bal_vars[MUNICIPALITY]^^}
declare -a bal_codes=()
bal_start=$(date '+%s')
# reset LIMIT if STOP_TIME
[ "${bal_vars[STOP_TIME]}" != 0 ] && [ ${bal_vars[LIMIT]} -gt 0 ] && bal_vars[LIMIT]=0
set_env --schema_name fr &&
{
    [ "${bal_vars[PROGRESS]}" = no ] || set_log_echo no
} &&
{
    case "${bal_vars[MUNICIPALITY_CODE]}" in
    ALL)
        bal_list_municipalities --list bal_codes
        ;;
    *)
        bal_check_municipality --code "${bal_vars[MUNICIPALITY_CODE]}" &&
        bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
        ;;
    esac
} || exit $ERROR_CODE

bal_error=0
bal_vars[PROGRESS_TOTAL]=${#bal_codes[@]}
for ((bal_i=0; bal_i<${#bal_codes[@]}; bal_i++)); do
    [ "${bal_vars[STOP_TIME]}" != 0 ] && {
        # stop loop if allowed time is expired
        [[ "$(date +'%m-%d-%T')" > "${bal_vars[STOP_TIME]}" ]] && break
    }

    bal_vars[PROGRESS_CURRENT]=$((bal_i +1))
    bal_vars[MUNICIPALITY_CODE]=${bal_codes[$bal_i]}

    echo ${bal_vars[MUNICIPALITY_CODE]} &&
    execute_query \
        --name BAL_ADDRESSES_${bal_vars[MUNICIPALITY_CODE]} \
        --query "
            SELECT q FROM fr.bal_municipality_addresses(code => '${bal_vars[MUNICIPALITY_CODE]}')
        " \
        --return bal_query &&
#     echo $bal_query &&
#     read &&
    $POW_DIR_BATCH/address_match.sh \
        --source_name BAL_${bal_vars[MUNICIPALITY_CODE]} \
        --source_query "${bal_query}" \
        --steps STANDARDIZE \
        --format $POW_DIR_BATCH/bal/format.sql \
        --force ${bal_vars[FORCE]} || ((bal_error++))
done

[ "${bal_vars[DRY_RUN]}" = no ] &&
[ "${bal_vars[PROGRESS_CURRENT]}" -gt 10 ] && {
    vacuum \
        --schema_name fr \
        --table_name address_match_request,address_match_code,address_match_element,address_match_result \
        --mode ANALYZE || bal_error=1
}

_rc=$(( bal_error != 0 ? ERROR_CODE : SUCCESS_CODE ))
exit $_rc
