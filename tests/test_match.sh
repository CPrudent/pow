#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests match

set_request() {
    local -A _opts &&
    pow_argv \
        --args_n '
            from:Source des données;
            key:Propriété recherchée;
            value:Valeur retournée
        ' \
        --args_m '
            from;
            key;
            value
        ' \
        --pow_argv _opts "$@" || return $?

    local _data _source _format _parameters _filter _query
    local -n _value_ref=${_opts[VALUE]}

    case ${_opts[FROM]} in
    FILE)
        _data=$POW_DIR_ROOT/tests/data/test_match_file.csv
        _format=$POW_DIR_ROOT/tests/data/test_match_file_format.sql
        _parameters=$POW_DIR_ROOT/tests/data/test_match_parameters.sql
        ;;
    TABLE)
        _data=$POW_DIR_ROOT/tests/data/test_match_table.csv
        _format=$POW_DIR_ROOT/tests/data/test_match_table_format.sql
        _source=test_match_table_1
        ;;
    QUERY)
        _data=test_match_table_1
        _format=$POW_DIR_ROOT/tests/data/test_match_table_format.sql
        _source=test_match_query_1
        _filter="municipality = 'ANTIBES'"
        _query='SELECT rowid,code,complement,housenumber,street,municipality_old,postcode,municipality FROM fr.test_match_table_1'
        ;;
    *)
        log_error "Source(${_opts[FROM]}) non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    case ${_opts[KEY]}^^} in
    DATA)           _value_ref=$_data           ;;
    SOURCE)         _value_ref=$_source         ;;
    FORMAT)         _value_ref=$_format         ;;
    PARAMETERS)     _value_ref=$_parameters     ;;
    FILTER)         _value_ref=$_filter         ;;
    QUERY)          _value_ref=$_query          ;;
    *)
        log_error "Propriété(${_opts[KEY]}) non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    return $SUCCESS_CODE
}

set_context() {
    local -A _opts &&
    pow_argv \
        --args_n "
            test:Préciser le test à réaliser;
        " \
        --args_m '
            test;
        ' \
        --pow_argv _opts "$@" || return $?

    case "${_opts[TEST]}" in
    STANDARDIZE_*)
        case "${_opts[TEST]}" in
        *_FILE|*_TABLE)
            expect file "${test_request[DATA]}"
            ;;
        esac &&
        expect file "${test_request[FORMAT]}" &&
        {
            [ -n "${test_request[PARAMETERS]}" ] &&
            expect file "${test_request[PARAMETERS]}" || true
        } &&
        case "${_opts[TEST]}" in
        *_TABLE)
            table_exists --schema_name fr --table_name "${test_request[SOURCE]}" ||
            import_file \
                --file_path "${test_request[DATA]}" \
                --schema_name fr \
                --table_name "${test_request[SOURCE]}" \
                --load_mode OVERWRITE_TABLE
            ;;
        *_QUERY)
            # NOTE: need previous TABLE request to have data
            table_exists --schema_name fr --table_name "${test_request[DATA]}"
            ;;
        esac || return $ERROR_CODE
        ;;
    *)
        set_log_echo no &&
        test_request[ID]=$($POW_DIR_ROOT/bin/fr/address_match.sh \
            --source_name "${test_request[SOURCE]:-${test_request[DATA]}}" \
            --source_filter "${test_request[FILTER]}" \
            --source_query "${test_request[QUERY]}" \
            --only_info ID
        ) &&
        set_log_echo yes &&
        execute_query \
            --name STATUS_REQUEST \
            --query "
                SELECT CONCAT_WS(' ', is_normalized, is_match_code, is_match_element)
                FROM fr.address_match_request
                WHERE id = ${test_request[ID]}
            " \
            --return test_request[STATUS]
        ;;
    esac

    return $SUCCESS_CODE
}

pow_argv \
    --args_n '
        test:Préciser le test à réaliser;
        force:Forcer le traitement même si celui-ci a déjà été fait
    ' \
    --args_m '
        test
    ' \
    --args_v '
        test:STANDARDIZE_FROM_FILE|STANDARDIZE_FROM_TABLE|STANDARDIZE_FROM_QUERY|MATCH_CODE_FROM_FILE|MATCH_CODE_FROM_TABLE|MATCH_CODE_FROM_QUERY|MATCH_ELEMENT_FROM_FILE|MATCH_ELEMENT_FROM_TABLE|MATCH_ELEMENT_FROM_QUERY;
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    --args_p '
        tag:test@XN,force@bool
    ' \
    "$@" || exit $?

declare -A test_request
[[ ${POW_ARGV[TEST]} =~ ^(.*)_FROM ]] && test_request[STEP]=${BASH_REMATCH[1]}
[[ ${POW_ARGV[TEST]} =~ FROM_(.*)$ ]] && test_request[FROM]=${BASH_REMATCH[1]}
for _item in DATA SOURCE FORMAT PARAMETERS FILTER QUERY; do
    set_request --from ${test_request[FROM]} --key $_item --value test_request[$_item]
done &&
set_env --schema_name fr &&
set_context --test "${POW_ARGV[TEST]}" &&
case "${POW_ARGV[TEST]}" in
STANDARDIZE_*)
    $POW_DIR_ROOT/bin/fr/address_match.sh \
        --source_name "${test_request[SOURCE]:-${test_request[DATA]}}" \
        --steps IMPORT,STANDARDIZE \
        --format "${test_request[FORMAT]}" \
        --parameters "${test_request[PARAMETERS]}" \
        --source_filter "${test_request[FILTER]}" \
        --source_query "${test_request[QUERY]}" \
        --verbose yes \
        --force ${POW_ARGV[FORCE]}
    ;;
MATCH_CODE_*)
    {
        [ "${test_request[STATUS]:0:1}" = t ] || {
            log_error 'manque étape Standardization'
            false
        }
    } &&
    $POW_DIR_ROOT/bin/fr/address_match.sh \
        --source_name "${test_request[SOURCE]:-${test_request[DATA]}}" \
        --steps MATCH_CODE \
        --source_filter "${test_request[FILTER]}" \
        --source_query "${test_request[QUERY]}" \
        --verbose yes \
        --force ${POW_ARGV[FORCE]}
    ;;
MATCH_ELEMENT_*)
    {
        [ "${test_request[STATUS]:2:1}" = t ] || {
            log_error 'manque étape Match Code'
            false
        }
    } &&
    $POW_DIR_ROOT/bin/fr/address_match.sh \
        --source_name "${test_request[SOURCE]:-${test_request[DATA]}}" \
        --steps MATCH_ELEMENT \
        --source_filter "${test_request[FILTER]}" \
        --source_query "${test_request[QUERY]}" \
        --verbose yes \
        --force ${POW_ARGV[FORCE]}
    ;;
esac || exit $ERROR_CODE

exit $SUCCESS_CODE
