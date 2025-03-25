#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests libpg

declare -a TESTS=(EXECUTE_QUERY_OUTPUT EXECUTE_QUERY_RETURN TABLE_EXISTS VIEW_EXISTS TABLE_SEQUENCES)
TESTS_JOIN_PIPE=${TESTS[@]}
TESTS_JOIN_PIPE=${TESTS_JOIN_PIPE// /|}
TESTS_JOIN_PIPE+="|ALL"

declare -A env_libpg=(
    [ERROR]=0
) &&
pow_argv \
    --args_n '
        test:Préciser le test à réaliser (ALL pour tous)
    ' \
    --args_m '
        test
    ' \
    --args_v '
        test:'${TESTS_JOIN_PIPE}';
    ' \
    --args_p '
        reset:no
    ' \
    --pow_argv env_libpg "$@" || exit $ERROR_CODE

declare -a test_libpg
[ "${env_libpg[TEST]}" = ALL ] && test_libpg=( "${TESTS[@]}" ) || test_libpg[0]="${env_libpg[TEST]}"
declare -A result_libpg

# tests
set_log_echo no
for ((_test=0; _test<${#test_libpg[@]}; _test++)); do
    _rc=1
    case "${test_libpg[$_test]}" in
    EXECUTE_QUERY_RETURN)
        execute_query --name RETURN_VALUE --query 'SELECT 1+2' --return _value &&
        [ "$_value" -eq 3 ] &&
        _rc=0
        ;;
    EXECUTE_QUERY_OUTPUT)
        _output=$POW_DIR_TMP/test_libpg.output.tmp
        execute_query --name OUTPUT_VALUE --query 'SELECT 1+2' --output $_output &&
        [ -s "$_output" ] &&
        _value=$(< $_output) &&
        [ "$_value" -eq 3 ] &&
        _rc=0
        ;;
    TABLE_EXISTS)
        table_exists --schema_name public --table_name io_history
        _rc=$?
        ;;
    VIEW_EXISTS)
        view_exists --schema_name fr --view_name address_view
        _rc=$?
        ;;
    TABLE_SEQUENCES)
        _seq=$(get_table_sequences --schema_name public --table_name io_history) &&
        [ "$_seq" = io_history_id_seq ] &&
        _rc=0
        ;;
    esac
    [[ $_rc -ne 0 ]] && ((env_libpg[ERROR]++))
    result_libpg+=(["${test_libpg[$_test]}"]=$_rc)
done

# results
_error=
[[ ${env_libpg[ERROR]} -gt 0 ]] && _error="\navec ${env_libpg[ERROR]} erreur"
[[ ${env_libpg[ERROR]} -gt 1 ]] && _error+=s
_rc=$SUCCESS_CODE
[ -n "$_error" ] && {
    echo -e "$_error\n"
    _rc=$ERROR_CODE
}
for ((_test=0; _test<${#test_libpg[@]}; _test++)); do
    _to_print=$((40 - ${#test_libpg[$_test]}))
    _spaces=$(printf ' %.0s' $(seq 1 $_to_print))
    printf "%s%s[%s]\n" \
        "${test_libpg[$_test]}" \
        "$_spaces" \
        $( [[ ${result_libpg["${test_libpg[$_test]}"]} -eq 0 ]] && echo OK || echo KO )
done

exit $_rc
