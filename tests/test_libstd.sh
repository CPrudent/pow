#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests libstd

declare -a TESTS=(
    IN_ARRAY
    NOT_IN_ARRAY
    IN_HASH
    NOT_IN_HASH
    CLONE_ARRAY
    ELAPSED_TIME
    IS_YES
    DELIMITER
    GET_FILE_NAME
    GET_FILE_EXTENSION
    GET_FILE_NROWS
)
TESTS_JOIN_PIPE=${TESTS[@]}
TESTS_JOIN_PIPE=${TESTS_JOIN_PIPE// /|}
TESTS_JOIN_PIPE+="|ALL"

declare -A env_lib=(
    [ERROR]=0
) &&
pow_argv \
    --args_n '
        test:Préciser le test à réaliser;
        clean:Purger les fichiers temporaires
    ' \
    --args_m '
        test
    ' \
    --args_v '
        test:'${TESTS_JOIN_PIPE}';
        clean:no|yes
    ' \
    --args_d '
        clean:yes
    ' \
    --args_p '
        reset:no
    ' \
    --pow_argv env_lib "$@" || exit $ERROR_CODE

declare -a test_lib
[ "${env_lib[TEST]}" = ALL ] && test_lib=( "${TESTS[@]}" ) || test_lib[0]="${env_lib[TEST]}"
declare -A result_lib

# tests
set_log_echo no &&
set_env --schema_name fr &&
for ((_test=0; _test<${#test_lib[@]}; _test++)); do
    _rc=1

    case "${test_lib[_test]}" in
    IN_ARRAY)
        declare -a _array=([0]=ZERO [1]=ONE [2]=TWO)
        in_array --array _array --item ONE --position _position &&
        [[ $_position -eq 1 ]] &&
        _rc=0
        ;;
    NOT_IN_ARRAY)
        declare -a _array=([0]=ZERO [1]=ONE [2]=TWO)
        in_array --array _array --item THREE --position _position
        [[ $? -ne 0 ]] &&
        [[ $_position -eq -1 ]] &&
        _rc=0
        ;;
    IN_HASH)
        declare -A _hash=([ZERO]=0 [ONE]=1 [TWO]=2)
        in_array --array _hash --item TWO --search KEY &&
        _rc=0
        ;;
    NOT_IN_HASH)
        declare -A _hash=([ZERO]=0 [ONE]=1 [TWO]=2)
        in_array --array _hash --item THREE --position _position --search KEY
        [[ $? -ne 0 ]] &&
        _rc=0
        ;;
    CLONE_ARRAY)
        declare -a _array=([0]=ZERO [1]=ONE [2]=TWO)
        clone_array --from_array _array --to_array _array2 &&
        [[ ${#_array[@]} -eq ${#_array2[@]} ]] &&
        [ "${_array[1]}" = "${_array2[1]}" ] &&
        _rc=0
        ;;
#     CLONE_HASH)
#         declare -A _hash=([ZERO]=0 [ONE]=1 [TWO]=2)
#         clone_array --from_array _hash --to_array _hash2 &&
#         [[ ${#_hash[@]} -eq ${#_hash2[@]} ]] &&
#         [ "${!_hash[@]}" = "${!_hash2[@]}" ] &&
#         _rc=0
#         ;;
    ELAPSED_TIME)
        _start=$(date +%s)
        sleep 1
        get_elapsed_time --start $_start --result _result &&
        [ "$_result" = "0h:0m:1s" ] &&
        _rc=0
        ;;
    IS_YES)
        declare -a _array=([0]=yes [1]=oui [2]=ok [3]=true)
        _count=0
        for _v in "${_array[@]}"; do
            is_yes --var _v &&
            _t=${_v^^} &&
            is_yes --var _t &&
            _t=${_v:0:1} &&
            is_yes --var _t &&
            ((_count++))
        done
        [[ $_count -eq 4 ]] &&
        _rc=0
        ;;
    DELIMITER)
        _count=0
        for _d in ${!POW_DELIMITER[@]}; do
            set_delimiter --delimiter_code $_d --delimiter_value _delimiter &&
            [ "$_delimiter" = "${POW_DELIMITER[$_d]}" ] &&
            ((_count++))
        done
        [[ $_count -eq ${#POW_DELIMITER[@]} ]] &&
        _rc=0
        ;;
    GET_FILE_NAME)
        _fp=$POW_DIR_ROOT/tests/data/test_spreadsheet.csv
        get_file_name --file_path "$_fp" --file_name _f1 --stdout no &&
        _f2=$(get_file_name --file_path "$_fp") &&
        [ "$_f1" = "$_f2" ] &&
        get_file_name --file_path "$_fp" --file_name _f1 --stdout no --with_extension yes &&
        _f2=$(get_file_name --file_path "$_fp" --with_extension yes) &&
        [ "$_f1" = "$_f2" ] &&
        _rc=0
        ;;
    GET_FILE_EXTENSION)
        _fp=$POW_DIR_ROOT/tests/data/test_spreadsheet.csv
        get_file_extension --file_path "$_fp" --file_extension _e1 --stdout no &&
        _e2=$(get_file_extension --file_path "$_fp") &&
        [ "$_e1" = "$_e2" ] &&
        _rc=0
        ;;
    GET_FILE_NROWS)
        _fp=$POW_DIR_ROOT/tests/data/test_spreadsheet.csv
        get_file_nrows --file_path "$_fp" --file_nrows _n1 --stdout no &&
        _n2=$(get_file_extension --file_path "$_fp") &&
        [ "$_e1" = "$_e2" ] &&
        _rc=0
        ;;

    esac

    [[ $_rc -ne 0 ]] && ((env_lib[ERROR]++))
    result_lib+=(["${test_lib[$_test]}"]=$_rc)
    # https://stackoverflow.com/questions/5349718/how-can-i-repeat-a-character-in-bash
    _len=$((36 - ${#test_lib[$_test]}))
    _spaces=$(printf ' %.0s' $(seq 1 $_len))
    printf "%s%s[%s]\n" \
        "${test_lib[$_test]}" \
        "$_spaces" \
        $( [[ ${result_lib["${test_lib[$_test]}"]} -eq 0 ]] && echo OK || echo KO )
done

# purge
[ "${env_lib[CLEAN]}" = yes ] && {
    :
}

# results
_error=
[[ ${env_lib[ERROR]} -gt 0 ]] && _error="avec ${env_lib[ERROR]} erreur"
[[ ${env_lib[ERROR]} -gt 1 ]] && _error+=s
_rc=$SUCCESS_CODE
[ -n "$_error" ] && {
    printf '\n%40s\n' "$_error"
    _rc=$ERROR_CODE
}

exit $_rc
