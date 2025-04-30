#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests libstd

t_pow_argv_1() {
    pow_argv \
        --args_n '
            mandatory:paramètre obligatoire;
            optional_w_d:paramètre optionnel (avec valeur par défaut);
            optional_wo_d:paramètre optionnel (sans valeur par défaut)
        ' \
        --args_m '
            mandatory
        ' \
        --args_v '
            optional_w_d:no|yes
        ' \
        --args_d '
            optional_w_d:no
        ' \
        "$@" || return $?

    # no process, only getopt
    return $SUCCESS_CODE
}

t_pow_argv_2() {
    pow_argv \
        --args_n '
            k1:paramètre optionnel (avec valeur par défaut);
            k2:paramètre optionnel (sans valeur par défaut)
        ' \
        --args_v '
            k1:no|yes
        ' \
        --args_d '
            k1:no;
            k2:@k1
        ' \
        "$@" || return $?

    # no process, only getopt
    return $SUCCESS_CODE
}

t_pow_argv_3() {
    pow_argv \
        --args_n '
            k:paramètre avec valeur(s) unique/multiple (selon définition via args_p)
        ' \
        --args_v '
            k:ONE|TWO|THREE
        ' \
        "$@" || return $?

    # no process, only getopt
    return $SUCCESS_CODE
}

t_pow_debug_1() {
    local _code=t_pow_debug_1 _step
    local -A _with_steps _with_bps

    #POW_DEBUG_JSON='{"codes":[{"name":"t_pow_debug_1","steps":["step1@break","step3"]}],"properties":{"init":"always"}}'
    POW_DEBUG_JSON='{"codes":[{"name":"t_pow_debug_1","steps":["step1@break","step3"]}]}'
    #echo $POW_DEBUG_JSON
    get_env_debug $_code _with_steps _with_bps 'step1 step2 step3 step4'
    #declare -p _with_steps _with_bps
    #declare -p POW_DEBUG_STEPS POW_DEBUG_BREAKPOINTS POW_DEBUG_PROPERTIES
    for _step in ${POW_DEBUG_STEPS[${_code}]}; do
        [[ ${_with_steps[$_step]} -eq 0 ]] && {
            echo "###DEBUG($_step)"
            [[ ${_with_bps[$_step]} -eq 0 ]] && read
        }
    done

    return $SUCCESS_CODE
}

# libstd tests
declare -a TESTS=(
    POW_ARGV
    POW_DEBUG
    POW_DEBUG_2
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

# pow_argv tests
declare -a cmd_pow_argv=(
    # help
    [0]="is_yes --help"
    # miss mandatory argument
    [1]="t_pow_argv_1 --optional_w_d yes"
    # default value of optional argument
    [2]="t_pow_argv_1 --mandatory M"
    # assign value to optional argument
    [3]="t_pow_argv_1 --mandatory M --optional_wo_d V"
    [4]="t_pow_argv_1 --mandatory M --optional_wo_d V --pow_argv argv"
    # w/o value, w/ BOOL tag (waiting for yes as default)
    # NOTE don't use simple quote around args_p's value!
    [5]="t_pow_argv_1 --args_p RESET:yes;TAG:optional_w_d@bool --mandatory M --optional_w_d"
    # value beginning w/ --
    [6]="t_pow_argv_1 --mandatory M --optional_wo_d \--opt1 VALUE1 \--opt2 VALUE2"
    # only 1 argument, w/o value
    [7]="set_env_pg --args_p TAG:print@bool --print"
    # INT tag
    [8]="t_pow_argv_1 --args_p TAG:optional_wo_d@int --mandatory M --optional_wo_d 123"
    # INT tag (error)
    [9]="t_pow_argv_1 --args_p TAG:optional_wo_d@int --mandatory M --optional_wo_d 123A"
    # FLOAT tag
    [10]="t_pow_argv_1 --args_p TAG:optional_wo_d@float --mandatory M --optional_wo_d -123.5"
    # FLOAT tag (error)
    [11]="t_pow_argv_1 --args_p TAG:optional_wo_d@float --mandatory M --optional_wo_d +12.3+"
    # default value from another defined key
    [12]="t_pow_argv_2 --args_p TAG:k1@bool --k1"
    # default value from another undefined key (as default)
    [13]="t_pow_argv_2 --args_p TAG:k1@bool"
    # no value, and no default (empty waited)
    [14]="t_pow_argv_1 --mandatory M --optional_wo_d"
    # count argument(s)
    [15]="t_pow_argv_1 --mandatory M --optional_wo_d"
    [16]="t_pow_argv_1 --mandatory M --optional_wo_d --pow_argc argc"
    # OK uniq|multiple value(s)
    [17]="t_pow_argv_3 --args_p TAG:k@1N --k ONE"
    [18]="t_pow_argv_3 --args_p TAG:k@1+N --k TWO TWO"
    [19]="t_pow_argv_3 --args_p TAG:k@XN --k ONE TWO"
    [20]="t_pow_argv_3 --args_p TAG:k@X+N --k ONE TWO TWO"
    # KO uniq|multiple value(s)
    [21]="t_pow_argv_3 --args_p TAG:k@1N --k FOR"
    [22]="t_pow_argv_3 --args_p TAG:k@1+N --k ONE TWO"
    [23]="t_pow_argv_3 --args_p TAG:k@XN --k ONE ONE"
    [24]="t_pow_argv_3 --args_p TAG:k@X+N --k ONE TWO THREE FOR"
    # (ALL, -) syntax
    [25]="t_pow_argv_3 --args_p TAG:k@X+N --k ALL"
    [26]="t_pow_argv_3 --args_p TAG:k@X+N --k -TWO"
    # bool w/ value
    [27]="t_pow_argv_1 --args_p TAG:optional_w_d@bool --mandatory M --optional_w_d no"
)
_tests_pow_argv=${#cmd_pow_argv[@]}
# return code
declare -a rc_pow_argv=()
# waiting success
for ((_i=1; _i<${_tests_pow_argv}; _i++)); do
    rc_pow_argv[$_i]=$SUCCESS_CODE
done
# waiting warning
rc_pow_argv[0]=$WARNING_CODE
# waiting error
for _i in 1 9 11 $(seq 21 24); do
    rc_pow_argv[$_i]=$ERROR_CODE
done
# assert conditions
declare -a assert_pow_argv=(
    [0]='[[ "$_log" == '"'"'var : Variable à tester, obligatoire'"'"' ]]'
    [2]='[ "${POW_ARGV[OPTIONAL_W_D]}" = no ]'
    [3]='[ "${POW_ARGV[OPTIONAL_WO_D]}" = V ]'
    [4]='[ "${argv[OPTIONAL_WO_D]}" = V ]'
    [5]='[ "${POW_ARGV[OPTIONAL_W_D]}" = yes ]'
    [6]='[ "${POW_ARGV[OPTIONAL_WO_D]}" = '"'"'--opt1 VALUE1 --opt2 VALUE2'"'"' ]'
    [7]='[[ "$(grep '"'"'host:port'"'"' $_logfile)" =~ '"'"'host:port'"'"' ]]'
    [8]='[[ ${POW_ARGV[OPTIONAL_WO_D]} -eq 123 ]]'
    [10]='[ "$(awk '"'"'BEGIN { print ('"'"'${POW_ARGV[OPTIONAL_WO_D]}'"'"' == -123.5) ? "OK" : "KO" }'"'"')" = OK ]'
    [12]='[ "${POW_ARGV[K1]}" = yes ] && [ "${POW_ARGV[K2]}" = yes ]'
    [13]='[ "${POW_ARGV[K1]}" = no ] && [ "${POW_ARGV[K2]}" = no ]'
    [14]='[ -z "${POW_ARGV[OPTIONAL_WO_D]}" ]'
    [15]='[[ ${POW_ARGC} -eq 3 ]]'
    [16]='[[ ${argc} -eq 3 ]]'
    [17]='[ "${POW_ARGV[K]}" = ONE ]'
    [18]='[ "${POW_ARGV[K]}" = '"'"'TWO TWO'"'"' ]'
    [19]='[ "${POW_ARGV[K]}" = '"'"'ONE TWO'"'"' ]'
    [20]='[ "${POW_ARGV[K]}" = '"'"'ONE TWO TWO'"'"' ]'
    [25]='[ "${POW_ARGV[K]}" = '"'"'ONE TWO THREE'"'"' ]'
    [26]='[ "${POW_ARGV[K]}" = '"'"'ONE THREE'"'"' ]'
    [27]='[ "${POW_ARGV[OPTIONAL_W_D]}" = no ]'
)
# no condition (when error waited)
for ((_i=0; _i<${_tests_pow_argv}; _i++)); do
    [ -z "${assert_pow_argv[$_i]}" ] && assert_pow_argv[$_i]='true'
done

declare -A argv
declare -A env_lib=(
    [ERROR]=0
    [POW_ARGV]=
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
        reset:no;
        tag:clean@bool,test@X+N
    ' \
    --pow_argv env_lib "$@" || exit $?

declare -a test_lib=(${env_lib[TEST]})
declare -A result_lib

# tests
set_log_echo no &&
set_env --schema_name fr &&
for ((_test=0; _test<${#test_lib[@]}; _test++)); do
    _rc=1

    case "${test_lib[_test]}" in
    POW_ARGV)
        _ok_pow_argv=0
        _logfile=$POW_DIR_TMP/pow_argv.log
        _len=${#_tests_pow_argv}
        for ((_i=0; _i<${_tests_pow_argv}; _i++)); do
            [ -n "${cmd_pow_argv[$_i]}" ] && {
                rm --force $_logfile
                ${cmd_pow_argv[$_i]} > $_logfile 2>&1
                _rc=$?
                printf "POW_ARGV/%*d: rc=%d/%d\n" ${_len} $_i $_rc ${rc_pow_argv[$_i]}
                _log=$(< $_logfile)
                [[ $_i -gt 26 ]] && {
                    declare -p POW_ARGV POW_ARGC
                    [ -n "$_log" ] && echo "log=$_log"
                }

                ([ $_rc -eq ${rc_pow_argv[$_i]} ] && $(eval ${assert_pow_argv[$_i]})) || {
                    env_lib[POW_ARGV]+="$_i "
                    continue
                }
            }

            ((_ok_pow_argv++))
        done
        [[ $_ok_pow_argv -eq ${_tests_pow_argv} ]] &&
        _rc=0 || {
            echo "POW_ARGV:  ok=$_ok_pow_argv/${_tests_pow_argv}"
            echo "POW_ARGV: err=${env_lib[POW_ARGV]}"
        }
        ;;
    POW_DEBUG)
        # export POW_DEBUG_JSON='{"codes":[{"name":"t_pow_debug_1","steps":["step1@break","step3"]}],"properties":{"init":"always"}}'
        # or inline def
        t_pow_debug_1
        _rc=0
        ;;
    POW_DEBUG_2)
        # export POW_DEBUG_JSON='{"codes":[{"name":"pow_argv","steps":["all"]}],"properties":{"init":"always"}}'
        declare -p POW_DEBUG_STEPS POW_DEBUG_BREAKPOINTS POW_DEBUG_PROPERTIES
        _rc=0
        ;;
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
    [ -n "$_logfile" ] && rm --force $_logfile
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
