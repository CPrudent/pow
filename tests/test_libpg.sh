#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests libpg

test_ddl() {
    local -A _opts &&
    pow_argv \
        --args_n '
            action:Niveau Adresses
        ' \
        --args_m '
            action
        ' \
        --args_v '
            action:CREATE|DROP
        ' \
        --pow_argv _opts "$@" || return $?

    case "${_opts[ACTION]}" in
    CREATE)
        execute_query \
            --name CREATE_TABLE \
            --query 'CREATE TABLE IF NOT EXISTS fr.test_lib (
                id SERIAL NOT NULL,
                name VARCHAR(50) NOT NULL,
                date_data TIMESTAMP NOT NULL,
                attributes VARCHAR
                );

                CREATE UNIQUE INDEX IF NOT EXISTS iux_test_libpg_id ON fr.test_lib(id);
            ' &&
        execute_query \
            --name INSERT_VALUES \
            --query "
                INSERT INTO fr.test_lib(name, date_data, attributes)
                VALUES
                    ('TEST1', TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE, 'ITEM1'),
                    ('TEST2', TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE, 'ITEM1'),
                    ('TEST2', TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE, 'ITEM2'),
                    ('TEST3', TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE, 'ITEM1'),
                    ('TEST3', TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE, 'ITEM2'),
                    ('TEST3', TIMEOFDAY()::TIMESTAMP WITHOUT TIME ZONE, 'ITEM3')
                "
        ;;
    DROP)
        execute_query \
            --name DROP_TABLE \
            --query 'DROP TABLE IF EXISTS fr.test_lib'
        ;;
    esac || return $ERROR_CODE

    return $SUCCESS_CODE
}

declare -a TESTS=(
    EXECUTE_QUERY_OUTPUT
    EXECUTE_QUERY_RETURN
    TABLE_EXISTS
    VIEW_EXISTS
    TABLE_SEQUENCES
    ARRAY_SQL_TO_BASH
    VACUUM_1
    VACUUM_N
    BACKUP_TABLE
    RESTORE_TABLE
)
TESTS_JOIN_PIPE=${TESTS[@]}
TESTS_JOIN_PIPE=${TESTS_JOIN_PIPE// /|}

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
        reset:no;
        tag:test@X+N
    ' \
    --pow_argv env_lib "$@" || exit $?

declare -a test_lib=(${env_lib[TEST]})
declare -A result_lib

# tests
set_log_echo no &&
set_env --schema_name fr &&
test_ddl --action CREATE &&
for ((_test=0; _test<${#test_lib[@]}; _test++)); do
    _rc=1
    case "${test_lib[$_test]}" in
    EXECUTE_QUERY_RETURN)
        execute_query --name RETURN_VALUE --query 'SELECT 1+2' --return _value &&
        [ "$_value" -eq 3 ] &&
        _rc=0
        ;;
    EXECUTE_QUERY_OUTPUT)
        _output=$POW_DIR_TMP/test_lib.output.tmp
        execute_query --name OUTPUT_VALUE --query 'SELECT 1+2' --output $_output &&
        [ -s "$_output" ] &&
        _value=$(< $_output) &&
        [ "$_value" -eq 3 ] &&
        _rc=0
        ;;
    TABLE_EXISTS)
        table_exists --schema_name fr --table_name address_match_request
        _rc=$?
        ;;
    VIEW_EXISTS)
        view_exists --schema_name fr --view_name address_view
        _rc=$?
        ;;
    TABLE_SEQUENCES)
        _seq=$(get_table_sequences --schema_name fr --table_name address_match_request) &&
        [ "$_seq" = address_match_request_id_seq ] &&
        _rc=0
        ;;
    ARRAY_SQL_TO_BASH)
        execute_query \
            --name ARRAY \
            --query "SELECT ARRAY_AGG(DISTINCT name) FROM fr.test_lib" \
            --return _array_sql &&
        array_sql_to_bash \
            --array_sql "${_array_sql}" \
            --count 3 \
            --array_bash _array_bash &&
        [[ ${#_array_bash[@]} -eq 3 ]] &&
        _rc=0
        ;;
    VACUUM_1)
        vacuum --schema_name fr --table_name ign_region &&
        _rc=0
        ;;
    VACUUM_N)
        vacuum --schema_name fr --table_name ign_region,ign_epci,ign_department &&
        _rc=0
        ;;
    BACKUP_TABLE)
        backup_table \
            --schema_name fr \
            --table_name test_lib \
            --output $POW_DIR_TMP/fr.test_lib.backup &&
        [ -s $POW_DIR_TMP/fr.test_lib.backup ] &&
        _rc=0
        ;;
    RESTORE_TABLE)
        [ ! -f $POW_DIR_TMP/fr.test_lib.backup ] && {
            log_error 'besoin de lancer le test BACKUP_TABLE auparavant!'
            _rc=1
        } || {
            restore_table \
                --schema_name fr \
                --table_name test_lib \
                --input $POW_DIR_TMP/fr.test_lib.backup \
                --backup_before_restore no \
                --sql_to_filter "NEW.name = 'TEST3'" &&
            execute_query \
                --name COUNT \
                --query "SELECT COUNT(*) FROM fr.test_lib" \
                --return _value &&
            [[ $_value -eq 3 ]] &&
            _rc=0
        }
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
    test_ddl --action DROP
    rm --force $POW_DIR_TMP/fr.test_lib.backup
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
