#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests libio

test_csv() {
    local -A _opts &&
    pow_argv \
        --args_n '
            path:Nom complet du fichier;
            nrows:Nombre de lignes
        ' \
        --args_m '
            path;nrows
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -n _path_ref=${_opts[PATH]}
    local -n _nrows_ref=${_opts[NROWS]}
    local _tmpfile _csvfile="$POW_DIR_TMP/test_libio.csv"

    [ -f "$_csvfile" ] || {
        get_tmp_file --tmpfile _tmpfile --tmpext csv &&
        cat <<EOF > $_tmpfile &&
str,num,date
A,1,2025-04-01 17:00:00
BB,2,2025-04-01 17:01:00
CCC,3,2025-04-01 17:02:00
EOF
        mv "$_tmpfile" "$_csvfile" || return $ERROR_CODE
    }

    _path_ref="$_csvfile"
    _nrows_ref=4

    return $SUCCESS_CODE
}

declare -a TESTS=(
    NEWER_DATE
    NEWER_TIME
    CSV_OVERWRITE_TABLE
    CSV_APPEND
    XLS_CSV
    ODS_CSV
    CSV_XLS
    ODS_OVERWRITE_TABLE
    XLS_OVERWRITE_TABLE
    XLS_OVERWRITE_TABLE_LOWER
    CSV_IMPORT_FILE
    JSON_IMPORT_FILE
    SHP_IMPORT_FILE
    HISTORY_BEGIN
    HISTORY_END_OK
    HISTORY_END_KO
    HISTORY_UPDATE
)
TESTS_JOIN_PIPE=${TESTS[@]}
TESTS_JOIN_PIPE=${TESTS_JOIN_PIPE// /|}
TESTS_JOIN_PIPE+="|ALL"

declare -A env_libio=(
    [ERROR]=0
    [IO_NAME]=TEST_LIBIO
    [IO_ID]=
    [IO_BEGIN]=
    [IO_END]=$(date +%F)
    [IO_ROWS]=10
    [CSV_TABLE]=test_libio_csv
    [CSV_PATH]=
    [CSV_NROWS]=
    [XLS_TABLE]=test_libio_xls
    [XLS_NROWS]=3
    [JSON_TABLE]=test_libio_json
    [SHP_TABLE]=test_libio_shp
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
    --pow_argv env_libio "$@" || exit $ERROR_CODE

declare -a test_libio
[ "${env_libio[TEST]}" = ALL ] && test_libio=( "${TESTS[@]}" ) || test_libio[0]="${env_libio[TEST]}"
declare -A result_libio

# tests
set_log_echo no &&
set_env --schema_name fr &&
test_csv --path env_libio[CSV_PATH] --nrows env_libio[CSV_NROWS] &&
for ((_test=0; _test<${#test_libio[@]}; _test++)); do
    _rc=1

    case "${test_libio[_test]}" in
    NEWER_DATE)
        echo 'FROM TMPFILE'
        get_tmp_file --tmpfile _tmp1 --create yes
        _epoch1=$(stat --format '%Y' "$_tmp1")
        ls -l $_tmp1
        echo "epoch1=$_epoch1"
        touch -m -t 202501311003.15 $_tmp1
        _epoch1=$(stat --format '%Y' "$_tmp1")
        ls -l $_tmp1
        echo "epoch1=$_epoch1 (after touch)"

        _epoch2=$(date '+%s' --date "2025-01-31 04:07:29")
        _delta=$((_epoch1 - _epoch2))
        echo -n "epoch1=$_epoch1 epoch2=$_epoch2 delta=$_delta "
        [[ $_delta -lt 0 ]] && echo 'NEWER (yes)' || echo 'NEWER (no)'

        _epoch2=$(date '+%s' --date "2025-01-31 11:07:29")
        _delta=$((_epoch1 - _epoch2))
        echo -n "epoch1=$_epoch1 epoch2=$_epoch2 delta=$_delta "
        [[ $_delta -lt 0 ]] && echo 'NEWER (yes)' || echo 'NEWER (no)'

        echo
        echo 'FROM DB (INSEE 44109 if exists)'
        execute_query \
            --name BAL_44109_DATE \
            --query "
                SELECT last_update
                FROM fr.bal_municipality
                WHERE code = '44109'
            " \
            --return _date
        [ -n "$_date" ] && {
            _epoch2=$(date '+%s' --date "$_date")

            _ref=$POW_DIR_COMMON_GLOBAL/fr/bal/44109.json
            [ -f "$_ref" ] && {
                _epoch1=$(stat --format '%Y' "$_ref")
                _delta=$((_epoch1 - _epoch2))
                echo -n "epoch1=$_epoch1 epoch2=$_epoch2 delta=$_delta "
                [[ $_delta -lt 0 ]] && echo 'NEWER (yes)' || echo 'NEWER (no)'
            }
        }

        rm $_tmp1
        _rc=0
        ;;
    NEWER_TIME)
        _rc=0
        ;;

    CSV_OVERWRITE_TABLE)
        import_csv_file \
            --file_path "${env_libio[CSV_PATH]}" \
            --table_name ${env_libio[CSV_TABLE]} \
            --load_mode OVERWRITE_TABLE &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "SELECT COUNT(1) FROM fr.${env_libio[CSV_TABLE]}" \
            --return _nrows &&
        [[ $_nrows -eq $((env_libio[CSV_NROWS] -1)) ]] &&
        _rc=0
        ;;
    CSV_APPEND)
        import_csv_file \
            --file_path "${env_libio[CSV_PATH]}" \
            --table_name ${env_libio[CSV_TABLE]} \
            --load_mode OVERWRITE_TABLE &&
        import_csv_file \
            --file_path "${env_libio[CSV_PATH]}" \
            --table_name ${env_libio[CSV_TABLE]} \
            --load_mode APPEND &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "SELECT COUNT(1) FROM fr.${env_libio[CSV_TABLE]}" \
            --return _nrows &&
        [[ $_nrows -eq $((2 * (env_libio[CSV_NROWS] -1))) ]] &&
        _rc=0
        ;;

    XLS_CSV)
        _xls=$POW_DIR_ROOT/tests/data/test_spreadsheet.xlsx
        _csv=$POW_DIR_TMP/test_spreadsheet.csv
        expect file "$_xls" &&
        rm --force "$_csv" &&
        excel_to_csv \
            --from_file_path "$_xls" \
            --delimiter PIPE &&
        expect file "$_csv" &&
        _field=$(tail -n 1 "$_csv" | cut --delimiter '|' --field 1) &&
        [ "$_field" = 3 ] &&
        _rc=0
        ;;
    ODS_CSV)
        _ods=$POW_DIR_ROOT/tests/data/test_spreadsheet.ods
        _csv=$POW_DIR_TMP/test_spreadsheet.csv
        expect file "$_ods" &&
        rm --force "$_csv" &&
        excel_to_csv \
            --from_file_path "$_ods" &&
        expect file "$_csv" &&
        _field=$(tail -n 2 "$_csv" | head -n 1 | cut --delimiter ',' --field 3) &&
        [ "$_field" = WORLD ] &&
        _rc=0
        ;;
    CSV_XLS)
        _csv=$POW_DIR_ROOT/tests/data/test_spreadsheet.csv
        _xls=$POW_DIR_TMP/test_spreadsheet.xls
        expect file "$_csv" &&
        rm --force "$_xls" &&
        csv_to_excel \
            --from_file_path "$_csv" &&
        expect file "$_xls" &&
        _rc=0
        ;;

    ODS_OVERWRITE_TABLE)
        _xls=$POW_DIR_ROOT/tests/data/test_spreadsheet-lower.ods
        expect file "$_xls" &&
        import_excel_file \
            --file_path "$_xls" \
            --table_name ${env_libio[XLS_TABLE]} \
            --load_mode OVERWRITE_TABLE &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "SELECT COUNT(1) FROM fr.${env_libio[XLS_TABLE]}" \
            --return _nrows &&
        [[ $_nrows -eq ${env_libio[XLS_NROWS]} ]] &&
        _rc=0
        ;;
    XLS_OVERWRITE_TABLE)
        _xls=$POW_DIR_ROOT/tests/data/test_spreadsheet.xlsx
        expect file "$_xls" &&
        import_excel_file \
            --file_path "$_xls" \
            --table_name ${env_libio[XLS_TABLE]} \
            --load_mode OVERWRITE_TABLE &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "
                SELECT "'"'"NB_VALUE"'"'"
                FROM fr.${env_libio[XLS_TABLE]}
                WHERE "'"'"ID"'"'" = '2'
            " \
            --return _value &&
        [[ "$_value" = "2E+04" ]] &&
        _rc=0
        ;;
    # lowering column names
    XLS_OVERWRITE_TABLE_LOWER)
        _xls=$POW_DIR_ROOT/tests/data/test_spreadsheet.xlsx
        expect file "$_xls" &&
        import_excel_file \
            --file_path "$_xls" \
            --table_name ${env_libio[XLS_TABLE]} \
            --table_columns HEADER_TO_LOWER_CODE \
            --load_mode OVERWRITE_TABLE &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "
                SELECT nb_value
                FROM fr.${env_libio[XLS_TABLE]}
                WHERE id = '1'
            " \
            --return _value &&
        [[ "$_value" = "1E+03" ]] &&
        _rc=0
        ;;

    CSV_IMPORT_FILE)
        _csv=$POW_DIR_COMMON_GLOBAL/fr/bal/communes-summary.csv
        expect file "$_csv" &&
        import_file \
            --file_path "$_csv" \
            --table_name ${env_libio[CSV_TABLE]} \
            --load_mode OVERWRITE_TABLE &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "
                SELECT COUNT(1)
                FROM fr.${env_libio[CSV_TABLE]}
            " \
            --return _nrows &&
        [[ $_nrows -gt 30000 ]] &&
        _rc=0
        ;;
    JSON_IMPORT_FILE)
        _json=$POW_DIR_COMMON_GLOBAL/fr/bal/17309.json
        expect file "$_json" &&
        execute_query \
            --name JSON_TABLE_CREATE \
            --query "
                DROP TABLE IF EXISTS fr.${env_libio[JSON_TABLE]};
                CREATE TABLE fr.${env_libio[JSON_TABLE]} (data JSON);
            " &&
        import_file \
            --file_path "$_json" \
            --table_name ${env_libio[JSON_TABLE]} \
            --import_options column_name=data \
            --load_mode OVERWRITE_DATA &&
        execute_query \
            --name "${test_libio[_test]}" \
            --query "
                SELECT data->>'nbVoies'
                FROM fr.${env_libio[JSON_TABLE]}
            " \
            --return _nrows &&
        [[ $_nrows -eq 95 ]] &&
        _rc=0
        ;;
    SHP_IMPORT_FILE)
        _ign=$(ls -1 $POW_DIR_COMMON_GLOBAL/fr/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_* | tail -n 1) &&
        [ -n "$_ign" ] &&
        extract_archive \
            --archive_path "$_ign" \
            --extract_path "$POW_DIR_TMP/IGN" &&
        _shp=$(find $POW_DIR_TMP/IGN/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX*/*/1*/* -type f -iname DEPARTEMENT.shp) &&
        expect file "$_shp" &&
        get_pg_passwd --user_name $POW_PG_USERNAME --password _passwd &&
        import_geo_file \
            --file_path "$_shp" \
            --table_name "${env_libio[SHP_TABLE]}" \
            --password "$_passwd" \
            --geometry_type PROMOTE_TO_MULTI \
            --load_mode OVERWRITE_TABLE \
            --spatial_index no &&
        execute_query \
            --name "${test_libio[_test]}_COUNT" \
            --query "
                SELECT COUNT(1)
                FROM fr.${env_libio[SHP_TABLE]}
            " \
            --return _nrows &&
        [[ $_nrows -eq 96 ]] &&
        execute_query \
            --name "${test_libio[_test]}_DATA" \
            --query "
                SELECT nom_m
                FROM fr.${env_libio[SHP_TABLE]}
                WHERE insee_dep = '39'
            " \
            --return _value &&
        [[ "$_value" = "JURA" ]] &&
        _rc=0
        ;;

    HISTORY_BEGIN)
        io_history_begin \
            --io "${env_libio[IO_NAME]}" \
            --date_begin "${env_libio[IO_BEGIN]:-1970-01-01}" \
            --date_end "${env_libio[IO_END]}" \
            --nrows_todo ${env_libio[IO_ROWS]} \
            --id env_libio[IO_ID] &&
        execute_query \
            --name "${test_libio[_test]}_BEGIN" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}', 'EN_COURS')).status
            " \
            --return _state &&
        [ "$_state" = EN_COURS ] &&
        _rc=0
        ;;
    HISTORY_END_OK)
        execute_query \
            --name "${test_libio[_test]}_ID" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}', 'EN_COURS')).id
            " \
            --return env_libio[IO_ID] &&
        [ -n "${env_libio[IO_ID]}" ] &&
        io_history_end_ok \
            --nrows_processed ${env_libio[IO_ROWS]} \
            --id ${env_libio[IO_ID]} &&
        execute_query \
            --name "${test_libio[_test]}_STATE" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}')).status
            " \
            --return _state &&
        [ "$_state" = SUCCES ] &&
        _rc=0
        ;;
    HISTORY_END_KO)
        execute_query \
            --name "${test_libio[_test]}_ID" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}', 'EN_COURS')).id
            " \
            --return env_libio[IO_ID] &&
        [ -n "${env_libio[IO_ID]}" ] &&
        io_history_end_ko \
            --id ${env_libio[IO_ID]} &&
        execute_query \
            --name "${test_libio[_test]}_ID" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}', 'ERREUR')).id
            " \
            --return _id &&
        [[ $_id -eq ${env_libio[IO_ID]} ]]
        _rc=0
        ;;
    HISTORY_UPDATE)
        execute_query \
            --name "${test_libio[_test]}_ID" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}')).id
            " \
            --return env_libio[IO_ID] &&
        [ -n "${env_libio[IO_ID]}" ] &&
        io_history_update \
            --nrows_processed 5 \
            --id ${env_libio[IO_ID]} &&
        execute_query \
            --name "${test_libio[_test]}_STATE" \
            --query "
                SELECT (get_last_io('${env_libio[IO_NAME]}')).nb_rows_processed
            " \
            --return _nrows &&
        [[ $_nrows -eq 5 ]] &&
        _rc=0
        ;;

    esac

    [[ $_rc -ne 0 ]] && ((env_libio[ERROR]++))
    result_libio+=(["${test_libio[$_test]}"]=$_rc)
    # https://stackoverflow.com/questions/5349718/how-can-i-repeat-a-character-in-bash
    _len=$((36 - ${#test_libio[$_test]}))
    _spaces=$(printf ' %.0s' $(seq 1 $_len))
    printf "%s%s[%s]\n" \
        "${test_libio[$_test]}" \
        "$_spaces" \
        $( [[ ${result_libio["${test_libio[$_test]}"]} -eq 0 ]] && echo OK || echo KO )
done

# purge
[ "${env_libio[CLEAN]}" = yes ] && {
    rm --force "${env_libio[CSV_PATH]}"
    rm --force --recursive $POW_DIR_TMP/IGN
}

# results
_error=
[[ ${env_libio[ERROR]} -gt 0 ]] && _error="avec ${env_libio[ERROR]} erreur"
[[ ${env_libio[ERROR]} -gt 1 ]] && _error+=s
_rc=$SUCCESS_CODE
[ -n "$_error" ] && {
    printf '\n%40s\n' "$_error"
    _rc=$ERROR_CODE
}

exit $_rc
