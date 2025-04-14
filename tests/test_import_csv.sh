#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests imports CSV

declare -a TEST_IMPORT_CSV_FILES=(
    tab.csv
    comma.csv
    semicolon.csv
    colon.csv
    pipe.csv
)

declare -a TEST_IMPORT_CSV_DELIMITERS=(
    TAB
    COMMA
    SEMICOLON
    COLON
    PIPE
)

bash_args \
    --args_p '
        clean:Effacement des données de tests;
        dry_run:Afficher les traitements sans les exécuter
    ' \
    --args_v '
        clean:no|yes;
        dry_run:no|yes
    ' \
    --args_d '
        clean:yes;
        dry_run:no
    ' \
    "$@" || exit $?

# FIXME not found to insert TAB into Kate!
> $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[0]}
echo -e "COL1\tCOL2\tCOL3"     >> $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[0]}
echo -e "data1\t1\t2023-02-15" >> $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[0]}
echo -e "data2\t2\t2023-02-16" >> $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[0]}
echo -e "data3\t3\t2023-02-17" >> $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[0]}

cat <<EOF > $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[1]}
COL1,COL2,COL3
data1,1,2023-02-15
data2,2,2023-02-16
data3,3,2023-02-17
EOF

cat <<EOF > $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[2]}
COL1;COL2;COL3
data1;1;2023-02-15
data2;2;2023-02-16
data3;3;2023-02-17
EOF

cat <<EOF > $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[3]}
COL1:COL2:COL3
data1:1:2023-02-15
data2:2:2023-02-16
data3:3:2023-02-17
EOF

cat <<EOF > $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[4]}
COL1|COL2|COL3
data1|1|2023-02-15
data2|2|2023-02-16
data3|3|2023-02-17
EOF

set_env --schema_name public

# tests
for ((_i=0; _i<${#TEST_IMPORT_CSV_FILES[@]}; _i++)); do
    [ $_i -gt 0 ] && echo
    # check header detection
    echo "CHECK HEADER"
    delimiter_value=${POW_DELIMITER[${TEST_IMPORT_CSV_DELIMITERS[$_i]}]}
    cat $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]} \
        | grep --max-count 1 --line-number --perl-regexp '([^"'$delimiter_value']"|'$delimiter_value'[^"'$delimiter_value']*|'$delimiter_value'"[^"'$delimiter_value']+")$'

    _info="Import ${TEST_IMPORT_CSV_DELIMITERS[$_i]} (AUTODETECT, OVERWRITE_TABLE) :"
    [ "$get_arg_dry_run" = no ] && {
        import_csv_file \
            --file_path $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]} \
            --load_mode OVERWRITE_TABLE \
        && echo "$_info OK" || echo "$_info KO"
    } || {
        echo "$_info TODO"
    }

    _info="Import ${TEST_IMPORT_CSV_DELIMITERS[$_i]} (AUTODETECT, OVERWRITE_DATA) :"
    [ "$get_arg_dry_run" = no ] && {
        import_csv_file \
            --file_path $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]} \
        && echo "$_info OK" || echo "$_info KO"
    } || {
        echo "$_info TODO"
    }

    _info="Import ${TEST_IMPORT_CSV_DELIMITERS[$_i]} (DELIMITER, OVERWRITE_DATA) :"
    [ "$get_arg_dry_run" = no ] && {
        import_csv_file \
            --file_path $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]} \
            --delimiter ${TEST_IMPORT_CSV_DELIMITERS[$_i]} \
        && echo "$_info OK" || echo "$_info KO"
    } || {
        echo "$_info TODO"
    }

    [ "$get_arg_dry_run" = no ] && {
        table_name=$(get_file_name --file_path "${TEST_IMPORT_CSV_DELIMITERS[$_i]}")
        execute_query \
            --name "DROP_${table_name}" \
            --query "DROP TABLE IF EXISTS ${table_name}"
    }

    _info="Import ${TEST_IMPORT_CSV_DELIMITERS[$_i]} (DELIMITER, OVERWRITE_DATA, HEADER_TO_LOWER_CODE) :"
    [ "$get_arg_dry_run" = no ] && {
        import_csv_file \
            --file_path $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]} \
            --delimiter ${TEST_IMPORT_CSV_DELIMITERS[$_i]} \
            --table_columns HEADER_TO_LOWER_CODE \
        && echo "$_info OK" || echo "$_info KO"
    } || {
        echo "$_info TODO"
    }

    _info="Import ${TEST_IMPORT_CSV_DELIMITERS[$_i]} (DELIMITER, APPEND, LIST) :"
    [ "$get_arg_dry_run" = no ] && {
        import_csv_file \
            --file_path $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]} \
            --delimiter ${TEST_IMPORT_CSV_DELIMITERS[$_i]} \
            --load_mode APPEND \
            --table_columns LIST \
            --table_columns_list 'col1,col2,col3' \
        && echo "$_info OK" || echo "$_info KO"
    } || {
        echo "$_info TODO"
    }
done

# clean
[ "$get_arg_clean" = yes ] && {
    for ((_i=0; _i<${#TEST_IMPORT_CSV_FILES[@]}; _i++)); do
        table_name=$(get_file_name --file_path "${TEST_IMPORT_CSV_DELIMITERS[$_i]}")
        execute_query \
            --name "DROP_${table_name}" \
            --query "DROP TABLE IF EXISTS ${table_name}"
        rm --force $POW_DIR_TMP/${TEST_IMPORT_CSV_FILES[$_i]}
    done
}

exit $SUCCESS_CODE
