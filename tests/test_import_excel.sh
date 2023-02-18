#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests imports SPREADSHEET

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
    "$@" || exit $ERROR_CODE

set_env --schema_code public

cp $POW_DIR_ROOT/tests/data/test_spreadsheet.{xlsx,ods} $POW_DIR_TMP

_info="Conversion (EXCEL, DEFAULT) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_TMP/test_spreadsheet.xlsx &&
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (EXCEL, OUTPUT, TAB) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_TMP/test_spreadsheet.xlsx \
        --to_file_path $POW_DIR_TMP/test_spreadsheet-tab.txt \
        --delimiter TAB &&
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (EXCEL, STDOUT, PIPE) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_TMP/test_spreadsheet.xlsx \
        --to_file_path STDOUT \
        --delimiter PIPE &&
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (CSV, DEFAULT) :"
[ "$get_arg_dry_run" = no ] && {
    csv_to_excel \
        --from_file_path $POW_DIR_TMP/test_spreadsheet.xlsx.csv \
        --to_file_path $POW_DIR_TMP/test_spreadsheet2.xlsx
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (ODS, DEFAULT) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_TMP/test_spreadsheet.ods &&
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (ODS, OUTPUT, COLON) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_TMP/test_spreadsheet.ods \
        --to_file_path $POW_DIR_TMP/test_spreadsheet-colon.txt \
        --delimiter COLON &&
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Import (EXCEL, DEFAULT) :"
[ "$get_arg_dry_run" = no ] && {
    import_excel_file \
        --file_path $POW_DIR_TMP/test_spreadsheet.xlsx \
        --table_columns HEADER_TO_LOWER_CODE &&
    echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

# clean
[ "$get_arg_clean" = yes ] && {
    rm $POW_DIR_TMP/test_spreadsheet.*

    table_name=$(get_file_name --file_path "$POW_DIR_TMP/test_spreadsheet.xlsx")
    execute_query \
        --name "DROP_${table_name}" \
        --query "DROP TABLE IF EXISTS ${table_name}"
}

exit $SUCCESS_CODE
