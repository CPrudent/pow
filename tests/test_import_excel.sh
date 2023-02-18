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

_info="Conversion (EXCEL, DEFAULT) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DOR_ROOT/tests/data/test_spreadsheet.xlsx \
        && echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (EXCEL, OUTPUT, TAB) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_ROOT/tests/data/test_spreadsheet.xlsx \
        --to_file_path $POW_DIR_TMP/test_spreadsheet.tab.csv \
        --delimiter TAB
        && echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (ODS, DEFAULT) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_ROOT/tests/data/test_spreadsheet.ods \
        && echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

_info="Conversion (ODS, OUTPUT, COLON) :"
[ "$get_arg_dry_run" = no ] && {
    excel_to_csv \
        --from_file_path $POW_DIR_ROOT/tests/data/test_spreadsheet.ods \
        --to_file_path $POW_DIR_TMP/test_spreadsheet.colon.csv \
        --delimiter COLON
        && echo "$_info OK" || echo "$_info KO"
} || {
    echo "$_info TODO"
}

exit $SUCCESS_CODE
