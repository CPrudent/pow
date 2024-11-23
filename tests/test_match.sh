#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests match

set_context() {
    bash_args \
        --args_p "
            test:Préciser le test à réaliser;
        " \
        --args_o '
            test;
        ' \
        "$@" || return $ERROR_CODE

    case "$get_arg_test" in
    STANDARDIZE_FROM_FILE)
        expect file $POW_DIR_ROOT/tests/data/test_match_file.csv &&
        expect file $POW_DIR_ROOT/tests/data/test_match_file_format.sql &&
        expect file $POW_DIR_ROOT/tests/data/test_match_parameters.sql || return $ERROR_CODE
        ;;
    STANDARDIZE_FROM_TABLE|STANDARDIZE_FROM_QUERY)
        expect file $POW_DIR_ROOT/tests/data/test_match_table.csv &&
        expect file $POW_DIR_ROOT/tests/data/test_match_table_format.sql &&
        import_file \
            --file_path "$POW_DIR_ROOT/tests/data/test_match_table.csv" \
            --schema_name fr \
            --table_name test_match_table_1 \
            --load_mode OVERWRITE_TABLE || return $ERROR_CODE
        ;;
    esac

    return $SUCCESS_CODE
}

bash_args \
    --args_p '
        test:Préciser le test à réaliser;
        force:Forcer le traitement même si celui-ci a déjà été fait
    ' \
    --args_o '
        test
    ' \
    --args_v '
        test:STANDARDIZE_FROM_FILE|STANDARDIZE_FROM_TABLE|STANDARDIZE_FROM_QUERY;
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

set_env --schema_name fr &&
set_context --test "$get_arg_test" &&
case "$get_arg_test" in
STANDARDIZE_FROM_FILE)
    $POW_DIR_ROOT/bin/fr/address_match.sh \
        --source_name $POW_DIR_ROOT/tests/data/test_match_file.csv \
        --steps IMPORT,STANDARDIZE \
        --verbose yes \
        --format $POW_DIR_ROOT/tests/data/test_match_file_format.sql \
        --parameters $POW_DIR_ROOT/tests/data/test_match_parameters.sql \
        --force $get_arg_force
    ;;
STANDARDIZE_FROM_TABLE)
    $POW_DIR_ROOT/bin/fr/address_match.sh \
        --source_name test_match_table_1 \
        --steps IMPORT,STANDARDIZE \
        --verbose yes \
        --format $POW_DIR_ROOT/tests/data/test_match_table_format.sql \
        --force $get_arg_force
    ;;
STANDARDIZE_FROM_QUERY)
    $POW_DIR_ROOT/bin/fr/address_match.sh \
        --source_name test_match_query_1 \
        --steps IMPORT,STANDARDIZE \
        --verbose yes \
        --format $POW_DIR_ROOT/tests/data/test_match_table_format.sql \
        --source_filter "municipality = 'ANTIBES'" \
        --source_query 'SELECT rowid,code,complement,housenumber,street,municipality_old,postcode,municipality FROM fr.test_match_table_1' \
        --force $get_arg_force
    ;;
esac
