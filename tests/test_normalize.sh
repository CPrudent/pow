#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests normalize
    #   SPLIT: fr.split_name_of_street_as_descriptor()
    #   DESCRIPTOR_DIFF: fr.get_descriptor_of_street(), for ALL
    #   DESCRIPTOR: fr.get_descriptor_of_street()

source $POW_DIR_ROOT/tests/data/test_normalize-data.sh || exit $ERROR_CODE


echo_number_line() {
    printf "%*d: " ${#TEST_A4S_SZ} $1
}

bash_args \
    --args_p '
        test:Lister les communes même si elles possèdent déjà des altitudes;
        number_line:Ajouter le numéro de chaque test
    ' \
    --args_o '
        test
    ' \
    --args_v '
        test:SPLIT|DESCRIPTOR_DIFF|DESCRIPTOR;
        number_line:no|yes
    ' \
    --args_d '
        number_line:no
    ' \
    "$@" || exit $ERROR_CODE

_ok=0
_ko=0

[ "$get_arg_test" = SPLIT ] && {
    declare -a _array
    set_log_echo no
    for ((_i=0; _i < ${#TEST_A4S_NAME[*]}; _i++)); do
        [ ${#TEST_A4S_NAME_NORMALIZED[$_i]} -gt 0 ] && {
            _name=${TEST_A4S_NAME_NORMALIZED[$_i]}
            _descriptor=${TEST_A4S_DESCRIPTOR_NORMALIZED[$_i]}
            _split_name=${TEST_A4S_SPLIT_NAME_NORMALIZED[$_i]}
            _split_descriptor=${TEST_A4S_SPLIT_DESCRIPTOR_NORMALIZED[$_i]}
            _is_normalized=1
        } || {
            _name=${TEST_A4S_NAME[$_i]}
            _descriptor=${TEST_A4S_DESCRIPTOR[$_i]}
            _split_name=${TEST_A4S_SPLIT_NAME[$_i]}
            _split_descriptor=${TEST_A4S_SPLIT_DESCRIPTOR[$_i]}
            _is_normalized=0
        }
        execute_query \
        --name NORMALIZE_STREET_NAME \
        --query "
            SELECT words, descriptors FROM fr.split_name_of_street_as_descriptor(
                name => '$_name'
                , descriptor => '$_descriptor'
                $([ $_is_normalized -eq 1 ] && echo ', is_normalized => TRUE')
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/NORMALIZE_STREET_NAME.error.log
            exit $ERROR_CODE
        }

        #echo "($_normalize)"
        _tmp=${_normalize// /:}
        _array=(${_tmp//|/ })
        _array[0]=${_array[0]//:/ }
        _array[1]=${_array[1]//:/ }
        #declare -p _array
        [ "$get_arg_number_line" = yes ] && { echo_number_line $((_i +1)); }
        echo -n "nom='$_name' descripteur='$_descriptor' : "
        [ "${_array[0]}" = "$_split_name" ] &&
        [ "${_array[1]}" = "$_split_descriptor" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            [ "${_array[0]}" != "$_split_name" ] && echo " nom ${_array[0]} : $_split_name"
            [ "${_array[1]}" != "$_split_descriptor" ] && echo " descripteur ${_array[1]}) : $_split_descriptor"
        }
    done

    echo
    echo "Avec succès: $_ok"
    echo "Avec erreur: $_ko"

    [ $_ko -gt 0 ] && _rc=$ERROR_CODE || _rc=$SUCCESS_CODE
    exit $_rc
}

[ "$get_arg_test" = DESCRIPTOR_DIFF ] && {
    execute_query \
        --name DESCRIPTOR_DIFF \
        --query "
            COPY (
                WITH
                descriptors AS (
                    SELECT
                        fr.get_descriptor_of_street(name) AS descriptor_pow
                        , descriptors AS descriptor_laposte
                        , name
                    FROM
                        fr.laposte_address_street_uniq
                )
                SELECT
                    UNNEST(
                        fr.get_differences_between_descriptors(
                            reference => descriptor_laposte
                            , other => descriptor_pow
                        )
                    ) descriptor_diff
                    , descriptor_laposte
                    , descriptor_pow
                    , name
                FROM
                    descriptors
                WHERE
                    descriptor_pow IS DISTINCT FROM descriptor_laposte
                ORDER BY
                    1
            ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
            " \
        --output $POW_DIR_TMP/descriptors_diff.txt || exit $ERROR_CODE

    exit $SUCCESS_CODE
}

[ "$get_arg_test" = DESCRIPTOR ] && {
    set_log_echo no
    for ((_i=0; _i < ${#TEST_A4S_NAME[*]}; _i++)); do
        execute_query \
        --name NORMALIZE_STREET_DESCRIPTOR \
        --query "
            SELECT fr.get_descriptor_of_street(
                name => '${TEST_A4S_NAME[$_i]}'
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/NORMALIZE_STREET_DESCRIPTOR.error.log
            exit $ERROR_CODE
        }

        [ "$get_arg_number_line" = yes ] && { echo_number_line $((_i +1)); }
        echo -n "nom='${TEST_A4S_NAME[$_i]}' descripteur='${TEST_A4S_DESCRIPTOR[$_i]}' : "
        [ "$_normalize" = "${TEST_A4S_DESCRIPTOR[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            echo " descripteur $_normalize : ${TEST_A4S_DESCRIPTOR[$_i]}"
        }
    done

    echo
    echo "Avec succès: $_ok"
    echo "Avec erreur: $_ko"

    [ $_ko -gt 0 ] && _rc=$ERROR_CODE || _rc=$SUCCESS_CODE
    exit $_rc
}
