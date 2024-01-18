#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests normalize
    #   SPLIT: fr.split_name_of_street_as_descriptor()
    #   DESCRIPTOR_DIFF: fr.get_descriptor_of_street()

source $POW_DIR_ROOT/tests/data/test_normalize-data.sh || exit $ERROR_CODE

bash_args \
    --args_p '
        test:Lister les communes même si elles possèdent déjà des altitudes;
    ' \
    --args_o '
        test
    ' \
    --args_v '
        test:SPLIT|DESCRIPTOR_DIFF|DESCRIPTOR
    ' \
    "$@" || exit $ERROR_CODE

_ok=0
_ko=0

[ "$get_arg_test" = SPLIT ] && {
    declare -a _array
    set_log_echo no
    for ((_i=0; _i < ${#TEST_STREET_NAME[*]}; _i++)); do
        [ ${#TEST_STREET_NAME_NORMALIZED[$_i]} -gt 0 ] && {
            _name=${TEST_STREET_NAME_NORMALIZED[$_i]}
            _is_normalized=1
        } || {
            _name=${TEST_STREET_NAME[$_i]}
            _is_normalized=0
        }
        execute_query \
        --name NORMALIZE_STREET_NAME \
        --query "
            SELECT words, descriptors FROM fr.split_name_of_street_as_descriptor(
                name => '$_name'
                , descriptor => '${TEST_STREET_DESCRIPTOR[$_i]}'
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
        echo -n "$((_i +1)): "
        echo -n "nom='$_name' codage='${TEST_STREET_DESCRIPTOR[$_i]}' : "
        [ "${_array[0]}" = "${SPLIT_NORMALIZED_NAME[$_i]}" ] &&
        [ "${_array[1]}" = "${SPLIT_NORMALIZED_DESCRIPTOR[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            [ "${_array[0]}" != "${SPLIT_NORMALIZED_NAME[$_i]}" ] && echo " nom ${_array[0]} : ${SPLIT_NORMALIZED_NAME[$_i]}"
            [ "${_array[1]}" != "${SPLIT_NORMALIZED_DESCRIPTOR[$_i]}" ] && echo " codage ${_array[1]}) : ${SPLIT_NORMALIZED_DESCRIPTOR[$_i]}"
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
                SELECT
                    descriptor_laposte
                    , descriptor_pow
                    , name
                FROM (
                    SELECT
                        fr.get_descriptor_of_street(name) AS descriptor_pow
                        , descriptors AS descriptor_laposte
                        , name
                    FROM
                        fr.laposte_address_street_uniq
                    ) t
                WHERE
                    descriptor_pow IS DISTINCT FROM descriptor_laposte
                ORDER BY
                    2
            ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
            " \
        --output $POW_DIR_TMP/descriptors_diff.txt || exit $ERROR_CODE

    exit $SUCCESS_CODE
}

[ "$get_arg_test" = DESCRIPTOR ] && {
    set_log_echo no
    for ((_i=0; _i < ${#TEST_STREET_NAME[*]}; _i++)); do
        execute_query \
        --name NORMALIZE_STREET_DESCRIPTOR \
        --query "
            SELECT fr.get_descriptor_of_street(
                name => '${TEST_STREET_NAME[$_i]}'
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/NORMALIZE_STREET_DESCRIPTOR.error.log
            exit $ERROR_CODE
        }

        echo -n "$((_i +1)): "
        [ "$_normalize" = "${TEST_STREET_DESCRIPTOR[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            echo " descripteur $_normalize : ${TEST_STREET_DESCRIPTOR[$_i]}"
        }
    done

    echo
    echo "Avec succès: $_ok"
    echo "Avec erreur: $_ko"

    [ $_ko -gt 0 ] && _rc=$ERROR_CODE || _rc=$SUCCESS_CODE
    exit $_rc
}
