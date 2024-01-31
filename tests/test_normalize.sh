#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests normalize
    #   SPLIT: fr.split_name_of_street_as_descriptor()
    #   DESCRIPTORS_DIFF: fr.get_descriptors_of_street(), for ALL
    #   DESCRIPTORS_LIST: fr.get_descriptors_of_street(), for LIST
    #   DESCRIPTORS_CASE: fr.get_descriptors_of_street(), for USECASE

source $POW_DIR_ROOT/tests/data/test_normalize-data.sh || exit $ERROR_CODE


echo_number_line() {
    printf "%*d: " ${#1} $2
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
        test:SPLIT|DESCRIPTORS_DIFF|DESCRIPTORS_LIST|DESCRIPTORS_CASE;
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
            _descriptor=${TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}
            _split_name=${TEST_A4S_SPLIT_NAME_NORMALIZED[$_i]}
            _split_descriptor=${TEST_A4S_SPLIT_DESCRIPTORS_NORMALIZED[$_i]}
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
                , descriptors_in => '$_descriptor'
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
        [ "$get_arg_number_line" = yes ] && { echo_number_line $TEST_A4S_SZ $((_i +1)); }
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

[ "$get_arg_test" = DESCRIPTORS_DIFF ] && {
    execute_query \
        --name DESCRIPTORS_DIFF \
        --query "
            COPY (
                WITH
                descriptors AS (
                    SELECT
                        ds.descriptors descriptor_pow
                        , u.descriptors descriptor_laposte
                        , u.name
                    FROM
                        fr.laposte_address_street_uniq u
                            CROSS JOIN fr.get_descriptors_of_street(u.name) ds
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

[ "$get_arg_test" = DESCRIPTORS_LIST ] && {
    set_log_echo no
    for ((_i=0; _i < ${#TEST_A4S_NAME[*]}; _i++)); do
        execute_query \
        --name DESCRIPTORS_LIST \
        --query "
            SELECT descriptors FROM fr.get_descriptors_of_street(
                name => '${TEST_A4S_NAME[$_i]}'
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/DESCRIPTORS_LIST.error.log
            exit $ERROR_CODE
        }

        [ "$get_arg_number_line" = yes ] && { echo_number_line $TEST_A4S_SZ $((_i +1)); }
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

[ "$get_arg_test" = DESCRIPTORS_CASE ] && {
    declare -a _TEST_A4S_NAME=(
        # successive titles (two of one word, one of two words)
        'LE PETIT HAUT CHEMIN'                                                  #  1
        'PLACE DE LA 32E DIVISION INFANTERIE'                                   #  2
        'PLACE DE L EGLISE NOTRE DAME'                                          #  3
        'LA MAISON FORESTIERE DE LA BORNE'                                      #  4
        # wrong number
        'IMMEUBLE MC DO'                                                        #  5
        'RUE DU CM'                                                             #  6
        # ok number (road_network)
        'ROUTE DEPARTEMENTALE 151E1'                                            #  7
        'CHEMIN DEPARTEMENTAL CD15E'                                            #  8
        'CHEMIN DEPARTEMENTAL 34E7'                                             #  9
        'PISTE FORESTIERE T28 BARBEIRANNE'                                      # 10
        'DEPARTEMENTALE 106 E2'                                                 # 11
        'IMPASSE DE LA BRUYERE C3'                                              # 12
        'ROND POINT DE SIAILLES G6'                                             # 13
        # exception only if len(descriptor) = 1
        'RUE DU SOUS PREFET BARRE'                                              # 14
        # ending REGEX '(T|V)+C*$'
        'PASSAGE A NIVEAU PASSAGE A NIVEAU 67'                                  # 15
        'IMPASSE DU PASSAGE A NIVEAU 7'                                         # 16
        'GRAND ANSE 2'                                                          # 17
        'ALLEE DES GRANDS BOIS 1'                                               # 18
        'LIEU DIT LA TOUR BAS I'                                                # 19
        # wrong number
        'RUE SAINT EVE'                                                         # 20
        # article exception
        'RUE DU SOUS LIEUTENANT DE POURTALES'                                   # 21
        'RUE DU SOUS PREFET BARRE'                                              # 22
        # focus on type (compared w/ title)
        'QUARTIER LA TOUR ET LES COMBES'                                        # 23
        # ending REGEX V+[CE]*$
        'GRANDE RUE PROLONGEE'                                                  # 24
        'CORNICHE SUPERIEURE'                                                   # 25
        'RUE 1954 1962'                                                         # 26
    )
    declare -a _TEST_A4S_DESCRIPTOR=(
        ATNN                                                                    #  1
        VAACTN                                                                  #  2
        VAATNN                                                                  #  3
        ATTAAN                                                                  #  4
        VNN                                                                     #  5
        VAN                                                                     #  6
        VNC                                                                     #  7
        VNC                                                                     #  8
        VNC                                                                     #  9
        NNCN                                                                    # 10
        NCC                                                                     # 11
        VAANC                                                                   # 12
        VVANC                                                                   # 13
        VATTN                                                                   # 14
        VVVNNNC                                                                 # 15
        VANNNC                                                                  # 16
        TNC                                                                     # 17
        VATNC                                                                   # 18
        VVATNC                                                                  # 19
        VTN                                                                     # 20
        VATTAN                                                                  # 21
        VATTN                                                                   # 22
        VATAAN                                                                  # 23
        NNE                                                                     # 24
        NE                                                                      # 25
        NCC                                                                     # 26
    )

    set_log_echo no
    for ((_i=0; _i < ${#_TEST_A4S_NAME[*]}; _i++)); do
        execute_query \
        --name DESCRIPTORS_CASE \
        --query "
            SELECT descriptors FROM fr.get_descriptors_of_street(
                name => '${_TEST_A4S_NAME[$_i]}'
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/DESCRIPTORS_CASE.error.log
            exit $ERROR_CODE
        }

        [ "$get_arg_number_line" = yes ] && { echo_number_line ${#_TEST_A4S_NAME[*]} $((_i +1)); }
        echo -n "nom='${_TEST_A4S_NAME[$_i]}' descripteur='${_TEST_A4S_DESCRIPTOR[$_i]}' : "
        [ "$_normalize" = "${_TEST_A4S_DESCRIPTOR[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            _ko=$((_ko +1))
            echo "<<<$_normalize>>>, KO"
        }
    done

    echo
    echo "Avec succès: $_ok"
    echo "Avec erreur: $_ko"

    [ $_ko -gt 0 ] && _rc=$ERROR_CODE || _rc=$SUCCESS_CODE
    exit $_rc
}
