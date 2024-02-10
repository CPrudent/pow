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
        number_line:Ajouter le numéro de chaque test;
        limit:Limiter la requête
    ' \
    --args_o '
        test
    ' \
    --args_v '
        test:SPLIT|DESCRIPTORS_DIFF|DESCRIPTORS_LIST|DESCRIPTORS_CASE|NAME_DIFF|NAME_LIST|NAME_CASE;
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
            _descriptor=${TEST_A4S_DESCRIPTORS[$_i]}
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
                    $([ -n "$get_arg_limit" ] && echo ' LIMIT '$get_arg_limit)
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
        echo -n "nom='${TEST_A4S_NAME[$_i]}' descripteur='${TEST_A4S_DESCRIPTORS[$_i]}' : "
        [ "$_normalize" = "${TEST_A4S_DESCRIPTORS[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            echo " descripteur $_normalize : ${TEST_A4S_DESCRIPTORS[$_i]}"
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
    declare -a _TEST_A4S_DESCRIPTORS=(
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
        echo -n "nom='${_TEST_A4S_NAME[$_i]}' descripteur='${_TEST_A4S_DESCRIPTORS[$_i]}' : "
        [ "$_normalize" = "${_TEST_A4S_DESCRIPTORS[$_i]}" ] && {
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

[ "$get_arg_test" = NAME_DIFF ] && {
    execute_query \
        --name NAME_DIFF \
        --query "
            COPY (
                WITH
                nn_words AS (
                    SELECT
                        nn.nwords
                        , nn.name_as_words
                        , nn.descriptors_as_words
                        , nn.name_normalized_as_words name_normalized_as_words_pow
                        , nn.descriptors_normalized_as_words descriptors_normalized_as_words_pow
                        , aw.name_normalized_as_words name_normalized_as_words_laposte
                        , aw.descriptors_normalized_as_words descriptors_normalized_as_words_laposte
                        , u.name
                    FROM
                        fr.laposte_address_street_uniq u
                            CROSS JOIN fr.normalize_street_name(u.name) nn
                            CROSS JOIN fr.normalize_name_get_as_words(
                                name_normalized => u.name_normalized
                                , name_as_words => nn.name_as_words
                                , name_abbreviated_as_words => nn.name_abbreviated_as_words
                                , descriptors_as_words => nn.descriptors_as_words
                                , nwords => nn.nwords
                            ) aw
                    WHERE
                        u.name_normalized IS NOT NULL
                    $([ -n "$get_arg_limit" ] && echo ' LIMIT '$get_arg_limit)
                )
                SELECT
                    dnn.differences
                    , name
                    , name_normalized_as_words_laposte
                    , name_normalized_as_words_pow
                FROM
                    nn_words nn
                        CROSS JOIN                         fr.get_differences_between_normalized_name(
                            name_as_words => name_as_words
                            , descriptors_as_words => descriptors_as_words
                            , nwords => nwords
                            , reference_name_normalized_as_words => name_normalized_as_words_laposte
                            , reference_descriptors_normalized_as_words => descriptors_normalized_as_words_laposte
                            , other_name_normalized_as_words => name_normalized_as_words_pow
                            , other_descriptors_normalized_as_words => descriptors_normalized_as_words_pow
                        ) dnn
                WHERE
                    ARRAY_TO_STRING(descriptors_normalized_as_words_pow, '') IS DISTINCT FROM ARRAY_TO_STRING(descriptors_normalized_as_words_laposte, '')
                ORDER BY
                    1
            ) TO STDOUT WITH (DELIMITER E',', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
            " \
        --output $POW_DIR_TMP/normalized_name_diff.txt || exit $ERROR_CODE

    exit $SUCCESS_CODE
}

[ "$get_arg_test" = NAME_LIST ] && {
    set_log_echo no
    for ((_i=0; _i < ${#TEST_A4S_NAME[*]}; _i++)); do
        execute_query \
        --name NAME_LIST \
        --query "
            SELECT ARRAY_TO_STRING(name_normalized_as_words, ' '), ARRAY_TO_STRING(descriptors_normalized_as_words, '')
            FROM fr.normalize_street_name(
                name => '${TEST_A4S_NAME[$_i]}'
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/NAME_LIST.error.log
            exit $ERROR_CODE
        }

        _tmp=${_normalize// /:}
        _array=(${_tmp//|/ })
        _array[0]=${_array[0]//:/ }
        _array[1]=${_array[1]//:/ }

        [ "$get_arg_number_line" = yes ] && { echo_number_line $TEST_A4S_SZ $((_i +1)); }
        echo -n "nom='${TEST_A4S_NAME[$_i]}' : "
        [ "${_array[0]}" = "${TEST_A4S_NAME_NORMALIZED[$_i]}" ] &&
        [ "${_array[1]}" = "${TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            [ "${_array[0]}" != "${TEST_A4S_NAME_NORMALIZED[$_i]}" ] && echo " nom ${_array[0]} : ${TEST_A4S_NAME_NORMALIZED[$_i]}"
            [ "${_array[1]}" != "${TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}" ] && echo " descripteur ${_array[1]} : ${TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}"
        }
    done

    echo
    echo "Avec succès: $_ok"
    echo "Avec erreur: $_ko"

    [ $_ko -gt 0 ] && _rc=$ERROR_CODE || _rc=$SUCCESS_CODE
    exit $_rc
}

[ "$get_arg_test" = NAME_CASE ] && {
    declare -a _TEST_A4S_NAME=(
        # abbr at the end (N|P)
        'CHEMIN VICINAL 5 DE BRATEAU A LA GARE DE BOURAY'                       #  1
        'RESTAURANT BON APPETIT SAINT VINCENT DURFORT'                          #  2
        'QUARTIER LES CHENELETTES DE SAINT JULIEN EN SAINT ALBAN'               #  3
        # restore full type (previously abbreviated) at the end
        'MAISON RETRAITE VAGUEMESTRE ROSERAIE'                                  #  4
        'AVENUE DU CORPS EXPEDITIONNAIRE FRANCAIS EN ITALIE'                    #  5
        # no abbr firstname but delete article
        'CHEMIN ROMAIN D ARLES A SAINT REMY'                                    #  6
        # w/ abbr BAS/BASSE, ARC/ARCADES
        'IMPASSE DES BAS DE SAINTE RADEGONDE'                                   #  7
        'LOTISSEMENT JEANNE D ARC LES BONNETTES'                                #  8
        # abbr title
        'LE COTEAU DE SAINT DENIS DU TERTRE'                                    #  9
        # abbr type
        'CHEMIN DE LA SOUS STATION DE TIVERNON'                                 # 10
        # abbr name (at the end, N|P) again!
        'RUE NATIONALE AVENUE DU PRESIDENT FRANCOIS MITTERRAND'                 # 11
        'ROUTE DE BEAUCAIRE ROUTE DEPARTEMENTALE 15'                            # 12
        'VOIE COMMUNALE NUMERO 5 DE BEAUTHEIL A COULOMMIERS'                    # 13
        #
        'PLACE DES ANCIENS COMBATTANTS D AFRIQUE DU NORD 1952 1962'             # 14
        'RUE DU 1ER REGIMENT DE CHASSEURS PARACHUTISTES'                        # 15
        'PLACE DU MONUMENT AUX MORTS 11 NOVEMBRE 1918'                          # 16
        'SQUARE DES ECRIVAINS COMBATTANTS MORTS POUR LA FRANCE'                 # 17
        'ALLEE DE L ABBAYE NOTRE DAME DU GRAND MARCHE'                          # 18
        # typo, but!
        'ZONE ARTISANALE ZAC DU PRE DE PAQUES'                                  # 19
        #
        'RUE DE LA ZONE D AMENAGEMENT CONCERTE'                                 # 20

    )
    declare -a _TEST_A4S_DESCRIPTORS=(
        VVCANAATAN                                                              #  1
        TNNTPN                                                                  #  2
        VANATPATN                                                               #  3
        TNNN                                                                    #  4
        VANNTAN                                                                 #  5
        VPANATN                                                                 #  6
        VATATN                                                                  #  7
        VPANAN                                                                  #  8
        ATATPAN                                                                 #  9
        VAANTAN                                                                 # 10
        VNTATPN                                                                 # 11
        VANTNC                                                                  # 12
        VNNCANAN                                                                # 13
        VANNANANCC                                                              # 14
        VACTANN                                                                 # 15
        VANANCNC                                                                # 16
        VATNNNAN                                                                # 17
        VAATTTATN                                                               # 18
        VVNATAN                                                                 # 19
        VAANNNN                                                                 # 20
    )

    declare -a _TEST_A4S_NAME_NORMALIZED=(
        #         1         2         3
        #12345678901234567890123456789012
        'CHEMIN VICINAL 5 B GARE BOURAY'                                        #  1
        'RESTAURANT B A ST V DURFORT'                                           #  2
        'QUA CHENELETTES ST J ST ALBAN'                                         #  3
        'MAISON R VAGUEMESTRE ROSERAIE'                                         #  4
        'AV C EXPEDITIONNAIRE FR ITALIE'                                        #  5
        'CHEMIN ROMAIN ARLES A SAINT REMY'                                      #  6
        'IMPASSE DES BAS SAINTE RADEGONDE'                                      #  7
        'LOTISSEMENT JEANNE ARC BONNETTES'                                      #  8
        'LE COTE DE SAINT DENIS DU TERTRE'                                      #  9
        'CHE LA SOUS STATION DE TIVERNON'                                       # 10
        'R NATIONALE AV PDT F MITTERRAND'                                       # 11
        'ROUTE BEAUCAIRE RTE D 15'                                              # 12
        'VOIE C N 5 BEAUTHEIL COULOMMIERS'                                      # 13
        'PL A COMB AFRIQUE NORD 1952 1962'                                      # 14
        'RUE 1ER RGT C PARACHUTISTES'                                           # 15
        'PLACE M MORTS 11 NOVEMBRE 1918'                                        # 16
        'SQ ECRIV COMB MORTS POUR FRANCE'                                       # 17
        'ALL DE ABBAYE ND DU GRAND MARCHE'                                      # 18
        'ZA ZAC DU PRE DE PAQUES'                                               # 19
        'R LA ZONE D AMENAGEMENT CONCERTE'                                      # 20
    )

    declare -a _TEST_A4S_DESCRIPTORS_NORMALIZED=(
        VVCNTN                                                                  #  1
        TNNTPN                                                                  #  2
        VNTPTN                                                                  #  3
        TNNN                                                                    #  4
        VNNTN                                                                   #  5
        VPNATN                                                                  #  6
        VATTN                                                                   #  7
        VPNN                                                                    #  8
        ATATPAN                                                                 #  9
        VANTAN                                                                  # 10
        VNTTPN                                                                  # 11
        VNTNC                                                                   # 12
        VNNCNN                                                                  # 13
        VNNNNCC                                                                 # 14
        VCTNN                                                                   # 15
        VNNCNC                                                                  # 16
        VTNNNN                                                                  # 17
        VATTATN                                                                 # 18
        VNATAN                                                                  # 19
        VANNNN                                                                  # 20
    )

    set_log_echo no
    for ((_i=0; _i < ${#_TEST_A4S_NAME[*]}; _i++)); do
        execute_query \
        --name NAME_CASE \
        --query "
            SELECT ARRAY_TO_STRING(name_normalized_as_words, ' '), ARRAY_TO_STRING(descriptors_normalized_as_words, '')
            FROM fr.normalize_street_name(
                name => '${_TEST_A4S_NAME[$_i]}'
            )
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _normalize || {
            cat $POW_DIR_ARCHIVE/NAME_CASE.error.log
            exit $ERROR_CODE
        }

        _tmp=${_normalize// /:}
        _array=(${_tmp//|/ })
        _array[0]=${_array[0]//:/ }
        _array[1]=${_array[1]//:/ }

        [ "$get_arg_number_line" = yes ] && { echo_number_line ${#_TEST_A4S_NAME[*]} $((_i +1)); }
        echo -n "nom='${_TEST_A4S_NAME[$_i]}' : "
        [ "${_array[0]}" = "${_TEST_A4S_NAME_NORMALIZED[$_i]}" ] &&
        [ "${_array[1]}" = "${_TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}" ] && {
            echo 'OK'
            _ok=$((_ok +1))
        } || {
            echo 'KO'
            _ko=$((_ko +1))
            [ "${_array[0]}" != "${_TEST_A4S_NAME_NORMALIZED[$_i]}" ] && echo " nom ${_array[0]} : ${_TEST_A4S_NAME_NORMALIZED[$_i]}"
            [ "${_array[1]}" != "${_TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}" ] && echo " descripteur ${_array[1]} : ${_TEST_A4S_DESCRIPTORS_NORMALIZED[$_i]}"
        }
    done

    echo
    echo "Avec succès: $_ok"
    echo "Avec erreur: $_ko"

    [ $_ko -gt 0 ] && _rc=$ERROR_CODE || _rc=$SUCCESS_CODE
    exit $_rc
}
