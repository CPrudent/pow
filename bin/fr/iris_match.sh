#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # match LAPOSTE municipality addresses w/ IRIS-GE
    #
    # NOTE script called by address_iris_ge_match.sh (w/ parallel)

declare -A global_vars=() &&
pow_argv \
    --args_n '
        municipality:Code Commune INSEE à traiter;
        mode:Mode de traitement;
        tmpdir:Dossier résultat du traitement;
        version:Version algorithme de Rapprochement IRIS-GE;
        iris_id:ID historique du référentiel IRIS-GE
    ' \
    --args_m '
        municipality;mode;tmpdir
    ' \
    --args_v '
        mode:INIT|DELTA
    ' \
    --args_d '
        mode:INIT
    ' \
    --pow_argv global_vars "$@" || exit $?

execute_query \
    --name "IRIS_MATCH_${global_vars[MUNICIPALITY]}" \
    --query "
        SELECT nrows FROM fr.set_laposte_address_match_iris_ge(
            municipality => '${global_vars[MUNICIPALITY]}',
            mode => '${global_vars[MODE]}',
            version => '${global_vars[VERSION]}',
            iris_id => ${global_vars[IRIS_ID]}
        )
    " \
    --output "${global_vars[TMPDIR]}/IRIS_${global_vars[MUNICIPALITY]}.dat" \
    --temporary UNIQ

# key= create ???
# 2025-05-16T15:59:25Z|error|85348|christophe|/data/devel/pow/bin/fr/iris_match.sh|create: Valeur unique attendue
# rc      command
# 3       /data/devel/pow/bin/fr/iris_match.sh 98812 INIT /data/app/pow/tmp/fr/82514 1.0 49822

# try w/o pow_argv, but same error!
# execute_query \
#     --name "IRIS_MATCH_$1" \
#     --temporary UNIQ \
#     --output "$3/IRIS_$1.dat" \
#     --query "
#         SELECT nrows FROM fr.set_laposte_address_match_iris_ge(
#             municipality => '$1',
#             mode => '$2',
#             version => '$4',
#             iris_id => $5
#         )
#     "

# error comes from get_tmp_file() w/o --args_p definition (create@bool) !

exit $?
