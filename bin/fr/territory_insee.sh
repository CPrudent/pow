#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories (INSEE)

pow_argv \
    --args_n "
        force:Forcer le traitement même si celui-ci a déjà été fait
    " \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    --args_p '
        tag:force@bool
    ' \
    "$@" || exit $?

set_env --schema_name fr &&
$POW_DIR_BATCH/territory_insee_hierarchy.sh --force ${POW_ARGV[FORCE]} &&
$POW_DIR_BATCH/territory_insee_event.sh --force ${POW_ARGV[FORCE]} || exit $ERROR_CODE

exit $SUCCESS_CODE
