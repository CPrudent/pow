#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build FR territories (INSEE)

bash_args \
    --args_p "
        force:Forcer le traitement même si celui-ci a déjà été fait
    " \
    --args_v '
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    "$@" || exit $ERROR_CODE

force="$get_arg_force"
set_env --schema_name fr &&
$POW_DIR_BATCH/territory_insee_hierarchy.sh --force $force &&
$POW_DIR_BATCH/territory_insee_event.sh --force $force || exit $ERROR_CODE

exit $SUCCESS_CODE
