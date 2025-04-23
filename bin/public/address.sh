#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build addresses (for all available countries)

pow_argv \
    --args_n '
        force:Forcer le traitement même si celui-ci a déjà été fait;
    ' \
    --args_v '
        force:yes|no;
    ' \
    --args_d '
        force:no;
    ' \
    --args_p '
        tag:force@bool
    ' \
    "$@" || exit $?

set_env --schema_name public &&
log_info "Publication des Adresses" &&
execute_query \
    --name SET_ADDRESS \
    --query "SELECT public.set_address('${POW_ARGV[FORCE]}'='yes')" || exit $ERROR_CODE

exit $SUCCESS_CODE
