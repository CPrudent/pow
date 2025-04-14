#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build addresses (for all available countries)

bash_args \
    --args_p '
        force:Forcer le traitement même si celui-ci a déjà été fait;
    ' \
    --args_v '
        force:yes|no;
    ' \
    --args_d '
        force:no;
    ' \
    "$@" || exit $?

force="$get_arg_force"
set_env --schema_name public &&
log_info "Publication des adresses"

execute_query \
    --name SET_ADDRESS \
    --query "SELECT public.set_address('$force'='yes')" || exit $ERROR_CODE

exit $SUCCESS_CODE
