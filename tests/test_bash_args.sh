#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests bash_args

# pass only parameters, and see global vars (get_arg_*) after
t_bash_args_1() {
    bash_args \
        --args_p '
            mandatory:paramètre obligatoire;
            optional_w_d:paramètre optionnel (avec valeur par défaut);
            optional_wo_d:paramètre optionnel (sans valeur par défaut);
        ' \
        --args_o '
            mandatory
        ' \
        --args_v '
            optional_w_d:no|yes
        ' \
        --args_d '
            optional_w_d:no
        ' \
        "$@" || return $ERROR_CODE

    # no process
    return $SUCCESS_CODE
}

# test calling twice, and behavior on global vars
t_bash_args_1 --mandatory TEST1 --optional_wo_d OPT_WO_D &&
echo "mandatory= ($get_arg_mandatory)" &&
echo "optional_w_d= ($get_arg_optional_w_d)" &&
echo "optional_wo_d= ($get_arg_optional_wo_d)" &&
t_bash_args_1 --mandatory TEST2 --optional_w_d yes &&
echo "mandatory= ($get_arg_mandatory)" &&
echo "optional_w_d= ($get_arg_optional_w_d)" &&
echo "optional_wo_d= ($get_arg_optional_wo_d)"

# test built-in hash to get results (w/o get_arg_*)
declare -A argv
t_bash_args_1 --mandatory TEST3 --optional_wo_d VALUE --bash_args_argv argv &&
declare -p argv

exit $SUCCESS_CODE
