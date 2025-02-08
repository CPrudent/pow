#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests GETOPT-functions

_getopt=${1:-POW_ARGV}

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

    # no process, only getopt
    return $SUCCESS_CODE
}

# pass only parameters, and see returns after
t_pow_argv_1() {
    pow_argv \
        --args_n '
            mandatory:paramètre obligatoire;
            optional_w_d:paramètre optionnel (avec valeur par défaut);
            optional_wo_d:paramètre optionnel (sans valeur par défaut)
        ' \
        --args_m '
            mandatory
        ' \
        --args_v '
            optional_w_d:no|yes
        ' \
        --args_d '
            optional_w_d:no
        ' \
        "$@" || return $ERROR_CODE

    # no process, only getopt
    return $SUCCESS_CODE
}

declare -A argv
errors=0
case "${_getopt^^}" in
BASH_ARGS)
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
    t_bash_args_1 --mandatory TEST3 --optional_wo_d VALUE --bash_args_argv argv &&
    declare -p argv
    ;;
POW_ARGV)
    echo "TEST1 (help)" &&
    t_pow_argv_1 --help &&
    echo "TEST2 (mandatory)" &&
    {
        # mandatory missing
        t_pow_argv_1 --optional_w_d yes && errors=$((errors +1))
        true
    } &&
    echo "TEST3 (check default value)" &&
    {
        t_pow_argv_1 --mandatory TEST3 && {
            echo "default= ${POW_ARGV[OPTIONAL_W_D]}"
            [ "${POW_ARGV[OPTIONAL_W_D]}" != no ] && errors=$((errors +1))
            true
        }
    }
    echo "TEST4 (print default hash)" &&
    t_pow_argv_1 --mandatory TEST4 --optional_wo_d VALUE4 &&
    declare -p POW_ARGV &&
    echo "TEST5 (print user hash)" &&
    t_pow_argv_1 --mandatory TEST5 --optional_wo_d VALUE5 --optional_w_d yes --pow_argv argv &&
    declare -p argv

    echo "TEST6 (w/o value, as boolean)" &&
    t_pow_argv_1 --args_p 'RESET:yes' --mandatory TEST6 --optional_w_d --optional_wo_d &&
    declare -p POW_ARGV
    ;;
esac

exit $SUCCESS_CODE
