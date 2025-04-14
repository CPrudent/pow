    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define STD

    # HELP
    # https://stackoverflow.com/questions/11180714/how-to-iterate-over-an-array-using-indirect-reference
    # https://artificialworlds.net/blog/2012/10/17/bash-associative-array-examples/
    # <<<
    # https://stackoverflow.com/questions/7950268/what-does-the-bash-operator-i-e-triple-less-than-sign-mean

# best practices: see https://gist.github.com/outro56/4a2403ae8fefdeb832a5
set -o pipefail

    #
    # log
    #

set_log_active() {
    POW_LOG_ACTIVE=$1
}

set_log_echo() {
	POW_LOG_ECHO=$1
}

[ ! -f "$POW_DIR_LOG/$POW_LOG_FILE" ] && set_log_active no
[ -z "$POW_LOG_ACTIVE" ] && set_log_active yes
[ -z "$POW_LOG_ECHO" ] && set_log_echo yes

log() {
    local _severity=${1:-info}
    local _message=$(echo $2 | tr \| \_)
    local _state=$3
    local _command="$(realpath $0)"
    local _log_entry="$(date --utc +%FT%TZ)|$1|$$|${USER}|$_command|$_message"
    ([ "$POW_LOG_ECHO" = yes ] || [ "$_severity" = error ]) && {
        case "$_severity" in
        error)
            >&2 echo $_log_entry
            ;;
        *)
            echo $_log_entry
            ;;
        esac
    }
    [ "$POW_LOG_ACTIVE" = yes ] && echo $_log_entry >> $POW_DIR_LOG/$POW_LOG_FILE

    return $SUCCESS_CODE
}

log_info() {
    log info "$1" "$2"

    return $?
}

log_error() {
    log error "$1" "$2"

    return $?
}

    #
    # general
    #

# print expression and wait ENTER
breakpoint() {
    echo "###BREAK($1)"
    read
}

# expect expression
# in: $1=(argc, isnum, env, file) as (#args, is numeric, variable is defined, file exists)
expect() {
    local _ctrl _var

    case $1 in

        # expect argc func $# nr_args
    argc)
        if [ $3 -ne $4 ]; then
            log_error "$2: argument ($3/$4)"
            return $ERROR_CODE
        fi
        ;;

        # expect isnum <data>
    isnum)
        expect argc isnum $# 2 || return $ERROR_CODE

            # Search for none numeric character (return null if not found)
        _ctrl=$(expr "$2" : '.*\([^0-9]\).*' \| "")
        if [ ! -z "$_ctrl" ]; then
            log_error "variable non numerique ($2)"
            return $ERROR_CODE
        fi
        ;;

        # expect env <var>
    env)
        expect argc env $# 2 || return $ERROR_CODE

        _var=$(eval echo \${$2})
        [ -z "$_var" ] && {
            log_error "manque variable ($2)"
            return $ERROR_CODE
        }
        ;;

        # expect file <path>
    file)
        expect argc env $# 2 || return $ERROR_CODE

        [ ! -f "$2" ] && {
            log_error "manque fichier ($2)"
            return $ERROR_CODE
        }
        ;;

    esac

    return $SUCCESS_CODE
}

# build index on array w/ an associative index array (key=value, value=position)
array_index() {
    local -A _opts &&
    pow_argv \
        --args_n '
            array:Tableau;
            index:Index du tableau
        ' \
        --args_m '
            array;
            index
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _array_ref=${_opts[ARRAY]}
    local -n _index_ref=${_opts[INDEX]}
    local _i

    for _i in "${!_array_ref[@]}"; do
        _index_ref["${_array_ref[$_i]}"]=$_i
    done

    return $SUCCESS_CODE
}

# item in array
# https://stackoverflow.com/questions/8082947/how-to-pass-an-array-to-a-bash-function
# another solution w/ print
# https://stackoverflow.com/questions/3685970/check-if-a-bash-array-contains-a-value
in_array() {
    local -A _opts &&
    pow_argv \
        --args_n '
            array:Tableau;
            item:Elèment recherché;
            search:Recherche Clé/Valeur;
            position:Position de l élément trouvé
        ' \
        --args_m '
            array;
            item
        ' \
        --args_v '
            search:KEY|VALUE
        ' \
        --args_d '
            search:VALUE
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _array_ref=${_opts[ARRAY]}
    local _pos _rc

    [ ${#_array_ref[@]} -eq 0 ] && return 1
    case "${_opts[SEARCH]}" in
    VALUE)
        _pos=$(printf '%s\n' "${_array_ref[@]}" | grep --fixed-strings --line-number --line-regexp -- "${_opts[ITEM]}")
        ;;
    KEY)
        _pos=$(printf '%s\n' "${!_array_ref[@]}" | grep --fixed-strings --line-number --line-regexp -- "${_opts[ITEM]}")
        ;;
    esac
    _rc=$?
    # returning position (useful for array, but not for hash)
    [ -n "${_opts[POSITION]}" ] && {
        local -n _pos_ref=${_opts[POSITION]}
        # exists item ?
        [ $_rc -eq 0 ] && {
            _pos_ref=${_pos%:*}
            # decrement because grep is 1-base
            ((--_pos_ref))
            return 0
        } || {
            _pos_ref=-1
        }
    }

    # found
    [ $_rc -eq 0 ] && return 0
    # not found
    return 1
}


clone_array() {
    local -A _opts &&
    pow_argv \
        --args_n '
            from_array:Tableau à cloner;
            to_array:Tableau cloné
        ' \
        --args_m '
            from_array;
            to_array
        ' \
        --pow_argv _opts "$@" || return $?

#     local _tmp=$(declare -p ${_opts[FROM_ARRAY]}) _tmp2
# syntax error near unexpected token `('
#     echo "$_tmp"
#     echo $_to_array_ref
#     _tmp2="${_tmp/${_opts[FROM_ARRAY]}=/${_to_array_ref}=}"
#     echo $_tmp2
#     eval $_tmp2

    local -n _to_array_ref=${_opts[TO_ARRAY]}
    local _indirect

    # https://unix.stackexchange.com/questions/280006/copy-array-with-array-name-inside-string-in-bash
    _indirect=${_opts[FROM_ARRAY]}[@]
    _to_array_ref=("${!_indirect}")

    return $?
}

# eval duration of treatment (w/ its beginning)
get_elapsed_time() {
    local -A _opts &&
    pow_argv \
        --args_n '
            start:Horodatage du début de traitement;
            format:Format de présentation;
            result:Durée calculée
        ' \
        --args_m '
            start;
            result
        ' \
        --args_v '
            format:BCAA|POW
        ' \
        --args_d '
            format:POW
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _result_ref=${_opts[RESULT]}
    local _end=$(date +%s)
    local _timex _days

    _timex=$(($_end - ${_opts[START]}))
    case ${_opts[FORMAT]} in
    BCAA)
        _result_ref="$((_timex/3600))h:$((_timex%3600/60))m:$((_timex%60))s"
        ;;
    POW)
        _result_ref="$(date --date @${_timex} --utc +%-Hh:%-Mm:%-Ss)"
        [[ $_timex -gt 86400 ]] && {
            _days=$(($(date --date @${_timex} --utc +%d) -1))
            _result_ref="${_days}j:$_result_ref"
        }
        ;;
    esac

    return $SUCCESS_CODE
}

# check mandatory root
is_user_root() {
    [ "$USER" != root ] && {
        log_error "Ce script est à exécuter par l'utilisateur root"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# check user exists
user_exists() {
    getent passwd $1 > /dev/null 2&>1
    return $?
}

# OK if var contains a Yes value
is_yes() {
    local -A _opts &&
    pow_argv \
        --args_n '
            var:Variable à tester
        ' \
        --args_m '
            var
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _var_ref=${_opts[VAR]}

    [[ $_var_ref =~ ^(yes|YES|y|Y|oui|OUI|o|O|ok|OK|t|T|true|TRUE)$ ]] && return $SUCCESS_CODE
    return $ERROR_CODE
}

# define delimiter w/o worry of bash_args!
set_delimiter() {
    local -A _opts &&
    pow_argv \
        --args_n '
            delimiter_code:Code délimiteur;
            delimiter_value:Valeur délimiteur' \
        --args_m '
            delimiter_code;
            delimiter_value' \
        --pow_argv _opts "$@" || return $?

    local -n _delimiter_ref=${_opts[DELIMITER_VALUE]}

    # https://linuxhint.com/associative_array_bash/
    [ ${POW_DELIMITER[${_opts[DELIMITER_CODE]}]+_} ] && {
        _delimiter_ref=${POW_DELIMITER[${_opts[DELIMITER_CODE]}]}
    } || {
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

    #
    # command line
    #

# custom getopt (clone bash_args w/ some improvements)

# option(s) are defined by list (items separated by ;)
# --args_n : name (as key:value)
# --args_m : mandatory (or optional if none)
# --args_v : values (as ORed declaration, va1|val2, ...)
# --args_d : defaults (or NULL if none)
# --args_p : property (to custom returns)
#            RESET (no|yes)     reset returned hash, before
#            CASE (UPPER|LOWER|USER)
#                               apply to keys (of returned hash), USER takes name from given list
#            TAG (k1@a1,k2@a2,...,kn@an)
#                               apply rule to value (of key), as BOOL|PSQL|INT|FLOAT
# --pow_argv <user variable>    to overload default POW_ARGV    (returned hash w/ argument(s))
# --pow_argc <user variable>    to overload default POW_ARGC    (count of argument(s), except args_*)

# NOTE
# arguments counter takes into account argument w/ default value (even missing on command line)

# NOTE
# can't call another function which implements pow_argv, else infinite loop (deadlock)!
# so can't use in_array(), is_yes() functions, has to do itself the job here...

# NOTE
# if other name than default POW_ARGV is requested, caller code has to declare it before (as HASH)
# as above in implementation of a function:
# local -A _opts &&
# pow_argv \
#    --args_n '
#        opt1:Option 1;
#        opt2:Option 2;
#        opt3:Option 3;
#    ' \
#    --args_m '
#        opt1|opt3
#    ' \
#    --pow_argv _opts "$@" || return $?

pow_argv() {
    local _step=1 _error _end=0 _key _value _i _info _valid _property _as_opt _k _tmp
    local _trick=", astuce : utilisez l'option --help pour l'aide ou --interactive pour une utilisation intéractive"
    local _argv_name _argc_name _argv_ref _argc_ref
    local -A _argv
    local -a _args_n_list _args_m_list _args_v_list _args_d_list _args_p_list
    local -A _args_n_kv _args_v_kv _args_d_kv _args_p_kv
    local -A _args_n_a
    local -a _mandatory_keys _among_values _args_a

    # prepare user parameters (from given lists)
    _pow_argv_list() {
        #echo "$#: $@"
        # $1= list value
        # $2= list array (result)
        # $3= optional 'key/value' hash (result)
        local _list="$1" _tmp _with_kv=0
        local -n _list_ref=$2

        [ -n "$3" ] && {
            local -n _kv_ref=$3
            _with_kv=1
        }

        # be careful w/ <<< if list contains '\n' and spaces (prefix, suffix)
        #  often so w/ declaration of bash_args's parameters convention
        # other solution can be built w/ substitution (same needing of deleting \n and spaces)
        #  IFS=';' read -a _args_n_list < <(printf "${_argv[args_n]}" | sed ...)
        IFS=';' read -a _list_ref <<< $(printf "${_list}" | sed -e 's/^[ ]*//' -e 's/[ ]*$//' | tr -d '\n')
        #declare -p _list_ref ; read
        [ $_with_kv -eq 1 ] && {
            for _tmp in "${_list_ref[@]}"; do
                #echo "$_tmp"
                _kv_ref+=([${_tmp%%:*}]=${_tmp#*:})
            done
            #declare -p _kv_ref ; read
        }

        return $SUCCESS_CODE
    }

    # get value of property (user if defined, or default)
    _pow_argv_property() {
        # $1= user values (array)
        # $2= option name
        # $3= option value (result)
        local -n _array_ref=$1
        local _key_upper=$2
        local _key_lower=${_key_upper,,}
        local -n _value_ref=$3

        [ ${_array_ref[${_key_lower}]+_} ] && {
            _value_ref=${_array_ref[${_key_lower}]}
            return $SUCCESS_CODE
        }
        [ ${_array_ref[${_key_upper}]+_} ] && {
            _value_ref=${_array_ref[${_key_upper}]}
            return $SUCCESS_CODE
        }
        [ ${POW_ARGV_PROPERTIES[${_key_upper}]+_} ] && {
            _value_ref=${POW_ARGV_PROPERTIES[${_key_upper}]}
            return $SUCCESS_CODE
        }

        return $ERROR_CODE
    }

    # read from command line
    while :; do
        #echo "step=($_step) key=($_key) \$1=$1"
        case $_step in
        # name of argument
        1)
            ([ $1 = -- ] || [ -z "$1" ]) && _step=90 || {
                [[ $1 =~ ^--(.*)$ ]] && {
                    _key=${BASH_REMATCH[1]}
                    _value=
                    # waiting for value
                    _step=10
                    shift
                } || {
                    _error="premier argument attendu --args_n (au lieu de: $1)"
                    _step=99
                }
            }
            ;;

        # value of argument
        10)
            if [[ $1 =~ ^-- ]]; then
                _step=11
            elif [ -z "$1" ]; then
                _step=90
            else
                # deal w/ protected '\--option' as value (and not as option!), stripping anti-slash
                _as_opt=0
                [[ $1 =~ ^\\-- ]] && _as_opt=1
                [ -n "$_value" ] && {
                    # args_? has only one list value
                    if [[ $_key =~ args_[nmvdp] ]]; then
                        _error="option attendue, du type --option (au lieu de: $1)"
                        _step=99
                    else
                        case $_as_opt in
                        0)  _value+=" $1"               ;;
                        1)  _value+=" ${1//\\--/--}"    ;;
                        esac
                        shift
                    fi
                } || {
                    case $_as_opt in
                    0)  _value="$1"                 ;;
                    1)  _value="${1//\\--/--}"      ;;
                    esac
                    shift
                }
            fi
            ;;
        11)
            #echo "$_key=$_value"
            case "${_args_n_a[$_key]}" in
            BOOL)
                # option w/o value is considered true
                [ -z "$_value" ] && _argv[$_key]=yes
                #echo "$_key=${_argv[$_key]}"
                ;;
            PSQL)
                # PSQL command doesn't have LF and blanks at beginning
                _argv[$_key]=$(echo "$_value" | tr '\n' ' ' | sed --expression 's/^[ \t]*//')
                ;;
            *)
                case $_key in
                help|interactive)
                    _argv[$_key]=yes
                    ;;
                *)
                    _argv[$_key]=${_value}
                    ;;
                esac
            esac

            case $_key in
            # lists
            args_[nmvdp])
                        _step=12    ;;
            pow_arg[cv]|help|interactive)
                        _step=$(( _end == 1 ? 91 : 1 )) ;;
            # user parameters
            *)          _step=20    ;;
            esac
            ;;
        12)
            local _argx_list_name=_${_key}_list
            local _argx_kv_name=_${_key}_kv
            local -n _argx_list_ref="$_argx_list_name"
            # no key/value for mandatory list
            [ "$_key" = args_m ] && _argx_kv_name=

            if [[ ${#_argx_list_ref[@]} -eq 0 ]]; then
                #declare -p _argv ; read
                _pow_argv_list "${_argv[$_key]}" $_argx_list_name $_argx_kv_name
                #[ -n "$_argx_kv_name" ] && declare -p $_argx_kv_name ; read
                [ "$_key" = args_p ] && {
                    local _tag _a
                    #declare -p _args_p_kv
                    _pow_argv_property _args_p_kv TAG _tag
                    #echo $_tag
                    [ -n "${_tag}" ] && {
                        IFS=, read -a _args_a <<< ${_tag}
                        for _tmp in "${_args_a[@]}"; do
                            _k=${_tmp%%@*}
                            _a=${_tmp#*@}
                            _args_n_a+=([$_k]=${_a^^})
                        done
                        #declare -p _args_n_a
                    }
                }
                _step=$(( _end == 1 ? 91 : 1 ))
            else
                _error="définition --$_key multiple!"
                _step=99
            fi
            ;;

        # check argument (among allowed ones)
        20)
            #declare -p _args_n_kv
            [[ -v "_args_n_kv[$_key]" ]] && _step=$(( _end == 1 ? 91 : 1 )) || {
                _error="L'argument $_key ne fait pas partie des arguments possibles"
                _step=99
            }
            ;;

        # end OK
        90)
            _end=1
            _step=11
            ;;
        91)
            break
            ;;

        # end ERROR
        99)
            break
            ;;
        esac

        #printf 'step=%d\n' $_step ; declare -p _argv ; read
    done
    #declare -p _argv _args_n_a _args_n_kv ; read

    # return options (values and count) eventually w/ overload of default POW_ARGV|POW_ARGC
    [ ${_argv[pow_argv]+_} ] && _argv_name="${_argv[pow_argv]}" || _argv_name="POW_ARGV"
    local -n _argv_ref="$_argv_name"
    [ ${_argv[pow_argc]+_} ] && _argc_name="${_argv[pow_argc]}" || _argc_name="POW_ARGC"
    local -n _argc_ref="$_argc_name"

    # reset returned hash (default behavior)
    #echo "pow_argv_property=(RESET)"
    _pow_argv_property _args_p_kv RESET _property &&
        #echo "reset=($_property)" &&
        [ "$_property" = yes ] &&
        #echo 'reset ARGV' &&
        _argv_ref=()
        _argc_ref=0

    # break on error, after reseting returned values
    [ -n "$_error" ] && {
        log_error "$_error"
        return $ERROR_CODE
    }

    # help requested ?
    [ "${_argv[help]}" = yes ] && {
        for _key in ${!_args_n_kv[@]}; do
            _info="${_key} : ${_args_n_kv[$_key]}"
            [[ " ${_args_m_list[*]} " == *" $_key "* ]] && _tmp=obligatoire || _tmp=facultatif
            _info+=", $_tmp"
            [ ${_args_v_kv[$_key]+_} ] && _info+=", valeurs possibles : ${_args_v_kv[$_key]}"
            [ ${_args_d_kv[$_key]+_} ] && _info+=", valeur par défaut : ${_args_d_kv[$_key]}"

            echo $_info
        done

        # not SUCCESS_CODE else following code pow_argv() is spawned!
        return $WARNING_CODE
    }

    # default values
    for _key in ${!_args_d_kv[@]}; do
        #echo "def($_key)"
        [ ! ${_argv[$_key]+_} ] && {
            #echo "def($_key)=${_args_d_kv[$_key]}"
            # duplicate from another key
            [[ ${_args_d_kv[$_key]} =~ ^@(.*)$ ]] && {
                #declare -p _argv
                _k=${BASH_REMATCH[1]}
                #echo "from($_k)"
                _argv[$_key]=${_argv[$_k]:-${_args_d_kv[$_k]}}
                #declare -p _argv
                continue
            }

            _argv[$_key]=${_args_d_kv[$_key]}
        }
    done

    # respect of mandatory option(s)
    # TODO implements mandatory grammar w/ |&^ operators (OR, AND, XOR) and () combinaisons
    for ((_i=0; _i<${#_args_m_list[@]}; _i++)); do
        IFS='|' read -a _mandatory_keys <<< "${_args_m_list[$_i]}"
        _valid=0
        for _key in ${_mandatory_keys[@]}; do
            [ ${_argv[$_key]+_} ] && {
                _valid=1
                break
            }
        done
        [ $_valid -eq 1 ] || {
            log_error "La condition d'argument obligatoire ${_args_m_list[$_i]} n'est pas remplie${_trick}"
            return $ERROR_CODE
        }
    done

    # check values
    for _key in ${!_args_n_kv[@]}; do
        case "${_args_n_a[$_key]}" in
        INT)
            [[ ${_argv[$_key]} =~ ^[0-9]+$ ]] || {
                _error="$_key: La valeur (${_argv[$_key]}) n'est pas un nombre entier"
                false
            }
            ;;
        FLOAT)
            # https://stackoverflow.com/questions/12643009/regular-expression-for-floating-point-numbers
            [[ ${_argv[$_key]} =~ ^[+-]?([0-9]*[.])?[0-9]+$ ]] || {
                _error="$_key: La valeur (${_argv[$_key]}) n'est pas un nombre flottant"
                false
            }
            ;;
        *)
            # with defined check ?
            [ -z "${_args_v_kv[$_key]}" ] || {
                IFS='|' read -ra _among_values <<< "${_args_v_kv[$_key]}"

                # special case: all requested
                [ "${_argv[$_key]^^}" == ALL ] && _argv[$_key]="${_among_values[@]}"

                # eventually many values (separated by space)
                local -a _check_values=(${_argv[$_key]})
                local -A _verify_values=()
                local -a _keys _delete_values

                #declare -p _among_values _check_values &&

                # build hash w/ value as key, and occurence as value
                for ((_i=0; _i<${#_check_values[@]}; _i++)); do
                    # value to delete (as: -KEY)
                    [ "${_check_values[$_i]:0:1}" != - ] || {
                        _k=${_check_values[$_i]:1}
                        [[ " ${_delete_values[*]} " == *" ${_k} "* ]] && continue
                        _delete_values+=($_k)
                        continue
                    }
                    [ ${_verify_values[${_check_values[$_i]}]+_} ] && _verify_values[${_check_values[$_i]}]=$((_verify_values[${_check_values[$_i]}] +1)) || _verify_values[${_check_values[$_i]}]=1
                done &&
                #declare -p _delete_values &&
                {
                    if ([[ ${#_verify_values[@]} -gt 0 ]] &&
                        [[ ${#_delete_values[@]} -gt 0 ]]); then
                        _error="$_key: Mixité (+/-) de valeur(s) non valide"
                        false
#                     elif [[ ${#_delete_values[@]} == ${#_among_values[@]} ]]; then
#                         _error="$_key: Effacement complet des valeurs possibles"
#                         false
                    else
                        [[ ${#_delete_values[@]} -eq 0 ]] || {
                            _check_values=()
                            for ((_i=0; _i<${#_among_values[@]}; _i++)); do
                                [[ " ${_delete_values[*]} " == *" ${_among_values[$_i]} "* ]] || {
                                    _check_values+=(${_among_values[$_i]})
                                    _verify_values[${_among_values[$_i]}]=1
                                }
                            done
                            #declare -p _check_values _verify_values
                            # assign new value (w/ deleted items)
                            _argv[$_key]="${_check_values[@]}"
                        }
                    fi
                } &&
                _keys=(${!_verify_values[@]}) &&
                #declare -p _verify_values &&
                case "${_args_n_a[$_key]}" in
                # one (eventually multiple)
                1+N)
                    _error="$_key: Valeur unique attendue, éventuellement multiple"
                    [[ ${#_verify_values[@]} -eq 1 ]]
                    ;;
                # many (but each uniq)
                XN)
                    _error="$_key: Plusieurs valeurs attendues, chacune unique"
                    [[ ${#_verify_values[@]} -ge 1 ]] &&
                    [[ ${_verify_values[${_keys[0]}]} == 1 ]] &&
                    [[ $(echo "${_verify_values[@]}" | tr ' ' '\n' | sort | uniq -c | wc -l) == 1 ]]
                    ;;
                # many
                X+N)
                    _error="$_key: Plusieurs valeurs attendues, chacune éventuellement multiple"
                    [[ ${#_verify_values[@]} -ge 1 ]] &&
                    [[ $(echo "${_verify_values[@]}" | tr ' ' '\n' | sort | uniq -c | wc -l) -ge 1 ]]
                    ;;
                # default ([01]N) one uniq value, eventually none (0N)
                *)
                    _error="$_key: Valeur unique attendue"
                    local _min=${_args_n_a[$_key]:0:1} _max=1
                    # due to other tags, different from among list  (as bool, ...)
                    ([[ $_min == 0 ]] || [[ $_min == 1 ]]) || _min=1
                    [[ ${#_verify_values[@]} -ge ${_min:-1} ]] &&
                    [[ ${#_verify_values[@]} -le $_max ]] &&
                    (([[ $_min == 1 ]] && [[ ${_verify_values[${_keys[0]}]} == 1 ]]) || [[ $_min == 0 ]])
                    ;;
                esac &&
                # check each value belongs to defined list
                for ((_i=0; _i<${#_check_values[@]}; _i++)); do
                    [[ " ${_among_values[*]} " == *" ${_check_values[$_i]} "* ]] || {
                        _error="$_key: La valeur (${_check_values[$_i]}) ne fait pas partie des valeurs possibles (${_args_v_kv[$_key]})${_trick}"
                        false
                    }
                done
            }
            ;;
        esac || {
            [ -n "$_error" ] && log_error "$_error"
            return $ERROR_CODE
        }
    done

    # case of keys in returned hash
    #echo "pow_argv_property=(CASE)"
    _pow_argv_property _args_p_kv CASE _property
    #echo "case=($_property)"
    _argc_ref=0
    for _key in ${!_argv[@]}; do
        #echo "key=($_key)"
        [[ "$_key" =~ pow_arg[cv]|args_[nmvdp] ]] && continue
        case "$_property" in
        UPPER)  _k=${_key^^}    ;;
        LOWER)  _k=${_key,,}    ;;
        USER)   _k=${_key}      ;;
        esac
        #echo "k=($_k)"
        _argv_ref[$_k]=${_argv[$_key]}
        ((_argc_ref++))
    done

    return $SUCCESS_CODE
}

# getopts improved (w/ list of values, default value, ...)
# voir wiki https://wiki.net.extra.laposte.fr/confluence/pages/viewpage.action?pageId=824282297
bash_args() {
    local get_arg_help=
    local get_arg_interactif=
    #a voir : local get_arg_verbose=

    local tmp_arg_name
    local tmp_arg_value

    local get_arg_args_p
    local tmp_liste_args_p=()
    local tmp_arg_p
    local tmp_arg_p_name
    local tmp_arg_p_libelle
    local tmp_arg_p_value
    local tmp_arg_p_help
    local tmp_arg_p_ok

    local get_arg_args_o=
    local tmp_liste_args_o=()
    local tmp_liste_args_o_or
    local tmp_arg_o
    local tmp_arg_o_or_name
    local tmp_arg_o_ok

    local get_arg_args_v=
    local tmp_liste_args_v=()
    local tmp_arg_v
    local tmp_arg_v_name
    local tmp_arg_v_values
    local tmp_liste_arg_v_values
    local tmp_arg_v_value
    local tmp_arg_v_ok

    local get_arg_args_d=
    local tmp_liste_args_d=()
    local tmp_arg_d
    local tmp_arg_d_name
    local tmp_arg_d_value

    # tableau associatif local des {clé/valeur} des arguments passés
    declare -A _argv

    while echo "$1" | grep -q ^--; do
        tmp_arg_name=$(echo "$1" | sed 's/^--//')
        #echo "argument $tmp_arg_name"
        tmp_arg_value=
        #eval get_arg_$tmp_arg_name=$tmp_arg_value

        shift
        while echo $(eval echo \"$1\") | grep -q ^[^--]; do
            #echo "valeur $1"
            if [ -z "$tmp_arg_value" ]; then
                tmp_arg_value="$1"
            else
                tmp_arg_value="$tmp_arg_value $1"
            fi
            shift
        done
        if [ -z "$1" ]; then
            #si l'argument est sans valeur / valeur vide, alors on passe à l'argument suivant
            shift
        fi
        #PROVOQUE UN COMPORTEMENT INNATENDU (cf set_pg_cluster, à considérer sur tous les rappels de fonctions repassant tous les paramètres)
        #si l'argument est présent, mais sans valeur, on considère que c'est un indicateur type OUI/NON et que sa précense vaut OUI(YES/Y)
        if [ -z "$tmp_arg_value" ]; then
            # le retour des paramètres dans un tableau nécessite le nom de ce tableau
            if [ "$tmp_arg_name" = bash_args_argv ]; then
                log_error "L'argument $tmp_arg_name nécessite une valeur! : --$tmp_arg_name <argv>"
                return 1
            fi
            # sans valeur, l'argument est considéré comme booléen (Y/N)
            if [ "$tmp_arg_name" = 'help' ] || [ "$tmp_arg_name" = 'interactif' ]; then
                tmp_arg_value='Y'
            fi
        fi
        #echo "$tmp_arg_name = $tmp_arg_value"
        #On fait en sorte de ne pas évaluer les variables utilisées dans la valeur en préfixant les $ par un \
        #car sinon pose problème pour la définition des valeurs par défaut avec l'utilisation de variables faisant référence à d'autres paramètres
        #et on entoure la valeur par des doubles côtes pour permettre que la valeur contienne une simple cote
        tmp_arg_value=${tmp_arg_value//$/\\$}
        #protection des doubles cotes
        tmp_arg_value=${tmp_arg_value//\"/\\\"}
        eval get_arg_$tmp_arg_name=\"$tmp_arg_value\"
        #anciennement :
            #eval get_arg_$tmp_arg_name=\'$tmp_arg_value\'

        # range les arguments dans le tableau associatif local
        _argv+=([$tmp_arg_name]=$tmp_arg_value)

        if [ -z "$get_arg_args_p" ]; then
            echo "Veuillez définir les argument possibles en premier paramètres avec args_p, exemple : --args_p 'nom_argument1:libelle;nom_argument2:libelle'"
            return 1
        elif [ "$tmp_arg_name" = 'args_p' ]; then
            IFS=';' read -ra tmp_liste_args_p <<< "${get_arg_args_p}"
            for tmp_arg_p in "${tmp_liste_args_p[@]}"; do
                tmp_arg_p_name=$(echo $tmp_arg_p | grep -o '^[^:]*')
                #echo "Init argument $tmp_arg_name2 à vide"
                eval get_arg_$tmp_arg_p_name=''
            done
        elif [ "$tmp_arg_name" != 'help' ] && [ "$tmp_arg_name" != 'interactif' ] && [ "$tmp_arg_name" != 'args_o' ] && [ "$tmp_arg_name" != 'args_v' ] && [ "$tmp_arg_name" != 'args_d' ]; then
            tmp_arg_p_ok='N'
            for tmp_arg_p in "${tmp_liste_args_p[@]}"; do
                tmp_arg_p_name=$(echo $tmp_arg_p | grep -o '^[^:]*')
                if [ "$tmp_arg_name" = "$tmp_arg_p_name" -o "$tmp_arg_name" = bash_args_argv ]; then
                    tmp_arg_p_ok='Y'
                    break
                fi
            done
            if [ "$tmp_arg_p_ok" = 'N' ]; then
                log_error "L'argument $tmp_arg_name ne fait pas partie des arguments possibles"
                return 1
            fi
        fi
    done

    if [ -n "$get_arg_args_d" ]; then
        IFS=';' read -ra tmp_liste_args_d <<< "${get_arg_args_d}"
    fi
    if [ -n "$get_arg_args_v" ]; then
        IFS=';' read -ra tmp_liste_args_v <<< "${get_arg_args_v}"
    fi
    if [ -n "$get_arg_args_o" ]; then
        IFS=';' read -ra tmp_liste_args_o <<< "${get_arg_args_o}"
    fi

    if [ -n "$get_arg_help" ] || [ -n "$get_arg_interactif" ]; then
        for tmp_arg_p in "${tmp_liste_args_p[@]}"; do
            tmp_arg_p_name=$(echo $tmp_arg_p | grep -o '^[^:]*')
            tmp_arg_p_libelle=$(echo $tmp_arg_p | grep -o '[^:]*$')
            tmp_arg_p_help="$tmp_arg_p_name : $tmp_arg_p_libelle"

            for tmp_arg_o in "${tmp_liste_args_o[@]}"; do
                tmp_arg_o_ok='N'
                tmp_arg_o_name=$(echo $tmp_arg_o | grep -o '^[^:]*')
                if [ "$tmp_arg_o_name" = "$tmp_arg_p_name" ]; then
                    tmp_arg_o_value=$(echo $tmp_arg_o | grep -o '[^:]*$')
                    tmp_arg_p_help="$tmp_arg_p_help, obligatoire"
                    tmp_arg_o_ok='O'
                    break
                fi
            done
            if [ "$tmp_arg_o_ok" = 'N' ]; then
                tmp_arg_p_help="$tmp_arg_p_help, facultatif"
            fi

            for tmp_arg_v in "${tmp_liste_args_v[@]}"; do
                tmp_arg_v_name=$(echo $tmp_arg_v | grep -o '^[^:]*')
                if [ "$tmp_arg_v_name" = "$tmp_arg_p_name" ]; then
                    tmp_arg_v_value=$(echo $tmp_arg_v | grep -o '[^:]*$')
                    tmp_arg_p_help="$tmp_arg_p_help, valeurs possibles : $tmp_arg_v_value"
                    break
                fi
            done

            for tmp_arg_d in "${tmp_liste_args_d[@]}"; do
                tmp_arg_d_name=$(echo $tmp_arg_d | grep -o '^[^:]*')
                if [ "$tmp_arg_d_name" = "$tmp_arg_p_name" ]; then
                    tmp_arg_d_value=$(echo $tmp_arg_d | grep -o '[^:]*$')
                    if [ -n "$get_arg_interactif" ]; then
                        eval tmp_arg_d_value=\"$tmp_arg_d_value\"
                    fi
                    tmp_arg_p_help="$tmp_arg_p_help, valeur par défaut : $tmp_arg_d_value"
                    break
                fi
            done

            if [ -n "$get_arg_interactif" ]; then
                bash_question --question "$tmp_arg_p_help"
                eval get_arg_$tmp_arg_p_name=\"$get_reponse\"
            else
                echo "$tmp_arg_p_help"
            fi
        done
        if [ -n "$get_arg_help" ]; then
            # script.sh --help : arrêter le traitement du script, une fois l'aide affichée
            # je ne comprends pas comment s'arrête : fonction --help avec un return 0
            return 1
        fi
    fi

    #application si besoin des valeurs par défaut
    for tmp_arg_d in "${tmp_liste_args_d[@]}"; do
        tmp_arg_d_name=$(echo $tmp_arg_d | grep -o '^[^:]*')
        tmp_arg_d_value=$(echo $tmp_arg_d | grep -o '[^:]*$')
        eval tmp_arg_value=\"\$get_arg_$tmp_arg_d_name\"
        #echo "DEFAULT($tmp_arg_d_name)=($tmp_arg_value)"
        if [ -z "$tmp_arg_value" ]; then
            eval get_arg_$tmp_arg_d_name=\"$tmp_arg_d_value\"
            #eval tmp_arg_d_value=\"$tmp_arg_d_value\"
            #echo "Valeur par défaut $tmp_arg_d_value pour $tmp_arg_d_name"
            _argv+=([$tmp_arg_d_name]=$tmp_arg_d_value)
        fi
    done

    #controle des arguments obligatoires
    for tmp_arg_o in "${tmp_liste_args_o[@]}"; do
        tmp_arg_o_ok='N'
        #prise en compte d'une éventuelle condition de type OU
        IFS='|' read -ra tmp_liste_args_o_or <<< "${tmp_arg_o}"
        for tmp_arg_o_or_name in "${tmp_liste_args_o_or[@]}"; do
            eval tmp_arg_value=\"\$get_arg_$tmp_arg_o_or_name\"
            #si la valeur de l'argument est renseignée, la condition est remplie
            if [ -n "$tmp_arg_value" ]; then
                tmp_arg_o_ok='O'
                break
            fi
        done
        if [ "$tmp_arg_o_ok" != 'O' ]; then
            log_error "La condition d'argument obligatoire $tmp_arg_o n'est pas remplie, astuce : utilisez l'option --help pour l'aide ou --interactif pour une utilisation interactive"
            return 1
        fi
    done

    #controle des valeurs possibles
    for tmp_arg_v in "${tmp_liste_args_v[@]}"; do
        tmp_arg_v_name=$(echo $tmp_arg_v | grep -o '^[^:]*')
        tmp_arg_v_values=$(echo $tmp_arg_v | grep -o '[^:]*$')
        #si l'argument est renseigné
        eval tmp_arg_value=\"\$get_arg_$tmp_arg_v_name\"
        if [ -n "$tmp_arg_value" ]; then
            #on vérifie que sa valeur fait partie des valeurs possibles
            tmp_arg_v_ok='N'
            IFS='|' read -ra tmp_liste_arg_v_values <<< "${tmp_arg_v_values}"
            for tmp_arg_v_value in "${tmp_liste_arg_v_values[@]}"; do
                #echo "Valeur possible pour $tmp_arg_v_name : $tmp_arg_v_value"
                if [ "$tmp_arg_v_value" = "$tmp_arg_value" ]; then
                    tmp_arg_v_ok='Y'
                    break
                fi
            done
            if [ "$tmp_arg_v_ok" = 'N' ]; then
                log_error "La valeur de $tmp_arg_v_name ($tmp_arg_value) ne fait pas partie des valeurs possibles ($tmp_arg_v_values), astuce : utilisez l'option --help pour l'aide ou --interactif pour une utilisation interactive"
                return 1
            fi
        fi
    done

    # retour des paramètres {clé/valeur} au programme appelant
    [[ " ${!_argv[@]} " =~ " bash_args_argv " ]] && {
        local -n _argv_ref=${_argv[bash_args_argv]}
        _argv_ref=()
        #echo "reset argv: #${#_argv_ref[@]}"
        for _key in ${!_argv[@]}; do
            [[ "$_key" =~ bash_args_argv|args_(p|o|v|d) ]] && continue
            #echo '+'$_key
            _argv_ref[$_key]=${_argv[$_key]}
        done
    }

    return 0
}

    #
    # file
    #

# compare
is_different() {
    local -A _opts &&
    pow_argv \
        --args_n '
            dir_a:Dossier à comparer;
            dir_b:Autre dossier à comparer à dir_a;
            file_name:Nom ou masque optionnel de sélection des fichiers ou dossiers à comparer;
            verbose:Mode verbeux (Y/N)' \
        --args_m 'dir_a;dir_b' \
        --args_v '
            verbose:yes|no
        ' \
        --args_d '
            verbose:no
        ' \
        --pow_argv _opts "$@" || return $?

    local _tmp_path _tmp_file _size_a _size_b _git_diff

    [ -z "${_opts[FILE_NAME]}" ] && _opts[FILE_NAME]='*'
    [ "${_opts[VERBOSE]}" = yes ] && echo "Lecture de ${_opts[DIR_A]}/${_opts[FILE_NAME]}"
    for _tmp_path in "${_opts[DIR_A]}/"${_opts[FILE_NAME]}; do
        #si c'est un fichier
        if [ -f "$_tmp_path" ]; then
            _tmp_file=$(basename "$_tmp_path")
            if [ ! -f "${_opts[DIR_B]}/$_tmp_file" ]; then
                echo "Fichier ${_opts[DIR_A]}/$_tmp_file non présent dans ${_opts[DIR_B]}"
                return $SUCCESS_CODE
            fi
            if file_is_binary --file_path "$_tmp_path"; then
                _size_a=$(wc -c "${_opts[DIR_A]}/$_tmp_file" | cut -d' ' -f1)
                _size_b=$(wc -c "${_opts[DIR_B]}/$_tmp_file" | cut -d' ' -f1)
                if [ "$_size_a" != "$_size_b" ]; then
                    echo "Fichier binaire ${_opts[DIR_A]}/$_tmp_file de taille différente dans ${_opts[DIR_B]}"
                    return $SUCCESS_CODE
                elif [ "${_opts[VERBOSE]}" = yes ]; then
                    echo "Fichier binaire ${_opts[DIR_A]}/$_tmp_file de taille identique dans ${_opts[DIR_B]}"
                fi
            else
                _git_diff=$(git diff "${_opts[DIR_A]}/$_tmp_file" "${_opts[DIR_B]}/$_tmp_file")
                if [ "$_git_diff" ]; then
                    echo "Fichier texte ${_opts[DIR_A]}/$_tmp_file différent dans ${_opts[DIR_B]}"
                    return $SUCCESS_CODE
                elif [ "${_opts[VERBOSE]}" = yes ]; then
                    echo "Fichier texte ${_opts[DIR_A]}/$_tmp_file identique dans ${_opts[DIR_B]}"
                fi
            fi
        #sinon, si c'est un dossier
        elif [ -d "$_tmp_path" ]; then
            _tmp_file=$(basename "$_tmp_path")
            if [ ! -d "${_opts[DIR_B]}/$_tmp_file" ]; then
                echo "Dossier ${_opts[DIR_A]}/$_tmp_file non présent dans ${_opts[DIR_B]}"
                return $SUCCESS_CODE
            else
                [ "${_opts[VERBOSE]}" = yes ] && echo "Dossier ${_opts[DIR_A]}/$_tmp_file présent dans ${_opts[DIR_B]}"
                est_different --dir_a "${_opts[DIR_A]}/$_tmp_file" --dir_b "${_opts[DIR_B]}/$_tmp_file" --file_name "${_opts[FILE_NAME]}" --verbose "${_opts[VERBOSE]}" &&
                return $SUCCESS_CODE
            fi
        fi
    done

    [ "${_opts[VERBOSE]}" = yes ] && echo "Lecture de ${_opts[DIR_B]}/${_opts[FILE_NAME]}"
    for _tmp_path in "${_opts[DIR_B]}/"${_opts[FILE_NAME]}; do
        if [ -f "$_tmp_path" ]; then
            _tmp_file=$(basename "$_tmp_path")
            if [ ! -f "${_opts[DIR_A]}/$_tmp_file" ]; then
                echo "Fichier ${_opts[DIR_B]}/$_tmp_file non présent dans ${_opts[DIR_A]}"
                return $SUCCESS_CODE
            fi
        #sinon, si c'est un dossier
        elif [ -d "$_tmp_path" ]; then
            _tmp_file=$(basename "$_tmp_path")
            if [ ! -d "${_opts[DIR_A]}/$_tmp_file" ]; then
                echo "Dossier ${_opts[DIR_B]}/$_tmp_file non présent dans ${_opts[DIR_A]}"
                return $SUCCESS_CODE
            fi
        fi
    done

    return $ERROR_CODE
}

# name of file
get_file_name() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin complet du fichier;
            file_name:Nom du fichier;
            with_extension:Avec/sans extension;
            stdout:Sortie résultat sur écran
        ' \
        --args_m 'file_path' \
        --args_v '
            with_extension:yes|no;
            stdout:yes|no;
        ' \
        --args_d '
            with_extension:no;
            stdout:yes
        ' \
        --pow_argv _opts "$@" || return $?

    local _file_name _result

    _file_name=$(basename -- "${_opts[FILE_PATH]}")
    case ${_opts[WITH_EXTENSION]} in
    yes)    _result=$_file_name         ;;
    no)     _result=${_file_name%%.*}   ;;
    esac
    [ "${_opts[STDOUT]}" = yes ] && echo $_result
    [ -n "${_opts[FILE_NAME]}" ] && {
        local -n _file_name_ref=${_opts[FILE_NAME]}
        _file_name_ref=$_result
    }

    return $SUCCESS_CODE
}

# extension of file
get_file_extension() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin complet du fichier;
            file_extension:Extension du fichier;
            lazy:Extraction extension (complète|partielle), similaire REGEXP (greedy|lazy);
            stdout:Sortie résultat sur écran
        ' \
        --args_m 'file_path' \
        --args_v '
            lazy:yes|no;
            stdout:yes|no
        ' \
        --args_d '
            lazy:yes;
            stdout:yes
        ' \
        --pow_argv _opts "$@" || return $?

    # NOTE be careful, not use _file_name (same as this in get_file_name) : empty return!
    local _name _file_extension _result

    get_file_name \
        --file_path "${_opts[FILE_PATH]}" \
        --file_name _name \
        --stdout no \
        --with_extension yes &&
    case ${_opts[LAZY]} in
    yes)    _file_extension="${_name##*.}"  ;;
    no)     _file_extension="${_name#*.}"   ;;
    esac
    # lower
    _result="${_file_extension,,}"

    [ "${_opts[STDOUT]}" = yes ] && echo $_result
    [ -n "${_opts[FILE_EXTENSION]}" ] && {
        local -n _file_extension_ref=${_opts[FILE_EXTENSION]}
        _file_extension_ref=$_result
    }

    return $SUCCESS_CODE
}

# get MIME's type of file
get_file_mimetype() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin complet du fichier;
            file_mime:MIME du fichier;
            stdout:Sortie résultat sur écran
        ' \
        --args_m 'file_path' \
        --args_v '
            stdout:yes|no
        ' \
        --args_d '
            stdout:yes
        ' \
        --pow_argv _opts "$@" || return $?

    local _mime_type=$(file --mime-type "${_opts[FILE_PATH]}" | sed 's/.*: //')

    [ "${_opts[STDOUT]}" = yes ] && echo $_mime_type
    [ -n "${_opts[FILE_MIME]}" ] && {
        local -n _file_mime_ref=${_opts[FILE_MIME]}
        _file_mime_ref=$_mime_type
    }

    return $SUCCESS_CODE
}

# get number of rows
get_file_nrows() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin complet du fichier;
            file_nrows:Nombre de ligne(s) du fichier;
            stdout:Sortie résultat sur écran
        ' \
        --args_m 'file_path' \
        --args_v '
            stdout:yes|no
        ' \
        --args_d '
            stdout:yes
        ' \
        --pow_argv _opts "$@" || return $?

    local _file_nrows

    expect file "${_opts[FILE_PATH]}" &&
    _file_nrows=$(wc --lines "${_opts[FILE_PATH]}" | cut --delimiter ' ' --fields 1) &&
    {
        [ "${_opts[STDOUT]}" = no ] || echo $_file_nrows
    } &&
    {
        [ -z "${_opts[FILE_NROWS]}" ] || {
            local -n _file_nrows_ref=${_opts[FILE_NROWS]}
            _file_nrows_ref=$_file_nrows
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}

# known if file is binary
file_is_binary() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin complet du fichier
        ' \
        --args_m 'file_path' \
        --pow_argv _opts "$@" || return $?

    expect file "${_opts[FILE_PATH]}" &&
    file --brief --dereference --mime "${_opts[FILE_PATH]}" | grep --silent 'charset=binary'

    return $?
}

# backup file (with uniq extension as: .backup.#)
backup_file_as_uniq() {
    local -A _opts &&
    pow_argv \
        --args_n 'path:nom complet' \
        --args_m 'path' \
        --pow_argv _opts "$@" || return $?

    local _suffix=1

    [ ! -f "${_opts[PATH]}" ] && {
        log_info 'Sauvegarde unique demandée pour fichier '"${_opts[PATH]}"' inexistant!'
        return $SUCCESS_CODE
    }
    while [ -f "${_opts[PATH]}.backup.${_suffix}" ]; do
        ((_suffix++))
    done
    cp "${_opts[PATH]}" "${_opts[PATH]}.backup.${_suffix}"

    return $?
}

# get temporary file (w/ uniq name)
get_tmp_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            tmpfile:Nom de la variable dans laquelles est retourné le chemin du fichier temporaire demandé;
            tmpdir:Dossier du fichier temporaire;
            tmpext:Extension du fichier temporaire;
            tool:Méthode à utiliser;
            suffix:Suffixe du fichier temporaire;
            chmod:Permissions (rwx) à donner à ce fichier;
            create:Créer le fichier temporaire' \
        --args_m '
            tmpfile' \
        --args_v '
            tool:MKTEMP|DATE|BASH|UUID|HEXDUMP|OPENSSL;
            create:no|yes' \
        --args_d '
            tmpdir:'$POW_DIR_TMP';
            tmpext:tmp;
            tool:MKTEMP;
            chmod:666;
            create:no' \
        --pow_argv _opts "$@" || return $?

    local _tmp_file _tmp_create=0
    local -n _tmp_ref=${_opts[TMPFILE]}

    # time get_tmp_file --tmpfile _tmp
    # real    0m0,431s
    # user    0m0,125s
    # sys     0m0,143s

    # time get_tmp_file --tmpfile _tmp --tool DATE
    # real    0m0,263s
    # user    0m0,173s
    # sys     0m0,176s

    # time get_tmp_file --tmpfile _tmp --tool BASH
    # real    0m1,103s
    # user    0m0,303s
    # sys     0m1,166s

    # time get_tmp_file --tmpfile _tmp --tool UUID
    # real    0m0,814s
    # user    0m0,257s
    # sys     0m0,824s

    # time get_tmp_file --tmpfile _tmp --tool HEXDUMP
    # real    0m1,072s
    # user    0m0,338s
    # sys     0m1,092s

    # time get_tmp_file --tmpfile _tmp --tool OPENSSL
    # real    0m0,786s
    # user    0m0,254s
    # sys     0m0,782s

    # time get_tmp_file --tmpfile _tmp --tool MKTEMP
    # real    0m0,785s
    # user    0m0,240s
    # sys     0m0,797s

    case "${_opts[TOOL]}" in
    MKTEMP)
        _tmp_create=1
        # suffix concat given one (if any) w/ extension
        _tmp_file=$(mktemp --tmpdir="${_opts[TMPDIR]}" --suffix "${_opts[SUFFIX]}.${_opts[TMPEXT]}" pow_XXXXX)
        ;;
    *)
        # https://stackoverflow.com/questions/2793812/generate-a-random-filename-in-unix-shell
        case "${_opts[TOOL]}" in
        DATE)
            _tmp_file=$(date '+%M%S%N')
            ;;
        BASH)
            printf -v _tmp_file '%X' $(printf '%.2s ' $((RANDOM%16))' '{00..31})
            ;;
        UUID)
            _tmp_file=$(uuidgen)
            ;;
        HEXDUMP)
            _tmp_file=$(hexdump -n 16 -v -e '/1 "%02X"' /dev/urandom)
            ;;
        OPENSSL)
            _tmp_file=$(openssl rand -hex 16)
            ;;
        esac
        _tmp_file="${_opts[TMPDIR]}/${_tmp_file}${_opts[SUFFIX]}.${_opts[TMPEXT]}"

        [ "${_opts[CREATE]}" = yes ] && {
            touch $_tmp_file
            _tmp_create=1
        }
    esac
    [[ $_tmp_create -eq 1 ]] && {
        [ "${_opts[CREATE]}" = no ] && rm --force "$_tmp_file" || chmod ${_opts[CHMOD]} "$_tmp_file"
    }
    _tmp_ref="$_tmp_file"

    return $SUCCESS_CODE
}

# sync to wait for file
wait_for_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:chemin complet vers le fichier attendu;
            wait_file_minute:combien de temps en minutes faut-il attendre que le fichier soit présent ?;
            max_age_file_minute:quel age maximum en minutes doit avoir le fichier ?
        ' \
        --args_m 'file_path' \
        --args_d 'wait_file_minute:0;max_age_file_minute:0' \
        --pow_argv _opts "$@" || return $?

    local _size_before _size_after

    # waiting delay
    # AND
    #   file not available
    #   OR
    #   present file is too old
    while [ ${_opts[WAIT_FILE_MINUTE]} -gt 0 ] && { [ ! -f ${_opts[FILE_PATH]} ] || ([ ${_opts[MAX_AGE_FILE_MINUTE]} -gt 0 ] && [ $(find ${_opts[FILE_PATH]} -mmin +${_opts[MAX_AGE_FILE_MINUTE]}) ]) }; do
        echo "En attente du fichier ${_opts[FILE_PATH]} (temps restant : ${_opts[WAIT_FILE_MINUTE]} minutes, age maximum du fichier : ${_opts[MAX_AGE_FILE_MINUTE]} minutes, fichier présent mais trop ancien : $([ -f ${_opts[FILE_PATH]} ] && echo 'oui' || echo 'non'))"
        sleep 60
        ((_opts[WAIT_FILE_MINUTE]--))
    done

    if [ -f "${_opts[FILE_PATH]}" ]; then
        # older ?
        [ ${_opts[MAX_AGE_FILE_MINUTE]} -gt 0 ] && [ $(find ${_opts[FILE_PATH]} -mmin +${_opts[MAX_AGE_FILE_MINUTE]}) ] && {
            log_error "Le fichier ${_opts[FILE_PATH]} est présent mais trop ancien et l'éventuel temps d'attente est dépassé"
            return $ERROR_CODE
        }

        # not currently growing?
        _size_before=$(stat --printf="%s" ${_opts[FILE_PATH]})
        sleep 5
        _size_after=$(stat --printf="%s" ${_opts[FILE_PATH]})
        while [ "$_size_before" != "$_size_after" ]; do
            echo "La taille du fichier ${_opts[FILE_PATH]} a changée, attente de 5 secondes supplémentaires"
            _size_before=$_size_after
            sleep 5
            _size_after=$(stat --printf="%s" ${_opts[FILE_PATH]})
        done
    else
        echo "Le fichier ${_opts[FILE_PATH]} n'est pas présent et l'éventuel temps d'attente est dépassé"
        return $ERROR_CODE
    fi

    return $SUCCESS_CODE
}

    #
    # archive
    #

is_archive() {
    local -A _opts &&
    pow_argv \
        --args_n "
            archive_path:chemin complet de l'archive;
            archive_type:obtenir le type de l'archive (MIME)
        " \
        --args_m 'archive_path' \
        --pow_argv _opts "$@" || return $?

    [ -f "${_opts[ARCHIVE_PATH]}" ] || return $ERROR_CODE
    [ -n "${_opts[ARCHIVE_TYPE]}" ] && local -n _type_ref=${_opts[ARCHIVE_TYPE]}

    # TODO: (to add 7z and rar) apt install p7zip-full p7zip-rar
	[[ $(file --mime-type "${_opts[ARCHIVE_PATH]}") =~ application/(zip|gzip|x-bzip2|x-7z-compressed) ]] && {
        _type_ref=${BASH_REMATCH[1]}
        return $SUCCESS_CODE
    }
    return $ERROR_CODE
}

# extract data from archive (zip, gz, ...)
extract_archive() {
    local -A _opts &&
    pow_argv \
        --args_n "
            archive_path:chemin complet de l'archive;
            extract_path:chemin complet du résultat de l'extraction de l'archive (STDOUT pour écran)
        " \
        --args_m '
            archive_path;
            extract_path
        ' \
        --pow_argv _opts "$@" || return $?

    local _start=$(date +%s)
    local _archive_name=$(basename "${_opts[ARCHIVE_PATH]}")
    local _log_tmp_path="$POW_DIR_TMP/extract_$_archive_name.log"
    local _log_archive_path="$POW_DIR_ARCHIVE/extract_$_archive_name.log"
    local _archive_type

    is_archive --archive_path "${_opts[ARCHIVE_PATH]}" --archive_type _archive_type || {
        log_error "${FUNCNAME[0]}: le fichier ${_opts[ARCHIVE_PATH]} n'est pas une archive"
        return $ERROR_CODE
    }
    case $_archive_type in
    zip)
        if [ "${_opts[EXTRACT_PATH]}" = STDOUT ]; then
            # -p : extract files to pipe (stdout)
            unzip -p "${_opts[ARCHIVE_PATH]}" 2> $_log_tmp_path
        else
            # -o : overwrite files WITHOUT prompting
            # -d : extract files into dir
            unzip -o "${_opts[ARCHIVE_PATH]}" -d "${_opts[EXTRACT_PATH]}" > $_log_tmp_path 2>&1
        fi
        ;;
    gzip)
        if [ "${_opts[EXTRACT_PATH]}" = STDOUT ]; then
            gunzip --stdout "${_opts[ARCHIVE_PATH]}" 2> $_log_tmp_path
        else
            gunzip --stdout "${_opts[ARCHIVE_PATH]}" > "${_opts[EXTRACT_PATH]}/${_archive_name%.*}"
        fi
        ;;
    x-bzip2)
        if [ "${_opts[EXTRACT_PATH]}" = STDOUT ]; then
            bunzip2 --stdout "${_opts[ARCHIVE_PATH]}" 2> $_log_tmp_path
        else
            bunzip2 --stdout "${_opts[ARCHIVE_PATH]}" > "${_opts[EXTRACT_PATH]}/${_archive_name%.*}"
        fi
        ;;
    x-7z-compressed)
        if [ "${_opts[EXTRACT_PATH]}" = STDOUT ]; then
            log_error "Mode d'extraction sur le type d'archive .7z non pris en charge pour le moment"
            return $ERROR_CODE
        else
            7z x "${_opts[ARCHIVE_PATH]}" -o"${_opts[EXTRACT_PATH]}" -y > $_log_tmp_path 2>&1
        fi
        ;;
    *)
        log_error "${FUNCNAME[0]}: format d'archive ($_archive_type) non pris en charge"
        return $ERROR_CODE
        ;;
    esac

    # https://stackoverflow.com/questions/19120263/why-exit-code-141-with-grep-q
    [[ ! $? =~ 0|141 ]] && {
        archive_file "$_log_tmp_path"
        log_error "${FUNCNAME[0]}: erreur lors de l'extraction de l'archive $_archive_name, veuillez consulter $_log_archive_path"
        return $ERROR_CODE
    } || {
        local _last
        get_elapsed_time --start $_start --result _last
        log_info "Extraction avec succès de l'archive $_archive_name en $_last"
    }

    return $SUCCESS_CODE
}

create_archive() {
    local -A _opts &&
    pow_argv \
        --args_n '
            archive_type:Type archive;
            output:Chemin complet archive résultat;
            input:Données à archiver
        ' \
        --args_m 'output;input' \
        --args_d 'archive_type:gzip' \
        --pow_argv _opts "$@" || return $?

    case "${_opts[ARCHIVE_TYPE]}" in
    zip)
        zip --filesync --recurse-paths --junk-paths "${_opts[OUTPUT]}" "${_opts[INPUT]}"
        ;;
    gzip)
        gzip --recursive --stdout "${_opts[INPUT]}" > "${_opts[OUTPUT]}"
        ;;
    x-bzip2)
        bzip2 --stdout "${_opts[INPUT]}" > "${_opts[OUTPUT]}"
        ;;
    *)
        log_error "${FUNCNAME[0]}: format d'archive (${_opts[ARCHIVE_TYPE]}) non pris en charge"
        return $ERROR_CODE
        ;;
    esac

    return $?
}

    #
    # mail
    #

# send mail
#
# message from a file
#  send_mail --subject 'test MAIL body as file' --body ./test_send_mail.txt --attachment "data.txt.gz,full2part.txt.gz" --to <mail>
# message from command line
#  send_mail --subject 'test MAIL body as text' --body 'test envoi message' --attachment "data.txt.gz,full2part.txt.gz" --to <mail>
#
# argument --encoding to convert (UTF8 to LATIN1 (Windows))
#
# see:
#  https://stackoverflow.com/questions/5395082/how-to-send-html-body-email-with-multiple-text-attachments-using-sendmail
#
send_mail() {
    local -A _opts &&
    pow_argv \
        --args_n '
            subject:Objet du message;
            body:Texte du message;
            to:Destinataire(s);
            cc:Copie(s);
            attachment:Liste des fichiers à insérer en pièce jointe (séparés par une virgule, sans espace);
            encoding:Changer le jeu de caractères (la source étant en UTF8);
            format:Format du texte;
            compress:Compression piéce jointe (si pas déjà fait);
            tool:Outil de codage;
            debug:Activer le mode debug;
            verbose:Activer le mode bavard' \
        --args_m '
            subject;
            body;
            to' \
        --args_v '
            encoding:ISO8859-1|WINDOWS-1252|LATIN1;
            format:PLAIN|HTML;
            compress:no|yes;
            tool:uuencode|base64;
            debug:no|yes;
            verbose:no|yes' \
        --args_d '
            format:PLAIN;
            compress:no;
            tool:base64;
            debug:no;
            verbose:no' \
        --pow_argv _opts "$@" || return $?

    # message
    local _body
    [ -f "${_opts[BODY]}" ] && _body=$(< "${_opts[BODY]}") || {
        [ -n "${_opts[BODY]}" ] && _body="${_opts[BODY]}" || {
            log_error 'manque texte du message!'
            return $ERROR_CODE
        }
    }
    # encoding
    [ -n "${_opts[ENCODING]}" ] && {
        _body=$(echo "$_body" | iconv --from-code UTF8 --to-code ${_opts[ENCODING]})
        [ $? -ne 0 ] && {
            log_error "conversion texte du message (UTF8 vers ${_opts[ENCODING]})!"
            return $ERROR_CODE
        }
    }
    # prepare message (w/ optional attachment)
    local _msg=$POW_DIR_TMP/mail_$$.msg
    {
        echo "To: ${_opts[TO]}"
        [ -n "${_opts[CC]}" ] && echo "Cc: ${_opts[CC]}"
        echo "Subject: ${_opts[SUBJECT]}"
        echo 'MIME-Version: 1.0'
        [ "${_opts[DEBUG]}" = yes ] && [ ! -z "${_opts[ATTACHMENT]}" ] && echo '###DEBUG: avec pj' > /dev/stderr
        [ ! -z "${_opts[ATTACHMENT]}" ] && {
            [ "${_opts[DEBUG]}" = yes ] && echo '###DEBUG: ajout BOUNDARY' > /dev/stderr
            echo 'Content-Type: multipart/mixed; boundary="###BOUNDARY"'
            echo '--###BOUNDARY'
        }
        echo 'Content-Type: text/'${get_arg_format,,}
        # seems necessary, this écho! (even in mode PLAIN)
        echo
        echo "$_body"

        [ ! -z "${_opts[ATTACHMENT]}" ] && {
            [ "${_opts[DEBUG]}" = yes ] && echo '###DEBUG: ajout PJ(s)' > /dev/stderr
            local _tmplist='' _file _todo _mime _list=(${_opts[ATTACHMENT]}//,/ }) _i
            for ((_i=0; _i<${#_list[*]}; _i++)); do
                _file=${_list[$_i]}
                [ ! -s "$_file" ] && continue
                [ "${_opts[DEBUG]}" = yes ] && echo '###DEBUG: ajout PJ:'$_file > /dev/stderr
                _todo="$_file"
                if ! is_archive "$_file" ; then
                    [ "${_opts[COMPRESS]}" = yes ] && {
                        _todo+='.gz'
                        gzip --force --stdout "$_file" > "$_todo"
                        [ -n "$_tmplist" ] && _tmplist+=' '
                        _tmplist+="'$_todo'"
                    }
                fi

                _mime=$(get_file_mimetype --file_path "$_todo")

                echo '--###BOUNDARY'
                echo 'Content-Type: '$_mime
                echo 'Content-Transfer-Encoding: '${_opts[TOOL]}
                echo 'Content-Disposition: attachment; filename="'$(basename "$_todo")'"'

                # seems necessary, this écho!
                echo
                # encoding... (base64 as default)
                case ${_opts[TOOL]} in
                uuencode)
                    uuencode "$_todo" "$_todo"
                    ;;
                base64)
                    base64 "$_todo"
                    ;;
                esac
                echo
            done
            # close attachment, w/ last --
            echo '--###BOUNDARY--'
        }
    } > $_msg

    case "${_opts[DEBUG]}" in
    yes)
        cat $_msg
        ;;
    no)
        cat $_msg | sendmail -t
        ;;
    esac

    local _rc=$?
    [ -n "$_tmplist" ] && rm $_tmplist
    [ "${_opts[DEBUG]}" = no ] && [ -f $_msg ] && rm $_msg
    return $_rc
}
