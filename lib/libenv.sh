    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define ENV

# assume POW is correctly installed (w/ POW_DIR_ROOT defined)
source $POW_DIR_ROOT/lib/librun.sh  &&
source $POW_DIR_ROOT/lib/libstd.sh  &&
source $POW_DIR_ROOT/lib/libpg.sh   &&
source $POW_DIR_ROOT/lib/libio.sh   &&
source $POW_DIR_ROOT/lib/bashenv.sh || exit ${ERROR_CODE:-3}

# build piped-values of all delimiters
POW_DELIMITER_JOIN_PIPE="${!POW_DELIMITER[@]}"
POW_DELIMITER_JOIN_PIPE=${POW_DELIMITER_JOIN_PIPE// /|}

# get value of config (associative array POW_CONF defined into bashenv.sh)
get_conf() {
    local _param=$1
    #exemple de lecture prioritaire :
    #POW_CDYI0627_DEV_TRELAZE_CONF
    #POW_DEV_TRELAZE_CONF
    #POW_TRELAZE_CONF
    #POW_PROD_CONF
    #POW_CONF
    local _param_value=

    #NOTE : indispensable pour éviter des erreurs en cas de tableau inexistant :
    #si le tableau existe = n'est pas vide, alors essaye d'y trouver la valeur du paramètre recherché
    eval "[ ! \${#POW_${HOST_ID}_${ENV}_${DATACENTER}_CONF[@]} -eq 0 ] && param_value=\${POW_${HOST_ID}_${ENV}_${DATACENTER}_CONF['${_param}']}"
    if [ -n "$param_value" ]; then
        echo $param_value
    else
        eval "[ ! \${#POW_${ENV}_${DATACENTER}_CONF[@]} -eq 0 ] && param_value=\${POW_${ENV}_${DATACENTER}_CONF['${_param}']}"
        if [ -n "$param_value" ]; then
            echo $param_value
        else
            eval "[ ! \${#POW_${DATACENTER}_CONF[@]} -eq 0 ] && param_value=\${POW_${DATACENTER}_CONF['${_param}']}"
            if [ -n "$param_value" ]; then
                echo $param_value
            else
                eval "[ ! \${#POW_${ENV}_CONF[@]} -eq 0 ] && param_value=\${POW_${ENV}_CONF['${_param}']}"
                if [ -n "$param_value" ]; then
                    echo $param_value
                else
                    eval "[ ! \${#POW_CONF[@]} -eq 0 ] && param_value=\${POW_CONF['${_param}']}"
                    if [ -n "$param_value" ]; then
                        echo $param_value
                    else
                        log_error "Le paramètre ${_param} n'existe pas"
                        return $ERROR_CODE
                    fi
                fi
            fi
        fi
    fi

    return $SUCCESS_CODE
}

# set parameter of config
set_param_conf_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            conf_file:Chemin complet vers le fichier de configuration;
            param_code:Code du paramètre;
            param_value:Valeur du paramètre;
            param_separator:Séparateur entre le code et la valeur;
            param_is_multiple:Indique si le paramètre peut être présent de multiples fois avec des valeurs différentes tel que le paramètre extension de PHP' \
        --args_m 'conf_file;param_code;param_value' \
        --args_v 'param_is_multiple:yes|no' \
        --args_d 'param_is_multiple:no;param_separator:=' \
        --args_p '
            tag:param_is_multiple@bool
        ' \
        --pow_argv _opts "$@" || return $?

    local _line_content _search_pattern _line_number

    [ ! -f "${_opts[CONF_FILE]}" ] && {
        log_info "Le fichier ${_opts[CONF_FILE]} n'existe pas, création"
        touch ${_opts[CONF_FILE]} || exit $ERROR_CODE
    }
    _line_content="${_opts[PARAM_CODE]}${_opts[PARAM_SEPARATOR]}${_opts[PARAM_VALUE]}"

    #test : regexpEscape '.*?+[](){}|$^\'
    regexpEscape() {
        #NOTE : on part du principe que les regex avancées sur activées si on échappe pour une utilisation avec grep (--perl-regex) et sed (--regexp-extended)
        #NOTE : il faut aussi doubler les \ pour qu'ils ne soient pas considérés comme des caractères d'échappement
        echo $@ | sed --regexp-extended 's/([.*?+[(){}|$^\\]|\])/\\\0/g'
    }
    #test : sedEscape 'http://toto'
    sedEscape() {
        #NOTE : on part du principe qu'on utilise le / comme séparateur de pattern/remplacement
        echo $@ | sed --regexp-extended 's/\//\\\0/g'
    }

    #Si le paramère déjà correctement configuré (avec éventuellement des tabulations avant, ainsi que des espaces autour du séparateur clé/valeur)
    _search_pattern="(\s*)$(regexpEscape ${_opts[PARAM_CODE]})( ?${_opts[PARAM_SEPARATOR]} ?)$(regexpEscape ${_opts[PARAM_VALUE]})"
    grep --perl-regexp --quiet "^${_search_pattern}$" ${_opts[CONF_FILE]} && {
        #echo "Le motif ^${_search_pattern}$ est trouvé dans ${_opts[CONF_FILE]}, il n'y a rien à faire"
        return $SUCCESS_CODE
    }

    _search_pattern="(\s*)$(regexpEscape ${_opts[PARAM_CODE]})( ?${_opts[PARAM_SEPARATOR]} ?).*"
    #Si le paramètre est déjà configuré (implicitement avec une autre valeur)
    grep --perl-regexp --quiet "^${_search_pattern}$" ${_opts[CONF_FILE]} && {
        #Et que le paramètre n'est pas multiple
        [ "${_opts[PARAM_IS_MULTIPLE]}" = 'no' ] && {
            #Alors on le remplace
            echo "Le motif ^${_search_pattern}$ est trouvé dans ${_opts[CONF_FILE]}, on le remplace par ${_line_content}"
            sed \
                --in-place \
                --regexp-extended \
                "s/^$(sedEscape ${_search_pattern})$/\1$(sedEscape ${_opts[PARAM_CODE]})\2$(sedEscape ${_opts[PARAM_VALUE]})/" \
                ${_opts[CONF_FILE]} && return $SUCCESS_CODE || return $ERROR_CODE
        } || {
            #Sinon (le paramètre est multiple)
            #Alors on ajoute le paramètre à la ligne suivante du dernier trouvé (exemple : "extension=toto.so" est présent et on veut ajouter juste après "extension=tata.so")
            echo "Le motif ^${_search_pattern}$ est trouvé dans ${_opts[CONF_FILE]}, on ajoute ${_line_content} à la ligne suivante"
            _line_number=$(grep --perl-regexp -n "^${_search_pattern}$" ${_opts[CONF_FILE]} \
                | cut --delimiter ':' --field 1 \
                | tail --lines 1)
            ((_line_number++))
            sed \
                --in-place \
                --regexp-extended \
                "${_line_number}i$(sedEscape ${_line_content})" \
                ${_opts[CONF_FILE]} && return $SUCCESS_CODE || return $ERROR_CODE
        }
    }

    #Sinon, si le paramètre est présent mais commenté (caractère # ou ; avec éventuellement des tabulations avant ou après)
    _search_pattern="(\s*)(#|;)${_search_pattern}"
    grep --perl-regexp --quiet "^${_search_pattern}$" ${_opts[CONF_FILE]} && {
        #Alors on ajoute le paramètre à la ligne suivante du dernier trouvé
        echo "Le motif ^${_search_pattern}$ est trouvé dans ${_opts[CONF_FILE]}, on ajoute ${_line_content} à la ligne suivante"
        _line_number=$(grep --perl-regexp -n "^${_search_pattern}$" ${_opts[CONF_FILE]} \
                | cut --delimiter ':' --field 1 \
                | tail --lines 1)
        ((_line_number++))
        sed \
            --in-place \
            --regexp-extended \
            "${_line_number}i$(sedEscape ${_line_content})" \
            ${_opts[CONF_FILE]} && return $SUCCESS_CODE || return $ERROR_CODE
    }

    #Sinon, le paramètre est absent, on l'ajoute à la fin du fichier
    echo "Rien n'a été trouvé dans ${_opts[CONF_FILE]}, on ajoute ${_line_content} à la fin du fichier"
    echo "$_line_content" >> ${_opts[CONF_FILE]} && return $SUCCESS_CODE || return $ERROR_CODE
}

# set parameters of config
set_params_conf_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            conf_file:Chemin complet vers le fichier de configuration;
            param_codes:Codes des paramètres séparés par des espaces;
            param_values:Valeurs des paramètres séparés par des espaces;
            param_separator:Séparateur entre le code et la valeur;
            param_is_multiple:Indique si le paramètre est une liste tel que le paramètre extension dans la configuration Apache par exemple' \
        --args_m 'conf_file;param_codes;param_values' \
        --args_v 'param_is_multiple:yes|no' \
        --args_d 'param_is_multiple:no;param_separator:=' \
        --args_p '
            tag:param_is_multiple@bool
        ' \
        --pow_argv _opts "$@" || return $?

    local _index_param_code=0 _param_code

    if [ "${#param_codes[@]}" != "${#param_values[@]}" ]; then
        log_error "${FUNCNAME[0]} : il n'y a pas autant de codes que de valeurs"
        return $ERROR_CODE
    fi
    for _param_code in "${_opts[PARAM_CODES]}[@]}"; do
        set_param_conf_file \
            --conf_file "${_opts[CONF_FILE]}" \
            --param_code "$_param_code" --param_value "${_opts[PARAM_VALUES]}[$_index_param_code]}" \
            --param_separator "${_opts[PARAM_SEPARATOR]}" \
            --param_is_multiple "${_opts[PARAM_IS_MULTIPLE]}" || return $ERROR_CODE
        ((_index_param_code++))
    done

    return $SUCCESS_CODE
}

# get password from .pgpass
get_pg_passwd() {
    local -A _opts &&
    pow_argv \
        --args_n '
            user_name:login;
            password:password
        ' \
        --args_m '
            user_name;
            password
        ' \
        --pow_argv _opts "$@" || return $?

    local _passwd_file=.pgpass _passwd_home _dir_home _tmp
    local -n _passwd_ref=${_opts[PASSWORD]}

    _dir_home=$(getent passwd $POW_USER | cut --delimiter : --field 6)
    [ -z "$_dir_home" ] && {
        log_error "erreur dossier personnel ($POW_USER)"
        return $ERROR_CODE
    }
    _passwd_home="$_dir_home/$_passwd_file"
    [ ! -f "$_passwd_home" ] && {
        log_error "non existence .pgpass ($POW_USER)"
        return $ERROR_CODE
    }

    # search for user name
    _tmp=$(grep --perl-regexp \
            '^[^:]*:[^:]*:[^:]*:'${_opts[USER_NAME]}':[^:]*$' < "$_passwd_home" | \
            cut --delimiter : --field 5)
    [ -z "$_tmp" ] && {
        log_error "non existence login (${_opts[USER_NAME]})"
        return $ERROR_CODE
    }
    _passwd_ref="$_tmp"

    return $SUCCESS_CODE
}

# create logins file in user home directory
_set_pg_passwd() {
    local _passwd_file=.pgpass _passwd_home _dir_home _tmp _passwd_files _i _pg_login

    _dir_home=$(getent passwd $POW_USER | cut --delimiter : --field 6)
    [ -z "$_dir_home" ] && {
        log_error "erreur dossier personnel ($POW_USER)"
        return $ERROR_CODE
    }

    # create logins file (if not exists)
    _passwd_home="$_dir_home/$_passwd_file"
    [ ! -f "$_passwd_home" ] && {
        get_tmp_file --tmpfile _tmp --create yes --chmod 600    &&
        mv $_tmp "$_passwd_home"                                || {
            log_error "erreur création .pgpass ($POW_USER)"
            return $ERROR_CODE
        }
    }

    # check all pgpass (adding new one)
    declare -a _passwd_files=( $POW_DIR_ROOT/bin/admin/install.d/.pgpass* )
    for ((_i=0; _i<${#_passwd_files[*]}; _i++)); do
        _pg_login=$(cut --delimiter : --field 4 < ${_passwd_files[$_i]})
        [ -n "$_pg_login" ] && {
            grep --silent "$_pg_login" "$_passwd_home"
            [ $? -eq 1 ] && {
                sed \
                    --expression 's/%PG_HOST%/'$POW_PG_HOST'/' \
                    --expression 's/%PG_PORT%/'$POW_PG_PORT'/' \
                    --expression 's/%PG_DBNAME%/'$POW_PG_DBNAME'/' \
                    < ${_passwd_files[$_i]} >> "$_passwd_home" || {
                        log_error "erreur ajout .pgpass (${_passwd_files[$_i]})"
                        return $ERROR_CODE
                }
            } || true
        } || {
            log_error "erreur format (${_passwd_files[$_i]}) : manque pg_login!"
            return $ERROR_CODE
        }
    done

    return $SUCCESS_CODE
}

# initialize PostgreSQL's context (user, passwd, default_schema)
_set_pg_env() {
    local -A _opts &&
    pow_argv \
        --args_n 'schema_name:code applicatif du schéma à utiliser' \
        --args_d 'schema_name:public' \
        --pow_argv _opts "$@" || return $?

    local _std=(admin public)

    in_array --array _std --item "${_opts[SCHEMA_NAME]}" && {
        POW_PG_USERNAME=postgres
        POW_PG_DEFAULT_SCHEMA=public
    } || {
        POW_PG_USERNAME=${_opts[SCHEMA_NAME]}
        POW_PG_DEFAULT_SCHEMA=${_opts[SCHEMA_NAME]}
    }

    POW_PG_DBNAME=$(get_conf PG_DBNAME)
    # path for tools (psql, pg_dump, pg_restore, ...)
    POW_DIR_PG_BIN="/usr/lib/postgresql/$(get_conf PG_VERSION)/bin"
    export POW_PG_DBNAME POW_DIR_PG_BIN POW_PG_USERNAME POW_PG_DEFAULT_SCHEMA
    _set_pg_passwd || return $ERROR_CODE

    return $SUCCESS_CODE
}

# custom PostgreSQL's context (w/ given schema)
set_env_pg() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:code applicatif du schéma à utiliser;
            host:Hostname du moteur PostgreSQL;
            port:Port IP du moteur PostgreSQL;
            reset:Réinitialise les valeurs par défaut;
            print:Affichage du contexte
        ' \
        --args_v '
            reset:no|yes;
            print:no|yes
        ' \
        --args_d '
            reset:no;
            print:no;
            schema_name:public
        ' \
        --args_p '
            tag:reset@bool,print@bool
        ' \
        --pow_argv _opts "$@" || return $?

    [ "${_opts[PRINT]}" = yes ] && {
        echo "POW's PostgreSQL context"
        echo -e "\thost:port=(${POW_PG_HOST}:${POW_PG_PORT})"
        echo -e "\tlogin=${POW_PG_USERNAME}"
        echo -e "\tdefault_schema=$POW_PG_DEFAULT_SCHEMA"

        return $SUCCESS_CODE
    }

    [ "${_opts[RESET]}" = yes ] && {
        POW_PG_HOST=
        POW_PG_PORT=
    }

    # initialize from argument (or default value)
    [ -n "${_opts[HOST]}" ] && POW_PG_HOST=${_opts[HOST]} || {
        [ -z "$POW_PG_HOST" ] && POW_PG_HOST=$(get_conf PG_HOST)
    }
    [ -n "${_opts[PORT]}" ] && POW_PG_PORT=${_opts[PORT]} || {
        [ -z "$POW_PG_PORT" ] && POW_PG_PORT=$(get_conf PG_PORT)
    }

    _set_pg_env --schema_name ${_opts[SCHEMA_NAME]} &&
    export POW_PG_HOST POW_PG_PORT || {
        log_error 'contexte PostgreSQL'
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# custom Host's environment (w/ given schema)
set_env_dirs() {
    local -A _opts &&
    pow_argv \
        --args_n 'schema_name:code applicatif du schéma à utiliser' \
        --args_d 'schema_name:public' \
        --pow_argv _opts "$@" || return $?

    # define DIRs
    local _dir
    declare -A _dirs

    _dirs[POW_DIR_BATCH]="$POW_DIR_ROOT/bin/${_opts[SCHEMA_NAME]}"
    _dirs[POW_DIR_BATCH_ADMIN]="$POW_DIR_ROOT/bin/admin"
    _dirs[POW_DIR_BATCH_PUBLIC]="$POW_DIR_ROOT/bin/public"

    _dirs[POW_DIR_IMPORT]="$POW_DIR_DATA/import/${_opts[SCHEMA_NAME]}"
    _dirs[POW_DIR_EXPORT]="$POW_DIR_DATA/export/${_opts[SCHEMA_NAME]}"
    _dirs[POW_DIR_TMP]="$POW_DIR_DATA/tmp/${_opts[SCHEMA_NAME]}"
    _dirs[POW_DIR_ARCHIVE]="$POW_DIR_DATA/archive/${_opts[SCHEMA_NAME]}/$(date +%Y%m%d-%T)"
    _dirs[POW_DIR_COMMON_GLOBAL]="$POW_DIR_DATA/common"
    _dirs[POW_DIR_COMMON_GLOBAL_SCHEMA]="$POW_DIR_DATA/common/${_opts[SCHEMA_NAME]}"

    #declare -p _dirs
    for _dir in ${!_dirs[@]}; do
        mkdir --parents ${_dirs[$_dir]} || {
            log_error "erreur création dossier ${_dirs[$_dir]}"
            return $ERROR_CODE
        }

        export "$_dir=${_dirs[$_dir]}"
    done

    return $SUCCESS_CODE
}

# custom POW's environment (w/ given schema)
set_env() {
    local -A _opts &&
    pow_argv \
        --args_n 'schema_name:code applicatif du schéma à utiliser' \
        --args_d 'schema_name:public' \
        --pow_argv _opts "$@" || return $?

    # check for schema (from directory source)
    {
        local _schemas=($(ls -1d "$POW_DIR_ROOT/bin/"* | xargs --max-args 1 basename))
        in_array --array _schemas --item "${_opts[SCHEMA_NAME]}" || {
            log_error 'schéma non valide!'
            false
        }
    } &&
    set_env_pg --schema_name ${_opts[SCHEMA_NAME]} &&
    set_env_dirs --schema_name ${_opts[SCHEMA_NAME]} || {
        log_error 'contexte POW'
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# set DEBUG env (from global variable: POW_DEBUG_JSON)
# TODO
#+ add "parent" of code to limit (or not if null) at this parent (solution w/ caller?)
#+ add "init" field ONCE|EVERYTIME of code to do (or not) JSON init
set_env_debug() {
    local _tmp _tmp2 _code _steps _step _break _list_steps _list_breaks
    local -a _array_steps _array_codes

    [ -n "$POW_DEBUG_JSON" ] && {
        _tmp=$(jq --compact-output --raw-output '.codes // empty' <<< $POW_DEBUG_JSON)
        [ -n "$_tmp" ] && {
            # steps of code(s)
            for _tmp in $(jq \
                --compact-output \
                --raw-output \
                '.codes[] | .name + ":" + (.steps | join(","))' <<< "$POW_DEBUG_JSON"
            ); do
                #echo $_tmp
                _code=${_tmp%%:*}
                # minus case
                [ "${_code:0:1}" = - ] && {
                    [ ${POW_DEBUG_STEPS[${_code:1}]+_} ] && {
                        unset 'POW_DEBUG_STEPS[${_code:1}]'
                        [ ${POW_DEBUG_BREAKPOINTS[${_code:1}]+_} ] && unset 'POW_DEBUG_BREAKPOINTS[${_code:1}]'
                    }
                    continue
                }
                # add new code
                _array_codes+=($_code)
                # get steps
                _steps=${_tmp#*:}
                _array_steps=(${_steps//,/ })
                POW_DEBUG_BREAKPOINTS[$_code]=
                for _tmp2 in "${_array_steps[@]}"; do
                    #echo tmp2=$_tmp2
                    _step=${_tmp2%%@*}
                    #echo step=$_step
                    _break=${_tmp2#*@}
                    #echo break=$_break

                    [ -n "$_list_steps" ] && _list_steps+=' '
                    _list_steps+=$_step
                    #echo list_steps=$_list_steps

                    [ "${_break^^}" = BREAK ] && {
                        [ -n "$_list_breaks" ] && _list_breaks+=' '
                        _list_breaks+=$_step
                    }
                    #echo list_breaks=$_list_breaks
                    #read
                done
                # add code w/ it(s) step(s) ...
                POW_DEBUG_STEPS[$_code]=$_list_steps
                # ... and optional breakpoint(s)
                [ -n "$_list_breaks" ] && POW_DEBUG_BREAKPOINTS[$_code]=$_list_breaks
            done
        }

        # purge old codes
        for _code in ${!POW_DEBUG_STEPS[@]}; do
            [[ " ${_array_codes[*]} " == *" $_code "* ]] || {
                unset 'POW_DEBUG_STEPS[${_code}]'
                [ ${POW_DEBUG_BREAKPOINTS[${_code}]+_} ] && unset 'POW_DEBUG_BREAKPOINTS[${_code}]'
            }
        done

        # properties
        local _k _v _key
        local -A _properties
        _tmp=$(jq --compact-output --raw-output '.properties // empty' <<< $POW_DEBUG_JSON)
        [ -n "$_tmp" ] && {
            for _tmp in $(jq \
                --compact-output \
                --raw-output \
                '.properties | to_entries[] | .key + "=" + .value' <<< $POW_DEBUG_JSON
            ); do
                _k=${_tmp%%=*}
                _v=${_tmp#*=}
                properties[$_k]=$_v
            done
            #declare -p _properties ; read
            for _key in ${!_properties[@]}; do
                # UPPER(k,v)
                POW_DEBUG_PROPERTIES[${_key^^}]=${_properties[$_key]^^}
            done
        }
    }

    return $SUCCESS_CODE
}

# get DEBUG env for a given code
get_env_debug() {

#     can't call pow_argv(), to enable debug of this function (else deadlock!)

#     local -A _opts &&
#     pow_argv \
#         --args_n '
#             code:Code applicatif (script ou fonction);
#             steps:Ensemble des étapes avec DEBUG;
#             breakpoints:Ensemble des points arrêt (sur étape);
#             all:Ensemble des étapes du Code
#         ' \
#         --args_m 'code;steps;breakpoints' \
#         --pow_argv _opts "$@" || return $?
#
#     local -n _steps_ref=${_opts[STEPS]} _breakpoints_ref=${_opts[BREAKPOINTS]}

    local _code=$1 _all_steps="$4" _list_steps _step
    local -a _array_steps _array_bps
    local -n _debug_steps_ref=$2 _debug_bps_ref=$3

    set_env_debug
    [ ${POW_DEBUG_STEPS[${_code}]+_} ] && {
        case "${POW_DEBUG_STEPS[${_code}]}" in
        all|ALL)
            _list_steps="$_all_steps"
            ;;
        *)
            _list_steps="${POW_DEBUG_STEPS[${_code}]}"
            ;;
        esac

        [ -n "$_list_steps" ] && {
            _array_steps=($_list_steps)
            _array_bps=(${POW_DEBUG_BREAKPOINTS[${_code}]})

            for _step in $_list_steps; do
                _debug_steps_ref[$_step]=$([[ " ${_array_steps[*]} " == *" $_step "* ]] ; echo $?)
                _debug_bps_ref[$_step]=$([[ " ${_array_bps[*]} " == *" $_step "* ]] ; echo $?)
            done

#             echo "with_steps=${_debug_steps_ref[@]}"
#             echo "with_bps=${_debug_bps_ref[@]}"
        }
    }

    return $SUCCESS_CODE
}
