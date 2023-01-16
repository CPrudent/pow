    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define ENV

# echo "\$0: $(dirname $0)"
# echo "realpath \$0: $(dirname $(realpath $0))"
# echo "BASH_SOURCE: $(dirname ${BASH_SOURCE[0]})"
_dir_lib=$(dirname ${BASH_SOURCE[0]})
source $_dir_lib/librun.sh &&
source $_dir_lib/libstd.sh || exit ${ERROR_CODE:-3}

# best practices: see https://gist.github.com/outro56/4a2403ae8fefdeb832a5
set -o pipefail

# global config
declare -A POW_CONF=(
    [JAVA_HOME]=/usr/lib/jvm/default-java
    [PG_DBNAME]=pow
    [PG_VERSION]=15
    [POSTGIS_VERSION]=3
    [PG_PORT]=5432
)

# get value of config
get_conf() {
    #bash_args \
    #    --args_p 'param:Code du paramètre' \
    #    --args_o 'param' \
    #    "$@" || return $ERROR_CODE
    #local _param=$get_arg_param
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

set_param_conf_file() {
    bash_args \
		--args_p 'conf_file:Chemin complet vers le fichier de configuration;
                param_code:Code du paramètre;
                param_value:Valeur du paramètre;
                param_separator:Séparateur entre le code et la valeur;
                param_is_multiple:Indique si le paramètre peut être présent de multiples fois avec des valeurs différentes tel que le paramètre extension de PHP' \
		--args_o 'conf_file;param_code;param_value' \
        --args_v 'param_is_multiple:yes|no' \
        --args_d 'param_is_multiple:no;param_separator:=' \
		"$@" || return $ERROR_CODE

    local conf_file=$get_arg_conf_file
    local param_code=$get_arg_param_code
    local param_value=$get_arg_param_value
    local param_separator=$get_arg_param_separator
    [ "$param_separator" = 'ESPACE' ] && param_separator=' '; #contournement de bash_args qui ne lit pas une valeur d'argument ne contenant qu'un espace ' '
    local param_is_multiple=$get_arg_param_is_multiple
    [ ! -f "$conf_file" ] && {
        log_info "Le fichier $conf_file n'existe pas, création"
        touch $conf_file || exit $ERROR_CODE
    }
    _line_content="${param_code}${param_separator}${param_value}"

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
    _motif_recherche="(\s*)$(regexpEscape ${param_code})( ?${param_separator} ?)$(regexpEscape ${param_value})"
    grep --perl-regexp --quiet "^${_motif_recherche}$" $conf_file && {
        #echo "Le motif ^${_motif_recherche}$ est trouvé dans ${conf_file}, il n'y a rien à faire"
        return $SUCCESS_CODE
    }

    _motif_recherche="(\s*)$(regexpEscape ${param_code})( ?${param_separator} ?).*"
    #Si le paramètre est déjà configuré (implicitement avec une autre valeur)
    grep --perl-regexp --quiet "^${_motif_recherche}$" $conf_file && {
        #Et que le paramètre n'est pas multiple
        [ "$param_is_multiple" = 'no' ] && {
            #Alors on le remplace
            echo "Le motif ^${_motif_recherche}$ est trouvé dans ${conf_file}, on le remplace par ${_line_content}"
            sed --in-place --regexp-extended "s/^$(sedEscape ${_motif_recherche})$/\1$(sedEscape ${param_code})\2$(sedEscape ${param_value})/" $conf_file && return $SUCCESS_CODE || return $ERROR_CODE
        } || {
            #Sinon (le paramètre est multiple)
            #Alors on ajoute le paramètre à la ligne suivante du dernier trouvé (exemple : "extension=toto.so" est présent et on veut ajouter juste après "extension=tata.so")
            echo "Le motif ^${_motif_recherche}$ est trouvé dans ${conf_file}, on ajoute ${_line_content} à la ligne suivante"
            _line_number=$(grep --perl-regexp -n "^${_motif_recherche}$" $conf_file | cut -d':' -f1 | tail -1)
            ((_line_number++))
            sed --in-place --regexp-extended "${_line_number}i$(sedEscape ${_line_content})" $conf_file && return $SUCCESS_CODE || return $ERROR_CODE
        }
    }

    #Sinon, si le paramètre est présent mais commenté (caractère # ou ; avec éventuellement des tabulations avant ou après)
    _motif_recherche="(\s*)(#|;)${_motif_recherche}"
    grep --perl-regexp --quiet "^${_motif_recherche}$" $conf_file && {
        #Alors on ajoute le paramètre à la ligne suivante du dernier trouvé
        echo "Le motif ^${_motif_recherche}$ est trouvé dans ${conf_file}, on ajoute ${_line_content} à la ligne suivante"
        _line_number=$(grep --perl-regexp -n "^${_motif_recherche}$" $conf_file | cut -d':' -f1 | tail -1)
        ((_line_number++))
        sed --in-place --regexp-extended "${_line_number}i$(sedEscape ${_line_content})" $conf_file && return $SUCCESS_CODE || return $ERROR_CODE
    }

    #Sinon, le paramètre est absent, on l'ajoute à la fin du fichier
    echo "Rien n'a été trouvé dans ${conf_file}, on ajoute ${_line_content} à la fin du fichier"
    echo "$_line_content" >> $conf_file && return $SUCCESS_CODE || return $ERROR_CODE
}

set_params_conf_file() {
    bash_args \
		--args_p 'conf_file:Chemin complet vers le fichier de configuration;
                param_codes:Codes des paramètres séparés par des espaces;
                param_values:Valeurs des paramètres séparés par des espaces;
                param_separator:Séparateur entre le code et la valeur;
                param_is_multiple:Indique si le paramètre est une liste tel que le paramètre extension dans la configuration Apache par exemple' \
		--args_o 'conf_file;param_codes;param_values' \
        --args_v 'param_is_multiple:yes|no' \
        --args_d 'param_is_multiple:no;param_separator:=' \
		"$@" || return $ERROR_CODE

    local conf_file=$get_arg_conf_file
    local param_codes=($get_arg_param_codes)
    local param_values=($get_arg_param_values)
    local param_separator=$get_arg_param_separator
    local param_is_multiple=$get_arg_param_is_multiple
    if [ "${#param_codes[@]}" != "${#param_values[@]}" ]; then
        echo "set_params_conf_file : il n'y a pas autant de codes que de valeurs"
        return $ERROR_CODE
    fi
    _index_param_code=0
    for _param_code in "${param_codes[@]}"; do
        set_param_conf_file --conf_file "$conf_file" \
            --param_code "$_param_code" --param_value "${param_values[$_index_param_code]}" \
            --param_separator "$param_separator" --param_is_multiple "$param_is_multiple" || return $ERROR_CODE
        ((_index_param_code++))
    done
    return $SUCCESS_CODE
}

set_env_dirs() {
    bash_args \
        --args_p 'schema_code:code applicatif du schéma à utiliser' \
        --args_o 'schema_code' \
        --args_d 'schema_code:public' \
        "$@" || return $ERROR_CODE

    expect env POW_DIR_ROOT &&
    expect env POW_DIR_DATA || return $ERROR_CODE

    # define DIRs
    local _dirs _dir
    declare -A _dirs
    _dirs[dir_batch]="$POW_DIR_ROOT/bin/$get_arg_schema_code"
    _dirs[dir_batch_admin]="$POW_DIR_ROOT/bin/admin"
    _dirs[dir_batch_public]="$POW_DIR_ROOT/bin/public"

    _dirs[dir_import]="$POW_DIR_DATA/import/$get_arg_schema_code"
    _dirs[dir_export]="$POW_DIR_DATA/export/$get_arg_schema_code"
    _dirs[dir_tmp]="$POW_DIR_DATA/tmp/$get_arg_schema_code"
    _dirs[dir_archive]="$POW_DIR_DATA/archive/$get_arg_schema_code"
    _dirs[dir_common_global]="$POW_DIR_DATA/common"
    _dirs[dir_common_global_schema]="$POW_DIR_DATA/common/$get_arg_schema_code"

    #declare -p _dirs
    for _dir in ${!_dirs[@]}; do
        mkdir -p ${_dirs[$_dir]} || {
            log_error "erreur création dossier ${_dirs[$_dir]}"
            return $ERROR_CODE
        }

        export "$_dir=${_dirs[$_dir]}"
    done

    return $SUCCESS_CODE
}
