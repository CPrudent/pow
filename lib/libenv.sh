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
