#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build POW's db

# https://stackoverflow.com/questions/54773652/is-there-a-way-to-prepend-to-a-bash-array-without-writing-a-function
_t=1
# TODO dynamize land schemas
_schemas=(
    [((_t++))]=fr
)
# NOTE: begin with 'admin' schema
_schemas[0]=admin
# NOTE: end with 'public' schema
_schemas+=(public)

# transform as pipe-delimited (enable values of 'schema_only' below)
_SCHEMAS_JOIN_PIPE=${_schemas[@]}
_SCHEMAS_JOIN_PIPE=${_SCHEMAS_JOIN_PIPE// /|}

# TODO add reset option to delete all tables
# https://stackoverflow.com/questions/3327312/how-can-i-drop-all-the-tables-in-a-postgresql-database

pow_argv \
    --args_n '
        schema_only:Limiter la mise à jour à un schéma;
        constant:Indicateur de génération des constantes;
        relocate:Indicateur de changement de schéma (après restauration)
    ' \
	--args_v "
        schema_only:${_SCHEMAS_JOIN_PIPE};
        constant:no|yes;
        relocate:no|yes
    " \
    --args_d '
        constant:no;
        relocate:no
    ' \
    --args_p '
        tag:constant@bool,relocate@bool,schema_only@0N
    ' \
    "$@" || exit $?

[ -n "${POW_ARGV[SCHEMA_ONLY]}" ] && _schemas=(${POW_ARGV[SCHEMA_ONLY]})

_error='' &&
log_info "Mise à jour de la structure de la base de données" &&
# superuser
set_env --schema_name admin &&
# need drop_all_functions_if_exists()
execute_query \
    --name CREATE_DROP_FUNCTIONS \
    --query "$POW_DIR_BATCH/db.objects.d/functions/drop.sql" &&
# needed to avoid error "type geometry not exists"
execute_query \
    --name PREPARE_EXTENSION_POSTGIS \
    --query "$POW_DIR_BATCH/db.objects.d/actions/extension_postgis.sql" &&
for _schema in ${_schemas[@]}; do
    # begins w/ admin (core functions)
    # be careful, because relocation has to be run by superuser
    log_info "Traitement schéma($_schema)" &&
    # relocate backup data (from BCAA)
    {
        [ "${POW_ARGV[RELOCATE]}" = no ] ||
        [ ! -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/relocate.sql" ] ||
        execute_query \
            --name RELOCATE \
            --query "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/relocate.sql"
    } &&
    set_env --schema_name $_schema &&
    # objects of schema
    {
        [ ! -f "$POW_DIR_BATCH/db.objects.d/db.objects.sql" ] || {
            execute_query \
                --name CREATE_OBJECTS \
                --query "$POW_DIR_BATCH/db.objects.d/db.objects.sql"
        }
    } &&
    # create constants (if exist)
    {
        [ "${POW_ARGV[CONSTANT]}" = no ] ||
        [ ! -x "$POW_DIR_BATCH/constant.sh" ] ||
        $POW_DIR_BATCH/constant.sh
    } &&
    set_env --schema_name admin &&
    {
        [ "${POW_ARGV[RELOCATE]}" = no ] ||
        [ ! -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/relocate.sql" ] ||
        [ ! -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/purge_after_relocate.sql" ] ||
        execute_query \
            --name PURGE_AFTER_RELOCATE \
            --query "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/purge_after_relocate.sql"
    } || {
        _error "Echec mise à jour de la structure de $_schema"
        break
    }
done &&
[ -z "$_error" ] &&
execute_query \
    --name 'PERMISSIONS' \
    --query "$POW_DIR_BATCH/db.objects.d/actions/grant.sql" &&
execute_query \
    --name 'PURGE' \
    --query "$POW_DIR_BATCH/db.objects.d/actions/purge.sql" || {
    [ -n "$_error" ] && log_error "$_error"
    exit $ERROR_CODE
}

exit $SUCCESS_CODE
