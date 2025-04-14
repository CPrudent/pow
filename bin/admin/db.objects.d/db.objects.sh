#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build POW's db

# https://stackoverflow.com/questions/54773652/is-there-a-way-to-prepend-to-a-bash-array-without-writing-a-function
_t=1
_schemas=(
    [((_t++))]=fr
)
# NOTE: begin with 'admin' schema
_schemas[0]=admin
# NOTE: end with 'public' schema
_schemas+=(public)

# transform as pipe-delimited (to possible values 'schema_only' below)
_schemas_join_pipe=${_schemas[@]}
_schemas_join_pipe=${_schemas_join_pipe// /|}

# TODO add reset option to delete all tables
# https://stackoverflow.com/questions/3327312/how-can-i-drop-all-the-tables-in-a-postgresql-database

bash_args \
    --args_p '
        schema_only:Limiter la mise à jour à un schéma;
        constant:Indicateur de génération des constantes;
        relocate:Indicateur de changement de schéma (après restauration)
    ' \
	--args_v "
        schema_only:${_schemas_join_pipe};
        constant:no|yes;
        relocate:no|yes
    " \
    --args_d '
        constant:no;
        relocate:no
    ' \
    "$@" || exit $?

[ -n "$get_arg_schema_only" ] && _schemas=($get_arg_schema_only)

log_info "Mise à jour de la structure de la base de données"
# superuser
set_env --schema_name admin &&
# need drop_all_functions_if_exists()
execute_query \
    --name CREATE_DROP_FUNCTIONS \
    --query "$POW_DIR_BATCH/db.objects.d/functions/drop.sql" &&
# needed to avoid error "type geometry not exists"
execute_query \
    --name PREPARE_EXTENSION_POSTGIS \
    --query "$POW_DIR_BATCH/db.objects.d/actions/extension_postgis.sql" || exit $ERROR_CODE
for _schema in ${_schemas[@]}; do
    # begins w/ admin (core functions)
    # be careful, because relocation has to be run by superuser
    if [ -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/db.objects.sql" ] || ([ -f "$POW_DIR_BATCH/db.objects.d/actions/relocate.sql" ] && [ "$get_arg_relocate" = yes ]); then
        log_info "Traitement schéma($_schema)" &&
        {
            {
                if ([ -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/relocate.sql" ] && [ "$get_arg_relocate" = yes ]); then
                    execute_query \
                        --name RELOCATE \
                        --query "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/relocate.sql"
                fi
            } &&
            set_env --schema_name $_schema &&
            {
                if [ -f "$POW_DIR_BATCH/db.objects.d/db.objects.sql" ]; then
                    execute_query \
                        --name CREATE_OBJECTS \
                        --query "$POW_DIR_BATCH/db.objects.d/db.objects.sql"
                fi
            } &&
            {
                if [ "$get_arg_constant" = yes ] && [ -x "$POW_DIR_BATCH/constant.sh" ]; then
                    $POW_DIR_BATCH/constant.sh
                fi
            } &&
            set_env --schema_name admin &&
            {
                if ([ -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/relocate.sql" ] && [ "$get_arg_relocate" = yes ] && [ -f "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/purge_after_relocate.sql" ]); then
                    execute_query \
                        --name RELOCATE_PURGE \
                        --query "$POW_DIR_BATCH/../${_schema}/db.objects.d/actions/purge_after_relocate.sql"
                fi
            }
        } || {
            log_error "Echec mise à jour de la structure de $_schema"
            exit $ERROR_CODE
        }
    fi
done
execute_query \
    --name 'PERMISSIONS' \
    --query "$POW_DIR_BATCH/db.objects.d/actions/grant.sql" &&
execute_query \
    --name 'PURGE' \
    --query "$POW_DIR_BATCH/db.objects.d/actions/purge.sql" || exit $ERROR_CODE

exit $SUCCESS_CODE
