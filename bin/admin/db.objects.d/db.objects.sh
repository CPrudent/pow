#!/bin/bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build POW's db

# https://stackoverflow.com/questions/54773652/is-there-a-way-to-prepend-to-a-bash-array-without-writing-a-function
_t=1
_schemas=(
    [((_t++))]=bal
    [((_t++))]=divers
    [((_t++))]=geopad
    [((_t++))]=ign
    [((_t++))]=insee
    [((_t++))]=ran
)
# NOTE: begin with 'admin' schema
_schemas[0]=admin
# NOTE: end with 'public' schema
_schemas+=(public)

# transform as pipe-delimited (to possible values 'schema_only' below)
_schemas_join_pipe=${_schemas[@]}
_schemas_join_pipe=${_schemas_join_pipe// /|}

bash_args \
    --args_p '
        schema_only:Limiter la mise à jour à un schéma
    ' \
	--args_v "
        schema_only:${_schemas_join_pipe}
    " \
    "$@" || exit $ERROR_CODE

[ -n "$get_arg_schema_only" ] && _schemas=($get_arg_schema_only)

log_info "Mise à jour de la structure de la base de données"
set_env --schema_code admin &&
# need drop_all_functions_if_exists()
execute_query \
    --query "$POW_DIR_BATCH/db.objects.d/functions/drop.sql" &&
# needed to avoid error "type geometry not exists"
execute_query \
    --query "$POW_DIR_BATCH/db.objects.d/actions/extension_postgis.sql" || exit $ERROR_CODE

for _schema in ${_schemas[@]}; do
    # begins w/ admin (core functions)
    set_env --schema_code $_schema &&
    [ -f "$POW_DIR_BATCH/db.objects.d/db.objects.sql" ] && {
        log_info "schéma($_schema)"
        execute_query \
            --query "$POW_DIR_BATCH/db.objects.d/db.objects.sql" || {
            log_error "Echec mise à jour de la structure de $_schema"
            exit $ERROR_CODE
        }
    }
done

set_env --schema_code admin &&
execute_query \
    --query "$POW_DIR_BATCH/db.objects.d/actions/grant.sql" &&
execute_query \
    --query "$POW_DIR_BATCH/db.objects.d/actions/purge.sql" || exit $ERROR_CODE

exit $SUCCESS_CODE
