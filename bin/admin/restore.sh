#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--

    # restore backups (LA POSTE data)

declare -a SCHEMAS=(DIVERS GEOPAD PUBLIC RAN)
SCHEMAS_JOIN_PIPE=${SCHEMAS[@]}
SCHEMAS_JOIN_PIPE=${SCHEMAS_JOIN_PIPE// /|}
SCHEMAS_JOIN_PIPE+="|ALL"

bash_args	\
    --args_p '
        schema_name:Nom du schema à restaurer;
        table_except_re:REGEX pour exclure des tables;
        dry_run:Afficher les traitements sans les exécuter
    ' \
    --args_o '
        schema_name
    ' \
    --args_v '
        schema_name:'${SCHEMAS_JOIN_PIPE}';
        dry_run:no|yes
    ' \
    --args_d '
        dry_run:no
    ' \
    "$@" || exit $ERROR_CODE

schema_name=$get_arg_schema_name
table_except_re=$get_arg_table_except_re
dry_run=$get_arg_dry_run

for _schema_name in ${SCHEMAS[@]}; do
    # particular schema requested
    [ "$schema_name" != ALL ] && [ "$_schema_name" != "$schema_name" ] && continue

    declare -a tables=()

    case ${_schema_name} in
    DIVERS)
        tables=(source_orga source_orga_complement)
        ;;
    GEOPAD)
        tables=(pdi)
        ;;
    PUBLIC)
        tables=(adresse_ran_has_rao source_orga_laposte territoire)
        ;;
    RAN)
        tables=(l3 numero voie za adresse coord)
        ;;
    esac

    log_info 'Début de la restauration du  contexte '${_schema_name}
    for table in ${tables[@]}; do
        #declare -p tables table
        [ -n "$table_except_re" ] &&
        [[ $table =~ $table_except_re ]] && {
            log_info "table ($table) exclue..."
            continue
        }

        log_info "table ($table) à restaurer..."
        [ "$dry_run" = no ] && {
            [ -f "$POW_DIR_DATA/common/admin/${_schema_name,,}.$table.backup" ] && {
            # DON'T CARE about error due to missing role(s) like: apps_ciblage, ban, pnd, reex
            restore_table \
                --schema_name ${_schema_name,,} \
                --table_name $table \
                --restore_mode DROP \
                --backup_before_restore no \
                --input "$POW_DIR_DATA/common/admin/${_schema_name,,}.$table.backup" || true
            } || {
                log_error "Données ${_schema_name} ($table) manquantes!"
            }
        }
    done
    log_info 'Fin de la restauration du  contexte '${_schema_name}
done

exit $SUCCESS_CODE
