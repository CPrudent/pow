#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--

    # restore backups (LA POSTE data)

declare -a SCHEMAS=(DIVERS GEOPAD PUBLIC RAN)
SCHEMAS_JOIN_PIPE=${SCHEMAS[@]}
SCHEMAS_JOIN_PIPE=${SCHEMAS_JOIN_PIPE// /|}
SCHEMAS_JOIN_PIPE+="|ALL"

declare -a SOURCES=(BACKUP FILE)
SOURCES_JOIN_PIPE=${SOURCES[@]}
SOURCES_JOIN_PIPE=${SOURCES_JOIN_PIPE// /|}
SOURCES_JOIN_PIPE+="|ALL"

bash_args \
    --args_p '
        schema_name:Nom du schema à restaurer;
        data_except_re:REGEX pour exclure des données;
        sources:sources des données à restaurer;
        dry_run:Afficher les traitements sans les exécuter
    ' \
    --args_o '
        schema_name
    ' \
    --args_v '
        schema_name:'${SCHEMAS_JOIN_PIPE}';
        sources:'${SOURCES_JOIN_PIPE}';
        dry_run:no|yes
    ' \
    --args_d '
        sources:ALL;
        dry_run:no
    ' \
    "$@" || exit $ERROR_CODE

schema_name=$get_arg_schema_name
data_except_re=$get_arg_data_except_re
dry_run=$get_arg_dry_run
sources=$get_arg_sources

# superuser
set_env --schema_name admin

for _schema_name in ${SCHEMAS[@]}; do
    # particular schema requested
    [ "$schema_name" != ALL ] && [ "$_schema_name" != "$schema_name" ] && continue

    declare -a tables=()
    declare -a files=()

    _create_schema=1
    case ${_schema_name} in
    DIVERS)
        _query_schema="
            IF NOT role_exists('divers') THEN
                CREATE ROLE divers LOGIN
                    ENCRYPTED PASSWORD 'md533bd6c15a1964f2f2b66554a418ecd6e'
                    NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
            END IF;
            CREATE SCHEMA IF NOT EXISTS divers AUTHORIZATION divers;
            "
        tables=(source_orga source_orga_complement)
        ;;
    GEOPAD)
        _query_schema="
            IF NOT role_exists('geopad') THEN
                CREATE ROLE geopad LOGIN
                    ENCRYPTED PASSWORD 'md51d88f1c6ed1072c47354f19cc39f1305'
                    NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
            END IF;
            CREATE SCHEMA IF NOT EXISTS geopad AUTHORIZATION geopad;
            "
        tables=(pdi)
        ;;
    PUBLIC)
        _create_schema=0
        tables=(adresse_ran_has_rao source_orga_laposte territoire)
        ;;
    RAN)
        _query_schema="
            IF NOT role_exists('ran') THEN
                CREATE ROLE ran LOGIN
                    ENCRYPTED PASSWORD 'md51c1f24e7a573be92a0b05d38cf33014d'
                    NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
            END IF;
            CREATE SCHEMA IF NOT EXISTS ran AUTHORIZATION ran;
            "
        tables=(l3 numero voie za adresse coord)
        # NOTE convert fixed length to csv (adding header columns)
        # awk -f $POW_DIR_DATA/common/admin/perennite.awk < $POW_DIR_DATA/common/admin/hspraaaa.ai > $POW_DIR_DATA/common/admin/perennite.csv
        files=(perennite.csv street_faults_manual_correction.csv)
        ;;
    esac

    [ -n "$_query_schema" ] && {
        _query_schema="
            DO \$\$
            BEGIN
            $_query_schema
            END \$\$;
        "
    }

    _info='Début de la restauration du schéma '${_schema_name}
    [ "$sources" != ALL ] && _info+=" ($sources)"
    log_info "$_info"

    [[ $sources =~ ALL|BACKUP ]] && {
        for table in ${tables[@]}; do
            [ -n "$data_except_re" ] &&
            [[ $table =~ $data_except_re ]] && {
                log_info "table ($table) exclue..."
                continue
            }

            log_info "table ($table) à restaurer..."
            [ "$dry_run" = no ] && {
                [ -f "$POW_DIR_DATA/common/admin/${_schema_name,,}.$table.backup" ] && {
                    {
                        if [ $_create_schema -eq 1 ]; then
                            execute_query \
                                --name "CREATE_SCHEMA_${_schema_name}" \
                                --query "$_query_schema" &&
                            _create_schema=0
                        fi
                    } && {
                        # DON'T CARE about error due to missing role(s) like: apps_ciblage, ban, pnd, reex
                        restore_table \
                            --schema_name ${_schema_name,,} \
                            --table_name $table \
                            --restore_mode DROP \
                            --backup_before_restore no \
                            --input "$POW_DIR_DATA/common/admin/${_schema_name,,}.$table.backup" || true
                    }
                } || {
                    log_error "Arrêt sur Données ${_schema_name} ($table)"
                    exit $ERROR_CODE
                }
            }
        done
    }
    [[ $sources =~ ALL|FILE ]] && {
        for file in ${files[@]}; do
            [ -n "$data_except_re" ] &&
            [[ $file =~ $data_except_re ]] && {
                log_info "fichier ($file) exclu..."
                continue
            }

            log_info "fichier ($file) à restaurer..."
            [ "$dry_run" = no ] && {
                [ -f "$POW_DIR_DATA/common/admin/$file" ] && {
                    {
                        if [ $_create_schema -eq 1 ]; then
                            execute_query \
                                --name "CREATE_SCHEMA_${_schema_name}" \
                                --query "$_query_schema"
                        fi
                    } &&
                    import_file \
                        --file_path "$POW_DIR_DATA/common/admin/$file" \
                        --schema_name ${_schema_name,,} \
                        --table_name "$(get_file_name --file_path \"$POW_DIR_DATA/common/admin/$file\")" \
                        --rowid no \
                        --load_mode OVERWRITE_TABLE
                } || {
                    log_error "Arrêt sur Données ${_schema_name} ($file)"
                    exit $ERROR_CODE
                }
            }
        done
    }
    log_info "Fin de la restauration du schéma ${_schema_name} avec succès"
done

exit $SUCCESS_CODE
