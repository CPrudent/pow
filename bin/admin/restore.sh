#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--

    # restore backups (LA POSTE data)

declare -a SCHEMAS=(DIVERS GEOPAD PUBLIC RAN)
SCHEMAS_JOIN_PIPE=${SCHEMAS[@]}
SCHEMAS_JOIN_PIPE=${SCHEMAS_JOIN_PIPE// /|}

declare -a SOURCES=(BACKUP FILE)
SOURCES_JOIN_PIPE=${SOURCES[@]}
SOURCES_JOIN_PIPE=${SOURCES_JOIN_PIPE// /|}

pow_argv \
    --args_n '
        schema_name:Nom du schema à restaurer (ALL pour tous);
        sources:Sources des données à restaurer;
        data_except_re:REGEX pour exclure des données;
        dry_run:Afficher les traitements sans les exécuter
    ' \
    --args_m '
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
    --args_p '
        tag:schema_name@XN,sources:XN,dry_run@bool
    ' \
    "$@" || exit $?

# superuser
set_env --schema_name admin
schemas=(${POW_ARGV[SCHEMA_NAME]})
sources=(${POW_ARGV[SOURCES]})
for schema_name in ${schemas[@]}; do
    declare -a tables=()
    declare -a files=()

    create_schema=1
    case ${schema_name} in
    DIVERS)
        query_schema="
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
        query_schema="
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
        create_schema=0
        tables=(adresse_ran_has_rao source_orga_laposte territoire)
        ;;
    RAN)
        query_schema="
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
        files=(perennite.csv)
        ;;
    esac

    [ -n "$query_schema" ] && {
        query_schema="
            DO \$\$
            BEGIN
            $query_schema
            END \$\$;
        "
    }

    info='Début de la restauration du schéma '${schema_name}
    (in_array --array sources --item BACKUP) && {
        for table in ${tables[@]}; do
            [ -n "${POW_ARGV[DATA_EXCEPT_RE]}" ] &&
            [[ $table =~ ${POW_ARGV[DATA_EXCEPT_RE]} ]] && {
                log_info "table ($table) exclue..."
                continue
            }

            log_info "table ($table) à restaurer..."
            [ "${POW_ARGV[DRY_RUN]}" = no ] && {
                [ -f "$POW_DIR_DATA/common/admin/${schema_name,,}.$table.backup" ] && {
                    {
                        if [ $create_schema -eq 1 ]; then
                            execute_query \
                                --name "CREATE_SCHEMA_${schema_name}" \
                                --query "$query_schema" &&
                            create_schema=0
                        fi
                    } && {
                        # DON'T CARE about error due to missing role(s) like: apps_ciblage, ban, pnd, reex
                        restore_table \
                            --schema_name ${schema_name,,} \
                            --table_name $table \
                            --restore_mode DROP \
                            --backup_before_restore no \
                            --input "$POW_DIR_DATA/common/admin/${schema_name,,}.$table.backup" || true
                    }
                } || {
                    log_error "Arrêt sur Données ${schema_name} ($table)"
                    exit $ERROR_CODE
                }
            }
        done
    }

    (in_array --array sources --item FILE) && {
        for file in ${files[@]}; do
            [ -n "${POW_ARGV[DATA_EXCEPT_RE]}" ] &&
            [[ $file =~ ${POW_ARGV[DATA_EXCEPT_RE]} ]] && {
                log_info "fichier ($file) exclu..."
                continue
            }

            log_info "fichier ($file) à restaurer..."
            [ "${POW_ARGV[DRY_RUN]}" = no ] && {
                [ -f "$POW_DIR_DATA/common/admin/$file" ] && {
                    {
                        if [ $create_schema -eq 1 ]; then
                            execute_query \
                                --name "CREATE_SCHEMA_${schema_name}" \
                                --query "$query_schema"
                        fi
                    } &&
                    import_file \
                        --file_path "$POW_DIR_DATA/common/admin/$file" \
                        --schema_name ${schema_name,,} \
                        --table_name "$(get_file_name --file_path "$POW_DIR_DATA/common/admin/$file")" \
                        --rowid no \
                        --load_mode OVERWRITE_TABLE
                } || {
                    log_error "Arrêt sur Données ${schema_name} ($file)"
                    exit $ERROR_CODE
                }
            }
        done
    }
    log_info "Fin de la restauration du schéma ${schema_name} avec succès"
done

exit $SUCCESS_CODE
