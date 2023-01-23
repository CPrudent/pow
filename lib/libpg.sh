    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define PG

    ###
    # execute query (from file or command line)
    # samples:
    # execute_query --name GET_VERSION --query 'select version()' --psql_arguments 'tuples-only:pset=format=unaligned' --return _pg_version
execute_query() {
    bash_args \
        --args_p '
            name:nommage de la commande;
            query:code SQL à exécuter (fichier ou ligne de commande);
            psql_arguments:paramètres supplémentaires, sous la forme (arg1:arg2:...:argn);
            return:résultat de la commande SELECT;
            with_log:avec log
        ' \
        --args_o '
            query
        ' \
        --args_v '
            with_log:no|yes
        ' \
        --args_d '
            with_log:yes
        ' \
        "$@" || return $ERROR_CODE

    local _start=$(date +%s) _log _opt _info _rc _last
    local _log_tmp_path _log_notice_tmp_path _log_error_tmp_path _log_error_archive_path
    [ -f "$get_arg_query" ] && {
        _log=$(basename "$get_arg_query")
        _opt=--file
        _info='du fichier'
    } || {
        _log="$get_arg_name"
        _opt=--command
        _info='de la commande SQL'
    }
    _log_tmp_path="$POW_DIR_TMP/$_log.log"
    _log_notice_tmp_path="$POW_DIR_TMP/$_log.notice.log"
    _log_error_tmp_path="$POW_DIR_TMP/$_log.error.log"
    _log_error_archive_path="$POW_DIR_ARCHIVE/$_log.error.log"
    [ -n "$get_arg_psql_arguments" ] && {
        local _ifs=$IFS _args _i
        # convert :-separated as list of option(s) of psql (w/ -- prefix for each)
        IFS=: ; _args=($get_arg_psql_arguments) ; IFS=$_ifs
        get_arg_psql_arguments=
        for ((_i=0; _i<${#_args[@]}; _i++)); do
            get_arg_psql_arguments+="--${_args[$_i]} "
        done
    }

    # with log
    is_yes --var get_arg_with_log && log_info "Lancement de l'exécution $_info $_log"
    # call psql
    env PGPASSWORD=$POW_PG_PASSWORD PGOPTIONS='-c client_min_messages=NOTICE' $POW_DIR_PG_BIN/psql \
        --host $POW_PG_HOST \
        --port $POW_PG_PORT \
        --username $POW_PG_USERNAME \
        --dbname $POW_PG_DBNAME \
        --variable ON_ERROR_STOP=1 \
        --no-password \
        $get_arg_psql_arguments \
        $_opt "$get_arg_query" \
        --output $_log_tmp_path 2> $_log_notice_tmp_path
    _rc=$?
    # purge & archive log
    grep --extended-regexp --invert-match 'ATTENTION:|NOTICE:|DÉTAIL : |DROP cascade sur ' $_log_notice_tmp_path >> $_log_error_tmp_path
    sed --in-place --expression '/^NOTICE:  la relation « [^ ]* » existe déjà/d' $_log_notice_tmp_path
    archive_file $_log_tmp_path
    archive_file $_log_notice_tmp_path
    archive_file $_log_error_tmp_path
    [ $_rc -ne 0 ] && {
        local _msg="Erreur lors de l'exécution de $_log"
        is_yes --var get_arg_with_log && _msg+=", veuillez consulter $_log_error_archive_path"
        log_error "$_msg"
        return $ERROR_CODE
    }
    # result message (w/ last)
    is_yes --var get_arg_with_log && {
        get_elapsed_time --start $_start --result _last
        log_info "Exécution avec succès de $_log en $_last"
    }
    # requested result of SELECT
    [ -n "$get_arg_return" ] && {
        local -n _select_ref=$get_arg_return
        _select_ref=$(< "$POW_DIR_ARCHIVE/$_log.log")
    }
    # no log
    is_yes --var get_arg_with_log || rm --force $_log_tmp_path $_log_notice_tmp_path $_log_error_tmp_path

    return $SUCCESS_CODE
}

    ###
    # check if table exists
    #
table_exists() {
    bash_args \
        --args_p '
            schema_name:schéma PG;
            table_name:nom de la table
        ' \
        --args_o '
            table_name
        ' \
        --args_d '
            schema_name:public
        ' \
        "$@" || return $ERROR_CODE

    local _exists _rc=$ERROR_CODE
    execute_query \
        --name "TABLE_EXISTS_${get_arg_schema}_${get_arg_table}" \
        --query "SELECT table_exists('${get_arg_schema}', '${get_arg_table}')" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _exists || return $ERROR_CODE
    is_yes --var _exists && _rc=$SUCCESS_CODE

    return $_rc
}

    ###
    # check if view exists
    #
view_exists() {
    bash_args \
        --args_p '
            schema_name:schéma PG;
            view:nom de la vue
        ' \
        --args_o '
            view
        ' \
        --args_d '
            schema_name:public
        ' \
        "$@" || return $ERROR_CODE

    local _exists _rc=$ERROR_CODE
    execute_query \
        --name "VIEW_EXISTS_${get_arg_schema}_${get_arg_table}" \
        --query "SELECT view_exists('${get_arg_schema}', '${get_arg_table}')" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _exists || return $ERROR_CODE
    is_yes --var _exists && _rc=$SUCCESS_CODE

    return $_rc
}

vacuum() {
    local pg_vacuum_schema=
    local pg_vacuum_table=
    local pg_vacuum_mode=
    local OPTIND
    while getopts :s:t:m: _opt
    do
        case $_opt in
            s)      # schéma spécifique
                pg_vacuum_schema=$OPTARG
                ;;
            t)      # table spécifique
                pg_vacuum_table=$OPTARG
                ;;
            m)      # mode spécifique
                pg_vacuum_mode=$OPTARG
                ;;
            ?)
                # calling error
                return $ERROR_CODE
                ;;
        esac
    done

    if [ -z "$pg_vacuum_mode" ]; then
        log_error "Veuillez préciser avec le paramètre -m le mode de VACUUM (ANALYSE, FULL). Exemple : -m ANALYSE"
        return $ERROR_CODE
    fi

    local log_tmp_path=$dir_tmp"/vacuum_"$pg_vacuum_mode
    if [ -n "$pg_vacuum_schema" ]; then
        log_tmp_path=$log_tmp_path"_"$pg_vacuum_schema
    fi
    if [ -n "$pg_vacuum_table" ]; then
        log_tmp_path=$log_tmp_path"_"$pg_vacuum_table
    fi
    log_tmp_path=$log_tmp_path".log"

    if [ "$pg_vacuum_mode" = 'FULL' ]; then
        df -m >> $log_tmp_path
        vacuum_command_options='(FULL, ANALYSE, VERBOSE)'
    elif [ "$pg_vacuum_mode" = 'ANALYSE' ]; then
        vacuum_command_options='(ANALYSE, VERBOSE)'
    fi

    # with table?
    if [ -n "$pg_vacuum_table" ]; then
        # with schema?
        if [ -n "$pg_vacuum_schema" ]; then
            pg_vacuum_table=$pg_vacuum_schema"."$pg_vacuum_table
        fi

        execute_query \
            --name "VACUUM_${pg_vacuum_mode}_${pg_vacuum_table}" \
            --query "VACUUM ${vacuum_command_options} ${pg_vacuum_table}" || {
            log_error "Erreur VACUUM ${pg_vacuum_mode} sur la table ${pg_vacuum_table}"
            return $ERROR_CODE
        }
    elif [ -n "$pg_vacuum_schema" ]; then
        log_info "Début VACUUM "$pg_vacuum_mode" sur les tables du schéma $pg_vacuum_schema"

        # http://stackoverflow.com/questions/29710618/vacuum-analyze-all-tables-in-a-schema-postgres
        # vacuum only the tables in the schema named in the variable $pg_vacuum_schema
        local _dt
        # extract schema table names from psql output and put them in a bash array
        execute_query \
            --name "${pg_vacuum_schema}_ALL_TABLES" \
            --query "\dt $pg_vacuum_schema."'*' \
            --psql_arguments 'tuples-only:pset=format=unaligned' \
            --return _dt || return $ERROR_CODE
        local tables_array=($(echo $_dt | tr ' ' '\n' | cut --delimiter '|' --field 2))
        # loop through the table names creating and executing a vacuum command for each one
        for t in "${tables_array[@]}"
        do
            execute_query \
                --name "VACUUM_${pg_vacuum_mode}_${pg_vacuum_schema}.${t}" \
                --query "VACUUM ${vacuum_command_options} ${pg_vacuum_schema}.${t}" || {
                log_error "Erreur VACUUM ${pg_vacuum_mode} sur la table ${pg_vacuum_schema}.${t}"
                return $ERROR_CODE
            }
        done

        log_info "Fin VACUUM "$pg_vacuum_mode" sur les tables du schéma $pg_vacuum_schema"
    else
        log_info "Début VACUUM "$pg_vacuum_mode" sur la base de données"
        execute_query \
            --name "VACUUM_${pg_vacuum_mode}_ALL" \
            --query "VACUUM ${vacuum_command_options}" || {
            log_error "Erreur VACUUM ${pg_vacuum_mode} sur la base de données"
            return $ERROR_CODE
        }
        log_info "Fin VACUUM "$pg_vacuum_mode" sur la base de données"
    fi

    if [ "$pg_vacuum_mode" = 'FULL' ]; then
        df -m >> $log_tmp_path
    fi
    archive_file $log_tmp_path

    return $SUCCESS_CODE
}

    ###
    # get sequences of a table
    #
get_table_sequences() {
    bash_args	\
        --args_p '
            schema_name:Nom du schema de la table;
            table_name:Nom de la table
        ' \
        --args_o '
            schema_name;
            table_name
        ' \
        "$@" || return $ERROR_CODE

    local schema_name="$get_arg_schema_name"
    local table_name="$get_arg_table_name"
    local _sequences

    # NOTE : sequence name can be prefixed by schema
    execute_query \
        --name GET_TABLE_SEQUENCES \
        --query "
            SELECT STRING_AGG(sequence_name, ',') AS liste_sequences_names FROM (
                SELECT (REGEXP_MATCHES(column_default, 'nextval\(''(${schema_name}\.)?([^:]+)''::regclass\)'))[2] AS sequence_name
                FROM information_schema.columns
                WHERE table_schema = '${schema_name}'
                AND table_name = '${table_name}'
                AND column_default LIKE 'nextval(%'
            ) AS t
            " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _sequences || return $ERROR_CODE

    echo $_sequences

    return $SUCCESS_CODE
}

    ###
    # backup table
    #
backup_table() {
    bash_args	\
        --args_p '
            schema_name:Nom du schéma de la table à copier;
            table_name:Nom de la table à copier;
            output:Sortie écran ou chemin complet vers un fichier de sauvegarde;
            format:Format de la sauvegarde;
            sections:Liste des sections à restaurer
        ' \
        --args_o '
            schema_name;
            table_name;
        ' \
        --args_v '
            format:custom|plain|directory|tar;
            sections:pre-data+data+post-data|pre-data|data|post-data|data+post-data
        ' \
        --args_d '
            output:STDOUT;
            format:custom;
            sections:pre-data+data+post-data
        ' \
        "$@" || return $ERROR_CODE

    local backup_schema_name="$get_arg_schema_name"
    local backup_table_name="$get_arg_table_name"
    local backup_libelle="${backup_schema_name}.${backup_table_name}"
    local backup_log=${POW_DIR_ARCHIVE}/pg_dump_${backup_libelle}.log
    local backup_format="$get_arg_format"
    local backup_output="$get_arg_output"
    local backup_sections=(${get_arg_sections//+/ })
    local pg_dump_file_arg pg_dump_section_arg pg_dump_sequence_arg backup_table_sequences backup_table_sequences_array backup_table_sequence
    local _previous_log_echo=$POW_LOG_ECHO

    backup_table_reset() {
        set_log_echo $_previous_log_echo
    }

    [ "$backup_output" = 'STDOUT' ] && set_log_echo no

    log_info "Début de sauvegarde de ${backup_libelle} dans $backup_output, sections ${backup_sections[*]}"

    local backup_output_tmp="$POW_DIR_TMP/${backup_libelle}_$$.backup"
    [ "$backup_output" != 'STDOUT' ] && pg_dump_file_arg="--file=$backup_output_tmp"
    for _backup_section in ${backup_sections[@]}; do
        pg_dump_section_arg+="--section $_backup_section "
    done

    # not useful because sequences are native into backup?
    #backup_table_sequences=$(get_table_sequences --schema_name ${backup_schema_name} --table_name ${backup_table_name})
    backup_table_sequences_array=(${backup_table_sequences//,/ })
    for backup_table_sequence in ${backup_table_sequences_array[@]}; do
        pg_dump_sequence_arg+="--table $backup_table_sequence "
    done

    # available disk space
    df -h $POW_DIR_DATA > $backup_log

    env PGPASSWORD=$POW_PG_PASSWORD $POW_DIR_PG_BIN/pg_dump	\
        --host=$POW_PG_HOST	\
        --port=$POW_PG_PORT \
        --username=$POW_PG_USERNAME \
        --no-password \
        --format=$backup_format \
        --verbose \
        $pg_dump_file_arg \
        $pg_dump_section_arg \
        $pg_dump_sequence_arg \
        --table=${backup_schema_name}.${backup_table_name} \
        $POW_PG_DBNAME \
        2>> $backup_log

    local _return_code=$?
    [ $_return_code -ne 0 ] && {
        log_error "Erreur pg_dump($_return_code), cf $backup_log"
        [ "$backup_output" != 'STDOUT' ] && [ -f "$backup_output_tmp" ] && rm $backup_output_tmp
        backup_table_reset
        return $ERROR_CODE
    }
    [ "$backup_output" != 'STDOUT' ] && mv $backup_output_tmp $backup_output
    log_info "Fin de sauvegarde"
    backup_table_reset

    return $SUCCESS_CODE
}

    ###
    # restore table (w/ backup)
    #
restore_table() {
    bash_args \
        --args_p '
            schema_name:Nom du schéma de la table à restaurer;
            table_name:Nom de la table à restaurer;
            restore_mode:Mode de restauration;
            input:Entrée écran ou chemin complet vers un fichier de sauvegarde;
            backup_before_restore:Faut-il sauvegarder la table avant la restauration ?;
            sql_to_filter:Requête SQL pour filtrer la table à recopier;
            restore_on_error:Faut-il restaurer automatiquement la table si la restauration initiale échoue ?;
            sections:Liste des sections à restaurer;
            subprocess:Indique si cet appel à restore_table est un sous processus;
            wait_file_minute:combien de temps en minutes faut-il attendre que le fichier de sauvegarde soit présent ?;
            max_age_file_minute:quel age maximum en minutes doit avoir le fichier de sauvegarde ?
        ' \
        --args_o '
            schema_name;
            table_name
        ' \
        --args_v '
            restore_mode:TRUNCATE|DROP|APPEND;
            backup_before_restore:yes|no;
            restore_on_error:yes|no;
            sections:pre-data+data+post-data|pre-data|data|post-data|data+post-data;
            subprocess:yes|no
        ' \
        --args_d '
            restore_mode:TRUNCATE;
            backup_before_restore:yes;
            input:STDIN;
            restore_on_error:yes;
            sections:pre-data+data+post-data;
            subprocess:no;
            wait_file_minute:0;max_age_file_minute:0
        ' \
        "$@" || return $ERROR_CODE

    local restore_schema_name="$get_arg_schema_name"
    local restore_table_name="$get_arg_table_name"
    #ATTENTION : maintenir avec la variable de même nom dans la fonction copy_tables
    local restore_libelle="${restore_schema_name}.${restore_table_name}"
    local restore_mode="$get_arg_restore_mode"
    local restore_input="$get_arg_input"
    local backup_before_restore="$get_arg_backup_before_restore"
    #ATTENTION : maintenir avec la variable de même nom dans la fonction copy_tables
    local backup_before_restore_full_path=$POW_DIR_TMP/${restore_libelle}_$$.backup.before_restore
    local restore_on_error="$get_arg_restore_on_error"
    local table_to_restore_exists=no
    local _restore_sections=(${get_arg_sections//+/ })
    local restore_sections=()
    local sql_to_filter="$get_arg_sql_to_filter"
    local subprocess=$get_arg_subprocess
    local wait_file_minute=$get_arg_wait_file_minute
    local max_age_file_minute=$get_arg_max_age_file_minute
    local pg_restore_file_arg pg_restore_section_arg restore_table_sequences restore_table_sequences_array restore_table_sequence

    if [ "${restore_input}" != STDIN ]; then
        wait_for_file --file_path "$restore_input" --wait_file_minute $wait_file_minute --max_age_file_minute $max_age_file_minute || {
            log_error "${FUNCNAME[0]}: La sauvegarde $restore_input n'existe pas"
            return $ERROR_CODE
        }
    fi

    if table_exists --schema_name "${restore_schema_name}" --table_name "${restore_table_name}"; then
        table_to_restore_exists=yes
        restore_table_sequences=$(get_table_sequences --schema_name ${restore_schema_name} --table_name ${restore_table_name})
        restore_table_sequences_array=(${restore_table_sequences//,/ })
        if [ "$restore_mode" = APPEND ] && [ ${#restore_table_sequences_array[*]} -gt 0 ]; then
            # risk of conflict w/ values (foreign columns)
            log_error "${FUNCNAME[0]}: Il n'est pas possible de restaurer cette table en mode APPEND car celle-ci utilise des séquences"
            return $ERROR_CODE
        fi
    else
        backup_before_restore=no
    fi

    for _restore_section in ${_restore_sections[@]}; do
        # not DROP: pre-data not useful
        [ "$_restore_section" = pre-data ] && [ "$table_to_restore_exists" = yes ] && [ "$restore_mode" != DROP ] && continue
        # idem for post-data
        [ "$_restore_section" = 'post-data' ] && [ "$table_to_restore_exists" = yes ] && [ "$restore_mode" = APPEND ] && continue
        restore_sections+=(${_restore_section})
    done

    local _previous_log_echo=$POW_LOG_ECHO
    restore_table_reset() {
        set_log_echo $_previous_log_echo
        if [ "$backup_before_restore" = yes ] && [ "$restore_on_error" = yes ] && [ -f $backup_before_restore_full_path ]; then
            log_info "Restauration de la sauvegarde avant restauration"
            restore_table \
                -schema_name "$restore_schema_name" \
                --table_name "$restore_table_name" \
                --input "$backup_before_restore_full_path" \
                --backup_before_restore no || return $ERROR_CODE
            rm -f $backup_before_restore_full_path
        fi
        return $SUCCESS_CODE
    }
    [ "$restore_input" = STDIN ] && set_log_echo no

    # backup before restore?
    if [ "$backup_before_restore" = yes ]; then
        [ -f $backup_before_restore_full_path ] && rm $backup_before_restore_full_path

        backup_table \
            --schema_name "$restore_schema_name" \
            --table_name "$restore_table_name" \
            --output "$backup_before_restore_full_path" || {
            [ -f $backup_before_restore_full_path ] && rm -f $backup_before_restore_full_path
            restore_table_reset
            return $ERROR_CODE
        }
    fi

    if  [ ! -z "$sql_to_filter" ] &&
        [[ " ${restore_sections[@]} " =~ " pre-data " ]] &&
        [[ " ${restore_sections[@]} " =~ " data " ]]; then
        #si on est en STDIN alors passage en fichier
        if [ "$restore_input" = STDIN ]; then
            log_info "Conversion de STDIN en fichier temporaire"
            cat > $POW_DIR_TMP/stdin_$$.backup || { restore_table_reset; return $ERROR_CODE; }
            restore_input="$POW_DIR_TMP/stdin_$$.backup"
        fi
        # section by one to prepare filter between DDL and DATA
        for _restore_section in ${restore_sections[@]}; do
            restore_table \
                --schema_name $restore_schema_name \
                --table_name $restore_table_name \
                --restore_mode $restore_mode \
                --input $restore_input \
                --backup_before_restore no  \
                --sql_to_filter "$sql_to_filter" \
                --sections $_restore_section \
                --subprocess yes || { restore_table_reset; return $ERROR_CODE; }
        done
        [ "$restore_input" = "$POW_DIR_TMP/stdin_$$.backup" ] && rm -f $restore_input
    else
        log_info "Début de restauration de ${restore_libelle} à partir de ${restore_input}, sections ${restore_sections[*]}"
        [ "$restore_input" != STDIN ] && pg_restore_file_arg="$restore_input"
        #actions préalables et préparation de pg_restore_section_arg
        for _restore_section in ${restore_sections[@]}; do
            pg_restore_section_arg+="--section $_restore_section "
            if [ "$table_to_restore_exists" = yes ]; then
                if [ "$_restore_section" = pre-data ]; then
                    if [ "$restore_mode" = DROP ]; then
                        # drop table if exists and DROP mode
                        execute_query \
                            --name "DROP_TABLE_${restore_libelle}" \
                            --query "
                                DO
                                \$\$
                                DECLARE
                                BEGIN
                                    IF table_exists('${restore_schema_name}', '${restore_table_name}') THEN
                                        DROP TABLE ${restore_schema_name}.${restore_table_name} CASCADE;COMMIT;
                                    ELSIF view_exists('${restore_schema_name}','${restore_table_name}') THEN
                                        DROP VIEW ${restore_schema_name}.${restore_table_name} CASCADE;COMMIT;
                                    END IF;
                                END
                                \$\$ LANGUAGE plpgsql;" || { restore_table_reset; return $ERROR_CODE; }

                        # idem for sequences
                        # not useful
                        #for restore_table_sequence in ${restore_table_sequences_array[@]}; do
                        #	execute_sql_command "DROP_SEQUENCE_${restore_table_sequence}.${restore_libelle}" "DROP SEQUENCE ${restore_schema_name}.${restore_table_sequence};COMMIT;" || { restore_table_reset; return $ERROR_CODE; }
                        #done

                        drop_cascade=$(grep 'NOTICE: \+DROP cascade' "${POW_DIR_ARCHIVE}/DROP_TABLE_${restore_libelle}.notice.log") &&
                        [ -n "$drop_cascade" ] &&
                        log_info "${drop_cascade}, cf ${POW_DIR_ARCHIVE}/DROP_TABLE_${restore_libelle}.notice.log"
                    fi
                elif [ "$_restore_section" = data ]; then
                    if [ "$restore_mode" = TRUNCATE ]; then
                        #Suppression des données en cascade (données faisant référence à ces données par contrainte de clé étrangère), des index, triggers et contraintes
                        execute_query \
                            --name "DROP_CONSTRAINTS_INDEX_TRIGGERS_TRUNCATE_${restore_libelle}" \
                            --query "
                                DO
                                \$\$
                                DECLARE
                                BEGIN
                                    PERFORM public.drop_table_constraints('${restore_schema_name}', '${restore_table_name}');
                                    PERFORM public.drop_table_indexes('${restore_schema_name}', '${restore_table_name}');
                                    PERFORM public.drop_table_triggers('${restore_schema_name}', '${restore_table_name}');
                                    IF table_exists('${restore_schema_name}','${restore_table_name}') THEN
                                        TRUNCATE TABLE ${restore_schema_name}.${restore_table_name} CASCADE;
                                    END IF;
                                END
                                \$\$ LANGUAGE plpgsql;
                            "  || { restore_table_reset; return $ERROR_CODE; }
                    fi

                    # filter to data? not for a view
                    if [ ! -z "$sql_to_filter" ] && ! view_exists --schema_name "${restore_schema_name}" --view_name "${restore_table_name}"; then
                        if [ -f "$sql_to_filter" ]; then
                            sql_to_filter=$(< "$sql_to_filter")
                        fi
                        log_info 'Préparation filtre données' &&
                        execute_query \
                            --name "RESTORE_FILTER_TABLE_$restore_libelle" \
                            --query "
                                SELECT public.drop_all_functions_if_exists('${restore_schema_name}','${restore_table_name}_restore_filter');
                                CREATE OR REPLACE FUNCTION ${restore_schema_name}.${restore_table_name}_restore_filter() RETURNS TRIGGER AS "'$$'"
                                    BEGIN
                                        IF
                                            ${sql_to_filter}
                                        THEN
                                            RETURN NEW;
                                        ELSE
                                            RETURN NULL;
                                        END IF;
                                    END;
                                "'$$'" LANGUAGE plpgsql;

                                DROP TRIGGER IF EXISTS trg_${restore_table_name}_restore_filter ON ${restore_schema_name}.${restore_table_name};
                                CREATE TRIGGER trg_${restore_table_name}_restore_filter
                                BEFORE INSERT ON ${restore_schema_name}.${restore_table_name}
                                    FOR EACH ROW
                                    EXECUTE PROCEDURE ${restore_schema_name}.${restore_table_name}_restore_filter();
                            " || {
                                restore_table_reset &&
                                return $ERROR_CODE
                            }
                    fi
                fi
            fi
        done

        env PGPASSWORD=$POW_PG_PASSWORD $POW_DIR_PG_BIN/pg_restore \
            --host $POW_PG_HOST \
            --port $POW_PG_PORT \
            --username $POW_PG_USERNAME \
            --dbname $POW_PG_DBNAME \
            --no-password \
            --format=custom \
            --verbose \
            --exit-on-error \
            $pg_restore_section_arg \
            $pg_restore_file_arg > $POW_DIR_ARCHIVE/pg_restore_${restore_table_name}.log 2>&1 || {
            log_error "Erreur pg_restore, cf $POW_DIR_ARCHIVE/pg_restore_${restore_table_name}.log"
            restore_table_reset
            return $ERROR_CODE
        }

        for _restore_section in ${restore_sections[@]}; do
            if [ "$_restore_section" = data ]; then
                # delete filter
                if [ ! -z "$sql_to_filter" ]; then
                    execute_query \
                        --name "DROP_RESTORE_FILTER_TABLE_${restore_libelle}" \
                        --query "
                            SELECT public.DROP_ALL_FUNCTIONS_IF_EXISTS('${restore_schema_name}','${restore_table_name}_restore_filter');
                            DROP TRIGGER IF EXISTS trg_${restore_table_name}_restore_filter ON ${restore_schema_name}.${restore_table_name};
                        " || { restore_table_reset; return $ERROR_CODE; }
                fi
            fi
        done
    fi

    if [ "$subprocess" = no ]; then
        vacuum -s $restore_schema_name -t $restore_table_name -m ANALYSE || {
            restore_table_reset;
            return $ERROR_CODE;
        }

        [ "$backup_before_restore" = "yes" ] && [ "$restore_on_error" = "yes" ] && rm --force $backup_before_restore_full_path
        restore_table_reset

        log_info "Fin de restauration"
    fi

    return $SUCCESS_CODE
}
