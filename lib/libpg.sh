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
            schema:schéma PG;
            table:nom de la table
        ' \
        --args_o '
            table
        ' \
        --args_d '
            schema:public
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
            schema:schéma PG;
            view:nom de la vue
        ' \
        --args_o '
            view
        ' \
        --args_d '
            schema:public
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

    ###
    # backup table
    #
backup_table() {
    bash_args	\
        --args_p '
            schema_name:Nom du schema de la table à copier;
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
            schema_name:Nom du schema de la table à restaurer;
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
    local restore_libelle="${restore_schema_name}.${restore_table_name}@$(get_host_libelle --host ${pg_host})"
    local restore_mode="$get_arg_restore_mode"
    local restore_input="$get_arg_input"
    local backup_before_restore="$get_arg_backup_before_restore"
    #ATTENTION : maintenir avec la variable de même nom dans la fonction copy_tables
    local backup_before_restore_full_path=$dir_tmp/${restore_libelle}_$$.backup.before_restore
    local restore_on_error="$get_arg_restore_on_error"
    local table_to_restore_exists='no'
    local _restore_sections=(${get_arg_sections//+/ })
    local restore_sections=()
    local sql_to_filter="$get_arg_sql_to_filter"
    local subprocess=$get_arg_subprocess
    local wait_file_minute=$get_arg_wait_file_minute
    local max_age_file_minute=$get_arg_max_age_file_minute

    local pg_restore_file_arg pg_restore_section_arg restore_table_sequences restore_table_sequences_array restore_table_sequence

    if [ "${restore_input}" != 'STDIN' ]; then
        wait_for_file --file_path "$restore_input" --wait_file_minute $wait_file_minute --max_age_file_minute $max_age_file_minute || {
            log_error "${FUNCNAME[0]}: La sauvegarde $restore_input n'existe pas"
            return $ERROR_CODE
        }
    fi

    if table_exists "${restore_schema_name}" "${restore_table_name}"; then
        table_to_restore_exists='yes'
        restore_table_sequences=$(get_table_sequences --schema_name ${restore_schema_name} --table_name ${restore_table_name})
        restore_table_sequences_array=(${restore_table_sequences//,/ })
        #si la table utilise des séquences et qu'on est en mode APPEND, il risque d'avoir un conflit sur les identifiants générés par la séquence
        if [ "$restore_mode" = 'APPEND' ] && [ ${#restore_table_sequences_array[*]} -gt 0 ]; then
            log_error "${FUNCNAME[0]}: Il n'est pas possible de restaurer cette table en mode APPEND car celle-ci utilise des séquences"
            return $ERROR_CODE
        fi
    else
        backup_before_restore='no'
    fi

    for _restore_section in ${_restore_sections[@]}; do
        #La table existe et qu'on est pas en mode DROP (donc TRUNCATE ou APPEND), on saute la section 'pre-data' car il ne faut pas créer la table
        [ "$_restore_section" = 'pre-data' ] && [ "$table_to_restore_exists" = 'yes' ] && [ "$restore_mode" != 'DROP' ] && continue
        #La table existe et qu'on est pas en mode DROP (donc TRUNCATE ou APPEND), on saute la section 'post-data' car il ne faut pas créer les contraintes/index/etc qui n'ont pas été supprimés
        [ "$_restore_section" = 'post-data' ] && [ "$table_to_restore_exists" = 'yes' ] && [ "$restore_mode" = 'APPEND' ] && continue
        restore_sections+=(${_restore_section})
    done

    local _previous_echo_info_messages=$echo_info_messages
    function restore_table_reset {
        set_echo_info_messages $_previous_echo_info_messages
        if [ "$backup_before_restore" = 'yes' ] && [ "$restore_on_error" = 'yes' ] && [ -f $backup_before_restore_full_path ]; then
            log_info "Restauration de la sauvegarde avant restauration"
            restore_table -schema_name "$restore_schema_name" --table_name "$restore_table_name" \
                --input "$backup_before_restore_full_path" \
                --backup_before_restore 'no' || return $ERROR_CODE
            rm -f $backup_before_restore_full_path
        fi
        return $SUCCESS_CODE
    }
    [ "$restore_input" = 'STDIN' ] && set_echo_info_messages 'no'

    #si il n'y a pas déjà un filtre de données défini et q'une une limite de département est définie sur cette plateforme
    #alors on cherche un éventuel filtre à appliquer à la restauration des données de cette table
    if [ -z "$sql_to_filter" ] && [ -n "$ENV_DEP_LIMIT" ]; then
        sql_to_filter=$(get_restore_table_dep_filter "${restore_schema_name}.${restore_table_name}")
        [ -n "$sql_to_filter" ] && log_info "Application automatique d'un filtre département à la restauration de ${restore_schema_name}.${restore_table_name}"
    fi

    #sauvegarde avant restauration
    if [ "$backup_before_restore" = 'yes' ]; then
        #si jamais il existe une ancienne sauvegarde avant restauration, on la supprime
        [ -f $backup_before_restore_full_path ] && rm $backup_before_restore_full_path
        backup_table --schema_name "$restore_schema_name" --table_name "$restore_table_name" --output "$backup_before_restore_full_path" || {
            [ -f $backup_before_restore_full_path ] && rm -f $backup_before_restore_full_path
            restore_table_reset
            return $ERROR_CODE
        }
    fi

    #si filtre de données et sections pre-data + data
    if [ ! -z "$sql_to_filter" ] && [[ " ${restore_sections[@]} " =~ " pre-data " ]] && [[ " ${restore_sections[@]} " =~ " data " ]]; then
        #on gère les sections séparément pour intercaler la préparation du filtre entre la création de la structure et le chargement des données
        #si on est en STDIN alors passage en fichier
        if [ "$restore_input" = 'STDIN' ]; then
            log_info "Conversion de STDIN en fichier temporaire"
            cat > $dir_tmp/stdin_$$.backup || { restore_table_reset; return $ERROR_CODE; }
            restore_input="$dir_tmp/stdin_$$.backup"
        fi
        for _restore_section in ${restore_sections[@]}; do
            restore_table --schema_name $restore_schema_name --table_name $restore_table_name \
                --restore_mode $restore_mode \
                --input $restore_input \
                --backup_before_restore 'no'  \
                --sql_to_filter "$sql_to_filter" \
                --sections $_restore_section \
                --subprocess 'yes' || { restore_table_reset; return $ERROR_CODE; }
        done
        [ "$restore_input" = "$dir_tmp/stdin_$$.backup" ] && rm -f $restore_input
    else
        log_info "Début de restauration de ${restore_libelle} à partir de ${restore_input}, sections ${restore_sections[*]}"
        [ "$restore_input" != 'STDIN' ] && pg_restore_file_arg="$restore_input"
        #actions préalables et préparation de pg_restore_section_arg
        for _restore_section in ${restore_sections[@]}; do
            pg_restore_section_arg+="--section $_restore_section "
            if [ "$table_to_restore_exists" = 'yes' ]; then
                if [ "$_restore_section" = 'pre-data' ]; then
                    if [ "$restore_mode" = 'DROP' ]; then
                        #la table existe et on est en mode DROP : on la supprime avant de la récréer
                        execute_sql_command "DROP_TABLE_${restore_libelle}" "
                            DO
                            \$\$
                            DECLARE
                            BEGIN
                                IF table_exists('${restore_schema_name}','${restore_table_name}') = TRUE THEN
                                    DROP TABLE ${restore_schema_name}.${restore_table_name} CASCADE;COMMIT;
                                ELSIF view_exists('${restore_schema_name}','${restore_table_name}') = TRUE THEN
                                    DROP VIEW ${restore_schema_name}.${restore_table_name} CASCADE;COMMIT;
                                END IF;
                            END
                            \$\$ LANGUAGE plpgsql;" || { restore_table_reset; return $ERROR_CODE; }
                        #idem pour les séquences de la table
                        #inutile car séquence incluse nativement dans le backup ?
                        #for restore_table_sequence in ${restore_table_sequences_array[@]}; do
                        #	execute_sql_command "DROP_SEQUENCE_${restore_table_sequence}.${restore_libelle}" "DROP SEQUENCE ${restore_schema_name}.${restore_table_sequence};COMMIT;" || { restore_table_reset; return $ERROR_CODE; }
                        #done
                        drop_cascade=$(grep 'NOTICE: \+DROP cascade' "${dir_archive}/DROP_TABLE_${restore_libelle}.notice.log") &&
                        [ -n "$drop_cascade" ] &&
                        log_info "${drop_cascade}, cf ${dir_archive}/DROP_TABLE_${restore_libelle}.notice.log"
                    fi
                elif [ "$_restore_section" = 'data' ]; then
                    if [ "$restore_mode" = 'TRUNCATE' ]; then
                        #Suppression des données en cascade (données faisant référence à ces données par contrainte de clé étrangère), des index, triggers et contraintes
                        execute_sql_command "DROP_CONSTRAINTS_INDEX_TRIGGERS_TRUNCATE_${restore_libelle}" "
                            DO
                            \$\$
                            DECLARE
                            BEGIN
                                PERFORM public.drop_table_constraints('${restore_schema_name}', '${restore_table_name}');
                                PERFORM public.drop_table_indexes('${restore_schema_name}', '${restore_table_name}');
                                PERFORM public.drop_table_triggers('${restore_schema_name}', '${restore_table_name}');
                                IF table_exists('${restore_schema_name}','${restore_table_name}') = TRUE THEN
                                    TRUNCATE TABLE ${restore_schema_name}.${restore_table_name} CASCADE;
                                END IF;
                            END
                            \$\$ LANGUAGE plpgsql;
                        "  || { restore_table_reset; return $ERROR_CODE; }
                    fi

                    # filtre à appliquer sur les données à charger
                    if [ ! -z "$sql_to_filter" ] && ! view_exists "${restore_schema_name}" "${restore_table_name}"; then #si un filtre de données est défini, il ne peut d'appliquer sur une vue
                        #si filtre de données défini et que c'est une référence vers un fichier, on lit son contenu
                        if [ -f "$sql_to_filter" ]; then
                            sql_to_filter=$(< "$sql_to_filter")
                        fi
                        log_info 'Préparation filtre données' &&
                        execute_sql_command "RESTORE_FILTER_TABLE_$restore_libelle" "
                            SELECT public.DROP_ALL_FUNCTIONS_IF_EXISTS('${restore_schema_name}','${restore_table_name}_restore_filter');
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

        $pg_bin_dir/pg_restore --host $pg_final_host --port $pg_final_port --username $pg_username --dbname $pg_dbname --no-password \
            --format=custom --verbose --exit-on-error \
            $pg_restore_section_arg \
            $pg_restore_file_arg > $dir_archive/pg_restore_${restore_table_name}.log 2>&1 || {
            log_error "Erreur pg_restore, cf $dir_archive/pg_restore_${restore_table_name}.log"
            restore_table_reset
            return $ERROR_CODE
        }

        for _restore_section in ${restore_sections[@]}; do
            if [ "$_restore_section" = 'data' ]; then
                # suppression filtre à appliqué sur les données à chargées
                if [ ! -z "$sql_to_filter" ]; then
                    execute_sql_command "DROP_RESTORE_FILTER_TABLE_${restore_libelle}" "
                        SELECT public.DROP_ALL_FUNCTIONS_IF_EXISTS('${restore_schema_name}','${restore_table_name}_restore_filter');
                        DROP TRIGGER IF EXISTS trg_${restore_table_name}_restore_filter ON ${restore_schema_name}.${restore_table_name};
                    " || { restore_table_reset; return $ERROR_CODE; }
                fi
            fi
        done
    fi

    if [ "$subprocess" = 'no' ]; then
        vacuum -s $restore_schema_name -t $restore_table_name -m ANALYSE || {
            restore_table_reset;
            return $ERROR_CODE;
        }

        #Suppression de la sauvegarde avant restauration, si la restauration automatique est activée
        [ "$backup_before_restore" = "yes" ] && [ "$restore_on_error" = "yes" ] && rm --force $backup_before_restore_full_path
        restore_table_reset

        log_info "Fin de restauration"
    fi
    return $SUCCESS_CODE
}
