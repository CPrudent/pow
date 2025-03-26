    #--------------------------------------------------------------------------
    # synopsis
    #--
    # PG library

# execute query (from file or command line)
#  example: retrieve version of PostgreSQL
#  execute_query --name GET_VERSION --query 'SELECT version()' --return _pg_version
execute_query() {
    local -A _opts &&
    pow_argv \
        --args_n '
            name:nommage de la commande;
            query:code SQL à exécuter (fichier ou ligne de commande);
            output:fichier résultat;
            return:résultat de la commande SELECT;
            psql_arguments:paramètres supplémentaires, sous la forme (arg1:arg2:...:argn);
            temporary:gestion du fichier temporaire;
            with_log:avec log
        ' \
        --args_m '
            name;query
        ' \
        --args_v '
            with_log:no|yes;
            temporary:USER|UNIQ
        ' \
        --args_d '
            with_log:yes;
            temporary:USER
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _start=$(date +%s) _info _rc _last _i _error
    local _opt _quiet _psql_level=NOTICE _psql_output
    local _log_tmp_ext=log _log_tmp_path _log_tmp_dir _log_tmp_file _log_tmp_wo_ext

    {
        # query option
        [ -f "${_opts[QUERY]}" ] && {
            _opt=--file
            _info=fichier
        } || {
            _opt=--command
            _info=requête
        }
    } &&
    {
        # temporary
        case "${_opts[TEMPORARY]}" in
        UNIQ)
            # accepting concurrent mode (parallelism)
            get_tmp_file --tmpfile _log_tmp_path --tmpext $_log_tmp_ext --create --suffix "-${_opts[NAME]}" &&
            _log_tmp_dir="${_log_tmp_path%/*}" &&
            _log_tmp_file="${_log_tmp_path##*/}" &&
            _log_tmp_wo_ext="${_log_tmp_file%.*}"
            ;;
        USER)
            # user defined
            _log_tmp_dir="${POW_DIR_TMP}" &&
            _log_tmp_file="${_opts[NAME]}.${_log_tmp_ext}" &&
            _log_tmp_wo_ext="${_opts[NAME]}" &&
            _log_tmp_path="${_log_tmp_dir}/${_log_tmp_file}"
            ;;
        esac
    } &&
    {
        # extra arguments
        [ -z "${_opts[PSQL_ARGUMENTS]}" ] || {
            local _ifs=$IFS _i
            local -a _args
            # convert colon-separated (:) as list of option(s) of psql w/ -- prefix for each
            IFS=: ; _args=(${_opts[PSQL_ARGUMENTS]}) ; IFS=$_ifs
            _opts[PSQL_ARGUMENTS]=
            for ((_i=0; _i<${#_args[@]}; _i++)); do
                _opts[PSQL_ARGUMENTS]+="--${_args[$_i]} "
            done
        }
    } &&
    {
        # output
        [ -z "${_opts[OUTPUT]}" ] && {
            _psql_output="$_log_tmp_path"
        } || {
            [ -n "${_opts[RETURN]}" ] && {
                _error='Les options --output et --return sont exclusives'
                false
            } || {
                _psql_output="${_opts[OUTPUT]}"
            }
        }
    } &&
    {
        # return
        # quiet: https://stackoverflow.com/questions/21777564/postgresql-is-there-a-way-to-disable-the-display-of-insert-statements-when-rea
        [ -z "${_opts[RETURN]}" ] || {
            _psql_level=ERROR
            _quiet=--quiet
        }
    } &&
    {
        ([ -z "${_opts[OUTPUT]}" ] && [ -z "${_opts[RETURN]}" ]) || {
            # set needing arguments to return result (if not defined)
            [ -n "${_opts[PSQL_ARGUMENTS]}" ] || {
                _opts[PSQL_ARGUMENTS]='--tuples-only --pset=format=unaligned'
            }
        }
    } &&
    {
        # with log: start message
        [ "${_opts[WITH_LOG]}" = no ] || log_info "Lancement de l'exécution de ${_opts[NAME]} ($_info)"
    } &&
    {
        # debug
        ([ -z "$POW_DEBUG" ] || [ "$POW_DEBUG" = no ]) || {
            echo "PGOPTIONS=-c client_min_messages=$_psql_level"
            echo "psql_arguments=${_opts[PSQL_ARGUMENTS]}"
            echo "input=$_opt ${_opts[QUERY]}"
            echo "output=$_psql_output"
        }
    } &&
    {
        # call psql
        env PGOPTIONS="-c client_min_messages=$_psql_level" $POW_DIR_PG_BIN/psql \
            --host $POW_PG_HOST \
            --port $POW_PG_PORT \
            --username $POW_PG_USERNAME \
            --dbname $POW_PG_DBNAME \
            --variable ON_ERROR_STOP=1 \
            --no-password \
            $_quiet \
            ${_opts[PSQL_ARGUMENTS]} \
            $_opt "${_opts[QUERY]}" \
            --output "$_psql_output" 2> "${_log_tmp_dir}/${_log_tmp_wo_ext}-notice.${_log_tmp_ext}"
        _rc=$?
    } &&
    {
        # debug
        ([ -z "$POW_DEBUG" ] || [ "$POW_DEBUG" = no ]) || {
            echo "output:"
            cat $_psql_output
        }
    } &&
    {
        # purge & archive log
        grep \
            --extended-regexp \
            --invert-match \
            'ATTENTION:|NOTICE:|DÉTAIL : |DROP cascade sur ' \
            "${_log_tmp_dir}/${_log_tmp_wo_ext}-notice.${_log_tmp_ext}" \
            >> "${_log_tmp_dir}/${_log_tmp_wo_ext}-error.${_log_tmp_ext}"
        sed \
            --in-place \
            --expression \
            '/^NOTICE:  la relation « [^ ]* » existe déjà/d' \
            "${_log_tmp_dir}/${_log_tmp_wo_ext}-notice.${_log_tmp_ext}"

        {
            # requested output, or default can be archived
            [ -n "${_opts[OUTPUT]}" ] || archive_file "$_log_tmp_path"
        } &&
        archive_file "${_log_tmp_dir}/${_log_tmp_wo_ext}-notice.${_log_tmp_ext}" &&
        archive_file "${_log_tmp_dir}/${_log_tmp_wo_ext}-error.${_log_tmp_ext}"
    } &&
    {
        # error
        [ $_rc -eq 0 ] || {
            _error="Erreur lors de l'exécution de ${_opts[NAME]}"
            [ "${_opts[WITH_LOG]}" = yes ] && _error+=", veuillez consulter ${POW_DIR_ARCHIVE}/${_log_tmp_wo_ext}-error.${_log_tmp_ext}"
            false
        }
    } &&
    {
        # with log: end message (w/ last)
        [ "${_opts[WITH_LOG]}" = no ] || {
            get_elapsed_time --start $_start --result _last
            log_info "Exécution avec succès de ${_opts[NAME]} en $_last"
        }
    } &&
    {
        # requested result of SELECT
        [ -z "${_opts[RETURN]}" ] || {
            local -n _select_ref=${_opts[RETURN]}
            _select_ref=$(< "${POW_DIR_ARCHIVE}/${_log_tmp_file}")
        }
    } || {
        [ -n "$_error" ] && log_error "$_error"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# check if table exists
table_exists() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:schéma PG;
            table_name:nom de la table
        ' \
        --args_m '
            table_name
        ' \
        --args_d '
            schema_name:public
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _exists _rc=$ERROR_CODE

    execute_query \
        --name "TABLE_EXISTS_${_opts[SCHEMA_NAME]}_${_opts[TABLE_NAME]}" \
        --query "SELECT table_exists('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}')" \
        --return _exists &&
    is_yes --var _exists && _rc=$SUCCESS_CODE

    return $_rc
}

# check if view exists
view_exists() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:schéma PG;
            view_name:nom de la vue
        ' \
        --args_m '
            view_name
        ' \
        --args_d '
            schema_name:public
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _exists _rc=$ERROR_CODE

    execute_query \
        --name "VIEW_EXISTS_${_opts[SCHEMA_NAME]}_${_opts[VIEW_NAME]}" \
        --query "SELECT view_exists('${_opts[SCHEMA_NAME]}', '${_opts[VIEW_NAME]}')" \
        --return _exists &&
    is_yes --var _exists && _rc=$SUCCESS_CODE

    return $_rc
}

# optimize table (date, index, statistics, ...)
vacuum() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:schéma PG;
            table_name:nom de la table (ou liste de noms séparés par une virgule);
            mode:mode VACUUM à appliquer;
            dry_run:traitement sans exécution SQL
        ' \
        --args_v '
            mode:ANALYZE|FULL;
            dry_run:no|yes
        ' \
        --args_d '
            mode:ANALYZE;
            dry_run:no
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    if [ -z "${_opts[MODE]}" ]; then
        log_error "Veuillez préciser le mode de VACUUM (ANALYZE, FULL). Exemple : --mode ANALYZE"
        return $ERROR_CODE
    fi

    local _log_tmp_path=$POW_DIR_TMP/vacuum_${_opts[MODE]}
    [ -n "${_opts[SCHEMA_NAME]}" ] && _log_tmp_path+=_${_opts[SCHEMA_NAME]}
    [ -n "${_opts[TABLE_NAME]}" ] && _log_tmp_path+=_${_opts[TABLE_NAME]}
    _log_tmp_path+=.log

    local _vacuum_options
    case "${_opts[MODE]}" in
    FULL)
        df -h >> $_log_tmp_path
        _vacuum_options='(FULL, ANALYZE, VERBOSE)'
        ;;
    ANALYZE)
        _vacuum_options='(ANALYZE, VERBOSE)'
        ;;
    esac

    # table
    if [ -n "${_opts[TABLE_NAME]}" ]; then
        # eventually list of table(s), comma-separated
        local _list_tables=(${_opts[TABLE_NAME]//,/ }) _table
        for ((_i=0; _i<${#_list_tables[*]}; _i++)); do
            _table="${_list_tables[$_i]}"
            #echo "$_table"
            [[ ! ${_table} =~ ^[^\.]*\..*$ ]] && {
                # with schema?
                [ -n "${_opts[SCHEMA_NAME]}" ] && _table=${_opts[SCHEMA_NAME]}.${_table}
            }

            log_info "VACUUM ${_opts[MODE]} sur la table ${_table}"
            [ "${_opts[DRY_RUN]}" = no ] && {
                execute_query \
                    --name "VACUUM_${_opts[MODE]}_${_table}" \
                    --query "VACUUM ${_vacuum_options} ${_table}" || {
                    log_error "Erreur VACUUM ${_opts[MODE]} sur la table ${_table}"
                    return $ERROR_CODE
                }
            }
        done
    # schema
    elif [ -n "${_opts[SCHEMA_NAME]}" ]; then
        log_info "Début VACUUM "${_opts[MODE]}" sur les tables du schéma ${_opts[SCHEMA_NAME]}"

        # old method (w/ psql command \dt)
        # http://stackoverflow.com/questions/29710618/vacuum-analyze-all-tables-in-a-schema-postgres
        # vacuum only the tables in the schema named in the variable ${_opts[SCHEMA_NAME]}
        # --query "\dt ${_opts[SCHEMA_NAME]}."'*' \
        # local _tables_array=($(echo $_all | tr ' ' '\n' | cut --delimiter '|' --field 2))

        local _vacuum_tables _vacuum_table
        execute_query \
            --name "${_vacuum_schema}_ALL_TABLES" \
            --query "
                SELECT STRING_AGG(table_name, ' ')
                FROM information_schema.tables
                WHERE table_schema = '${_opts[SCHEMA_NAME]}'
                AND table_type = 'BASE TABLE'
            " \
            --return _vacuum_tables || return $ERROR_CODE
        local _tables_array=($_vacuum_tables)
        for _vacuum_table in "${_tables_array[@]}"
        do
            log_info "VACUUM ${_opts[MODE]} sur la table ${_vacuum_table}"
            [ "${_opts[DRY_RUN]}" = no ] && {
                execute_query \
                    --name "VACUUM_${_opts[MODE]}_${_opts[SCHEMA_NAME]}.${_vacuum_table}" \
                    --query "VACUUM ${_vacuum_options} ${_opts[SCHEMA_NAME]}.${_vacuum_table}" || {
                    log_error "Erreur VACUUM ${_opts[MODE]} sur la table ${_opts[SCHEMA_NAME]}.${_vacuum_table}"
                    return $ERROR_CODE
                }
            }
        done

        log_info "Fin VACUUM "${_opts[MODE]}" sur les tables du schéma ${_opts[SCHEMA_NAME]}"
    # database
    else
        log_info "Début VACUUM "${_opts[MODE]}" sur la base de données"
        [ "${_opts[DRY_RUN]}" = no ] && {
            execute_query \
                --name "VACUUM_${_opts[MODE]}_DB" \
                --query "VACUUM ${_vacuum_options}" || {
                log_error "Erreur VACUUM ${_opts[MODE]} sur la base de données"
                return $ERROR_CODE
            }
        }
        log_info "Fin VACUUM "${_opts[MODE]}" sur la base de données"
    fi

    [ "${_opts[MODE]}" = FULL ] && df -h >> $_log_tmp_path
    [ -f "$_log_tmp_path" ] && archive_file $_log_tmp_path

    return $SUCCESS_CODE
}

# get sequences of a table
get_table_sequences() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:Nom du schema de la table;
            table_name:Nom de la table
        ' \
        --args_m '
            schema_name;
            table_name
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _sequences

    # NOTE : sequence name can be prefixed by schema
    execute_query \
        --name GET_TABLE_SEQUENCES \
        --query "SELECT STRING_AGG(sequence_name, ',')
            FROM (
                SELECT (REGEXP_MATCHES(column_default, 'nextval\(''(${_opts[SCHEMA_NAME]}\.)?([^:]+)''::regclass\)'))[2] AS sequence_name
                FROM information_schema.columns
                WHERE table_schema = '${_opts[SCHEMA_NAME]}'
                AND table_name = '${_opts[TABLE_NAME]}'
                AND column_default LIKE 'nextval(%'
            ) t
        " \
        --return _sequences || return $ERROR_CODE

    echo $_sequences

    return $SUCCESS_CODE
}

# backup table
backup_table() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:Nom du schéma de la table à copier;
            table_name:Nom de la table à copier;
            output:Sortie standard ou chemin complet vers un fichier;
            format:Format de la sauvegarde;
            sections:Liste des sections à sauvegarder
        ' \
        --args_m '
            schema_name;
            table_name
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
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _backup_label="${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}"
    local _backup_log=${POW_DIR_ARCHIVE}/pg_dump_${_backup_label}.log
    local _backup_sections=(${_opts[SECTIONS]//+/ }) _backup_section
    local _backup_arg_file _backup_arg_section _backup_arg_sequence
    local _backup_table_sequences _backup_table_sequences_array _backup_table_sequence
    local _previous_log_echo=$POW_LOG_ECHO

    backup_table_reset() {
        set_log_echo $_previous_log_echo
    }

    [ "${_opts[OUTPUT]}" = STDOUT ] && set_log_echo no
    log_info "Début de sauvegarde de ${_backup_label} dans ${_opts[OUTPUT]}, sections ${_backup_sections[*]}"

    local _backup_output_tmp="$POW_DIR_TMP/${_backup_label}_$$.backup"
    [ "${_opts[OUTPUT]}" != STDOUT ] && _backup_arg_file="--file=$_backup_output_tmp"
    for _backup_section in ${_backup_sections[@]}; do
        _backup_arg_section+="--section $_backup_section "
    done

    # not useful because sequences are native into pg_dump?
    #     _backup_table_sequences=$(get_table_sequences --schema_name ${_opts[SCHEMA_NAME]} --table_name ${_opts[TABLE_NAME]})
    #     _backup_table_sequences_array=(${_backup_table_sequences//,/ })
    #     for _backup_table_sequence in ${_backup_table_sequences_array[@]}; do
    #         _backup_arg_sequence+="--table $_backup_table_sequence "
    #     done

    # available disk space
    df -h $POW_DIR_DATA > $_backup_log

    $POW_DIR_PG_BIN/pg_dump	\
        --host=$POW_PG_HOST	\
        --port=$POW_PG_PORT \
        --username=$POW_PG_USERNAME \
        --no-password \
        --format=${_opts[FORMAT]} \
        --verbose \
        $_backup_arg_file \
        $_backup_arg_section \
        $_backup_arg_sequence \
        --table=${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} \
        $POW_PG_DBNAME \
        2>> $_backup_log

    local _return_code=$?
    [ $_return_code -ne 0 ] && {
        log_error "Erreur pg_dump($_return_code), voir $_backup_log"
        [ "${_opts[OUTPUT]}" != STDOUT ] && [ -f "$_backup_output_tmp" ] && rm $_backup_output_tmp
        backup_table_reset
        return $ERROR_CODE
    }
    [ "${_opts[OUTPUT]}" != STDOUT ] && mv $_backup_output_tmp ${_opts[OUTPUT]}
    log_info "Fin de sauvegarde de ${_backup_label}"
    backup_table_reset

    return $SUCCESS_CODE
}

# restore table (from backup)
restore_table() {
    local -A _opts &&
    pow_argv \
        --args_n '
            schema_name:Nom du schéma de la table à restaurer;
            table_name:Nom de la table à restaurer;
            mode:Mode de restauration;
            input:Entrée standard ou chemin complet vers un fichier;
            backup_before_restore:Faut-il sauvegarder la table avant la restauration ?;
            sql_to_filter:Requête SQL pour filtrer la table à recopier;
            restore_on_error:Faut-il restaurer automatiquement la table si la restauration initiale échoue ?;
            sections:Liste des sections à restaurer;
            subprocess:Indique si cet appel à restore_table est un sous processus;
            wait_file_minute:combien de temps en minutes faut-il attendre que le fichier de sauvegarde soit présent ?;
            max_age_file_minute:quel age maximum en minutes doit avoir le fichier de sauvegarde ?
        ' \
        --args_m '
            schema_name;
            table_name
        ' \
        --args_v '
            mode:TRUNCATE|DROP|APPEND;
            backup_before_restore:yes|no;
            restore_on_error:yes|no;
            sections:pre-data+data+post-data|pre-data|data|post-data|data+post-data;
            subprocess:yes|no
        ' \
        --args_d '
            mode:TRUNCATE;
            backup_before_restore:yes;
            input:STDIN;
            restore_on_error:yes;
            sections:pre-data+data+post-data;
            subprocess:no;
            wait_file_minute:0;
            max_age_file_minute:0
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    #ATTENTION : maintenir avec la variable de même nom dans la fonction copy_tables
    local _restore_label="${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}"
    local _backup_before_restore_path=$POW_DIR_TMP/${_restore_label}_$$.backup.before_restore

    local _table_to_restore_exists=no
    local _restore_sections=(${_opts[SECTIONS]//+/ })
    local _restore_sections_todo=()
    local _restore_arg_file _restore_arg_section
    local _restore_table_sequences _restore_table_sequences_array _restore_table_sequence
    local _previous_log_echo=$POW_LOG_ECHO

    if [ "${_opts[INPUT]}" != STDIN ]; then
        wait_for_file \
            --file_path "${_opts[INPUT]}" \
            --wait_file_minute ${_opts[WAIT_FILE_MINUTE]} \
            --max_age_file_minute ${_opts[MAX_AGE_FILE_MINUTE]} || {
            log_error "${FUNCNAME[0]}: La sauvegarde ${_opts[INPUT]} n'existe pas"
            return $ERROR_CODE
        }
    fi

    if table_exists --schema_name "${_opts[SCHEMA_NAME]}" --table_name "${_opts[TABLE_NAME]}"; then
        _table_to_restore_exists=yes
        _restore_table_sequences=$(get_table_sequences --schema_name ${_opts[SCHEMA_NAME]} --table_name ${_opts[TABLE_NAME]})
        _restore_table_sequences_array=(${_restore_table_sequences//,/ })
        if [ "${_opts[MODE]}" = APPEND ] && [ ${#_restore_table_sequences_array[*]} -gt 0 ]; then
            # risk of conflict w/ values (foreign columns)
            log_error "${FUNCNAME[0]}: Il n'est pas possible de restaurer cette table en mode APPEND car celle-ci utilise des séquences"
            return $ERROR_CODE
        fi
    else
        _opts[BACKUP_BEFORE_RESTORE]=no
    fi

    for _restore_section in ${_restore_sections[@]}; do
        # not DROP: pre-data not useful
        [ "$_restore_section" = pre-data ] &&
        [ "$_table_to_restore_exists" = yes ] &&
        [ "${_opts[MODE]}" != DROP ] && continue
        # idem for post-data
        [ "$_restore_section" = 'post-data' ] &&
        [ "$_table_to_restore_exists" = yes ] &&
        [ "${_opts[MODE]}" = APPEND ] && continue

        _restore_sections_todo+=(${_restore_section})
    done

    restore_table_reset() {
        set_log_echo $_previous_log_echo

        [ "${_opts[BACKUP_BEFORE_RESTORE]}" = yes ] &&
        [ "${_opts[RESTORE_ON_ERROR]}" = yes ] &&
        [ -f $_backup_before_restore_path ] && {
            log_info "Restauration de la sauvegarde de ${_restore_label}"
            restore_table \
                -schema_name "${_opts[SCHEMA_NAME]}" \
                --table_name "${_opts[TABLE_NAME]}" \
                --input "$_backup_before_restore_path" \
                --backup_before_restore no || return $ERROR_CODE
            rm -f $_backup_before_restore_path
        }

        return $SUCCESS_CODE
    }

    [ "${_opts[INPUT]}" = STDIN ] && set_log_echo no
    # backup before restore?
    [ "${_opts[BACKUP_BEFORE_RESTORE]}" = yes ] && {
        [ -f $_backup_before_restore_path ] && rm $_backup_before_restore_path

        backup_table \
            --schema_name "${_opts[SCHEMA_NAME]}" \
            --table_name "${_opts[TABLE_NAME]}" \
            --output "$_backup_before_restore_path" || {
            [ -f $_backup_before_restore_path ] && rm -f $_backup_before_restore_path
            restore_table_reset
            return $ERROR_CODE
        }
    }

    # with filter AND full call (w/ sections, not one by one)
    if  [ ! -z "${_opts[SQL_TO_FILTER]}" ] &&
        [[ " ${_restore_sections_todo[@]} " =~ " pre-data " ]] &&
        [[ " ${_restore_sections_todo[@]} " =~ " data " ]]; then
        if [ "${_opts[INPUT]}" = STDIN ]; then
            log_info "Conversion de STDIN en fichier temporaire"
            cat > $POW_DIR_TMP/stdin_$$.backup || { restore_table_reset; return $ERROR_CODE; }
            _opts[INPUT]="$POW_DIR_TMP/stdin_$$.backup"
        fi
        # section one by one to prepare filter between DDL and DATA, NOTE: recursive call (w/ subprocess)
        for _restore_section in ${_restore_sections_todo[@]}; do
            restore_table \
                --schema_name ${_opts[SCHEMA_NAME]} \
                --table_name ${_opts[TABLE_NAME]} \
                --mode ${_opts[MODE]} \
                --input ${_opts[INPUT]} \
                --backup_before_restore no  \
                --sql_to_filter "${_opts[SQL_TO_FILTER]}" \
                --sections $_restore_section \
                --subprocess yes || { restore_table_reset; return $ERROR_CODE; }
        done
        [ "${_opts[INPUT]}" = "$POW_DIR_TMP/stdin_$$.backup" ] && rm -f ${_opts[INPUT]}
    else
        log_info "Début de restauration de ${_restore_label} à partir de ${_opts[INPUT]}, sections ${_restore_sections_todo[*]}"
        [ "${_opts[INPUT]}" != STDIN ] && _restore_arg_file="${_opts[INPUT]}"
        # apply prior actions
        for _restore_section in ${_restore_sections_todo[@]}; do
            # prepare cumulative section arguments
            _restore_arg_section+="--section $_restore_section "
            if [ "$_table_to_restore_exists" = yes ]; then
                if [ "$_restore_section" = pre-data ]; then
                    if [ "${_opts[MODE]}" = DROP ]; then
                        # drop table if (exists and DROP mode), NOTE: COMMIT necessary
                        execute_query \
                            --name "DROP_TABLE_${_restore_label}" \
                            --query "
                                DO
                                \$\$
                                DECLARE
                                BEGIN
                                    IF table_exists('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}') THEN
                                        DROP TABLE ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} CASCADE;COMMIT;
                                    ELSIF view_exists('${_opts[SCHEMA_NAME]}','${_opts[TABLE_NAME]}') THEN
                                        DROP VIEW ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} CASCADE;COMMIT;
                                    END IF;
                                END
                                \$\$ LANGUAGE plpgsql;" || { restore_table_reset; return $ERROR_CODE; }

                        # idem for sequences
                        # not useful
                        #for _restore_table_sequence in ${_restore_table_sequences_array[@]}; do
                        #	execute_sql_command "DROP_SEQUENCE_${_restore_table_sequence}.${_restore_label}" "DROP SEQUENCE ${_opts[SCHEMA_NAME]}.${_restore_table_sequence};COMMIT;" || { restore_table_reset; return $ERROR_CODE; }
                        #done

                        local _drop_cascade
                        _drop_cascade=$(grep 'NOTICE: \+DROP cascade' "${POW_DIR_ARCHIVE}/DROP_TABLE_${_restore_label}-notice.log") &&
                        [ -n "$_drop_cascade" ] &&
                        log_info "${_drop_cascade}, voir ${POW_DIR_ARCHIVE}/DROP_TABLE_${_restore_label}-notice.log"
                    fi
                elif [ "$_restore_section" = data ]; then
                    if [ "${_opts[MODE]}" = TRUNCATE ]; then
                        #Suppression des données en cascade (données faisant référence à ces données par contrainte de clé étrangère), des index, triggers et contraintes
                        execute_query \
                            --name "DROP_CONSTRAINTS_INDEX_TRIGGERS_TRUNCATE_${_restore_label}" \
                            --query "
                                DO
                                \$\$
                                DECLARE
                                BEGIN
                                    PERFORM public.drop_table_constraints('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}');
                                    PERFORM public.drop_table_indexes('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}');
                                    PERFORM public.drop_table_triggers('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}');
                                    IF table_exists('${_opts[SCHEMA_NAME]}','${_opts[TABLE_NAME]}') THEN
                                        TRUNCATE TABLE ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} CASCADE;
                                    END IF;
                                END
                                \$\$ LANGUAGE plpgsql;
                            "  || { restore_table_reset; return $ERROR_CODE; }
                    fi

                    # filter to data? not for a view
                    if [ ! -z "${_opts[SQL_TO_FILTER]}" ] && ! view_exists --schema_name "${_opts[SCHEMA_NAME]}" --view_name "${_opts[TABLE_NAME]}"; then
                        if [ -f "${_opts[SQL_TO_FILTER]}" ]; then
                            _opts[SQL_TO_FILTER]=$(< "${_opts[SQL_TO_FILTER]}")
                        fi
                        log_info 'Préparation filtre données' &&
                        execute_query \
                            --name "RESTORE_FILTER_TABLE_$_restore_label" \
                            --query "
                                SELECT public.drop_all_functions_if_exists('${_opts[SCHEMA_NAME]}','${_opts[TABLE_NAME]}_restore_filter');
                                CREATE OR REPLACE FUNCTION ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}_restore_filter() RETURNS TRIGGER AS "'$$'"
                                    BEGIN
                                        IF
                                            ${_opts[SQL_TO_FILTER]}
                                        THEN
                                            RETURN NEW;
                                        ELSE
                                            RETURN NULL;
                                        END IF;
                                    END;
                                "'$$'" LANGUAGE plpgsql;

                                DROP TRIGGER IF EXISTS trg_${_opts[TABLE_NAME]}_restore_filter ON ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]};
                                CREATE TRIGGER trg_${_opts[TABLE_NAME]}_restore_filter
                                BEFORE INSERT ON ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}
                                    FOR EACH ROW
                                    EXECUTE PROCEDURE ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}_restore_filter();
                            " || {
                                restore_table_reset &&
                                return $ERROR_CODE
                            }
                    fi
                fi
            fi
        done

        $POW_DIR_PG_BIN/pg_restore \
            --host $POW_PG_HOST \
            --port $POW_PG_PORT \
            --username $POW_PG_USERNAME \
            --dbname $POW_PG_DBNAME \
            --no-password \
            --format custom \
            --verbose \
            --exit-on-error \
            $_restore_arg_section \
            $_restore_arg_file > $POW_DIR_ARCHIVE/pg_restore_${_opts[TABLE_NAME]}.log 2>&1 || {
            log_error "Erreur pg_restore, voir $POW_DIR_ARCHIVE/pg_restore_${_opts[TABLE_NAME]}.log"
            restore_table_reset
            return $ERROR_CODE
        }

        # apply post actions
        for _restore_section in ${_restore_sections_todo[@]}; do
            if [ "$_restore_section" = data ]; then
                # delete filter
                if [ ! -z "${_opts[SQL_TO_FILTER]}" ]; then
                    execute_query \
                        --name "DROP_RESTORE_FILTER_TABLE_${_restore_label}" \
                        --query "
                            SELECT public.drop_all_functions_if_exists('${_opts[SCHEMA_NAME]}','${_opts[TABLE_NAME]}_restore_filter');
                            DROP TRIGGER IF EXISTS trg_${_opts[TABLE_NAME]}_restore_filter ON ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]};
                        " || { restore_table_reset; return $ERROR_CODE; }
                fi
            fi
        done
    fi

    [ "${_opts[SUBPROCESS]}" = no ] && {
        vacuum --schema_name ${_opts[SCHEMA_NAME]} --table_name ${_opts[TABLE_NAME]} || {
            restore_table_reset;
            return $ERROR_CODE;
        }

        [ "${_opts[BACKUP_BEFORE_RESTORE]}" = yes ] &&
        [ "${_opts[RESTORE_ON_ERROR]}" = yes ] &&
        rm --force $_backup_before_restore_path
        # resume log echo
        restore_table_reset

        log_info "Fin de restauration de ${_restore_label}"
    }

    return $SUCCESS_CODE
}

# convert Postgresql's array to Bash's one, checking waited count
#  FIXME empty SQL array has to be set to {}, no empty string (else pow_argv's error)
array_sql_to_bash() {
    local -A _opts &&
    pow_argv \
        --args_n '
            array_sql:Tableau SQL ({val1,val2,...,valn});
            count:Taille attendue;
            array_bash:Entité du résultat
        ' \
        --args_m '
            array_sql;
            array_bash
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -n _array_ref=${_opts[ARRAY_BASH]}

    # convert into BASH array
    {
        [ -z "${_opts[ARRAY_SQL]}" ] || {
            # to delete braces
            if [[ ${_opts[ARRAY_SQL]} =~ ^\{(.*)\}$ ]]; then
                IFS=',' read -ra _array_ref <<< "${BASH_REMATCH[1]}"
            else
                log_error "tableau SQL mal formé (${_opts[ARRAY_SQL]})"
                false
            fi
        }
    } &&
    # check size
    {
        [ -z "${_opts[COUNT]}" ] || {
            [[ ${#_array_ref[@]} -eq ${_opts[COUNT]} ]] || {
                log_error "écart liste: obtenu=${#_array_ref[@]}, attendu=${_opts[COUNT]}"
                false
            }
        }
    } || return $ERROR_CODE

    return $SUCCESS_CODE
}
