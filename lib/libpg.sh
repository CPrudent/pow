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

    is_yes --var get_arg_with_log && log_info "Lancement de l'exécution $_info $_log"
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
    is_yes --var get_arg_with_log && {
        grep --extended-regexp --invert-match 'ATTENTION:|NOTICE:|DÉTAIL : |DROP cascade sur ' $_log_notice_tmp_path >> $_log_error_tmp_path
        sed --in-place --expression '/^NOTICE:  la relation « [^ ]* » existe déjà/d' $_log_notice_tmp_path
        archive_file $_log_tmp_path
        archive_file $_log_notice_tmp_path
        archive_file $_log_error_tmp_path
    } || {
        rm --force $_log_tmp_path $_log_notice_tmp_path $_log_error_tmp_path
    }
    [ $_rc -ne 0 ] && {
        local _msg="Erreur lors de l'exécution de $_log"
        is_yes --var get_arg_with_log && _msg+=", veuillez consulter $_log_error_archive_path"
        log_error "$_msg"
        return $ERROR_CODE
    }
    is_yes --var get_arg_with_log && {
        get_elapsed_time --start $_start --result _last
        log_info "Exécution avec succès de $_log en $_last"
    }
    # requested result of SELECT
    [ -n "$get_arg_return" ] && {
        local -n _select_ref=$get_arg_return
        _select_ref=$(< "$POW_DIR_ARCHIVE/$_log.log")
    }

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
