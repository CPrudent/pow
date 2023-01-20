    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define PG

    ###
    # execute query (from file or command line)
    #
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

execute_sql_file() {
    local startTime=$(date +%s)
    local file_path=$1

    expect file $file_path || return $ERROR_CODE

    local psql_arguments=$2
    local file_name=$(basename $file_path)
    local log_tmp_path=$POW_DIR_TMP/$file_name.log
    local log_notice_tmp_path=$POW_DIR_TMP/$file_name.notice.log
    local log_error_tmp_path=$POW_DIR_TMP/$file_name.error.log
    local log_error_archive_path=$POW_DIR_ARCHIVE/$file_name.error.log

    log_info "Lancement de l'exécution du fichier $file_name"
    env PGPASSWORD=$pg_password PGOPTIONS='-c client_min_messages=NOTICE' "$pg_bin_dir/psql" --host $pg_final_host --port $pg_final_port --username $pg_username --dbname $pg_dbname --variable ON_ERROR_STOP=1 --variable lco_env=\'$ENV\' --no-password $psql_arguments --file $file_path --output $log_tmp_path 2> $log_notice_tmp_path
    retour_psql=$?
    grep -E -v 'ATTENTION:|NOTICE:|DÉTAIL : |DROP cascade sur ' $log_notice_tmp_path >> $log_error_tmp_path
    sed -e '/^NOTICE:  la relation « [^ ]* » existe déjà/d' $log_notice_tmp_path

    archive_file $log_tmp_path
    archive_file $log_notice_tmp_path
    archive_file $log_error_tmp_path

    #if [ -s $log_error_archive_path ]; then
    if [ $retour_psql -ne 0 ]; then
        log_error "Erreur lors de l'exécution de $file_name, veuillez consulter $log_error_archive_path"
        return $ERROR_CODE
    fi

    local endTime=$(date +%s)
    local elapsedTime="$((($endTime-$startTime)/3600))h:$((($endTime-$startTime)%3600/60))m:$((($endTime-$startTime)%60))s"
    log_info "Exécution avec succès de $file_name en $elapsedTime"
    return $SUCCESS_CODE
}

    # exécuter une commande SQL
execute_sql_command() {
    local startTime=$(date +%s)
    local sql_command_name=$1
    local sql_command=$2
    local psql_arguments=$3
    local psql_output=$4

    local log_tmp_path=$POW_DIR_TMP"/"$sql_command_name".log"
    local log_notice_tmp_path=$POW_DIR_TMP"/"$sql_command_name".notice.log"
    local log_error_tmp_path=$POW_DIR_TMP"/"$sql_command_name".error.log"
    local log_error_archive_path=$POW_DIR_ARCHIVE"/"$sql_command_name".error.log"

    [ -z "$psql_output" ] && psql_output=$log_tmp_path || touch $log_tmp_path

    log_info "Lancement de l'exécution de la commande SQL $sql_command_name"
    env PGPASSWORD=$pg_password PGOPTIONS='-c client_min_messages=NOTICE' "$pg_bin_dir/psql" --host $pg_final_host --port $pg_final_port --username $pg_username --dbname $pg_dbname --variable ON_ERROR_STOP=1 --variable lco_env=\'$ENV\' --no-password $psql_arguments --command "$sql_command" --output $psql_output 2> $log_notice_tmp_path
    retour_psql=$?
    grep -E -v 'ATTENTION:|NOTICE:|DÉTAIL : |DROP cascade sur ' $log_notice_tmp_path > $log_error_tmp_path

    archive_file $log_tmp_path
    archive_file $log_notice_tmp_path
    archive_file $log_error_tmp_path

    #if [ -s $log_error_archive_path ]; then
    if [ $retour_psql -ne 0 ]; then
        log_error "Erreur lors de la commande SQL $sql_command_name, veuillez consulter $log_error_archive_path"
        return $ERROR_CODE
    fi

    local endTime=$(date +%s)
    local elapsedTime="$((($endTime-$startTime)/3600))h:$((($endTime-$startTime)%3600/60))m:$((($endTime-$startTime)%60))s"
    log_info "Exécution avec succès de la commande SQL $sql_command_name en $elapsedTime"

    return $SUCCESS_CODE
}

# exécuter une commande SQL sans log sql ni temps passé
# TODO : revoir à faire une seule fonction execute_sql_command avec bash_args ?
execute_sql_command_basic() {
    local sql_command_name=$1
    local sql_command=$2
    local psql_arguments=$3
    local _log
    [ -n "$BCAA_DEBUG" ] && _log=$POW_DIR_TMP/execute_sql_command_basic.log || _log=/dev/null

    env PGPASSWORD=$pg_password PGOPTIONS='-c client_min_messages=NOTICE' $pg_bin_dir/psql --host $pg_final_host --port $pg_final_port --username $pg_username --dbname $pg_dbname --variable ON_ERROR_STOP=1 --variable lco_env=\'$ENV\' --no-password $psql_arguments --command "$sql_command" 2>> $_log || {
        log_error "Erreur lors de la commande SQL $sql_command_name"
        return $ERROR_CODE
    }
    return $SUCCESS_CODE
}

execute_sql_select() {
	get_execute_sql_select=
	execute_sql_command "$1" "$2" "$3 --tuples-only --pset=format=unaligned" "$POW_DIR_TMP/$1.output" || return $ERROR_CODE
	get_execute_sql_select=$(< "$POW_DIR_TMP/$1.output")
	rm $POW_DIR_TMP/$1.output
	return $SUCCESS_CODE
}

#version avec appel à execute_sql_command_basic pour éviter la création de log trop importants
# TODO : revoir à faire une seule fonction execute_sql_select avec bash_args ?
execute_sql_select_basic() {
	get_execute_sql_select=
	get_execute_sql_select=$(execute_sql_command_basic "$1" "$2" "$3 --tuples-only --pset=format=unaligned") || return $ERROR_CODE
	return $SUCCESS_CODE
}

table_exists() {
	execute_sql_select_basic "TABLE_EXISTS_${1}_${2}" "SELECT table_exists('$1','$2') OR view_exists('$1','$2')" || return $ERROR_CODE
	if [ $get_execute_sql_select = 't' ]; then
		return $SUCCESS_CODE
	else
		return $ERROR_CODE
	fi
}

view_exists() {
	execute_sql_select_basic "VIEW_EXISTS_${1}_${2}" "SELECT view_exists('$1','$2')" || return $ERROR_CODE
	if [ $get_execute_sql_select = 't' ]; then
		return $SUCCESS_CODE
	else
		return $ERROR_CODE
	fi
}
