    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define IO

    #
    # IO history
    #

_io_manager() {
    bash_args \
        --args_p '
            method:méthode de mise à jour;
            status:état IO;
            type:code type IO;
            date_begin:date de début des données (format connu PostgreSQL);
            date_end:date de fin des données (format connu PostgreSQL);
            nrows_todo:nombre de données à traiter;
            nrows_processed:nombre de données traitées;
            infos:compléments infos (souvent au format JSON);
            id:identifiant IO (ou nom de la variable pour le récupérer);
            output:sortie pour export
        ' \
        --args_o '
            method;
            status
        ' \
        --args_v '
            status:EN_COURS|SUCCES|ERREUR
        ' \
        "$@" || return $ERROR_CODE

    local _query _return _output _with_log=no

    case $get_arg_method in
    EXISTS)
        local -n _io_id=$get_arg_id
        _return='--return _io_id'
        _query="
            SELECT id FROM get_all_io(
                type_in => '$get_arg_type'
                , date_end => '$get_arg_date_end'::TIMESTAMP
                , status_in => '$get_arg_status'
            )
        "
        ;;
    APPEND)
        local -n _io_id=$get_arg_id
        _return='--return _io_id'
        local _infos
        [ -z "$get_arg_infos" ] && _infos='NULL' || _infos="'${infos_data}'"
        _query="
            INSERT INTO public.io_history(
                co_type
                , dt_debut_donnees
                , dt_fin_donnees
                , co_etat
                , nb_enregistrements_a_traiter
                , infos_data
            )
            VALUES (
                '$get_arg_type'
                , '$get_arg_date_begin'::TIMESTAMP
                , '$get_arg_date_end'::TIMESTAMP
                , '${get_arg_status:-EN_COURS}'
                , $get_arg_nrows_todo
                , $_infos
            );
            SELECT CURRVAL('public.io_history_id_seq');
        "
        ;;
    UPDATE_OK)
        local _infos
        # itself (if no defined) to remain previous value
        [ -z "$get_arg_infos" ] && _infos='infos_data' || _infos="'${infos_data}'"
        _query="
            UPDATE public.io_history SET
                dt_fin_execution = NOW()
                , co_etat = 'SUCCES'
                , nb_enregistrements_traites = $get_arg_nrows_processed
                , infos_data = $_infos
            WHERE id = $get_arg_id
        "
        ;;
    UPDATE_KO)
        _query="
            UPDATE public.io_history SET
                dt_fin_execution = NOW()
                , co_etat = 'ERREUR'
            WHERE id = $get_arg_id
        "
        ;;
    EXPORT_LAST)
        _with_log=yes
        _output="--output $get_arg_output"
        _query="
            COPY (
                SELECT
                    co_type
                    , dt_debut_execution
                    , dt_fin_execution
                    , dt_debut_donnees
                    , dt_fin_donnees
                    , co_etat
                    , nb_enregistrements_a_traiter
                    , nb_enregistrements_traites
                    , co_etat_integration
                    , infos_data
                FROM
                    public.io_history
                WHERE
                    co_type ~ '$get_arg_type'
                    AND
                    co_etat = 'SUCCES'
                ORDER BY
                    dt_fin_execution DESC
                LIMIT
                    1
            ) TO STDOUT WITH (DELIMITER ';', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
        "
        ;;
    esac

    execute_query \
        --name IO_${get_arg_method}_${get_arg_type} \
        --query "$_query" \
        --psql_arguments '--tuples-only --pset=format=unaligned' \
        $_return \
        $_output \
        --with_log $_with_log || return $ERROR_CODE

    return $SUCCESS_CODE
}

# IO in progress
# io_exists --status EN_COURS
# IO already success
# io_exists --status SUCCES
io_exists() {
    bash_args \
        --args_p '
            type:code type IO;
            date:date de fin des données (format connu PostgreSQL);
            status:état IO;
            id:variable pour récupérer ID de IO
        ' \
        --args_o '
            type;
            date
        ' \
        --args_v '
            status:EN_COURS|SUCCES|ERREUR
        ' \
        --args_d '
            status:EN_COURS
        ' \
        "$@" || return $ERROR_CODE

    [ -n "$get_arg_id" ] && local -n _io_id=$get_arg_id || local _io_id

    _io_manager \
        --method EXISTS \
        --status $get_arg_status \
        --type $get_arg_type \
        --date_end $get_arg_date_end \
        --id _io_id || return $ERROR_CODE

    [ -z "$_io_id" ] && return $ERROR_CODE
    return $SUCCESS_CODE
}

export -n _t=0
POW_IO_SUCCESSFUL=$((_t++))
POW_IO_IN_PROGRESS=$((_t++))
POW_IO_TODO=$((_t++))
POW_IO_ERROR=$((_t++))

io_todo() {
    bash_args \
        --args_p '
            force:option de forçage du traitement;
            type:code type IO;
            date_end:date de fin des données (format connu PostgreSQL);
            id:variable pour récupérer ID de IO
        ' \
        --args_o '
            type;
            date_end
        ' \
        --args_v '
            force:no|yes
        ' \
        --args_d '
            force:no
        ' \
        "$@" || return $POW_IO_ERROR

    [ -n "$get_arg_id" ] && local -n _io_id=$get_arg_id || local _io_id

    [ "$get_arg_force" = no ] && {
        io_exists \
            --type $get_arg_type \
            --date_end "${get_arg_date_end}" \
            --status SUCCES \
            --id _io_id
    } && {
        log_info "Le traitement $get_arg_type a déjà été réalisé avec succès"
        return $POW_IO_SUCCESSFUL
    }

    {
        io_exists \
            --type $get_arg_type \
            --date_end "${get_arg_date_end}" \
            --status EN_COURS \
            --id _io_id
    } && {
        log_info "Le traitement $get_arg_type est déjà en cours"
        return $POW_IO_IN_PROGRESS
    }
    [ "$get_arg_force" = yes ] && {
        # purge previous history
        execute_query \
            --name "DELETE_IO_${get_arg_type}" \
            --query "DELETE FROM io_history WHERE co_type = '${get_arg_type}'" || return $POW_IO_ERROR
    }

    return $POW_IO_TODO
}

io_begin() {
    bash_args \
        --args_p '
            type:code type IO;
            date_begin:date de début des données (format connu PostgreSQL);
            date_end:date de fin des données (format connu PostgreSQL);
            nrows_todo:nombre de données à traiter;
            infos:compléments infos (souvent au format JSON);
            id:nom de la variable pour récupérer identifiant IO
        ' \
        --args_o '
            type;
            date_begin;
            date_end;
            nrows_todo;
            id
        ' \
        "$@" || return $ERROR_CODE

    local -n _io_id=$get_arg_id

    _io_manager \
        --method APPEND \
        --type $get_arg_type \
        --status EN_COURS \
        --date_begin "$get_arg_date_begin" \
        --date_end "$get_arg_date_end" \
        --nrows_todo $get_arg_nrows_todo \
        --infos "$get_arg_infos" \
        --id _io_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

io_end_ok() {
    bash_args \
        --args_p '
            nrows_processed:nombre de données traitées;
            infos:compléments infos (souvent au format JSON);
            id:identifiant IO
        ' \
        --args_o '
            nrows_processed;
            id
        ' \
        "$@" || return $ERROR_CODE

    _io_manager \
        --method UPDATE_OK \
        --nrows_processed $get_arg_nrows_processed \
        --infos "$get_arg_infos" \
        --id $get_arg_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

io_end_ko() {
    bash_args \
        --args_p '
            id:identifiant IO
        ' \
        --args_o '
            id
        ' \
        "$@" || return $ERROR_CODE

    _io_manager \
        --method UPDATE_KO \
        --id $get_arg_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

io_export_last() {
    bash_args \
        --args_p '
            type:code type IO;
            output:sortie pour export
        ' \
        --args_o '
            type;
        ' \
        "$@" || return $ERROR_CODE

    local -n _io_id=$get_arg_id

    _io_manager \
        --method EXPORT_LAST \
        --type $get_arg_type \
        --output "$get_arg_output" || return $ERROR_CODE

    return $SUCCESS_CODE
}

    #
    # TODO IO imports (CSV, EXCEL, ...)
    #
