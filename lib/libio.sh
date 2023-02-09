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
        local -n _io_id_manager=$get_arg_id
        _return='--return _io_id_manager'
        _query="
            SELECT id FROM get_all_io(
                type_in => '$get_arg_type'
                , date_end => '$get_arg_date_end'::TIMESTAMP
                , status_in => '$get_arg_status'
            )
        "
        ;;
    APPEND)
        local -n _io_id_manager=$get_arg_id
        _return='--return _io_id_manager'
        local _infos
        [ -z "$get_arg_infos" ] && _infos='NULL' || _infos="'${get_arg_infos}'"
        _query="
            INSERT INTO public.io_history(
                co_type
                , dt_data_begin
                , dt_data_end
                , co_status
                , nb_rows_todo
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
        [ -z "$get_arg_infos" ] && _infos='infos_data' || _infos="'${get_arg_infos}'"
        _query="
            UPDATE public.io_history SET
                dt_exec_end = NOW()
                , co_status = 'SUCCES'
                , nb_rows_processed = $get_arg_nrows_processed
                , infos_data = $_infos
            WHERE id = $get_arg_id
        "
        ;;
    UPDATE_KO)
        _query="
            UPDATE public.io_history SET
                dt_exec_end = NOW()
                , co_status = 'ERREUR'
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
                    , dt_exec_begin
                    , dt_exec_end
                    , dt_data_begin
                    , dt_data_end
                    , co_status
                    , nb_rows_todo
                    , nb_rows_processed
                    , co_status_integration
                    , infos_data
                FROM
                    public.io_history
                WHERE
                    co_type ~ '$get_arg_type'
                    AND
                    co_status = 'SUCCES'
                ORDER BY
                    dt_exec_end DESC
                LIMIT
                    1
            ) TO STDOUT WITH (DELIMITER ';', FORMAT CSV, HEADER TRUE, ENCODING UTF8)
        "
        ;;
    *)
        log_error "Méthode '$get_arg_method' non implémentée!"
        return $ERROR_CODE
        ;;
    esac

    execute_query \
        --name IO_${get_arg_method}_${get_arg_type} \
        --query "$_query" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
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
            date_end:date de fin des données (format connu PostgreSQL);
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
        --date_end "$get_arg_date_end" \
        --id _io_id || return $ERROR_CODE

    [ -z "$_io_id" ] && return $ERROR_CODE
    return $SUCCESS_CODE
}

POW_IO_SUCCESSFUL=10
POW_IO_IN_PROGRESS=11
POW_IO_TODO=12
POW_IO_ERROR=13

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

    [ -n "$get_arg_id" ] && local -n _io_id_todo=$get_arg_id || local _io_id_todo

    [ "$get_arg_force" = no ] && {
        io_exists \
            --type $get_arg_type \
            --date_end "${get_arg_date_end}" \
            --status SUCCES \
            --id _io_id_todo
    } && {
        log_info "Le traitement $get_arg_type a déjà été réalisé avec succès"
        return $POW_IO_SUCCESSFUL
    }

    {
        io_exists \
            --type $get_arg_type \
            --date_end "${get_arg_date_end}" \
            --status EN_COURS \
            --id _io_id_todo
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

#
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

    _io_manager \
        --method EXPORT_LAST \
        --type $get_arg_type \
        --output "$get_arg_output" || return $ERROR_CODE

    return $SUCCESS_CODE
}

    #
    # manage transfer
    #

# get available dates (list, details as URL)
io_get_list_online_available() {
    bash_args \
        --args_p '
            type_import:Produit en ligne recherché;
            details_file:Détail des millésimes disponibles;
            dates_list:Dates des millésimes disponibles
        ' \
        --args_o '
            type_import;
            details_file;
            dates_list
        ' \
        "$@" || return $ERROR_CODE

    local _url _re1 _re2 _only_matching_re1=--only-matching _i
    local -n _details_file_ref=$get_arg_details_file
    local -n _dates_ref=$get_arg_dates_list

    case $get_arg_type_import in
    IGN_ADMINEXPRESS)
        _url='https://geoservices.ign.fr/adminexpress'
        _re1='href="(http|ftp)[^"]+ADMIN-EXPRESS_(?(?!WM)[^"])+[0-9-]{10}\.7z[^"]*'
        _re2='[0-9-]{10}'
        ;;
    IGN_CONTOURS_IRIS)
        _url='https://geoservices.ign.fr/contoursiris#telechargement'
        _re1='href="(http|ftp)[^" ]+CONTOURS-IRIS[^" ]*(FXX|FRA)[^" ]*\.7z[^" ]*"'
        _re2='[0-9]{4}-01-01'
        ;;
    BANATIC_EPCI)
        _url='https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/fichiers-telech.php'
        _re1='Données mises à jour le :'
        _re2='[0-9]{2}/[0-9]{2}/[0-9]{4}'
        _only_matching_re1=
        ;;
    INSEE_DECOUPAGE_COMMUNAL)
        _url='https://www.insee.fr/fr/information/2028028'
        _re1='table-appartenance-geo-communes-[0-9]{2}[^.]*\.zip'
        _re2='[0-9]{2}'
        ;;
    *)
        log_error "produit $get_arg_type_import non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    # temporary file (to be deleted by caller)
    get_tmp_file --tmpext html --tmpfile _details_file_ref &&
    # download available dates
    io_download_file \
        --name $get_arg_type_import \
        --url "$_url" \
        --output_directory "$POW_DIR_TMP" \
        --output_file "$(basename $_details_file_ref)" \
        --overwrite yes &&
    # array of available dates (desc), transforming / to -
    _dates_ref=($(grep $_only_matching_re1 --perl-regexp "$_re1" $_details_file_ref | grep --only-matching --perl-regexp "$_re2" | sed -e 's@/@-@g' | uniq | sort --reverse)) || {
        log_error "Impossible de consulter la liste des millésimes disponibles de $get_arg_type_import"
        return $ERROR_CODE
    }

    # date need to be compatible w/ BASH date
    for ((_i=0; _i<${#_dates_ref[@]}; _i++)); do
        [[ ${_dates[$_i]} =~ [0-9]{4}-[0-9]{2}-[0-9]{2} ]] && continue

        # tranform DD-MM-YYYY to YYYY-MM-DD
        [[ ${_dates[$_i]} =~ ([0-9]{2})-([0-9]{2})-([0-9]{4}) ]] && {
            _dates[$_i]="${BASH_REMATCH[3]}-${BASH_REMATCH[2]}-${BASH_REMATCH[1]}"
            continue
        }

        # transform YY to CCYY-01-01
        [[ ${_dates[$_i]} =~ ([0-9]{2}) ]] && {
            _dates[$_i]="$(date '+%C')${BASH_REMATCH[1]}-01-01"
            continue
        }
    done

    return $SUCCESS_CODE
}

io_download_file() {
    bash_args \
        --args_p '
            name:nommage du fichier à télécharger;
            url:URL à télécharger;
            output_directory:dossier de destination;
            output_file:fichier de destination;
            overwrite:avec/sans écrasement;
            user:compte HTTP;
            password:mot de passe HTTP;
            use_proxy:utilisation proxy
        ' \
        --args_o '
            url;
            output_directory
        ' \
        --args_v '
            overwrite:no|yes;
            use_proxy:no|yes
        ' \
        --args_d '
            overwrite:no;
            use_proxy:no
        ' \
        "$@" || return $ERROR_CODE

    local _download_url="$get_arg_url"
    local _download_directory="$get_arg_output_directory"
    local _download_file="$get_arg_output_file"
    local _download_overwrite=$get_arg_overwrite
    [ -z "$_download_file" ] && _download_file=$(basename "$_download_url")
    local _download_name=${get_arg_name:-"$_download_file"}

    [ "$_download_overwrite" = no ] && {
        # already present
        [ -f "$_download_directory/$_download_file" ] && {
            log_info "Téléchargement de ${_download_name} inutile, car déjà présent dans ${_download_directory}"
            return $SUCCESS_CODE
        }

        # available into COMMON and import as target
        [ -f "$POW_DIR_COMMON_GLOBAL_SCHEMA/$_download_file" ] &&
        [[ "${_download_directory}" =~ ^"${POW_DIR_IMPORT}"/*$ ]] && {
            cp "$POW_DIR_COMMON_GLOBAL_SCHEMA/$_download_file" "$POW_DIR_IMPORT"
            log_info "Téléchargement de "$_download_name" inutile, car déjà présent dans le dossier POW_DIR_COMMON_GLOBAL_SCHEMA, copié dans import"
            return $SUCCESS_CODE
        }
    }

    log_info "Téléchargement de $_download_name"
    local _log_tmp_path="$POW_DIR_TMP/$_download_file.log"
    local _log_archive_path="$POW_DIR_ARCHIVE/$_download_file.log"
    local _cache_path _cache_dir
    # user/password
    local _user _password
    [ -n "$get_arg_user" ] && _user="--user $get_arg_user"
    [ -n "$get_arg_password" ] && _password="--password $get_arg_password"
    # temporary downloaded file
    local _download_file_tmp
    get_tmp_file --tmpfile _download_file_tmp

    wget \
        $_download_url \
        --output-document "$_download_file_tmp" \
        --no-check-certificate \
        --progress=dot:mega \
        --retry-on-http-error=503 \
        --wait=10 \
        --random-wait \
        $_user \
        $_password \
        > $_log_tmp_path 2>&1 || {

        archive_file "$_log_tmp_path"
        log_error "Erreur lors du téléchargement de $_download_name, veuillez consulter $_log_archive_path"
        [ -f "$_download_file_tmp" ] && rm --force "$_download_file_tmp"
        # use of previous file if present
        [ -f "$_download_directory/$_download_file" ] && {
            log_info "Utilisation du fichier déjà présent pour contourner l'erreur de téléchargement"
            return $SUCCESS_CODE
        }
        # available in cache?
        _cache_path=$(echo "${POW_DIR_COMMON_GLOBAL}/public/cache/${_download_url//[:#]/_}" | sed 's|/$||')
        [ -f "$_cache_path" ] && {
            log_info "Utilisation du fichier déjà présent en cache pour contourner l'erreur de téléchargement"
            cp "$_cache_path" "$_download_directory/$_download_file"
            return $SUCCESS_CODE
        }

        return $ERROR_CODE
    }

    if [ "$_download_overwrite" = no ]; then
        # not available into COMMON and import as target
        [ ! -f "$POW_DIR_COMMON_GLOBAL_SCHEMA/$_download_file" ] &&
        [[ "${_download_directory}" =~ ^"${POW_DIR_IMPORT}"/*$ ]] && {
            log_info "Copie de ${_download_name} sur le COMMON"
            cp "$_download_file_tmp" "${POW_DIR_COMMON_GLOBAL_SCHEMA}/${_download_file}"
        }
    else
        _cache_path=$(echo "${POW_DIR_COMMON_GLOBAL}/public/cache/${_download_url//[:#]/_}" | sed 's|/$||')
        _cache_dir=$(dirname "$_cache_path")
        mkdir -p "$_cache_dir"
        cp "$_download_file_tmp" "${_cache_path}"
    fi

    # result
    mv "$_download_file_tmp" "$_download_directory/$_download_file"
    archive_file "$_log_tmp_path"
    log_info "Téléchargement avec succès de $_download_name"

    return $SUCCESS_CODE
}

    #
    # TODO IO imports (CSV, EXCEL, ...)
    #
