    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define IO

    #
    # IO history
    #

    # big crunch! w/ bash_args : get_arg_<var> can be modified by another call
    # here, get_arg_name would be IO name, but also QUERY name
    #  error on io_todo_import if exists EN_COURS status
    #  because of get_arg_name containing IO_EXISTS_(get_arg_name)
_io_history_manager() {
    bash_args \
        --args_p '
            method:méthode de mise à jour;
            status:état IO;
            io:nom IO;
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
    [ "$POW_DEBUG" = yes ] && _with_log=yes

    case $get_arg_method in
    EXISTS)
        local -n _io_id_manager=$get_arg_id
        _return='--return _io_id_manager'
        _query="
            SELECT id FROM get_io(
                name => '$get_arg_io',
                status => '$get_arg_status',
                date_end => '$get_arg_date_end'::TIMESTAMP
            )
        "
        ;;
    APPEND)
        local -n _io_id_manager=$get_arg_id
        _return='--return _io_id_manager'
        _query="
            INSERT INTO public.io_history(
                name,
                date_data_begin,
                date_data_end,
                status,
                nb_rows_todo,
                attributes
            )
            VALUES (
                '$get_arg_io',
                '$get_arg_date_begin'::TIMESTAMP,
                '$get_arg_date_end'::TIMESTAMP,
                '${get_arg_status:-EN_COURS}',
                ${get_arg_nrows_todo:-NULL},
                CASE
                    WHEN LENGTH('$get_arg_infos') = 0 THEN NULL
                    ELSE '$get_arg_infos'
                END
            );
            SELECT CURRVAL('public.io_history_id_seq');
        "
        ;;
    UPDATE_OK)
        # itself (if no defined) to remain previous value
        _query="
            UPDATE public.io_history SET
                date_exec_end = NOW(),
                status = 'SUCCES',
                nb_rows_processed = ${get_arg_nrows_processed:-NULL},
                attributes =
                    CASE
                        WHEN LENGTH('$get_arg_infos') = 0 THEN attributes
                        ELSE '$get_arg_infos'
                    END
            WHERE id = $get_arg_id
        "
        ;;
    UPDATE_KO)
        _query="
            UPDATE public.io_history SET
                date_exec_end = NOW(),
                status = 'ERREUR'
            WHERE id = $get_arg_id
        "
        ;;
    EXPORT_LAST)
        _with_log=yes
        _output="--output $get_arg_output"
        _query="
            COPY (
                SELECT
                    name,
                    date_exec_begin,
                    date_exec_end,
                    status,
                    date_data_begin,
                    date_data_end,
                    nb_rows_todo,
                    nb_rows_processed,
                    attributes
                FROM
                    public.io_history
                WHERE
                    name ~ '$get_arg_io'
                    AND
                    status = 'SUCCES'
                ORDER BY
                    date_exec_end DESC
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
        --name IO_${get_arg_method}_${get_arg_name:-$get_arg_id} \
        --query "$_query" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        $_return \
        $_output \
        --with_log $_with_log || return $ERROR_CODE

    [ -n "$_return" ] &&
    [ "$POW_DEBUG" = yes ] && { echo io_id=$_io_id_manager; }

    return $SUCCESS_CODE
}

# IO in progress
# io_history_exists --status EN_COURS
# IO already success
# io_history_exists --status SUCCES
io_history_exists() {
    bash_args \
        --args_p '
            io:nom IO;
            date_end:date de fin des données (format connu PostgreSQL);
            status:état IO;
            id:variable pour récupérer ID de IO
        ' \
        --args_o '
            io;
            date
        ' \
        --args_v '
            status:EN_COURS|SUCCES|ERREUR
        ' \
        --args_d '
            status:SUCCES
        ' \
        "$@" || return $ERROR_CODE

    [ -n "$get_arg_id" ] && local -n _io_id=$get_arg_id || local _io_id

    _io_history_manager \
        --method EXISTS \
        --status $get_arg_status \
        --io $get_arg_io \
        --date_end "$get_arg_date_end" \
        --id _io_id || return $ERROR_CODE

    [ -z "$_io_id" ] && return $ERROR_CODE
    return $SUCCESS_CODE
}

POW_IO_SUCCESSFUL=10
POW_IO_IN_PROGRESS=11
POW_IO_TODO=12
POW_IO_ERROR=13

# IO import is todo?
io_todo_import() {
    bash_args \
        --args_p '
            force:option de forçage du traitement;
            io:nom IO;
            date_end:date de fin des données (format connu PostgreSQL);
            purge:purge historique précédent;
            id:variable pour récupérer ID de IO
        ' \
        --args_o '
            io;
            date_end
        ' \
        --args_v '
            force:no|yes;
            purge:no|yes
        ' \
        --args_d '
            force:no;
            purge:no
        ' \
        "$@" || return $POW_IO_ERROR

    [ -n "$get_arg_id" ] && local -n _io_id_todo=$get_arg_id || local _io_id_todo

    [ "$get_arg_force" = no ] && {
        io_history_exists \
            --io $get_arg_io \
            --date_end "${get_arg_date_end}" \
            --status SUCCES \
            --id _io_id_todo
    } && {
        log_info "Le traitement $get_arg_io a déjà été réalisé avec succès"
        return $POW_IO_SUCCESSFUL
    }

    {
        io_history_exists \
            --io $get_arg_io \
            --date_end "${get_arg_date_end}" \
            --status EN_COURS \
            --id _io_id_todo
    } && {
        log_info "Le traitement $get_arg_io est déjà en cours"
        return $POW_IO_IN_PROGRESS
    }
    [ "$get_arg_purge" = yes ] && {
        # purge previous history
        execute_query \
            --name "DELETE_IO_${get_arg_io}" \
            --query "DELETE FROM io_history WHERE name = '${get_arg_io}'" || return $POW_IO_ERROR
    }

    return $POW_IO_TODO
}

# get description of IO integration (HSTORE converted to associative array)
io_get_info_integration() {
    bash_args \
        --args_p '
            io:nom IO;
            to_hash:variable pour récupérer la description de cette intégration;
            to_string:variable pour récupérer la description de cette intégration
        ' \
        --args_o '
            io;
            to_hash
        ' \
        "$@" || return $POW_IO_ERROR

    local -n _hash_ref=$get_arg_to_hash
    [ -n "$get_arg_to_string" ] && local -n _str_ref=$get_arg_to_string
    local _tmpfile
    get_tmp_file --tmpfile _tmpfile &&
    execute_query \
        --name IO_IS_TODO \
        --query "SELECT io_is_todo('$get_arg_io')" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --output $_tmpfile || return $ERROR_CODE
    [ "$POW_DEBUG" = yes ] && cat $_tmpfile
    # each row contains: key=>value
    _hash_ref=()
    while read; do
        _hash_ref[${REPLY%=*}]=${REPLY#*>}
    done < <(sed --expression 's/"//g' --expression 's/,/\n/g' < $_tmpfile | sed --expression 's/^[ ]*//')
    [ -n "$get_arg_to_string" ] && _str_ref=$(< $_tmpfile)
    rm $_tmpfile
    return $SUCCESS_CODE
}

# build list of IDs as string (coded as JSON) for IO history of IO (as DEPENDS) or given by name
io_get_ids_integration() {
    bash_args \
        --args_p '
            from:méthode accès aux IDs de la dépendance à traiter;
            hash:tableau associatif de description de cette intégration;
            array:tableau des IDs de cette intégration;
            ids:variable pour récupérer les IDs;
            name:nom de la dépendance à traiter
        ' \
        --args_o '
            from;
            hash;
            ids
        ' \
        --args_v '
            from:HASH|ARRAY
        ' \
        --args_d '
            from:HASH
        ' \
        "$@" || return $POW_IO_ERROR

    # https://stackoverflow.com/questions/13219634/easiest-way-to-check-for-an-index-or-a-key-in-an-array
    # https://stackoverflow.com/questions/11180714/how-to-iterate-over-an-array-using-indirect-reference

    local -n _hash_ref=$get_arg_hash
    local -n _ids_ref=$get_arg_ids
    local _depends _steps _step _array_ptr _i _key _value

    [ -n "${get_arg_name}" ] && _depends=${get_arg_name}_d || _depends=DEPENDS

    case "$get_arg_from" in
    HASH)
        [[ -v "_hash_ref[$_depends]" ]] || {
            log_error "manque dépendances IO=($get_arg_name)"
            return $ERROR_CODE
        }

        _steps=(${_hash_ref[$_depends]//:/ })
        _array_ptr="_steps[@]"
        ;;
    ARRAY)
        [ -z "$get_arg_array" ] && {
            log_error "manque tableau IDs (option --array)"
            return $ERROR_CODE
        }
        local -n _array_ref=$get_arg_array
        _steps=(${_hash_ref[$_depends]//:/ })
        _array_ptr="_array_ref[@]"
        ;;
    esac

    _ids_ref=''
    _i=0
    for _step in "${!_array_ptr}"; do
        case "$get_arg_from" in
        HASH)
            [[ -v "_hash_ref[${_step}_i]" ]] || {
                log_error "manque ID IO=$_step"
                return $ERROR_CODE
            }
            _key=$_step
            _value=${_hash_ref[${_key}_i]}
            ;;
        ARRAY)
            _key=${_steps[$_i]}
            _value=$_step
            ;;
        esac

        _i=$((_i +1))
        # IO condition ?
        [ $_value -eq 0 ] && continue
        [ -n "$_ids_ref" ] && _ids_ref+=,
        _ids_ref+=$(printf '"%s":%d' $_key $_value)
    done
    [ -n "$_ids_ref" ] && _ids_ref="{${_ids_ref}}"

    return $SUCCESS_CODE
}

io_history_begin() {
    bash_args \
        --args_p '
            io:nom IO;
            date_begin:date de début des données (format connu PostgreSQL);
            date_end:date de fin des données (format connu PostgreSQL);
            nrows_todo:nombre de données à traiter;
            infos:compléments infos (souvent au format JSON);
            id:nom de la variable pour récupérer identifiant IO
        ' \
        --args_o '
            io;
            date_begin;
            date_end;
            nrows_todo;
            id
        ' \
        "$@" || return $ERROR_CODE

    local -n _io_id=$get_arg_id

    _io_history_manager \
        --method APPEND \
        --io $get_arg_io \
        --status EN_COURS \
        --date_begin "$get_arg_date_begin" \
        --date_end "$get_arg_date_end" \
        --nrows_todo $get_arg_nrows_todo \
        --infos "$get_arg_infos" \
        --id _io_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

io_history_end_ok() {
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

    _io_history_manager \
        --method UPDATE_OK \
        --nrows_processed "$get_arg_nrows_processed" \
        --infos "$get_arg_infos" \
        --id $get_arg_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

io_history_end_ko() {
    bash_args \
        --args_p '
            id:identifiant IO
        ' \
        --args_o '
            id
        ' \
        "$@" || return $ERROR_CODE

    _io_history_manager \
        --method UPDATE_KO \
        --id $get_arg_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

io_history_export_last() {
    bash_args \
        --args_p '
            io:nom IO;
            output:sortie pour export
        ' \
        --args_o '
            io
        ' \
        "$@" || return $ERROR_CODE

    _io_history_manager \
        --method EXPORT_LAST \
        --io $get_arg_io \
        --output "$get_arg_output" || return $ERROR_CODE

    return $SUCCESS_CODE
}

    #
    # transfer
    #

# get property
io_get_property_online_available() {
    bash_args \
        --args_p '
            name:nom IO à rechercher (en ligne);
            key:propriété recherchée;
            value:valeur
        ' \
        --args_o '
            name;
            key;
            value
        ' \
        "$@" || return $ERROR_CODE

    local _url_base _url_data _re1 _re2
    local -n _value_ref=$get_arg_value

    case $get_arg_name in
    FR-TERRITORY-IGN)
        _url_base='https://geoservices.ign.fr'
        _url_data=${_url_base}'/adminexpress'
        _re1='href="(http|ftp)[^"]+ADMIN-EXPRESS_(?(?!WM)[^"])+[0-9-]{10}\.7z[^"]*'
        _re2='[0-9-]{10}'
        ;;
    FR-TERRITORY-IGN-IRIS)
        _url_base='https://geoservices.ign.fr'
        _url_data=${_url_base}'/contoursiris#telechargement'
        _re1='href="(http|ftp)[^" ]+CONTOURS-IRIS[^" ]*(FXX|FRA)[^" ]*\.7z[^" ]*"'
        _re2='[0-9]{4}-01-01'
        ;;
    FR-TERRITORY-BANATIC)
        _url_base='https://www.collectivites-locales.gouv.fr'
        _url_data=${_url_base}'/institutions/liste-et-composition-des-epci-fiscalite-propre'
        _re1='^[ ]+[0-9]{4}[ ]*'
        _re2='[0-9]{4}'
        ;;
    FR-TERRITORY-INSEE)
        _url_base='https://www.insee.fr'
        _url_data=${_url_base}'/fr/information/7671844'
        _re1='table-appartenance-geo-communes-[0-9]{2}[^.]*\.zip'
        _re2='[0-9]{2}'
        ;;
    FR-MUNICIPALITY-EVENT-INSEE)
        _url_base='https://www.insee.fr'
        _url_data=${_url_base}'/fr/information/7766585'
        _re1='v_mvt_commune_[0-9]{4}.csv'
        _re2='[0-9]{4}'
        ;;
    *)
        log_error "IO $get_arg_name non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    case ${get_arg_key^^} in
    URL_BASE)       _value_ref=$_url_base       ;;
    URL_DATA)       _value_ref=$_url_data       ;;
    REGEXP1)        _value_ref=$_re1            ;;
    REGEXP2)        _value_ref=$_re2            ;;
    *)
        log_error "KEY $get_arg_key non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    return $SUCCESS_CODE
}

# get available dates (list, details as URL)
io_get_list_online_available() {
    bash_args \
        --args_p '
            name:nom IO à rechercher (en ligne);
            details_file:Détail des millésimes disponibles;
            dates_list:Dates des millésimes disponibles
        ' \
        --args_o '
            name;
            details_file;
            dates_list
        ' \
        "$@" || return $ERROR_CODE

    # NOTE _only_matching_re1=--only-matching
    # reset it if not only matching
    # no more used (previously needed for BANATIC)

    local _url _regexp1 _regexp2 _i
    local -n _details_file_ref=$get_arg_details_file
    local -n _dates_ref=$get_arg_dates_list

    io_get_property_online_available    \
        --name $get_arg_name            \
        --key URL_DATA                  \
        --value _url                    &&
    io_get_property_online_available    \
        --name $get_arg_name            \
        --key REGEXP1                   \
        --value _regexp1                &&
    io_get_property_online_available    \
        --name $get_arg_name            \
        --key REGEXP2                   \
        --value _regexp2                || {
        log_error "IO $get_arg_name récupération propriété!"
        return $ERROR_CODE
    }

    # temporary file (to be deleted by caller)
    get_tmp_file --tmpext html --tmpfile _details_file_ref &&
    # download available dates
    io_download_file \
        --name $get_arg_name \
        --url "$_url" \
        --output_directory "$POW_DIR_TMP" \
        --output_file "$(basename $_details_file_ref)" \
        --overwrite yes &&
    # array of available dates (desc), transforming / to -
    _dates_ref=($(grep --only-matching --perl-regexp "$_regexp1" $_details_file_ref | grep --only-matching --perl-regexp "$_regexp2" | sed --expression 's@/@-@g' | uniq | sort --reverse)) || {
        log_error "Impossible de consulter la liste des millésimes disponibles de $get_arg_name"
        return $ERROR_CODE
    }

    # date need to be compatible w/ BASH date
    for ((_i=0; _i<${#_dates_ref[@]}; _i++)); do
        [[ ${_dates_ref[$_i]} =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && continue

        # transform DD-MM-YYYY to YYYY-MM-DD
        [[ ${_dates_ref[$_i]} =~ ^([0-9]{2})-([0-9]{2})-([0-9]{4})$ ]] && {
            _dates_ref[$_i]="${BASH_REMATCH[3]}-${BASH_REMATCH[2]}-${BASH_REMATCH[1]}"
            continue
        }

        # transform YY to CCYY-01-01
        [[ ${_dates_ref[$_i]} =~ ^([0-9]{2})$ ]] && {
            _dates_ref[$_i]="$(date '+%C')${BASH_REMATCH[1]}-01-01"
            continue
        }

        # transform YYYY to YYYY-01-01
        [[ ${_dates_ref[$_i]} =~ ^([0-9]{4})$ ]] && {
            _dates_ref[$_i]="${BASH_REMATCH[1]}-01-01"
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
    # ETL
    #

# BOM: byte-order mark
# https://learn.microsoft.com/fr-fr/globalization/encoding/byte-order-mark
remove_bom() {
    bash_args \
        --args_p 'file_path:Chemin absolu vers le fichier à traiter;' \
        --args_o 'file_path' \
        "$@" || return $ERROR_CODE

    sed --in-place --expression '1s/^\xEF\xBB\xBF//' --expression '1s/^\xFF\xFE//' $get_arg_file_path
}

# import CSV into DB
import_csv_file() {
    bash_args --args_p '
        file_path:Chemin absolu vers le fichier à traiter;
        file_with_header:Fichier avec ou sans entête;
        schema_name:Nom du schema cible;
        table_name:Nom de la table cible;
        table_columns:Colonnes de la table cible;
        table_columns_list:Liste des colonnes de la table cible;
        load_mode:Mode de chargement des données;
        delimiter:Séparateur de valeurs;
        encoding:Encodage de caractères;
        limit:Limiter à n enregistrements;
        rowid:Générer un identifiant unique rowid;
        from_line_number:Numéro de ligne à partir de laquelle il faut lire les fichier;
        to_line_number:Numéro de ligne jusqu à laquelle il faut lire le fichier' \
    --args_o 'file_path' \
    --args_v '
        table_columns:HEADER|HEADER_TO_LOWER_CODE|LIST;
        load_mode:OVERWRITE_DATA|OVERWRITE_TABLE|APPEND;
        delimiter:AUTODETECT|'${POW_DELIMITER_JOIN_PIPE}';
        file_with_header:yes|no;
        encoding:UTF8|UTF16|WIN1252|LATIN1;
        rowid:yes|no' \
    --args_d '
        schema_name:'${POW_PG_DEFAULT_SCHEMA}';
        table_columns:HEADER;
        delimiter:AUTODETECT;
        file_with_header:yes;
        encoding:UTF8;
        load_mode:OVERWRITE_DATA;
        rowid:yes' \
    "$@" || return $ERROR_CODE

    expect file "$get_arg_file_path" || exit $ERROR_CODE

    local file_path="$get_arg_file_path"
    local file_with_header=$get_arg_file_with_header
    local file_name=$(get_file_name --file_path "$file_path")
    local file_extension=$(get_file_extension --file_path "$file_path")
    local file_to_tmp=no
    local schema_name=$get_arg_schema_name
    local table_name=$get_arg_table_name
    local table_columns=$get_arg_table_columns
    local table_columns_list="$get_arg_table_columns_list"
    local load_mode=$get_arg_load_mode
    local rowid=$get_arg_rowid
    local from_line_number=$get_arg_from_line_number
    local to_line_number=$get_arg_to_line_number
    local encoding=$get_arg_encoding
    local limit=$get_arg_limit

    [ "$POW_DEBUG" = yes ] && {
        echo "from_line_number=$from_line_number"
        echo "to_line_number=$to_line_number"
    }

    # only part of data?
    if [ -n "$from_line_number" ] || [ -n "$to_line_number" ]; then
        file_name="${file_name}.filtered"
        local new_file_path="$POW_DIR_TMP/$file_name.$file_extension"
        if [ -n "$from_line_number" ] && [ -n "$to_line_number" ]; then
            to_line_number=$(($to_line_number - $from_line_number + 1))
            tail --lines=+$from_line_number "${file_path}" | head -$to_line_number > "$new_file_path"
        elif [ -n "$from_line_number" ]; then
            tail --lines=+$from_line_number "${file_path}" > "$new_file_path"
        elif [ -n "$to_line_number" ]; then
            head --lines $to_line_number "${file_path}" > "$new_file_path"
        fi
        file_to_tmp=yes
        file_path="$new_file_path"
    fi

    local delimiter_code=$get_arg_delimiter
    local delimiter_value=
    if [ "$delimiter_code" = AUTODETECT ]; then
        # FIXME put first line into variable transforms TAB in SPACE
        #local _first_line=$(head --lines 1 "$file_path")

        # https://stackoverflow.com/questions/10806357/associative-arrays-are-local-by-default
        declare -A _tokens
        local _code
        for _code in ${!POW_DELIMITER[@]}; do
            # count number of tokens for each delimiter (into first line)
            # https://unix.stackexchange.com/questions/18736/how-to-count-the-number-of-a-specific-character-in-each-line
            _tokens[$_code]=$(head --lines 1 "$file_path" \
                | tr --delete --complement "${POW_DELIMITER[$_code]}\n" \
                | awk '{ print length }'
            )
            [ "$POW_DEBUG" = yes ] && echo "_tokens[$_code]=${_tokens[$_code]}"
        done

        local _ntokens=0
        for _code in ${!POW_DELIMITER[@]}; do
            [ ${_tokens[$_code]} -gt $_ntokens ] && {
                _ntokens=${_tokens[$_code]}
                delimiter_value=${POW_DELIMITER[$_code]}
            }
        done
    else
        set_delimiter --delimiter_code $delimiter_code --delimiter_value delimiter_value
    fi
    [ ${#delimiter_value} -eq 0 ] && {
        log_error "Non détection du séparateur CSV"
        return $ERROR_CODE
    }
    [ "$POW_DEBUG" = yes ] && echo "delimiter_value=[$delimiter_value]"

    if [ -z "$table_name" ]; then
        execute_query \
            --name LABEL_TO_CODE \
            --query "SELECT public.label_to_code('$file_name')" \
            --psql_arguments 'tuples-only:pset=format=unaligned' \
            --with_log no \
            --return table_name || return $ERROR_CODE
    fi
    [ "$POW_DEBUG" = yes ] && echo "table_name=$table_name"

    # encoding
    file --mime "$file_path" | grep --silent 'charset=iso-8859-1' && encoding=LATIN1
    file --mime "$file_path" | grep --silent 'charset=utf-16le' && encoding=UTF16
    # PostgreSQL doesn't stand up UTF16
    if [ $encoding = UTF16 ]; then
        file_name="$file_name.to_utf8"
        local new_file_path="$POW_DIR_TMP/$file_name.$file_extension"
        iconv --from-code UTF16 --to-code UTF8 "$file_path" > "$new_file_path"
        [ "$file_to_tmp" = yes ] && rm "$file_path"
        file_to_tmp=yes
        file_path="$new_file_path"
        encoding=UTF8
    fi

    local table_columns_create=
    if [ "$file_with_header" = yes ]; then
        if [[ $table_columns =~ HEADER|HEADER_TO_LOWER_CODE ]]; then
                # convert to local encoding (UTF8)
                # remove BOM
                # remove CR (Windows)
                # search for (into 1st line)
                #  - [^'$delimiter_value'"]": ending by double quote, not preceding by (delimiter or ")
                #  - '$delimiter_value'[^"'$delimiter_value']*: ending by delimiter, following all except (delimiter or ")
                #  - '$delimiter_value'"[^"'$delimiter_value']+": ending by (delimiter and "), following all except (delimiter or ")
            local _line_end_header=$(cat "$file_path" \
                | iconv --from-code $encoding \
                | sed --expression 's/^\xEF\xBB\xBF//' \
                | sed --expression 's/\r//g' \
                | grep --max-count 1 --line-number --perl-regexp '([^"'$delimiter_value']"|'$delimiter_value'[^"'$delimiter_value']*|'$delimiter_value'"[^"'$delimiter_value']+")$' \
                | cut --fields 1 --delimiter : \
            )
            [ -z "$_line_end_header" ] && _line_end_header=1

            table_columns_list=$(head --lines $_line_end_header "$file_path" \
                | iconv --from-code $encoding \
                | sed --expression 's/^\xEF\xBB\xBF//' \
                | sed --expression 's/\r//g' \
            )

            if [ "$table_columns" = HEADER_TO_LOWER_CODE ]; then
                    # to lower
                    # w/o accent
                    # replace no-alphanum by _ (except delimiter)
                    # replace delimiter by ,
                    # trim _ (begin or end)
                table_columns_list=$(echo "$table_columns_list" \
                    | tr '[:upper:]' '[:lower:]' \
                    | sed 'y/àáâãäåçêéèëìíîïìñòóôõöùúûüýÿ/aaaaaaceeeeiiiiinooooouuuuyy/' \
                    | tr 'œ' 'oe' \
                    | tr 'æ' 'ae' \
                    | sed "s/[^a-z0-9${delimiter_value}]\+/_/g" \
                    | sed "s/_\?${delimiter_value}_\?/,/g" \
                    | sed 's/^_\?//g' \
                    | sed 's/_\?$//g' \
                )
            else
                    # replace delimiter by "," (so surround each colmun by ")
                    # add " at begin
                    # add " at end
                table_columns_list=$(echo "$table_columns_list" \
                    | sed --expression "s/\"\?${delimiter_value}\"\?/\",\"/g" \
                    | sed --expression 's/^"\?/"/g' \
                    | sed --expression 's/"\?$/"/g' \
                )
            fi
        fi
        # each column as VARCHAR type
        table_columns_create=$(echo "$table_columns_list" \
            | sed "s/,/ VARCHAR,/g")' VARCHAR'
        # add SERIAL
        if [ "$rowid" = yes ]; then
            table_columns_create="rowid SERIAL,$table_columns_create"
        fi
        [ "$POW_DEBUG" = yes ] && echo "table_columns_create=$table_columns_create"
    fi

    local table_to_load_exists=no
    local schema_table="${schema_name}.${table_name}"
    local backup_post_data_full_path="$POW_DIR_TMP/${schema_table}_post-data_$$.backup"
    table_exists --schema_name "${schema_name}" --table_name "${table_name}" && table_to_load_exists=yes
    if [ "$table_to_load_exists" = yes ]; then
        [ "$POW_DEBUG" = yes ] && echo "load_mode=$load_mode"
        # only in APPEND mode (backup post-data); alternative: don't remove
        case "$load_mode" in
        OVERWRITE_DATA|APPEND)
            [ "$load_mode" = APPEND ] && {
                backup_table \
                    --schema_name "${schema_name}" \
                    --table_name "${table_name}" \
                    --sections 'post-data' \
                    --output "$backup_post_data_full_path" || return $ERROR_CODE
            }

            execute_query \
                --name "DROP_CONSTRAINTS_INDEXES_TRIGGERS_${schema_table}" \
                --query "
                    SELECT public.drop_table_constraints('${schema_name}', '${table_name}');
                    SELECT public.drop_table_indexes('${schema_name}', '${table_name}');
                    SELECT public.drop_table_triggers('${schema_name}', '${table_name}');
                    " || return $ERROR_CODE

            [ "$load_mode" = OVERWRITE_DATA ] && {
                execute_query \
                    --name "TRUNCATE_${schema_table}" \
                    --query "TRUNCATE TABLE ${schema_name}.${table_name} CASCADE" || return $ERROR_CODE
            }
            ;;
        OVERWRITE_TABLE)
            execute_query \
                --name "DROP_${schema_table}" \
                --query "DROP TABLE ${schema_name}.${table_name} CASCADE" || return $ERROR_CODE
            ;;
        esac
    fi
    if [ "$table_to_load_exists" = no ] || [ "$load_mode" = OVERWRITE_TABLE ]; then
        if [ "$file_with_header" != yes ] && [ -z "$table_columns_create" ]; then
            log_error "Erreur lors de l'import de $file_path, vous devez préciser le nom des colonnes cible, dans l'ordre des colonnes du fichier"
            return $ERROR_CODE
        fi
        execute_query \
            --name "CREATE_${schema_table}" \
            --query "
                CREATE TABLE IF NOT EXISTS ${schema_name}.${table_name} ($table_columns_create)
                " || return $ERROR_CODE
    fi

    local file_with_header_boolean=$([ "$file_with_header" = yes ] && echo TRUE || echo FALSE)
    local _copy_data
    # put query into a file, due to error w/ command line (bash_args eval!)
    get_tmp_file --tmpext sql --tmpfile _copy_data
    cat <<-EOF > "$_copy_data"
\\COPY ${schema_name}.${table_name} (${table_columns_list}) FROM $([ -n "$limit" ] && echo STDIN || echo "'$file_path'") WITH (DELIMITER E'$delimiter_value', FORMAT CSV, HEADER $file_with_header_boolean, QUOTE '"', ENCODING $encoding)
EOF

    if [ -n "$limit" ]; then
        # NOTE: ko if CR exist in values
        limit=$([ "$file_with_header" = yes ] && echo $((limit+1)) || echo $limit)
        head --lines $limit "$file_path" \
            | execute_query \
                --name "COPY_${table_name}_FROM_${file_name}" \
                --query "$_copy_data" || return $ERROR_CODE
    else
        execute_query \
            --name "COPY_${table_name}_FROM_${file_name}" \
            --query "$_copy_data" || return $ERROR_CODE
    fi
    rm --force "$_copy_data"

    # only in APPEND mode (to do by caller for others, sometimes need to delete duplicates before)
    if [ "$table_to_load_exists" = yes ] && [ "$load_mode" = APPEND ]; then
        # restore contraints/indexes/triggers after loading data
        restore_table \
            --schema_name "${schema_name}" \
            --table_name "${table_name}" \
            --sections 'post-data' \
            --input "$backup_post_data_full_path" &&
        rm --force "$backup_post_data_full_path" || return $ERROR_CODE
    fi

    # clean
    [ "$file_to_tmp" = yes ] && rm "$file_path"

    return $SUCCESS_CODE
}

# tr EXCEL to CSV
excel_to_csv() {
    bash_args \
        --args_p '
            from_file_path:Chemin absolu vers le fichier à traiter;
            to_file_path:Chemin absolu vers le fichier de sortie (ou STDOUT pour une sortie écran);
            worksheet_name:Nom de la feuille à extraire (si non précisé ce sera la feuille active à l ouverture du fichier);
            delimiter:Séparateur à utiliser pour la conversion vers CSV (ce caractère ne doit pas être utilisé dans les valeurs d entête)' \
        --args_o 'from_file_path' \
        --args_v '
            delimiter:'${POW_DELIMITER_JOIN_PIPE} \
        --args_d '
            delimiter:COMMA;
            to_file_path:${get_arg_from_file_path}.csv' \
        "$@" || return $ERROR_CODE

    expect file "$get_arg_from_file_path" || exit $ERROR_CODE

    local from_file_path="$get_arg_from_file_path"
    local to_file_path="$get_arg_to_file_path"
    [ "$to_file_path" = STDOUT ] && to_file_path=$(dirname "$get_arg_from_file_path")/STDOUT.txt
    local from_file_name=$(get_file_name --file_path "$from_file_path")
    local from_file_extension=$(get_file_extension --file_path "$from_file_path")
    local to_file_name=$(get_file_name --file_path "$to_file_path")
    local to_file_extension=$(get_file_extension --file_path "$to_file_path")
    local worksheet_name="$get_arg_worksheet_name"
    local delimiter_code=$get_arg_delimiter
    local delimiter_value
    set_delimiter --delimiter_code $delimiter_code --delimiter_value delimiter_value

    # MIME type
    # https://stackoverflow.com/questions/7076042/what-mime-type-should-i-use-for-csv
    local _mime=$(get_file_mimetype "$from_file_path") _spreadsheet
    case "$_mime" in
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet|application/vnd.ms-excel)
        _spreadsheet='MS Excel'
        ;;
    application/vnd.oasis.opendocument.spreadsheet)
        _spreadsheet='Open Office sheet'
        ;;
    esac
    [ "$POW_DEBUG" = yes ] && echo "spreadsheet (MIME)=$_mime"
    [ -z "$_mime" ] &&
    case "${file_extension,,}" in
    xls|xlsx|ods)
        :
        ;;
    *)
        log_error "Erreur excel_to_csv de $from_file_path, le fichier source ne semble pas être un classeur"
        return $ERROR_CODE
        ;;
    esac &&
    [ "$POW_DEBUG" = yes ] && echo "spreadsheet (EXTENSION)=$file_extension"

    # prefer .txt to custom separator
    local _sheet _convert
    [ -n "$worksheet_name" ] && _sheet="sheet=$worksheet_name"
    log_info "conversion $_spreadsheet de $from_file_path vers ${to_file_path}"
    get_tmp_file --tmpext txt --tmpfile _convert

    if [ -n "$worksheet_name" ]; then
        ssconvert --export-options "sheet=$worksheet_name separator=$delimiter_value format=preserve" "$from_file_path" "${_convert}" > $POW_DIR_ARCHIVE/ssconvert.log 2> $POW_DIR_ARCHIVE/ssconvert.error.log
    else
        ssconvert --export-options "separator=$delimiter_value format=preserve" "$from_file_path" "${_convert}" > $POW_DIR_ARCHIVE/ssconvert.log 2> $POW_DIR_ARCHIVE/ssconvert.error.log
    fi

    mv "$_convert" "${to_file_path}"
    [ "$to_file_name" = STDOUT ] &&
    [ -f "${to_file_path}" ] && {
        cat "$to_file_path"
        rm "$to_file_path"
    }

    return $SUCCESS_CODE
}

# tr CSV to EXCEL
csv_to_excel() {
    bash_args \
        --args_p '
            from_file_path:Chemin absolu vers le fichier à traiter;
            to_file_path:Chemin absolu vers le fichier de sortie' \
        --args_o 'from_file_path' \
        --args_d 'to_file_path:${get_arg_from_file_path}.xls' \
        "$@" || return $ERROR_CODE

    expect file "$get_arg_from_file_path" || exit $ERROR_CODE

    local from_file_path="$get_arg_from_file_path"
    local to_file_path="$get_arg_to_file_path"
    local from_file_name=$(get_file_name --file_path "$from_file_path")
    local from_file_extension=$(get_file_extension --file_path "$from_file_path")
    local to_file_name=$(get_file_name --file_path "$to_file_path")
    local to_file_extension=$(get_file_extension --file_path "$to_file_path")

    # MIME type
    # https://stackoverflow.com/questions/7076042/what-mime-type-should-i-use-for-csv
    local _mime=$(get_file_mimetype "$from_file_path")
    case "$_mime" in
    text/plain|text/csv|text/x-csv)
        # NULL command
        # https://www.shell-tips.com/bash/null-command
        :
        ;;
    *)
        log_error "Erreur csv_to_excel de $from_file_path, le fichier source ne semble pas être un CSV"
        return $ERROR_CODE
    esac

    log_info "conversion de $from_file_path vers ${to_file_path}"
    # protect alnum : starting w/ 0, including E (for exposant)
    sed -e 's/\(\("0[0-9]\+"\)\|\("[0-9]\+E[0-9]\+"\)\)/"="\0""/g' "$from_file_path" > "$POW_DIR_TMP/$from_file_name.csv_to_excel.txt"
    ssconvert "$POW_DIR_TMP/$from_file_name.csv_to_excel.txt" "$to_file_path" > /dev/null 2>&1
}

# import EXCEL in DB (before converting as CSV)
import_excel_file() {
    bash_args \
        --args_p '
            file_path:Chemin absolu vers le fichier à traiter;
            schema_name:Nom du schema cible;
            table_name:Nom de la table cible;
            table_columns:Colonnes de la table cible;
            table_columns_list:Liste des colonnes de la table cible;
            load_mode:Mode de chargement des données;
            worksheet_name:Nom de la feuille à extraire, si non précisé ce sera la feuille active à l ouverture du fichier;
            from_line_number:Numéro de ligne à partir de laquelle il faut lire le fichier;
            to_line_number:Numéro de ligne jusqu à laquelle il faut lire le fichier;
            delimiter:Séparateur à utiliser pour la conversion vers CSV, ce caractère ne doit pas être utilisé dans les valeurs d entête;
            limit:Limiter a n enregistrements;
            rowid:Générer un identifiant unique rowid' \
        --args_o 'file_path' \
        --args_v '
            delimiter:'${POW_DELIMITER_JOIN_PIPE}';
            table_columns:HEADER|HEADER_TO_LOWER_CODE|LIST;
            load_mode:OVERWRITE_DATA|OVERWRITE_TABLE|APPEND;
            rowid:yes|no' \
        --args_d '
            schema_name:'${POW_PG_DEFAULT_SCHEMA}';
            table_columns:HEADER;
            delimiter:PIPE;
            load_mode:OVERWRITE_DATA;
            rowid:yes' \
        "$@" || return $ERROR_CODE

    expect file "$get_arg_file_path" || exit $ERROR_CODE

    local file_path="$get_arg_file_path"
    local file_name=$(get_file_name --file_path "$file_path")
    local file_extension=$(get_file_extension --file_path "$file_path")
    local schema_name=$get_arg_schema_name
    local table_name=$get_arg_table_name
    local table_columns=$get_arg_table_columns
    local table_columns_list=$get_arg_table_columns_list
    local load_mode=$get_arg_load_mode
    local worksheet_name="$get_arg_worksheet_name"
    local from_line_number=$get_arg_from_line_number
    local to_line_number=$get_arg_to_line_number
    local delimiter=$get_arg_delimiter
    local limit=$get_arg_limit
    local rowid=$get_arg_rowid
    local delimiter_code=$get_arg_delimiter

    excel_to_csv \
        --from_file_path "$file_path" \
        --to_file_path "$file_path.txt" \
        --worksheet_name "$worksheet_name" \
        --delimiter "$delimiter_code" &&
    import_csv_file \
        --file_path "$file_path.txt" \
        --schema_name "$schema_name" \
        --table_name "$table_name" \
        --delimiter "$delimiter_code" \
        --load_mode "$load_mode" \
        --table_columns "$table_columns" \
        --table_columns_list "$table_columns_list" \
        --limit "$limit" \
        --from_line_number "$from_line_number" \
        --to_line_number "$to_line_number" \
        --rowid "$rowid" &&
    rm "$file_path.txt" &&
    return $SUCCESS_CODE ||
    return $ERROR_CODE
}

# import GEO (as shapefile, ...)
import_geo_file() {
    bash_args \
        --args_p '
            file_path:Chemin absolu vers le fichier à traiter;
            schema_name:Nom du schema cible;
            table_name:Nom de la table cible;
            password:Mot de passe;
            load_mode:Mode de chargement des données;
            encoding:Encodage de caractères;
            from_srid:Identifiant du système de projection des objets géographiques;
            to_srid:Identifiant du système de reprojection des objets géographiques;
            geometry_type:Type des objets geographiques;
            spatial_index:Indique si il faut créer un index géographique;
            limit:Limiter a n enregistrements;
            rowid:Générer un identifiant unique rowid' \
        --args_o 'file_path;table_name' \
        --args_v '
            load_mode:OVERWRITE_DATA|OVERWRITE_TABLE|APPEND;
            encoding:UTF-8|LATIN1;
            geometry_type:NONE|GEOMETRY|POINT|LINESTRING|POLYGON|GEOMETRYCOLLECTION|MULTIPOINT|MULTIPOLYGON|MULTILINESTRING|CIRCULARSTRING|COMPOUNDCURVE|CURVEPOLYGON|MULTICURVE|MULTISURFACE|PROMOTE_TO_MULTI|CONVERT_TO_LINEAR|CONVERT_TO_CURVE;
            spatial_index:yes|no;
            rowid:yes|no' \
        --args_d '
            schema_name:'${POW_PG_DEFAULT_SCHEMA}';
            encoding:UTF-8;
            load_mode:OVERWRITE_DATA;
            geometry_type:GEOMETRY;
            spatial_index:yes;
            rowid:yes' \
        "$@" || return $ERROR_CODE

    expect file "$get_arg_file_path" || exit $ERROR_CODE

    # geometry_type :
    # Define the geometry type for the created layer.
    # One of NONE, GEOMETRY, POINT, LINESTRING, POLYGON, GEOMETRYCOLLECTION, MULTIPOINT, MULTIPOLYGON or MULTILINESTRING.
    # And CIRCULARSTRING, COMPOUNDCURVE, CURVEPOLYGON, MULTICURVE and MULTISURFACE for GDAL 2.0 non-linear geometry types.
    # Add "Z", "M", or "ZM" to the name to get coordinates with elevation, measure, or elevation and measure.
    # Starting with GDAL 1.10, PROMOTE_TO_MULTI can be used to automatically promote layers that mix polygon or multipolygons to multipolygons, and layers that mix linestrings or multilinestrings to multilinestrings.
    # Can be useful when converting shapefiles to PostGIS and other target drivers that implement strict checks for geometry types.
    # Starting with GDAL 2.0, CONVERT_TO_LINEAR can be used to to convert non-linear geometries types into linear geometries by approximating them, and CONVERT_TO_CURVE to promote a non-linear type to its generalized curve type (POLYGON to CURVEPOLYGON, MULTIPOLYGON to MULTISURFACE, LINESTRING to COMPOUNDCURVE, MULTILINESTRING to MULTICURVE).
    # Starting with 2.1 the type can be defined as measured ("25D" remains as an alias for single "Z").
    # Some forced geometry conversions may result in invalid geometries, for example when forcing conversion of multi-part multipolygons with -nlt POLYGON, the resulting polygon will break the Simple Features rules.

    # load_mode :
    # overwrite (default) : Delete the output layer and recreate it empty
    # append : Append to existing layer instead of creating new
    # update : Open existing output datasource in update mode rather than trying to create a new one

    local file_path="$get_arg_file_path"
    local file_name=$(get_file_name --file_path "$file_path")
    local file_extension=$(get_file_extension --file_path "$file_path")
    local schema_name=$get_arg_schema_name
    local table_name=$get_arg_table_name
    local passwd=$get_arg_password
    local load_mode=$get_arg_load_mode
    local load_mode_ogr2ogr
    case "$load_mode" in
    OVERWRITE_DATA|OVERWRITE_TABLE)
        load_mode_ogr2ogr=overwrite
        ;;
    APPEND)
        load_mode_ogr2ogr=append
        ;;
    esac
    local encoding=$get_arg_encoding
    local from_srid=$get_arg_from_srid
    local to_srid=$get_arg_to_srid
    local geometry_type=$get_arg_geometry_type
    local spatial_index=$get_arg_spatial_index
    local limit=$get_arg_limit
    local rowid=$get_arg_rowid

    if [ -z "$table_name" ]; then
        execute_query \
            --name LABEL_TO_CODE \
            --query "SELECT public.label_to_code('$file_name')" \
            --psql_arguments 'tuples-only:pset=format=unaligned' \
            --with_log no \
            --return table_name || return $ERROR_CODE
    fi
    [ "$POW_DEBUG" = yes ] && echo "table_name=$table_name"

    # FIXME: try to run each word (shp, ...) as command!
    #[[ ! $file_extension =~ shp|mif|dbf|json ]] && {
    echo $file_extension | grep --perl-regexp --silent 'shp|mif|dbf|json' || {
        log_error "Le Fichier $file_path n'a pas une extension shp, mif, dbf ou json"
        return $ERROR_CODE
    }

    local log_tmp_path
    get_tmp_file --tmpfile log_tmp_path --create yes --tmpext log || {
        log_error "Erreur de création du fichier temporaire de LOG"
        return $ERROR_CODE
    }

    # http://www.bostongis.com/PrinterFriendly.aspx?content_name=ogr_cheatsheet
    # -t_srs srs_def : Reproject/transform to this SRS on output
    # -s_srs srs_def : Override source SRS
    local ogr_args=''
    [ -n "$to_srid" ] && ogr_args="$ogr_args -t_srs $to_srid"
    [ -n "$from_srid" ] && ogr_args="$ogr_args -s_srs $from_srid"
    [ -n "$limit" ] && ogr_args="$ogr_args -limit $limit"

    if [ "$file_extension" = mif ]; then
        # NOTE: remains origin dbf (not one this created by ogr2ogr), so use temporary directory
        local mif_dir=$(dirname "$file_path")
        local mif_to_shp_dir=$mif_dir/mif_to_shp
        local log_mif_to_shp="$POW_DIR_TMP/mif_to_shp_$file_name.log"
        mkdir --parents $mif_to_shp_dir
        ogr2ogr \
            -f 'ESRI Shapefile' \
            $mif_to_shp_dir \
            $file_path > "$log_mif_to_shp" 2>&1
        if [ $? -ne 0 ] || [ -n "$(grep --max-count 1 ERROR $log_mif_to_shp)" ]; then
            log_error "Erreur lors de la conversion de $file_name en shapefile, voir $log_mif_to_shp"
            return $ERROR_CODE
        fi

        archive_file "$log_mif_to_shp"
        log_info "Conversion avec succès de $file_name en shapefile"
        # NOTE: copy only new files (so origin dbf is not replaced
        mv --no-clobber $mif_to_shp_dir/* $mif_dir/
        rm --recursive $mif_to_shp_dir
        file_path="$mif_dir/${file_name}.shp"
    fi

    if [ -n "$encoding" ]; then
        _PGCLIENTENCODING_SAVE=$PGCLIENTENCODING
        export PGCLIENTENCODING=$encoding
    fi

    layer_creation_options='-lco FID=rowid -lco GEOMETRY_NAME=geom'
    [ "$spatial_index" = no ] && layer_creation_options+=' -lco SPATIAL_INDEX=no'

    local _rc
    [ -z "$passwd" ] && {
        get_pg_passwd --user_name $POW_PG_USERNAME --password passwd || {
            log_error "Erreur de récupération du mot de passe (user=$POW_PG_USERNAME)"
            return $ERROR_CODE
        }
    }

    ogr2ogr \
        -f "PostgreSQL" \
        PG:"host=$POW_PG_HOST user=$POW_PG_USERNAME dbname=$POW_PG_DBNAME password=$passwd" \
        "$file_path" \
        -$load_mode_ogr2ogr \
        -nln "${schema_name}.${table_name}" \
        -nlt $geometry_type \
        $ogr_args \
        $layer_creation_options 2> "$log_tmp_path"
    _rc=$?

    # restore previous encoding
    [ -n "$encoding" ] && PGCLIENTENCODING=$_PGCLIENTENCODING_SAVE

    # returns OK even if encoding error, so search for ERROR
    if [ $_rc -ne 0 ] || [ -n "$(grep --max-count 1 ERROR $log_tmp_path)" ]; then
        log_error "Erreur lors de l'import de $file_name, voir $log_tmp_path"
        return $ERROR_CODE
    fi

    if [ "$rowid" = no ]; then
        execute_query \
            --name DROP_COLUMN_ROWID \
            --query "ALTER TABLE $table_name DROP COLUMN IF EXISTS rowid"
    fi
    archive_file "$log_tmp_path"
    log_info "Import avec succès de $file_name dans $table_name"

    return $SUCCESS_CODE
}

# import file into DB
import_file() {
    bash_args \
    --args_p '
        file_path:Chemin absolu vers le fichier à importer;
        schema_name:Nom du schema cible;
        table_name:Nom de la table cible;
        load_mode:Mode de chargement des données;
        import_options:Options d import du fichier spécifiques à son format;
        limit:Limiter a n enregistrements;
        rowid:Générer un identifiant unique rowid' \
    --args_o 'file_path' \
    --args_v '
        load_mode:OVERWRITE_DATA|OVERWRITE_TABLE|APPEND;
        rowid:yes|no' \
    --args_d '
        schema_name:$POW_PG_DEFAULT_SCHEMA;
        load_mode:OVERWRITE_DATA;
        rowid:yes' \
    "$@" || return $ERROR_CODE

    expect file "$get_arg_file_path" || exit $ERROR_CODE

    local file_path="$get_arg_file_path"
    local file_name=$(get_file_name --file_path "$file_path")
    local file_extension=$(get_file_extension --file_path "$file_path")
    local schema_name=$get_arg_schema_name
    local table_name=$get_arg_table_name
    local load_mode=$get_arg_load_mode
    local limit=$get_arg_limit
    local import_options="$get_arg_import_options"
    local rowid=$get_arg_rowid

    local file_archive_extract_dir
    if is_archive --archive_path "$file_path"; then
        file_archive_extract_dir="$POW_DIR_TMP/$file_name"
        rm --recursive --force "$POW_DIR_TMP/$file_name" &&
        mkdir "$POW_DIR_TMP/$file_name" &&
        extract_archive \
            --archive_path "$file_path" \
            --extract_path "$file_archive_extract_dir" || return $ERROR_CODE

        local _files=($(ls -1 "$file_archive_extract_dir"/*)) _error=yes _msg
        case ${#_files[@]} in
        0)
            _msg="Auncun fichier trouvé dans l'archive $file_path, un attendu"
            ;;
        1)
            _error=no
            ;;
        *)
            _msg="Plusieurs fichiers trouvés dans l'archive $file_path, un seul attendu"
            ;;
        esac
        [ "$_error" = yes ] && {
            log_error "$_msg"
            return $ERROR_CODE
        }
        file_path=${_files[0]}
        file_name=$(get_file_name --file_path "$file_path")
        file_extension=$(get_file_extension --file_path "$file_path")
    fi

    if [ -z "$table_name" ]; then
        execute_query \
            --name LABEL_TO_CODE \
            --query "SELECT public.label_to_code('$file_name')" \
            --psql_arguments 'tuples-only:pset=format=unaligned' \
            --with_log no \
            --return table_name || return $ERROR_CODE
    fi
    [ "$POW_DEBUG" = yes ] && echo "table_name=$table_name"

    # options
    local tmp_liste_import_options=()
    local tmp_import_option tmp_import_option_name tmp_import_option_value import_options_string
    local tmp_import_option_prefix
    IFS=';' read -ra tmp_liste_import_options <<< "${import_options}"
    for tmp_import_option in "${tmp_liste_import_options[@]}"; do
        tmp_import_option_name=$(echo $tmp_import_option | grep --only-matching '^[^:]*')
        tmp_import_option_value=$(echo $tmp_import_option | grep --only-matching '[^:]*$')
        if [ $(expr length $tmp_import_option_name) -eq 1 ]; then
            tmp_import_option_prefix='-'
        else
            tmp_import_option_prefix='--'
        fi
        if [ -n "$import_options_string" ]; then
            tmp_import_option_prefix=" ${tmp_import_option_prefix}"
        fi
        import_options_string+="${tmp_import_option_prefix}${tmp_import_option_name} ${tmp_import_option_value}"
    done

    local _mime=$(get_file_mimetype "$from_file_path") _type_import _type_file
    case "$_mime" in
    text/plain|text/csv|text/x-csv)
        _type_import=CSV
        ;;
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet|application/vnd.ms-excel|application/vnd.oasis.opendocument.spreadsheet)
        _type_import=SPREADSHEET
        ;;
    application/*dbf*|application/octet-stream)
        _type_file=$(file "$file_path" | cut --delimiter : --fields 2)
        _type_file=${_type_file,,}
        [[ $_type_file =~ esri[[:space:]]shapefile|dbase|json ]] && _type_import=GEO
        ;;
    application/*json*)
        _type_import=JSON
        ;;
    esac
    [ "$POW_DEBUG" = yes ] && echo "_type_import (MIME)=$_type_import"
    [ -z "$_type_import" ] &&
    case "${file_extension,,}" in
    txt|[cdt]sv)    _type_import=CSV            ;;
    shp|dbf)        _type_import=GEO            ;;
    json)           _type_import=JSON           ;;
    xls|xlsx|ods)   _type_import=SPREADSHEET    ;;
    esac &&
    [ "$POW_DEBUG" = yes ] && {
        echo "_type_import (EXTENSION)=$_type_import"
        echo "import_options_string=$import_options_string"
        echo "limit=$limit"
        echo "rowid=$rowid"
    }

    case "$_type_import" in
    CSV)
        import_csv_file \
            --file_path "$file_path" \
            --schema_name "$schema_name" \
            --table_name "$table_name" \
            --load_mode "$load_mode" \
            --limit "$limit" \
            --rowid "$rowid" \
            $import_options_string
        ;;
    SPREADSHEET)
        import_excel_file \
            --file_path "$file_path" \
            --schema_name "$schema_name" \
            --table_name "$table_name" \
            --load_mode "$load_mode" \
            --limit "$limit" \
            --rowid "$rowid" \
            $import_options_string
        ;;
    GEO)
        import_geo_file \
            --file_path "$file_path" \
            --schema_name "$schema_name" \
            --table_name "$table_name" \
            --load_mode "$load_mode" \
            --limit "$limit" \
            --rowid "$rowid" \
            $import_options_string
        ;;
    JSON)
        local _i _column_name
        local -a _opt
        local -A _json_options
        [[ ${#tmp_liste_import_options[@]} -gt 0 ]] && {
            for ((_i=0; _i<${#tmp_liste_import_options[@]}; _i++)); do
                IFS='=' read -ra _opt <<< ${tmp_liste_import_options[$_i]}
                _json_options[${_opt[0]}]=${_opt[1]}
            done
            _column_name=${_json_options[column_name]}
        }
        # column undefined?
        [ -z "$_column_name" ] && {
            local _columns_str
            local -a _columns_array
            execute_query \
                --name TABLE_COLUMNS \
                --query "SELECT get_table_columns('$schema_name', '$table_name')" \
                --psql_arguments 'tuples-only:pset=format=unaligned' \
                --return _columns_str &&
            array_sql_to_bash --array_sql "$_columns_str" --array_bash _columns_array &&
            {
                [[ ${#_columns_array[@]} -eq 1 ]] || {
                    log_error "Table de chargement JSON ($schema_name.$table_name) ne doit avoir qu'une colonne de type JSON!"
                    false
                }
            } &&
            _column_name=${_columns_array[0]}
        } &&
        {
            [ "${load_mode}" = APPEND ] || {
                execute_query \
                    --name TABLE_TRUNCATE \
                    --query "TRUNCATE TABLE $schema_name.$table_name"
            }
        } &&
        jq --raw-output --compact-output '.' < "$file_path" | execute_query \
            --name LOAD_JSON \
            --query "COPY $schema_name.$table_name (${_column_name}) FROM STDIN"
        ;;
    *)
        log_error "Le fichier $file_path ne peut pas être traité!"
        false
    esac || return $ERROR_CODE

    [ -n "$file_archive_extract_dir" ] && rm --recursive --force "$file_archive_extract_dir"

    return $SUCCESS_CODE
}
