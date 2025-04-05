    #--------------------------------------------------------------------------
    # synopsis
    #--
    # IO library

    #
    # IO history
    #

_io_history_manager() {
    local -A _opts &&
    pow_argv \
        --args_n '
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
        --args_m '
            method;
            status
        ' \
        --args_v '
            status:EN_COURS|SUCCES|ERREUR
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _query _return _output _with_log=no
    [ "$POW_DEBUG" = yes ] && _with_log=yes

    case ${_opts[METHOD]} in
    EXISTS)
        local -n _io_id_manager=${_opts[ID]}
        _return='--return _io_id_manager'
        _query="
            SELECT id FROM get_io(
                name => '${_opts[IO]}',
                status => '${_opts[STATUS]}',
                date_end => '${_opts[DATE_END]}'::TIMESTAMP
            )
        "
        ;;
    APPEND)
        local -n _io_id_manager=${_opts[ID]}
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
                '${_opts[IO]}',
                '${_opts[DATE_BEGIN]}'::TIMESTAMP,
                '${_opts[DATE_END]}'::TIMESTAMP,
                '${_opts[STATUS]:-EN_COURS}',
                ${_opts[NROWS_TODO]:-NULL},
                CASE
                WHEN LENGTH('${_opts[INFOS]}') = 0 THEN NULL
                ELSE '${_opts[INFOS]}'
                END
            );
            SELECT CURRVAL('public.io_history_id_seq');
        "
        ;;
    UPDATE)
        _query="
            UPDATE public.io_history SET
                attributes =
                    CASE
                    WHEN LENGTH('${_opts[INFOS]}') = 0 THEN attributes
                    ELSE
                        CASE
                        WHEN attributes IS JSON OBJECT THEN
                            (jsonb_merge(attributes::JSONB, '${_opts[INFOS]}'::JSONB))::VARCHAR
                        ELSE '${_opts[INFOS]}'
                        END
                    END
                $([ -n "${_opts[NROWS_TODO]}" ] && echo ", nb_rows_todo = ${_opts[NROWS_TODO]}")
                $([ -n "${_opts[NROWS_PROCESSED]}" ] && echo ", nb_rows_processed = ${_opts[NROWS_PROCESSED]}")
            WHERE id = ${_opts[ID]}
        "
        ;;
    UPDATE_OK)
        # itself (if no defined) to remain previous value
        _query="
            UPDATE public.io_history SET
                date_exec_end = NOW(),
                status = 'SUCCES',
                nb_rows_processed = ${_opts[NROWS_PROCESSED]:-NULL},
                attributes =
                    CASE
                        WHEN LENGTH('${_opts[INFOS]}') = 0 THEN attributes
                        ELSE '${_opts[INFOS]}'
                    END
            WHERE id = ${_opts[ID]}
        "
        ;;
    UPDATE_KO)
        _query="
            UPDATE public.io_history SET
                date_exec_end = NOW(),
                status = 'ERREUR'
            WHERE id = ${_opts[ID]}
        "
        ;;
    EXPORT_LAST)
        _with_log=yes
        _output="--output ${_opts[OUTPUT]}"
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
                    name ~ '${_opts[IO]}'
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
        log_error "Méthode '${_opts[METHOD]}' non implémentée!"
        return $ERROR_CODE
        ;;
    esac

    execute_query \
        --name IO_${_opts[METHOD]}_${_opts[IO]:-${_opts[ID]}} \
        --query "$_query" \
        $_return \
        $_output \
        --with_log $_with_log || return $ERROR_CODE

    [ -n "$_return" ] &&
    [ "$POW_DEBUG" = yes ] && { echo io_id=$_io_id_manager; }

    return $SUCCESS_CODE
}

# IO in progress
#  io_history_exists --status EN_COURS
# IO already success
#  io_history_exists --status SUCCES
io_history_exists() {
    local -A _opts &&
    pow_argv \
        --args_n '
            io:nom IO;
            date_end:date de fin des données (format connu PostgreSQL);
            status:état IO;
            id:variable pour récupérer ID de IO
        ' \
        --args_m '
            io;
            date_end
        ' \
        --args_v '
            status:EN_COURS|SUCCES|ERREUR
        ' \
        --args_d '
            status:SUCCES
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    [ -n "${_opts[ID]}" ] && local -n _io_id=${_opts[ID]} || local _io_id

    _io_history_manager \
        --method EXISTS \
        --status ${_opts[STATUS]} \
        --io ${_opts[IO]} \
        --date_end "${_opts[DATE_END]}" \
        --id _io_id &&
    [ -n "$_io_id" ] &&
    return $SUCCESS_CODE ||
    return $ERROR_CODE
}

POW_IO_SUCCESSFUL=10
POW_IO_IN_PROGRESS=11
POW_IO_TODO=12
POW_IO_ERROR=13

# IO import is todo?
io_todo_import() {
    local -A _opts &&
    pow_argv \
        --args_n '
            force:option de forçage du traitement;
            io:nom IO;
            date_end:date de fin des données (format connu PostgreSQL);
            purge:purge historique précédent;
            id:variable pour récupérer ID de IO
        ' \
        --args_m '
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
        --pow_argv _opts "$@" || return $POW_IO_ERROR

    local _rc
    [ -n "${_opts[ID]}" ] && local -n _io_id_todo=${_opts[ID]} || local _io_id_todo

    io_history_exists \
        --io ${_opts[IO]} \
        --date_end "${_opts[DATE_END]}" \
        --status SUCCES \
        --id _io_id_todo
    _rc=$?
    [ "${_opts[FORCE]}" = no ] &&
    [ $_rc -eq 0 ] && {
        log_info "Le traitement ${_opts[IO]} a déjà été réalisé avec succès"
        return $POW_IO_SUCCESSFUL
    }

    {
        io_history_exists \
            --io ${_opts[IO]} \
            --date_end "${_opts[DATE_END]}" \
            --status EN_COURS \
            --id _io_id_todo
    } && {
        log_info "Le traitement ${_opts[IO]} est déjà en cours"
        return $POW_IO_IN_PROGRESS
    }
    [ "${_opts[PURGE]}" = yes ] && {
        # purge previous history
        execute_query \
            --name "DELETE_IO_${_opts[IO]}" \
            --query "DELETE FROM io_history WHERE name = '${_opts[IO]}'" || return $POW_IO_ERROR
    }

    return $POW_IO_TODO
}

# get description of IO integration (HSTORE converted to associative array)
io_get_info_integration() {
    local -A _opts &&
    pow_argv \
        --args_n '
            io:nom IO;
            to_hash:variable pour récupérer la description de cette intégration;
            to_string:variable pour récupérer la description de cette intégration
        ' \
        --args_m '
            io;
            to_hash
        ' \
        --pow_argv _opts "$@" || return $POW_IO_ERROR

    local -n _hash_ref=${_opts[TO_HASH]}
    [ -n "${_opts[TO_STRING]}" ] && local -n _str_ref=${_opts[TO_STRING]}
    local _tmpfile

    get_tmp_file --tmpfile _tmpfile &&
    execute_query \
        --name "TODO-${_opts[IO]}" \
        --query "SELECT io_is_todo('${_opts[IO]}')" \
        --output $_tmpfile || return $ERROR_CODE
    [ "$POW_DEBUG" = yes ] && cat $_tmpfile
    # each row contains: key=>value
    _hash_ref=()
    while read; do
        _hash_ref[${REPLY%=*}]=${REPLY#*>}
    done < <(sed --expression 's/"//g' --expression 's/,/\n/g' < $_tmpfile | sed --expression 's/^[ ]*//')
    [ -n "${_opts[TO_STRING]}" ] && _str_ref=$(< $_tmpfile)
    rm $_tmpfile

    return $SUCCESS_CODE
}

# build list of IDs as string (coded as JSON) for IO history of IO (as DEPENDS) or given by name
io_get_ids_integration() {
    local -A _opts &&
    pow_argv \
        --args_n '
            from:méthode accès aux IDs de la dépendance à traiter;
            hash:tableau associatif de description de cette intégration;
            array:tableau des IDs de cette intégration;
            ids:variable pour récupérer les IDs;
            group:groupe de la dépendance à traiter;
            item:élément de la dépendance à traiter (tous si élément non renseigné)
        ' \
        --args_m '
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
        --pow_argv _opts "$@" || return $POW_IO_ERROR

    # https://stackoverflow.com/questions/13219634/easiest-way-to-check-for-an-index-or-a-key-in-an-array
    # https://stackoverflow.com/questions/11180714/how-to-iterate-over-an-array-using-indirect-reference

    local -n _hash_ref=${_opts[HASH]}
    local -n _ids_ref=${_opts[IDS]}
    local _group _steps _step _array_ptr _i _key _value

    [ -n "${_opts[GROUP]}}" ] && _group=${_opts[GROUP]} || _group=DEPENDS
    [[ $_group =~ DEPENDS|RESSOURCES ]] || _group+=_d
    _steps=(${_hash_ref[$_group]//:/ })

    case "${_opts[FROM]}" in
    HASH)
        [[ -v "_hash_ref[$_group]" ]] || {
            log_error "manque dépendances IO=($_group)"
            return $ERROR_CODE
        }
        _array_ptr="_steps[@]"
        ;;
    ARRAY)
        [ -z "${_opts[ARRAY]}" ] && {
            log_error "manque tableau IDs (option --array)"
            return $ERROR_CODE
        }
        local -n _array_ref=${_opts[ARRAY]}
        _array_ptr="_array_ref[@]"
        ;;
    esac

    _ids_ref=''
    _i=0
    for _step in "${!_array_ptr}"; do
        case "${_opts[FROM]}" in
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
        [ -n "${_opts[ITEM]}" ] && [ "${_opts[ITEM]}" != "$_key" ] && continue
        [ -n "$_ids_ref" ] && _ids_ref+=,
        _ids_ref+=$(printf '"%s":%d' $_key $_value)
    done
    [ -n "$_ids_ref" ] && _ids_ref="{${_ids_ref}}"

    return $SUCCESS_CODE
}

# start history of IO (returning its ID)
io_history_begin() {
    local -A _opts &&
    pow_argv \
        --args_n '
            io:nom IO;
            date_begin:date de début des données (format connu PostgreSQL);
            date_end:date de fin des données (format connu PostgreSQL);
            nrows_todo:nombre de données à traiter;
            infos:compléments infos (souvent au format JSON);
            id:nom de la variable pour récupérer identifiant IO
        ' \
        --args_m '
            io;
            date_begin;
            date_end;
            nrows_todo;
            id
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -n _io_id=${_opts[ID]}

    _io_history_manager \
        --method APPEND \
        --io ${_opts[IO]} \
        --status EN_COURS \
        --date_begin "${_opts[DATE_BEGIN]}" \
        --date_end "${_opts[DATE_END]}" \
        --nrows_todo ${_opts[NROWS_TODO]} \
        --infos "${_opts[INFOS]}" \
        --id _io_id || return $ERROR_CODE

    return $SUCCESS_CODE
}

# end history of IO (w/ success)
io_history_end_ok() {
    local -A _opts &&
    pow_argv \
        --args_n '
            nrows_processed:nombre de données traitées;
            infos:compléments infos (souvent au format JSON);
            id:identifiant IO
        ' \
        --args_m '
            nrows_processed;
            id
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    _io_history_manager \
        --method UPDATE_OK \
        --nrows_processed "${_opts[NROWS_PROCESSED]}" \
        --infos "${_opts[INFOS]}" \
        --id ${_opts[ID]} || return $ERROR_CODE

    return $SUCCESS_CODE
}

# end history of IO (w/ error)
io_history_end_ko() {
    local -A _opts &&
    pow_argv \
        --args_n '
            id:identifiant IO
        ' \
        --args_m '
            id
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    _io_history_manager \
        --method UPDATE_KO \
        --id ${_opts[ID]} || return $ERROR_CODE

    return $SUCCESS_CODE
}

# export last history of IO
io_history_export_last() {
    local -A _opts &&
    pow_argv \
        --args_n '
            io:nom IO;
            output:sortie pour export
        ' \
        --args_m '
            io
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    _io_history_manager \
        --method EXPORT_LAST \
        --io ${_opts[IO]} \
        --output "${_opts[OUTPUT]}" || return $ERROR_CODE

    return $SUCCESS_CODE
}

# update history of IO (w/ its ID)
io_history_update() {
    local -A _opts &&
    pow_argv \
        --args_n '
            nrows_todo:nombre de données à traiter;
            nrows_processed:nombre de données traitées;
            infos:compléments infos (souvent au format JSON);
            id:identifiant IO
        ' \
        --args_m '
            id
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _todo _processed
    [ -n "${_opts[NROWS_TODO]}" ] && _todo="--nrows_todo ${_opts[NROWS_TODO]}"
    [ -n "${_opts[NROWS_PROCESSED]}" ] && _processed="--nrows_processed ${_opts[NROWS_PROCESSED]}"

    _io_history_manager \
        --method UPDATE \
        $_todo \
        $_processed \
        --infos "${_opts[INFOS]}" \
        --id ${_opts[ID]} || return $ERROR_CODE

    return $SUCCESS_CODE
}

    #
    # transfer
    #

# get property
io_get_property_online_available() {
    local -A _opts &&
    pow_argv \
        --args_n '
            name:nom IO à rechercher (en ligne);
            key:propriété recherchée;
            value:valeur
        ' \
        --args_m '
            name;
            key;
            value
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local _url_base _url_data _re1 _re2
    local -n _value_ref=${_opts[VALUE]}

    case "${_opts[NAME]}" in
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
    FR-TERRITORY-GOUV-EPCI)
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
        _url_data=${_url_base}'/fr/information/8377162'
        _re1='cog_ensemble_[0-9]{4}_csv.zip'
        _re2='[0-9]{4}'
        ;;
    *)
        log_error "IO ${_opts[NAME]} non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    case ${_opts[KEY]^^} in
    URL_BASE)       _value_ref=$_url_base       ;;
    URL_DATA)       _value_ref=$_url_data       ;;
    REGEXP1)        _value_ref=$_re1            ;;
    REGEXP2)        _value_ref=$_re2            ;;
    *)
        log_error "KEY ${_opts[KEY]} non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    return $SUCCESS_CODE
}

# get available dates (list, details as URL)
io_get_list_online_available() {
    local -A _opts &&
    pow_argv \
        --args_n '
            name:nom IO à rechercher (en ligne);
            details_file:Détail des millésimes disponibles;
            dates_list:Dates des millésimes disponibles
        ' \
        --args_m '
            name;
            details_file;
            dates_list
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    # NOTE _only_matching_re1=--only-matching
    # reset it if not only matching
    # no more used (previously needed for BANATIC)

    local _url _regexp1 _regexp2 _i
    local -n _details_file_ref=${_opts[DETAILS_FILE]}
    local -n _dates_ref=${_opts[DATES_LIST]}

    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key URL_DATA                  \
        --value _url                    &&
    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key REGEXP1                   \
        --value _regexp1                &&
    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key REGEXP2                   \
        --value _regexp2                || {
        log_error "IO ${_opts[NAME]} récupération propriété!"
        return $ERROR_CODE
    }

    # temporary file (to be deleted by caller)
    get_tmp_file --tmpext html --tmpfile _details_file_ref &&
    # download available dates
    io_download_file \
        --url "$_url" \
        --output_name ${_opts[NAME]} \
        --common_save no \
        --output_directory "$POW_DIR_TMP" \
        --output_file "$(basename $_details_file_ref)" &&
    # array of available dates (desc), transforming / to -
    _dates_ref=($(grep --only-matching --perl-regexp "$_regexp1" $_details_file_ref | grep --only-matching --perl-regexp "$_regexp2" | sed --expression 's@/@-@g' | uniq | sort --reverse)) || {
        log_error "Impossible de consulter la liste des millésimes disponibles de ${_opts[NAME]}"
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

# download ressource (wget)
POW_DOWNLOAD_OK=0                       # no problems
POW_DOWNLOAD_ALREADY_AVAILABLE=1        # file already available
POW_DOWNLOAD_BUT_AVAILABLE=2            # error (download), but present file can be used
POW_DOWNLOAD_ERROR=3                    # error (download)
POW_DOWNLOAD_ERROR_CONDITION=4          # error (missing condition)
POW_DOWNLOAD_ERROR_PROVISION=5          # error (provision)
io_download_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            url:URL à télécharger;
            output_name:nom du téléchargement;
            output_directory:dossier de destination;
            output_file:fichier de destination;
            overwrite_mode:avec/sans téléchargement, si fichier déjà présent;
            overwrite_key:condition de gestion du téléchargement;
            overwrite_value:valeur test de la condition (au delà téléchargement forcé);
            user:compte HTTP;
            password:mot de passe HTTP;
            common_save:Copier sur le dépôt;
            common_subdir:copie dans un sous-dossier du dépôt;
            verbose:Ajouter des détails sur les traitements
        ' \
        --args_m '
            url;output_directory
        ' \
        --args_v '
            overwrite_mode:no|yes|NEWER;
            overwrite_key:DATE|TIME;
            common_save:yes|no;
            verbose:yes|no
        ' \
        --args_d '
            overwrite_mode:yes;
            overwrite_key:DATE;
            common_save:yes;
            verbose:no
        ' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    _opts[ID]=-1                             # found file ID (not necessary newer!)
    _opts[FOUND]=0                           # (0) no, (1) output_directory, (2) common
    # deal space in URL, https://stackoverflow.com/questions/497908/is-a-url-allowed-to-contain-a-space
    _opts[URL]=${_opts[URL]// /%20}

    [ -z "${_opts[OUTPUT_FILE]}" ] && _opts[OUTPUT_FILE]=$(basename "${_opts[URL]}")
    [ -z "${_opts[OUTPUT_NAME]}" ] && _opts[OUTPUT_NAME]=${_opts[OUTPUT_FILE]}

    local -a _files=(
        [0]="${_opts[OUTPUT_DIRECTORY]}/${_opts[OUTPUT_FILE]}"
        [1]="$POW_DIR_COMMON_GLOBAL_SCHEMA"
    )
    [ -n "${_opts[COMMON_SUBDIR]}" ] && {
        mkdir -p "${_files[1]}/${_opts[COMMON_SUBDIR]}"
        _files[1]+="/${_opts[COMMON_SUBDIR]}"
    }
    _files[1]+="/${_opts[OUTPUT_FILE]}"
    [ "${_opts[VERBOSE]}" = yes ] && declare -p _opts _files

    # yes mode, nothing to test!
    [ "${_opts[OVERWRITE_MODE]}" = yes ] || {
        [ "${_opts[OVERWRITE_MODE]}" = NEWER ] &&
        [ -z "${_opts[OVERWRITE_VALUE]}" ] && {
            log_error 'valeur test de la condition non renseignée (option --overwrite_value)'
            return $POW_DOWNLOAD_ERROR_CONDITION
        }

        local _i
        for ((_i=0; _i<${#_files[@]}; _i++)); do
            # search for available data
            [ -f "${_files[$_i]}" ] || continue

            # file found
            _opts[ID]=$_i

            # no mode, break (already found)
            [ "${_opts[OVERWRITE_MODE]}" = no ] && {
                _opts[FOUND]=$((_i +1))
                break
            }

            # NEWER mode
            # last modification (of available data)
            local _epoch1=$(stat --format '%Y' "${_files[$_i]}")
            local _epoch2
            case ${_opts[OVERWRITE_KEY]} in
            DATE)
                # given date
                _epoch2=${_opts[OVERWRITE_VALUE]}
                ;;
            TIME)
                # now - given time
                _epoch2=$(($(date '+%s') - ${_opts[OVERWRITE_VALUE]}))
                ;;
            esac
            [ "${_opts[VERBOSE]}" = yes ] && {
                log_info "epoch(${_files[$_i]})=$_epoch1"
                log_info "epoch(OVERWRITE_VALUE)=$_epoch2"
            }
            #declare -p _i _epoch1 _epoch2
            [[ $_epoch1 -ge $_epoch2 ]] && {
                # available data is enough (not need to download again)
                _opts[FOUND]=$((_i +1))
                break
            }
        done

        # found localy (1), common (2)
        local _info
        case ${_opts[FOUND]} in
        1)
            _info="Téléchargement de ${_opts[OUTPUT_NAME]} inutile, car déjà présent"
            ;;
        2)
            cp "${_files[1]}" "${_opts[OUTPUT_DIRECTORY]}"
            _info="Téléchargement de ${_opts[OUTPUT_NAME]} inutile, car déjà présent dans le dépôt, copié dans ${_opts[OUTPUT_DIRECTORY]}"
            ;;
        esac

        [[ ${_opts[FOUND]} -gt 0 ]] && {
            log_info "$_info"
            return $POW_DOWNLOAD_ALREADY_AVAILABLE
        }
    }

    log_info "Téléchargement de ${_opts[OUTPUT_NAME]}"
    local _log_tmp_path="$POW_DIR_TMP/${_opts[OUTPUT_FILE]}.log"
    local _log_archive_path="$POW_DIR_ARCHIVE/${_opts[OUTPUT_FILE]}.log"
    # user/password
    local _user _password
    [ -n "${_opts[USER]}" ] && _user="--user ${_opts[USER]}"
    [ -n "${_opts[PASSWORD]}" ] && _password="--password ${_opts[PASSWORD]}"
    # temporary downloaded file
    local _tmp_path
    get_tmp_file --tmpfile _tmp_path
    [ "${_opts[VERBOSE]}" = yes ] && declare -p _log_tmp_path _log_archive_path _tmp_path

    wget \
        ${_opts[URL]} \
        --output-document "$_tmp_path" \
        --no-check-certificate \
        --progress=dot:mega \
        --retry-on-http-error=429,503 \
        --wait=10 \
        --random-wait \
        $_user \
        $_password \
        > "$_log_tmp_path" 2>&1 || {
            archive_file "$_log_tmp_path" &&
            log_error "Erreur lors du téléchargement de ${_opts[OUTPUT_NAME]}, veuillez consulter $_log_archive_path"
            [ -f "$_tmp_path" ] && rm --force "$_tmp_path"
            # use of previous file if present
            [ -f "${_files[0]}" ] && {
                log_info "Utilisation du fichier déjà présent pour contourner l'erreur de téléchargement"
                return $POW_DOWNLOAD_BUT_AVAILABLE
            }

            return $POW_DOWNLOAD_ERROR
        }

    [[ ${_opts[ID]} -gt -1 ]] && {
        # different from available version ?
        diff --brief "$_tmp_path" "${_files[${_opts[ID]}]}" > /dev/null
        [ $? -eq 0 ] && {
            log_info "Téléchargement de ${_opts[OUTPUT_NAME]} inutile, car sans changement"
            # update common
            [ -f "${_files[1]}" ] && {
                touch -m -r "$_tmp_path" "${_files[1]}" && rm "$_tmp_path"
            } || mv "$_tmp_path" "${_files[1]}"
            # copy on target (if not exists)
            [ ! -f "${_files[0]}" ] && cp "${_files[1]}" "${_opts[OUTPUT_DIRECTORY]}"
            archive_file "$_log_tmp_path"
            return $POW_DOWNLOAD_ALREADY_AVAILABLE
        }
    }

    # result
    {
        [ "${_opts[COMMON_SAVE]}" = no ] || {
            log_info "Copie de ${_opts[OUTPUT_FILE]} sur le Dépôt" &&
            cp "$_tmp_path" "${_files[1]}"
        }
    } &&
    mv "$_tmp_path" "${_files[0]}" &&
    archive_file "$_log_tmp_path" &&
    log_info "Téléchargement avec succès de ${_opts[OUTPUT_NAME]}" || return $POW_DOWNLOAD_ERROR_PROVISION

    return $POW_DOWNLOAD_OK
}

    #
    # ETL
    #

# BOM: byte-order mark
# https://learn.microsoft.com/fr-fr/globalization/encoding/byte-order-mark
remove_bom() {
    local -A _opts &&
    pow_argv \
        --args_n 'file_path:Chemin absolu vers le fichier à traiter;' \
        --args_m 'file_path' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    sed --in-place --expression '1s/^\xEF\xBB\xBF//' --expression '1s/^\xFF\xFE//' ${_opts[FILE_PATH]}
}

# import CSV into DB
import_csv_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
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
        --args_m 'file_path' \
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
        --pow_argv _opts "$@" || return $ERROR_CODE

    #echo ${FUNCNAME[0]} && declare -p _opts &&
    expect file "${_opts[FILE_PATH]}" &&
    _opts[FILE_NAME]=$(get_file_name --file_path "${_opts[FILE_PATH]}") &&
    _opts[FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FILE_PATH]}") &&
    _opts[FILE_TO_TMP]=no ||
    return $ERROR_CODE

    # only part of data?
    if [ -n "${_opts[FROM_LINE_NUMBER]}" ] || [ -n "${_opts[TO_LINE_NUMBER]}" ]; then
        [ "$POW_DEBUG" = yes ] && {
            echo "from_line_number=${_opts[FROM_LINE_NUMBER]}"
            echo "to_line_number=${_opts[TO_LINE_NUMBER]}"
        }

        _opts[FILE_NAME]+='.filtered'
        local _file_path="$POW_DIR_TMP/${_opts[FILE_NAME]}.${_opts[FILE_EXTENSION]}"
        if [ -n "${_opts[FROM_LINE_NUMBER]}" ] && [ -n "${_opts[TO_LINE_NUMBER]}" ]; then
            _opts[TO_LINE_NUMBER]=$((${_opts[TO_LINE_NUMBER]} - ${_opts[FROM_LINE_NUMBER]} + 1))
            tail \
                --lines=+${_opts[FROM_LINE_NUMBER]} \
                "${_opts[FILE_PATH]}" | head -${_opts[TO_LINE_NUMBER]} > "$_file_path"
        elif [ -n "${_opts[FROM_LINE_NUMBER]}" ]; then
            tail --lines=+${_opts[FROM_LINE_NUMBER]} "${_opts[FILE_PATH]}" > "$_file_path"
        elif [ -n "${_opts[TO_LINE_NUMBER]}" ]; then
            head --lines ${_opts[TO_LINE_NUMBER]} "${_opts[FILE_PATH]}" > "$_file_path"
        fi

        _opts[FILE_TO_TMP]=yes
        _opts[FILE_PATH]="$_file_path"
    fi

    local _delimiter_value
    if [ "${_opts[DELIMITER]}" = AUTODETECT ]; then
        # https://stackoverflow.com/questions/10806357/associative-arrays-are-local-by-default
        declare -A _tokens
        local _code
        for _code in ${!POW_DELIMITER[@]}; do
            # count number of tokens for each delimiter (into first line)
            # https://unix.stackexchange.com/questions/18736/how-to-count-the-number-of-a-specific-character-in-each-line
            _tokens[$_code]=$(head --lines 1 "${_opts[FILE_PATH]}" \
                | tr --delete --complement "${POW_DELIMITER[$_code]}\n" \
                | awk '{ print length }'
            )
            [ "$POW_DEBUG" = yes ] && echo "tokens[$_code]=${_tokens[$_code]}"
        done

        #echo '###DELIMITER/1' ; declare -p _tokens ; read

        local _ntokens=0
        for _code in ${!POW_DELIMITER[@]}; do
            [ ${_tokens[$_code]} -gt $_ntokens ] && {
                _ntokens=${_tokens[$_code]}
                _opts[DELIMITER]=$_code
            }
        done

        #echo '###DELIMITER/2' ; declare -p _opts ; read
    fi
    set_delimiter --delimiter_code "${_opts[DELIMITER]}" --delimiter_value _delimiter_value
    [ ${#_delimiter_value} -eq 0 ] && {
        log_error "Non détection du séparateur CSV"
        return $ERROR_CODE
    }
    [ "$POW_DEBUG" = yes ] && echo "delimiter_value=[$_delimiter_value]"

    if [ -z "${_opts[TABLE_NAME]}" ]; then
        execute_query \
            --name LABEL_TO_CODE \
            --query "SELECT public.label_to_code('${_opts[FILE_NAME]}')" \
            --with_log no \
            --return _opts[TABLE_NAME] || return $ERROR_CODE
    fi
    [ "$POW_DEBUG" = yes ] && echo "table_name=${_opts[TABLE_NAME]}"

    # encoding
    file --mime "${_opts[FILE_PATH]}" | grep --silent 'charset=iso-8859-1' && _opts[ENCODING]=LATIN1
    file --mime "${_opts[FILE_PATH]}" | grep --silent 'charset=utf-16le' && _opts[ENCODING]=UTF16
    # PostgreSQL doesn't stand up UTF16
    [ "${_opts[ENCODING]}" != UTF16 ] || {
        _opts[FILE_NAME]+='.to_utf8'
        local _file_path="$POW_DIR_TMP/${_opts[FILE_NAME]}.${_opts[FILE_EXTENSION]}"
        iconv --from-code UTF16 --to-code UTF8 "${_opts[FILE_PATH]}" > "$_file_path"
        [ "${_opts[FILE_TO_TMP]}" = yes ] && rm "${_opts[FILE_PATH]}"
        _opts[FILE_TO_TMP]=yes
        _opts[FILE_PATH]="$_file_path"
        _opts[ENCODING]=UTF8
    }

    local _table_columns_create
    if [ "${_opts[FILE_WITH_HEADER]}" = yes ]; then
        if [[ ${_opts[TABLE_COLUMNS]} =~ HEADER|HEADER_TO_LOWER_CODE ]]; then
                # convert to local encoding (UTF8)
                # remove BOM
                # remove CR (Windows)
                # search for (into 1st line)
                #  - [^'$_delimiter_value'"]": ending by double quote, not preceding by (delimiter or ")
                #  - '$_delimiter_value'[^"'$_delimiter_value']*: ending by delimiter, following all except (delimiter or ")
                #  - '$_delimiter_value'"[^"'$_delimiter_value']+": ending by (delimiter and "), following all except (delimiter or ")
            local _line_end_header=$(cat "${_opts[FILE_PATH]}" \
                | iconv --from-code ${_opts[ENCODING]} \
                | sed --expression 's/^\xEF\xBB\xBF//' \
                | sed --expression 's/\r//g' \
                | grep --max-count 1 --line-number --perl-regexp '([^"'$_delimiter_value']"|'$_delimiter_value'[^"'$_delimiter_value']*|'$_delimiter_value'"[^"'$_delimiter_value']+")$' \
                | cut --fields 1 --delimiter : \
            )
            [ -z "$_line_end_header" ] && _line_end_header=1

            _opts[TABLE_COLUMNS_LIST]=$(head --lines $_line_end_header "${_opts[FILE_PATH]}" \
                | iconv --from-code ${_opts[ENCODING]} \
                | sed --expression 's/^\xEF\xBB\xBF//' \
                | sed --expression 's/\r//g' \
            )

            #echo '###TABLE_COLUMNS_LIST/1' ; declare -p _opts ; read

            if [ "${_opts[TABLE_COLUMNS]}" = HEADER_TO_LOWER_CODE ]; then
                    # to lower
                    # w/o accent
                    # replace no-alphanum by _ (except delimiter)
                    # replace delimiter by ,
                    # trim _ (begin or end)
                _opts[TABLE_COLUMNS_LIST]=$(echo ${_opts[TABLE_COLUMNS_LIST]} \
                    | tr '[:upper:]' '[:lower:]' \
                    | sed 'y/àáâãäåçêéèëìíîïìñòóôõöùúûüýÿ/aaaaaaceeeeiiiiinooooouuuuyy/' \
                    | tr 'œ' 'oe' \
                    | tr 'æ' 'ae' \
                    | sed "s/[^a-z0-9${_delimiter_value}]\+/_/g" \
                    | sed "s/_\?${_delimiter_value}_\?/,/g" \
                    | sed 's/^_\?//g' \
                    | sed 's/_\?$//g' \
                )
            else
                    # replace delimiter by "," (so surround each column by ")
                    # add " at begin
                    # add " at end
                _opts[TABLE_COLUMNS_LIST]=$(echo ${_opts[TABLE_COLUMNS_LIST]} \
                    | sed --expression "s/\"\?${_delimiter_value}\"\?/\",\"/g" \
                    | sed --expression 's/^"\?/"/g' \
                    | sed --expression 's/"\?$/"/g' \
                )
            fi

            #echo '###TABLE_COLUMNS_LIST/2' ; declare -p _opts _delimiter_value ; read
        fi
        # each column as VARCHAR type
        _table_columns_create=$(echo "${_opts[TABLE_COLUMNS_LIST]}" \
            | sed "s/,/ VARCHAR,/g")' VARCHAR'
        # add SERIAL
        if [ "${_opts[ROWID]}" = yes ]; then
            _table_columns_create="rowid SERIAL,${_table_columns_create}"
        fi
        [ "$POW_DEBUG" = yes ] && echo "table_columns_create=${_table_columns_create}"
    fi

    local _table_to_load_exists=no
    local _schema_table="${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}"
    local _backup_post_data_full_path="$POW_DIR_TMP/${_schema_table}_post-data_$$.backup"
    table_exists --schema_name "${_opts[SCHEMA_NAME]}" --table_name "${_opts[TABLE_NAME]}" && _table_to_load_exists=yes
    if [ "$_table_to_load_exists" = yes ]; then
        [ "$POW_DEBUG" = yes ] && echo "load_mode=${_opts[LOAD_MODE]}"
        # only in APPEND mode (backup post-data); alternative: don't remove
        case "${_opts[LOAD_MODE]}" in
        OVERWRITE_DATA|APPEND)
            [ "${_opts[LOAD_MODE]}" = APPEND ] && {
                backup_table \
                    --schema_name "${_opts[SCHEMA_NAME]}" \
                    --table_name "${_opts[TABLE_NAME]}" \
                    --sections 'post-data' \
                    --output "$_backup_post_data_full_path" || return $ERROR_CODE
            }

            execute_query \
                --name "DROP_CONSTRAINTS_INDEXES_TRIGGERS_${_schema_table}" \
                --query "
                    SELECT public.drop_table_constraints('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}');
                    SELECT public.drop_table_indexes('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}');
                    SELECT public.drop_table_triggers('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}');
                    " || return $ERROR_CODE

            [ "${_opts[LOAD_MODE]}" = OVERWRITE_DATA ] && {
                execute_query \
                    --name "TRUNCATE_${_schema_table}" \
                    --query "TRUNCATE TABLE ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} CASCADE" || return $ERROR_CODE
            }
            ;;
        OVERWRITE_TABLE)
            execute_query \
                --name "DROP_${_schema_table}" \
                --query "DROP TABLE ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} CASCADE" || return $ERROR_CODE
            ;;
        esac
    fi
    if [ "$_table_to_load_exists" = no ] || [ "${_opts[LOAD_MODE]}" = OVERWRITE_TABLE ]; then
        if [ "${_opts[FILE_WITH_HEADER]}" != yes ] && [ -z "$_table_columns_create" ]; then
            log_error "Erreur lors de l'import de ${_opts[FILE_PATH]}, vous devez préciser le nom des colonnes cible, dans l'ordre des colonnes du fichier"
            return $ERROR_CODE
        fi
        execute_query \
            --name "CREATE_${_schema_table}" \
            --query "
                CREATE TABLE IF NOT EXISTS ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} ($_table_columns_create)
                " || return $ERROR_CODE
    fi

    local _file_with_header=$([ "${_opts[FILE_WITH_HEADER]}" = yes ] && echo TRUE || echo FALSE)
    # be careful starting SQL by \n
    # https://stackoverflow.com/questions/29632700/postgres-copy-syntax-error-in-sql-file
    local _query="
        \COPY ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} (${_opts[TABLE_COLUMNS_LIST]})
        FROM "
    [ -n "${_opts[LIMIT]}" ] && _query+=STDIN || _query+="'${_opts[FILE_PATH]}'"
    _query+="
        WITH (
            DELIMITER E'$_delimiter_value',
            FORMAT CSV,
            HEADER $_file_with_header,
            QUOTE '\"',
            ENCODING ${_opts[ENCODING]}
        )
    "
    # COPY command doesn't have \n (and blanks at beginning)
    _query=$(echo $_query | tr '\n' ' ' | sed --expression 's/^[ \t]*//')
    [ "$POW_DEBUG" = yes ] && echo "query=($_query)"

    #echo '###COPY' ; declare -p _opts _query ; read

    if [ -n "${_opts[LIMIT]}" ]; then
        # NOTE: ko if CR exist in values
        local _limit=${_opts[LIMIT]}
        [ "${_opts[FILE_WITH_HEADER]}" = yes ] && _limit=$((_limit+1))
        head --lines $_limit "${_opts[FILE_PATH]}" \
            | execute_query \
                --name "COPY_${_opts[TABLE_NAME]}_FROM_${_opts[FILE_NAME]}" \
                --query "$_query" || return $ERROR_CODE
    else
        execute_query \
            --name "COPY_${_opts[TABLE_NAME]}_FROM_${_opts[FILE_NAME]}" \
            --query "$_query" || return $ERROR_CODE
    fi

    # only in APPEND mode (to do by caller for others, sometimes need to delete duplicates before)
    if [ "$_table_to_load_exists" = yes ] && [ "${_opts[LOAD_MODE]}" = APPEND ]; then
        # restore contraints/indexes/triggers after loading data
        restore_table \
            --schema_name "${_opts[SCHEMA_NAME]}" \
            --table_name "${_opts[TABLE_NAME]}" \
            --sections 'post-data' \
            --input "$_backup_post_data_full_path" &&
        rm --force "$_backup_post_data_full_path" || return $ERROR_CODE
    fi

    # clean
    [ "${_opts[FILE_TO_TMP]}" = yes ] && rm "${_opts[FILE_PATH]}"

    return $SUCCESS_CODE
}

# tr EXCEL to CSV
excel_to_csv() {
    local -A _opts &&
    pow_argv \
        --args_n '
            from_file_path:Chemin absolu vers le fichier à traiter;
            to_file_path:Chemin absolu vers le fichier de sortie (ou STDOUT pour une sortie écran);
            worksheet_name:Nom de la feuille à extraire (si non précisé ce sera la feuille active à l ouverture du fichier);
            delimiter:Séparateur à utiliser pour la conversion vers CSV (ce caractère ne doit pas être utilisé dans les valeurs d entête)' \
        --args_m 'from_file_path' \
        --args_v '
            delimiter:'${POW_DELIMITER_JOIN_PIPE} \
        --args_d '
            delimiter:COMMA;
            to_file_path:INPUT' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -i _step=0
    local -a _steps=(
        "FROM_FILE_PATH existence"
        "FROM_FILE_PATH nom"
        "FROM_FILE_PATH extension"
        "TO_FILE_PATH cas spéciaux"
        "TO_FILE_PATH nom"
        "TO_FILE_PATH extension"
        "DELIMITER init"
        "MIME init"
        "MIME document"
        "DEBUG MIME"
        "MIME non reconnu, usage extension"
        "DEBUG extension"
        "WORKSHEET_NAME filtre, si renseigné"
        "LOG_INFO"
        "TMPFILE création"
        "SSCONVERT"
        "TO_FILE_PATH init"
        "STDOUT"
    )
    local _stdout=0 _delimiter_value _mime _spreadsheet _options _convert

    #echo ${FUNCNAME[0]} && declare -p _opts &&
    [ -f "${_opts[FROM_FILE_PATH]}" ] &&
    _step+=1 &&
    _opts[FROM_FILE_NAME]=$(get_file_name --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    _opts[FROM_FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    case "${_opts[TO_FILE_PATH]}" in
    STDOUT)
        _stdout=1
        _opts[TO_FILE_PATH]="$POW_DIR_TMP/STDOUT.$$.txt"
        ;;
    INPUT)
        _opts[TO_FILE_PATH]="$POW_DIR_TMP/${_opts[FROM_FILE_NAME]}.csv"
        ;;
    esac &&
    _step+=1 &&
    _opts[TO_FILE_NAME]=$(get_file_name --file_path "${_opts[TO_FILE_PATH]}") &&
    _step+=1 &&
    _opts[TO_FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[TO_FILE_PATH]}") &&
    _step+=1 &&
    set_delimiter --delimiter_code "${_opts[DELIMITER]}" --delimiter_value _delimiter_value &&
    _step+=1 &&
    # MIME type
    # https://stackoverflow.com/questions/7076042/what-mime-type-should-i-use-for-csv
    _mime=$(get_file_mimetype "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    case "$_mime" in
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet|application/vnd.ms-excel)
        _spreadsheet='MS Excel'
        ;;
    application/vnd.oasis.opendocument.spreadsheet)
        _spreadsheet='Open Office sheet'
        ;;
    esac &&
    _step+=1 &&
    {
        # debug
        ([ -z "$POW_DEBUG" ] || [ "$POW_DEBUG" = no ]) || {
            echo "spreadsheet (MIME)=$_mime"
        }
    } &&
    _step+=1 &&
    {
        [ -n "$_mime" ] || {
            case "${_opts[FROM_FILE_EXTENSION],,}" in
            xls|xlsx|ods)
                :
                ;;
            *)
                log_error "${FUNCNAME[0]}: le fichier source ${_opts[FROM_FILE_PATH]} ne semble pas être un classeur"
                false
                ;;
            esac
        }
    } &&
    _step+=1 &&
    {
        # debug
        ([ -z "$POW_DEBUG" ] || [ "$POW_DEBUG" = no ]) || {
            echo "spreadsheet (EXTENSION)=${_opts[FROM_FILE_EXTENSION]}"
        }
    } &&
    _step+=1 &&
    {
        _options="separator=$_delimiter_value format=preserve"
        [ -z "${_opts[WORKSHEET_NAME]}" ] || _options="sheet=${_opts[WORKSHEET_NAME]} $_options"
    } &&
    _step+=1 &&
    log_info "Conversion $_spreadsheet de ${_opts[FROM_FILE_PATH]} vers ${_opts[TO_FILE_PATH]}" &&
    _step+=1 &&
    get_tmp_file --tmpext txt --tmpfile _convert &&
    _step+=1 &&
    ssconvert \
        --export-options "$_options" \
        "${_opts[FROM_FILE_PATH]}" \
        "${_convert}" > $POW_DIR_ARCHIVE/ssconvert.log 2> $POW_DIR_ARCHIVE/ssconvert.error.log &&
    _step+=1 &&
    mv "$_convert" "${_opts[TO_FILE_PATH]}" &&
    _step+=1 &&
    {
        [[ $_stdout -eq 0 ]] || {
            [ -f "${_opts[TO_FILE_PATH]}" ] &&
            cat "${_opts[TO_FILE_PATH]}" &&
            rm "${_opts[TO_FILE_PATH]}"
        }
    } || {
        log_error "${FUNCNAME[0]}: étape #$_step (${_steps[$_step]}) en erreur"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# tr CSV to EXCEL
csv_to_excel() {
    local -A _opts &&
    pow_argv \
        --args_n '
            from_file_path:Chemin absolu vers le fichier à traiter;
            to_file_path:Chemin absolu vers le fichier de sortie' \
        --args_m 'from_file_path' \
        --args_d 'to_file_path:INPUT' \
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -i _step=0
    local _tmpfile _mime

    #echo ${FUNCNAME[0]} && declare -p _opts &&
    expect file "${_opts[FROM_FILE_PATH]}" &&
    _step+=1 &&
    _opts[FROM_FILE_NAME]=$(get_file_name --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    _opts[FROM_FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    case "${_opts[TO_FILE_PATH]}" in
    INPUT)
        _opts[TO_FILE_PATH]="$POW_DIR_TMP/${_opts[FROM_FILE_NAME]}.xls"
        ;;
    esac &&
    _step+=1 &&
    # MIME type
    # https://stackoverflow.com/questions/7076042/what-mime-type-should-i-use-for-csv
    _mime=$(get_file_mimetype "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    case "$_mime" in
    text/plain|text/csv|text/x-csv)
        # NULL command
        # https://www.shell-tips.com/bash/null-command
        :
        ;;
    *)
        log_error "${FUNCNAME[0]}: le fichier source ${_opts[FROM_FILE_PATH]} ne semble pas être un CSV"
        false
        ;;
    esac &&
    _step+=1 &&
    log_info "Conversion de ${_opts[FROM_FILE_PATH]} vers ${_opts[TO_FILE_PATH]}" &&
    _step+=1 &&
    _tmpfile="$POW_DIR_TMP/${_opts[FROM_FILE_NAME]}.csv_to_excel.txt" &&
    _step+=1 &&
    # protect alnum : starting w/ 0, including E (for exposant) as formula
    sed \
        --expression 's/\(\("0[0-9]\+"\)\|\("[0-9]\+E[0-9]\+"\)\)/"="\0""/g' \
        "${_opts[FROM_FILE_PATH]}" > "$_tmpfile" &&
    _step+=1 &&
    ssconvert "$_tmpfile" "${_opts[TO_FILE_PATH]}" > /dev/null 2>&1 &&
    _step+=1 &&
    rm --force "$_tmpfile" || {
        log_error "${FUNCNAME[0]}: étape #$_step en erreur"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# import EXCEL in DB (before converting as CSV)
import_excel_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
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
            limit:Limiter à n enregistrements;
            rowid:Générer un identifiant unique (rowid)' \
        --args_m 'file_path' \
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
        --pow_argv _opts "$@" || return $ERROR_CODE

    local -i _step=0
    local _tmpfile _sheet _list _limit _from _to

    #echo ${FUNCNAME[0]} && declare -p _opts &&
    expect file "${_opts[FILE_PATH]}" &&
    _step+=1 &&
    _opts[FILE_NAME]=$(get_file_name --file_path "${_opts[FILE_PATH]}") &&
    _step+=1 &&
    _opts[FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FILE_PATH]}") &&
    _step+=1 &&
    _tmpfile="$POW_DIR_TMP/${_opts[FILE_NAME]}.excel_to_csv.txt" &&
    _step+=1 &&
    # empty arguments
    {
        {
            [ -z "${_opts[WORKSHEET_NAME]}" ] || _sheet="--worksheet_name ${_opts[WORKSHEET_NAME]}"
        } &&
        {
            [ -z "${_opts[TABLE_COLUMNS_LIST]}" ] || _list="--table_columns_list ${_opts[TABLE_COLUMNS_LIST]}"
        } &&
        {
            [ -z "${_opts[LIMIT]}" ] || _limit="--limit ${_opts[LIMIT]}"
        } &&
        {
            [ -z "${_opts[FROM_LINE_NUMBER]}" ] || _from="--from_line_number ${_opts[FROM_LINE_NUMBER]}"
        } &&
        {
            [ -z "${_opts[TO_LINE_NUMBER]}" ] || _to="--to_line_number ${_opts[TO_LINE_NUMBER]}"
        }
    } &&
    _step+=1 &&
    excel_to_csv \
        --from_file_path "${_opts[FILE_PATH]}" \
        --to_file_path "$_tmpfile" \
        --delimiter "${_opts[DELIMITER]}" \
        "$_sheet" &&
    _step+=1 &&
    import_csv_file \
        --file_path "$_tmpfile" \
        --schema_name "${_opts[SCHEMA_NAME]}" \
        --table_name "${_opts[TABLE_NAME]}" \
        --delimiter "${_opts[DELIMITER]}" \
        --load_mode "${_opts[LOAD_MODE]}" \
        --table_columns "${_opts[TABLE_COLUMNS]}" \
        --rowid "${_opts[ROWID]}" \
        "$_list" \
        "$_limit" \
        "$_from" \
        "$_to" &&
    _step+=1 &&
    rm "$_tmpfile" &&
    return $SUCCESS_CODE || {
        log_error "${FUNCNAME[0]}: étape #$_step en erreur"
        return $ERROR_CODE
    }
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

    if [ -n "${_opts[ENCODING]}" ]; then
        _PGCLIENTENCODING_SAVE=$PGCLIENTENCODING
        export PGCLIENTENCODING=${_opts[ENCODING]}
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
    [ -n "${_opts[ENCODING]}" ] && PGCLIENTENCODING=$_PGCLIENTENCODING_SAVE

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

    local _mime=$(get_file_mimetype "${_opts[FROM_FILE_PATH]}") _type_import _type_file
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
        #declare -p tmp_liste_import_options
        {
            [[ ${#tmp_liste_import_options[@]} -eq 0 ]] || {
                for ((_i=0; _i<${#tmp_liste_import_options[@]}; _i++)); do
                    IFS='=' read -ra _opt <<< ${tmp_liste_import_options[$_i]}
                    _json_options[${_opt[0]}]=${_opt[1]}
                done
                _column_name=${_json_options[column_name]}
                #declare -p _json_options _column_name
            }
        } &&
        # column undefined?
        {
            [ -n "$_column_name" ] || {
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
            }
        } &&
        {
            [ "${load_mode}" = APPEND ] || {
                execute_query \
                    --name TABLE_TRUNCATE \
                    --query "TRUNCATE TABLE $schema_name.$table_name"
            }
        } &&
        {
            # fixed name log would be overwriten when session of multiple files, need to be saved
            jq --raw-output --compact-output '.' < "$file_path" | (execute_query \
                --name LOAD_JSON \
                --query "COPY $schema_name.$table_name (${_column_name}) FROM STDIN" || {
                    backup_file_as_uniq --path "$POW_DIR_ARCHIVE/LOAD_JSON-error.log"
                    false
                })
        }
        ;;
    *)
        log_error "Le fichier $file_path ne peut pas être traité (extension non gérée)!"
        false
        ;;
    esac || return $ERROR_CODE

    [ -n "$file_archive_extract_dir" ] && rm --recursive --force "$file_archive_extract_dir"

    return $SUCCESS_CODE
}
