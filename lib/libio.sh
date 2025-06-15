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
            method
        ' \
        --pow_argv _opts "$@" || return $?

    local _query _return _output _with_log=no

    # DEBUG steps
    # example: export POW_DEBUG_JSON='{"codes":[{"name":"_io_history_manager","steps":["argv","query@break"]}]}'
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv query return output'

    [[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
        declare -p _opts
        [[ ${_debug_bps[argv]} -eq 0 ]] && read
    }

    case ${_opts[METHOD]} in
    EXISTS)
        local -n _io_id_manager=${_opts[ID]}
        _return='--return _io_id_manager'
        _query="
            SELECT ARRAY_AGG(id) FROM get_io(
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
        local _empty_json='{}'
        _query="
            UPDATE public.io_history SET
                attributes =
                    CASE
                    WHEN LENGTH('${_opts[INFOS]}') = 0 THEN attributes
                    ELSE
                        CASE
                        WHEN attributes IS JSON OBJECT THEN
                            (jsonb_merge(attributes::JSONB, '${_opts[INFOS]:-$_empty_json}'::JSONB))::VARCHAR
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

    [[ ${_debug_steps[query]:-1} -eq 0 ]] && {
        echo "query=($_query)"
        [[ ${_debug_bps[query]} -eq 0 ]] && read
    }

    execute_query \
        --name IO_${_opts[METHOD]}_${_opts[IO]:-${_opts[ID]}} \
        --query "$_query" \
        $_return \
        $_output \
        --with_log $_with_log || return $ERROR_CODE

    [ -n "$_return" ] &&
    {
        [[ ${_debug_steps[return]:-1} -eq 0 ]] && {
            echo "io_id=($_io_id_manager)"
            [[ ${_debug_bps[return]} -eq 0 ]] && read
        }
    }

    [ -n "$_output" ] &&
    {
        [[ ${_debug_steps[output]:-1} -eq 0 ]] && {
            echo 'OUTPUT:' ; cat ${_opts[OUTPUT]}
            [[ ${_debug_bps[output]} -eq 0 ]] && read
        }
    }

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
            status;
            date_end
        ' \
        --args_v '
            status:EN_COURS|SUCCES|ERREUR
        ' \
        --args_d '
            status:SUCCES
        ' \
        --args_p '
            tag:status@1N
        ' \
        --pow_argv _opts "$@" || return $?

    [ -n "${_opts[ID]}" ] && local -n _io_id=${_opts[ID]} || local _io_id

    _io_history_manager \
        --method EXISTS \
        --status ${_opts[STATUS]} \
        --io ${_opts[IO]} \
        --date_end "${_opts[DATE_END]}" \
        --id _io_id &&
    [ -n "$_io_id" ] &&
    {
        [ -z "${_opts[ID]}" ] ||
            # only last IO (w/ given date_end)
            local -a _array
            array_sql_to_bash \
                --array_sql "$_io_id" \
                --array_bash _array &&
            _io_id=${_array[0]}
    } || return $ERROR_CODE

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
        --args_p '
            tag:nrows_todo@int
        ' \
        --pow_argv _opts "$@" || return $?

    local -n _io_id=${_opts[ID]} _infos

    [ -z "${_opts[INFOS]}" ] || _infos="--infos ${_opts[INFOS]}"
    _io_history_manager \
        --method APPEND \
        --io ${_opts[IO]} \
        --status EN_COURS \
        --date_begin "${_opts[DATE_BEGIN]}" \
        --date_end "${_opts[DATE_END]}" \
        --nrows_todo ${_opts[NROWS_TODO]} \
        --id _io_id \
        $_infos || return $ERROR_CODE

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
        --args_p '
            tag:id@int
        ' \
        --pow_argv _opts "$@" || return $?

    local _infos

    [ -z "${_opts[INFOS]}" ] || _infos="--infos ${_opts[INFOS]}"
    _io_history_manager \
        --method UPDATE_OK \
        --nrows_processed "${_opts[NROWS_PROCESSED]}" \
        --id ${_opts[ID]} \
        $_infos || return $ERROR_CODE

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
        --args_p '
            tag:id@int
        ' \
        --pow_argv _opts "$@" || return $?

    _io_history_manager \
        --method UPDATE_KO \
        --id ${_opts[ID]} || return $ERROR_CODE

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
        --args_p '
            tag:id@int
        ' \
        --pow_argv _opts "$@" || return $?

    local _todo _processed _infos
    [ -n "${_opts[NROWS_TODO]}" ] && _todo="--nrows_todo ${_opts[NROWS_TODO]}"
    [ -n "${_opts[NROWS_PROCESSED]}" ] && _processed="--nrows_processed ${_opts[NROWS_PROCESSED]}"
    [ -n "${_opts[INFOS]}" ] && _infos="--infos ${_opts[INFOS]}"

    _io_history_manager \
        --method UPDATE \
        --id ${_opts[ID]} \
        $_infos \
        $_todo \
        $_processed || return $ERROR_CODE

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
            io;
            output
        ' \
        --pow_argv _opts "$@" || return $?

    _io_history_manager \
        --method EXPORT_LAST \
        --io ${_opts[IO]} \
        --output "${_opts[OUTPUT]}" || return $ERROR_CODE

    return $SUCCESS_CODE
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
        --args_p '
            tag:force@bool,purge@bool
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

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv todo hash string'

    [[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
        declare -p _opts
        [[ ${_debug_bps[argv]} -eq 0 ]] && read
    }

    get_tmp_file --tmpfile _tmpfile &&
    execute_query \
        --name "TODO-${_opts[IO]}" \
        --query "SELECT io_is_todo('${_opts[IO]}')" \
        --output $_tmpfile || return $ERROR_CODE

    [[ ${_debug_steps[todo]:-1} -eq 0 ]] && {
        cat $_tmpfile
        [[ ${_debug_bps[todo]} -eq 0 ]] && read
    }
    # each row contains: key=>value
    _hash_ref=()
    while read; do
        _hash_ref[${REPLY%=*}]=${REPLY#*>}
    done < <(sed --expression 's/"//g' --expression 's/,/\n/g' < $_tmpfile | sed --expression 's/^[ ]*//')
    [ -n "${_opts[TO_STRING]}" ] && _str_ref=$(< $_tmpfile)
    [[ ${_debug_steps[hash]:-1} -eq 0 ]] && {
        echo "hash=${_hash_ref[@]}"
        [[ ${_debug_bps[hash]} -eq 0 ]] && read
    }
    [[ ${_debug_steps[string]:-1} -eq 0 ]] && {
        echo "string=${!_str_ref}"
        [[ ${_debug_bps[string]} -eq 0 ]] && read
    }
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
        --args_p '
            tag:from@1N
        ' \
        --pow_argv _opts "$@" || return $POW_IO_ERROR

    # https://stackoverflow.com/questions/13219634/easiest-way-to-check-for-an-index-or-a-key-in-an-array
    # https://stackoverflow.com/questions/11180714/how-to-iterate-over-an-array-using-indirect-reference

    local -n _hash_ref=${_opts[HASH]}
    local -n _ids_ref=${_opts[IDS]}
    local _group _step _array_ptr _i _key _value
    local -a _steps

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv context ref value return'

    [[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
        declare -p _opts
        [[ ${_debug_bps[argv]} -eq 0 ]] && read
    }

    [ -n "${_opts[GROUP]}" ] && _group=${_opts[GROUP]} || _group=DEPENDS
    [[ $_group =~ DEPENDS|RESSOURCES ]] || _group+=_d
    _steps=(${_hash_ref[$_group]//:/ })

    [[ ${_debug_steps[context]:-1} -eq 0 ]] && {
        echo "group=($_group)" ; declare -p _steps
        [[ ${_debug_bps[context]} -eq 0 ]] && read
    }

    _ids_ref=''
    case "${_opts[FROM]}" in
    HASH)
        # no depends?
        [[ ! -v "_hash_ref[$_group]" ]] && return $SUCCESS_CODE
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

    # retrieve key/value for each depended item (of group), as JSON syntax
    _i=0
    [[ ${_debug_steps[ref]:-1} -eq 0 ]] && {
        echo "ref=${!_array_ptr}"
        [[ ${_debug_bps[ref]} -eq 0 ]] && read
    }
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

        [[ ${_debug_steps[value]:-1} -eq 0 ]] && {
            echo "i=${_i} step=($_step) k=($_key) v=($_value)"
            [[ ${_debug_bps[value]} -eq 0 ]] && read
        }

        _i=$((_i +1))
        # IO condition ?
        [ $_value -eq 0 ] && continue
        [ -n "${_opts[ITEM]}" ] && [ "${_opts[ITEM]}" != "$_key" ] && continue
        [ -n "$_ids_ref" ] && _ids_ref+=,
        _ids_ref+=$(printf '"%s":%d' $_key $_value)
    done
    [ -n "$_ids_ref" ] && _ids_ref="{${_ids_ref}}"

    [[ ${_debug_steps[return]:-1} -eq 0 ]] && {
        echo "ids=${_ids_ref}"
        [[ ${_debug_bps[return]} -eq 0 ]] && read
    }

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
        --pow_argv _opts "$@" || return $?

    local _product _extension _backup=2 _url_base _url_data _items
    local _re1 _re2 _re_search _re_file _re_item
    local -n _value_ref=${_opts[VALUE]}

    case "${_opts[NAME]}" in
    FR-TERRITORY-IGN)
        _product=ADMIN-EXPRESS
        _backup=3
        _extension=7z
        _url_base='https://geoservices.ign.fr'
        _url_data=${_url_base}'/adminexpress'
        _items='FXX|GLP|MTQ|GUF|REU|MYT'
        _re1='href="(http|ftp)[^"]+'${_product}'_(?(?!WM)[^"])+[0-9-]{10}\.'${_extension}'[^"]*'
        _re2='[0-9-]{10}'
        _re_search='href="(http|ftp)[^"]+'${_product}'_(?(?!WM)[^"])+#DATE\.'${_extension}'[^"]*'
        # file: <product>_<version>__<format>_<projection>_<item>_<date>.<extension>
        #+ only up to <projection> to search for specific item
        _re_file=${_product}'_[0-9]-[0-9]__(SHP|GPKG)_[^_]+'
        _re_item=${_re_file}_#ITEM
        ;;
    # obsolete now, IRIS-GE better
    FR-TERRITORY-IGN-IRIS)
        _product=IRIS
        _extension=7z
        _url_base='https://geoservices.ign.fr'
        _url_data=${_url_base}'/contoursiris#telechargement'
        _items='FXX|FRA'
        _re1='href="(http|ftp)[^" ]+CONTOURS-IRIS[^" ]*('${_items}')[^" ]*\.'${_extension}'[^" ]*"'
        _re2='[0-9]{4}-01-01'
        ;;
    FR-TERRITORY-IGN-IRIS_GE)
        _product=IRIS-GE
        _extension=7z
        _url_base='https://geoservices.ign.fr'
        _url_data=${_url_base}'/irisge#telechargement'
        _items='FXX|GLP|MTQ|GUF|REU|SPM|MYT|BLM|MAF'
        _re1='href="(http|ftp)[^" ]+'${_product}'[^" ]*_('${_items}')_[^" ]*\.'${_extension}'[^" ]*"'
        _re2='[0-9]{4}-01-01'
        _re_search='href="(http|ftp)[^" ]+'${_product}'[^" ]*_('${_items}')_#DATE\.'${_extension}'[^"]*'
        # file: <product>_<version>__<format>_<projection>_<region>_<date>.<extension>
        # sample: IRIS-GE_3-0__SHP_RGAF09UTM20_BLM_2024-01-01.7z
        _re_file=${_product}'_[0-9]-[0-9]__SHP_[^_]+'
        _re_item=${_re_file}_#ITEM
        ;;
    FR-TERRITORY-GOUV-EPCI)
        _product=EPCI
        _extension=xlsx
        _url_base='https://www.collectivites-locales.gouv.fr'
        _url_data=${_url_base}'/institutions/liste-et-composition-des-epci-fiscalite-propre'
        _re1='^[ ]+[0-9]{4}[ ]*'
        _re2='[0-9]{4}'
        _items='epcicom|epcisanscom'
        # NOTE /files/Accueil/DESL/2025/epcicom2025-2.xlsx (2nd version ?)
        _re_search='/files/Accueil/DESL/#DATE/('$_items')#DATE[^.]*\.'${_extension}
        _re_file='('${_items}')[0-9]{4}[^.]*\.'${_extension}
        _re_item=#ITEM
        ;;
    FR-TERRITORY-INSEE)
        _product=INSEE
        _extension=zip
        _url_base='https://www.insee.fr'
        _url_data=${_url_base}'/fr/information/7671844'
        _re1='-[0-9]{4}[^.]*\.'${_extension}
        _re2='[0-9]{4}'
        _items=communes
        _re_search='/fr/statistiques/fichier/7671844/table-appartenance-geo-'${_items}'-#DATE[^.]*\.'${_extension}
        _re_item=table-appartenance-geo-#ITEM
        ;;
    FR-MUNICIPALITY-EVENT-INSEE)
        _product=INSEE-EVENT
        _extension=zip
        _url_base='https://www.insee.fr'
        _url_data=${_url_base}'/fr/information/8377162'
        _re1='_[0-9]{4}_csv\.'${_extension}
        _re2='[0-9]{4}'
        _items=ensemble
        _re_search='/fr/statistiques/fichier/8377162/cog_'${_items}'_#DATE_csv\.'${_extension}
        ;;
    *)
        log_error "IO ${_opts[NAME]} non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    case ${_opts[KEY]^^} in
    PRODUCT)                _value_ref=$_product                ;;
    BACKUP)                 _value_ref=$_backup                 ;;
    EXTENSION)              _value_ref=$_extension              ;;
    URL_BASE)               _value_ref=$_url_base               ;;
    URL_DATA)               _value_ref=$_url_data               ;;
    REGEXP_SEARCH)          _value_ref=$_re_search              ;;
    REGEXP1)                _value_ref=$_re1                    ;;
    REGEXP_DATE)            _value_ref=$_re2                    ;;
    REGEXP2)                _value_ref=$_re2                    ;;
    REGEXP_FILE)            _value_ref=$_re_file                ;;
    REGEXP_ITEM)            _value_ref=$_re_item                ;;
    ITEMS)                  _value_ref=$_items                  ;;
    *)
        log_error "KEY ${_opts[KEY]} non pris en charge!"
        return $ERROR_CODE
        ;;
    esac

    return $SUCCESS_CODE
}

# get available dates (list, details as URL)
io_get_years_online_available() {
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
        --pow_argv _opts "$@" || return $?

    local _url _regexp1 _regexp2 _i
    local -n _details_file_ref=${_opts[DETAILS_FILE]}
    local -n _dates_ref=${_opts[DATES_LIST]}

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'func argv def dl date' &&
    # NOTE to debug:
    # export POW_DEBUG_JSON='{"codes":[{"name":"io_purge_common","steps":["io@break","item@break"]}]}'
    {
        [[ ${_debug_steps[func]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]}
            [[ ${_debug_bps[func]} -ne 0 ]] || read
        }
    } &&
    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
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

    {
        [[ ${_debug_steps[def]:-1} -ne 0 ]] || {
            echo "url=($_url)"
            echo "re1=($_regexp1)"
            echo "re2=($_regexp2)"
            [[ ${_debug_bps[def]} -ne 0 ]] || read
        }
    } &&
    # temporary file (to be deleted by caller)
    get_tmp_file --tmpext html --tmpfile _details_file_ref &&
    # download available dates
    io_download_file \
        --url "$_url" \
        --output_name ${_opts[NAME]} \
        --common_save no \
        --output_directory "$POW_DIR_TMP" \
        --output_file "$(basename $_details_file_ref)" &&
    {
        [[ ${_debug_steps[dl]:-1} -ne 0 ]] || {
            echo "html=($_details_file_ref)"
            [[ ${_debug_bps[dl]} -ne 0 ]] || read
        }
    } &&
    # array of available dates (desc), transforming / to -
    #+ RE can start w/ - (so add --)
    _dates_ref=($(grep --only-matching --perl-regexp -- "$_regexp1" $_details_file_ref | grep --only-matching --perl-regexp "$_regexp2" | sed --expression 's@/@-@g' | uniq | sort --reverse)) || {
        log_error "Impossible de consulter la liste des millésimes disponibles de ${_opts[NAME]}"
        return $ERROR_CODE
    }

    {
        [[ ${_debug_steps[date]:-1} -ne 0 ]] || {
            echo "dates=(${_dates_ref[@]})"
            [[ ${_debug_bps[date]} -ne 0 ]] || read
        }
    } &&
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
    {
        [[ ${_debug_steps[date]:-1} -ne 0 ]] || {
            echo "dates=(${_dates_ref[@]})"
            [[ ${_debug_bps[date]} -ne 0 ]] || read
        }
    } &&

    return $SUCCESS_CODE
}

# purge common (old files)
io_purge_common() {
    local -A _opts &&
    pow_argv \
        --args_n '
            name:nom IO à rechercher (sur dépôt);
            interactive:confirmer la suppression des fichiers obsolètes
        ' \
        --args_m '
            name
        ' \
        --args_v '
            interactive:no|yes
        ' \
        --args_d '
            interactive:no
        ' \
        --args_p '
            tag:interactive@bool
        ' \
        --pow_argv _opts "$@" || return $?

    local _regexp _ref_subscript _error=0 _rc _item
    local -a _files _items _deletes
    local -A _io

    # DEBUG steps
    local -A _debug_steps _debug_bps
    # NOTE to debug:
    # export POW_DEBUG_JSON='{"codes":[{"name":"io_purge_common","steps":["io@break","item@break"]}]}'
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'func argv io item files' &&
    {
        [[ ${_debug_steps[func]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]}
            [[ ${_debug_bps[func]} -ne 0 ]] || read
        }
    } &&
    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key PRODUCT                   \
        --value _io[PRODUCT]            &&
    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key BACKUP                    \
        --value _io[BACKUP]             &&
    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key REGEXP_ITEM               \
        --value _io[REGEXP_ITEM]        &&
    io_get_property_online_available    \
        --name ${_opts[NAME]}           \
        --key ITEMS                     \
        --value _io[ITEMS]              &&
    {
        [[ ${_debug_steps[io]:-1} -ne 0 ]] || {
            declare -p _io
            [[ ${_debug_bps[io]} -ne 0 ]] || read
        }
    } &&
    {
        # 'find ! newer' takes itself, so +1 and finally BACKUP
        _ref_subscript=${_io[BACKUP]}

#         [[ $_ref_subscript -eq 0 ]] || {
#             # array is 0-based
#             _ref_subscript=$((_ref_subscript -1))
#         }
    } &&
    {
        if [ -n "${_io[ITEMS]}" ]; then
            # convert as array
            _items=(${_io[ITEMS]//\|/ })
            for _item in "${_items[@]}"; do
                _regexp=${_io[REGEXP_ITEM]/\#ITEM/$_item} &&
                {
                    [[ ${_debug_steps[item]:-1} -ne 0 ]] || {
                        declare -p _items
                        echo "item=($_item)"
                        echo "regexp=$_regexp"
                        [[ ${_debug_bps[item]} -ne 0 ]] || read
                    }
                } &&
                # bash sort array of files by date
                _files=(
                    $(find \
                        $POW_DIR_COMMON_GLOBAL_SCHEMA \
                        -regextype posix-egrep \
                        -regex '.*'"${_regexp}"'.*' \
                        -printf "%T+ %p\n" | \
                    sort --reverse | \
                    cut --delimiter ' ' --field 2)
                ) &&
                {
                    [[ ${_debug_steps[files]:-1} -ne 0 ]] || {
                        declare -p _files
                        echo "subscript=(${_ref_subscript})"
                        echo "ref=(${_files[${_ref_subscript}]})"
                        [[ ${_debug_bps[files]} -ne 0 ]] || read
                    }
                } &&
                {
                    # exists obsolete file(s) ?
                    [[ ${#_files[@]} -le ${_io[BACKUP]} ]] || {
                        # reference is backup-th file, older can be deleted
                        # https://unix.stackexchange.com/questions/98877/trouble-getting-regex-to-work-with-find
                        _deletes=(
                            $(find \
                                $POW_DIR_COMMON_GLOBAL_SCHEMA \
                                -regextype posix-egrep \
                                -regex '.*'"${_regexp}"'.*' \
                                ! -newer "${_files[${_ref_subscript}]}")
                        ) &&
                        {
                            [ "${_opts[INTERACTIVE]}" = no ] || {
                                declare -p _deletes
                                echo -n "Effacer ces fichiers (O/N) : "
                                read
                                is_yes --var REPLY || continue
                            }
                        } &&
                        rm ${_deletes[*]}
                    }
                } || {
                    _error=1
                    log_error "effacement fichiers obsolètes (${_opts[NAME]}/$_item)"
                }
            done
        fi
    }

    _rc=$(( _error == 1 ? ERROR_CODE : SUCCESS_CODE ))
    return $_rc
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
            common_subdir:copie dans un sous-dossier du dépôt
        ' \
        --args_m '
            url;output_directory
        ' \
        --args_v '
            overwrite_mode:no|yes|NEWER;
            overwrite_key:DATE|TIME;
            common_save:yes|no
        ' \
        --args_d '
            overwrite_mode:yes;
            overwrite_key:DATE;
            common_save:yes
        ' \
        --args_p '
            tag:overwrite_mode@1N,overwrite_key@1N,common_save@bool
        ' \
        --pow_argv _opts "$@" || return $?

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv files overwrite context diff result'

    _opts[ID]=-1                             # found file ID (not necessary newer!)
    _opts[FOUND]=0                           # (0) no, (1) output_directory, (2) common
    # deal space in URL, https://stackoverflow.com/questions/497908/is-a-url-allowed-to-contain-a-space
    _opts[URL]=${_opts[URL]// /%20}
    [ -z "${_opts[OUTPUT_FILE]}" ] && _opts[OUTPUT_FILE]=$(basename "${_opts[URL]}")
    [ -z "${_opts[OUTPUT_NAME]}" ] && _opts[OUTPUT_NAME]=${_opts[OUTPUT_FILE]}
    [[ ${_debug_steps[argv]:-1} -eq 0 ]] && {
        declare -p _opts
        [[ ${_debug_bps[argv]} -eq 0 ]] && read
    }

    local -a _files=(
        [0]="${_opts[OUTPUT_DIRECTORY]}/${_opts[OUTPUT_FILE]}"
        [1]="$POW_DIR_COMMON_GLOBAL_SCHEMA"
    )
    [ -n "${_opts[COMMON_SUBDIR]}" ] && {
        mkdir -p "${_files[1]}/${_opts[COMMON_SUBDIR]}"
        _files[1]+="/${_opts[COMMON_SUBDIR]}"
    }
    _files[1]+="/${_opts[OUTPUT_FILE]}"
    [[ ${_debug_steps[files]:-1} -eq 0 ]] && {
        declare -p _files
        [[ ${_debug_bps[files]} -eq 0 ]] && read
    }

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
            [[ ${_debug_steps[overwrite]:-1} -eq 0 ]] && {
                log_info "epoch(${_files[$_i]})=$_epoch1"
                log_info "epoch(OVERWRITE_VALUE)=$_epoch2"
                [[ ${_debug_bps[overwrite]} -eq 0 ]] && read
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
    [[ ${_debug_steps[context]:-1} -eq 0 ]] && {
        declare -p _log_tmp_path _log_archive_path _tmp_path
        [[ ${_debug_bps[context]} -eq 0 ]] && read
    }

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

    [[ ${_debug_steps[diff]:-1} -eq 0 ]] && {
        echo 'DIFF...' ; declare -p _opts
        [[ ${_debug_bps[diff]} -eq 0 ]] && read
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
    [[ ${_debug_steps[result]:-1} -eq 0 ]] && {
        echo 'RESULT...'
        [[ ${_debug_bps[result]} -eq 0 ]] && read
    }
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
        --pow_argv _opts "$@" || return $?

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
        --args_p '
            tag:file_with_header@bool,table_columns@1N,load_mode@1N,delimiter@1N,limit@int,encoding@1N,rowid@bool
        ' \
        --pow_argv _opts "$@" || return $?

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv some delimiter table columns'
    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]} ; declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    expect file "${_opts[FILE_PATH]}" &&
    _opts[FILE_NAME]=$(get_file_name --file_path "${_opts[FILE_PATH]}") &&
    _opts[FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FILE_PATH]}") &&
    _opts[FILE_TO_TMP]=no ||
    return $ERROR_CODE

    # only part of data?
    if [ -n "${_opts[FROM_LINE_NUMBER]}" ] || [ -n "${_opts[TO_LINE_NUMBER]}" ]; then
        [[ ${_debug_steps[some]:-1} -eq 0 ]] && {
            echo "from_line_number=${_opts[FROM_LINE_NUMBER]}"
            echo "to_line_number=${_opts[TO_LINE_NUMBER]}"
            [[ ${_debug_bps[some]} -eq 0 ]] && read
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
        local -A _tokens
        local _code
        for _code in ${!POW_DELIMITER[@]}; do
            # count number of tokens for each delimiter (into first line)
            # https://unix.stackexchange.com/questions/18736/how-to-count-the-number-of-a-specific-character-in-each-line
            _tokens[$_code]=$(head --lines 1 "${_opts[FILE_PATH]}" \
                | tr --delete --complement "${POW_DELIMITER[$_code]}\n" \
                | awk '{ print length }'
            )
            [[ ${_debug_steps[delimiter]:-1} -eq 0 ]] && {
                echo "tokens[$_code]=${_tokens[$_code]}"
                [[ ${_debug_bps[delimiter]} -eq 0 ]] && read
            }
        done

        [[ ${_debug_steps[delimiter]:-1} -eq 0 ]] && {
            declare -p _tokens
            [[ ${_debug_bps[delimiter]} -eq 0 ]] && read
        }

        local _ntokens=0
        for _code in ${!POW_DELIMITER[@]}; do
            [ ${_tokens[$_code]} -gt $_ntokens ] && {
                _ntokens=${_tokens[$_code]}
                _opts[DELIMITER]=$_code
            }
        done

        [[ ${_debug_steps[delimiter]:-1} -eq 0 ]] && {
            declare -p _opts
            [[ ${_debug_bps[delimiter]} -eq 0 ]] && read
        }
    fi
    set_delimiter --delimiter_code "${_opts[DELIMITER]}" --delimiter_value _delimiter_value
    [ ${#_delimiter_value} -eq 0 ] && {
        log_error "Non détection du séparateur CSV"
        return $ERROR_CODE
    }
    [[ ${_debug_steps[delimiter]:-1} -eq 0 ]] && {
        echo "delimiter_value=($_delimiter_value)"
        [[ ${_debug_bps[delimiter]} -eq 0 ]] && read
    }

    if [ -z "${_opts[TABLE_NAME]}" ]; then
        # NOTE can't use _opts[TABLE_NAME] as return (circular trouble?) !
        local _table_name
        execute_query \
            --name LABEL_TO_CODE \
            --query "SELECT public.label_to_code('${_opts[FILE_NAME]}')" \
            --return _table_name || return $ERROR_CODE
        _opts[TABLE_NAME]=$_table_name
    fi
    [[ ${_debug_steps[table]:-1} -eq 0 ]] && {
        echo "table_name=(${_opts[TABLE_NAME]})"
        [[ ${_debug_bps[table]} -eq 0 ]] && read
    }

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

            [[ ${_debug_steps[columns]:-1} -eq 0 ]] && {
                declare _opts
                [[ ${_debug_bps[columns]} -eq 0 ]] && read
            }

            # NOTE
            #+ be careful w/ bash subprocess as result=$(command)
            #+ this replace TAB in space
            if [ "${_opts[TABLE_COLUMNS]}" = HEADER_TO_LOWER_CODE ]; then
                    # to lower
                    # w/o accent
                    # replace no-alphanum by _ (except delimiter)
                    # replace delimiter by ,
                    # trim _ (begin or end)
                _opts[TABLE_COLUMNS_LIST]=$(head --lines $_line_end_header "${_opts[FILE_PATH]}" \
                    | iconv --from-code ${_opts[ENCODING]} \
                    | sed --expression 's/^\xEF\xBB\xBF//' \
                    | sed --expression 's/\r//g' \
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
                _opts[TABLE_COLUMNS_LIST]=$(head --lines $_line_end_header "${_opts[FILE_PATH]}" \
                    | iconv --from-code ${_opts[ENCODING]} \
                    | sed --expression 's/^\xEF\xBB\xBF//' \
                    | sed --expression 's/\r//g' \
                    | sed --expression "s/\"\?${_delimiter_value}\"\?/\",\"/g" \
                    | sed --expression 's/^"\?/"/g' \
                    | sed --expression 's/"\?$/"/g' \
                )
            fi

            [[ ${_debug_steps[columns]:-1} -eq 0 ]] && {
                declare _opts
                [[ ${_debug_bps[columns]} -eq 0 ]] && read
            }
        fi
        # each column as VARCHAR type
        _table_columns_create=$(echo "${_opts[TABLE_COLUMNS_LIST]}" \
            | sed "s/,/ VARCHAR,/g")' VARCHAR'
        # add SERIAL
        if [ "${_opts[ROWID]}" = yes ]; then
            _table_columns_create="rowid SERIAL,${_table_columns_create}"
        fi
        [[ ${_debug_steps[columns]:-1} -eq 0 ]] && {
            echo "table_columns_create=(${_table_columns_create})"
            [[ ${_debug_bps[columns]} -eq 0 ]] && read
        }
    fi

    local _table_to_load_exists=no
    local _schema_table="${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}"
    local _backup_post_data_full_path="$POW_DIR_TMP/${_schema_table}_post-data_$$.backup"
    table_exists --schema_name "${_opts[SCHEMA_NAME]}" --table_name "${_opts[TABLE_NAME]}" && _table_to_load_exists=yes
    if [ "$_table_to_load_exists" = yes ]; then
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
    if [ -n "${_opts[LIMIT]}" ]; then
        # NOTE: ko if CR exist in values
        local _limit=${_opts[LIMIT]}
        [ "${_opts[FILE_WITH_HEADER]}" = yes ] && _limit=$((_limit+1))
        head --lines $_limit "${_opts[FILE_PATH]}" \
            | execute_query \
                --args_p 'tag:query@psql' \
                --name "COPY_${_opts[TABLE_NAME]}_FROM_${_opts[FILE_NAME]}" \
                --query "$_query" || return $ERROR_CODE
    else
        execute_query \
            --args_p 'tag:query@psql' \
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
        --args_p '
            tag:delimiter@1N
        ' \
        --pow_argv _opts "$@" || return $?

    local -i _step=0
    local -a _steps=(
        'FROM_FILE_PATH existence'
        'FROM_FILE_PATH nom'
        'FROM_FILE_PATH extension'
        'TO_FILE_PATH cas spéciaux'
        'TO_FILE_PATH nom'
        'TO_FILE_PATH extension'
        'DELIMITER init'
        'MIME init'
        'MIME document'
        'MIME non reconnu, usage extension'
        'WORKSHEET_NAME filtre, si renseigné'
        LOG_INFO
        'TMPFILE création'
        SSCONVERT
        'TO_FILE_PATH init'
        STDOUT
    )
    local _stdout=0 _delimiter_value _mime _spreadsheet _options _convert _log_echo

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv mime extension option'

    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]} ; declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    [ -f "${_opts[FROM_FILE_PATH]}" ] &&
    _step+=1 &&
    _opts[FROM_FILE_NAME]=$(get_file_name --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    _opts[FROM_FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    case "${_opts[TO_FILE_PATH]}" in
    STDOUT)
        _log_echo=$POW_LOG_ECHO ; set_log_echo no
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
    _mime=$(get_file_mimetype --file_path "${_opts[FROM_FILE_PATH]}") &&
    _step+=1 &&
    case "$_mime" in
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet|application/vnd.ms-excel)
        _spreadsheet='MS Excel'
        ;;
    application/vnd.oasis.opendocument.spreadsheet)
        _spreadsheet='Open Office sheet'
        ;;
    esac &&
    {
        [[ ${_debug_steps[mime]:-1} -ne 0 ]] || {
            echo "spreadsheet/MIME=($_mime)"
            [[ ${_debug_bps[mime]} -ne 0 ]] || read
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
    {
        [[ ${_debug_steps[extension]:-1} -ne 0 ]] || {
            echo "spreadsheet/EXTENSION=(${_opts[FROM_FILE_EXTENSION]})"
            [[ ${_debug_bps[extension]} -ne 0 ]] || read
        }
    } &&
    _step+=1 &&
    {
        _options="separator=$_delimiter_value format=preserve"
        [ -z "${_opts[WORKSHEET_NAME]}" ] || _options="sheet=${_opts[WORKSHEET_NAME]} $_options"
    } &&
    {
        [[ ${_debug_steps[option]:-1} -ne 0 ]] || {
            echo "option=(${_options})"
            [[ ${_debug_bps[option]} -ne 0 ]] || read
        }
    } &&
    _step+=1 &&
    log_info "Conversion $_spreadsheet de ${_opts[FROM_FILE_PATH]} vers ${_opts[TO_FILE_PATH]}" &&
    _step+=1 &&
    get_tmp_file --tmpext txt --tmpfile _convert &&
    _step+=1 &&
    # ssconvert --export-options 'separator=\t' --export-type=Gnumeric_stf:stf_assistant IN OUT
    ssconvert \
        --export-options "$_options" \
        "${_opts[FROM_FILE_PATH]}" \
        "${_convert}" > $POW_DIR_ARCHIVE/ssconvert.log 2> $POW_DIR_ARCHIVE/ssconvert.error.log &&
    _step+=1 &&
    mv "$_convert" "${_opts[TO_FILE_PATH]}" &&
    _step+=1 &&
    {
        [[ $_stdout -eq 0 ]] || {
            # restore log echo
            set_log_echo $_log_echo
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
        --pow_argv _opts "$@" || return $?

    local -i _step=0
    local -a _steps=(
        'FROM_FILE_PATH existence'
        'FROM_FILE_PATH nom'
        'FROM_FILE_PATH extension'
        'TO_FILE_PATH cas spéciaux'
        'MIME init'
        'MIME filtre'
        LOG_INFO
        'TMPFILE init'
        'SED protection champ numérique commençant par 0'
        SSCONVERT
        'TMPFILE effacement'
    )
    local _tmpfile _mime

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv'

    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]} ; declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    [ -f "${_opts[FROM_FILE_PATH]}" ] &&
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
    _mime=$(get_file_mimetype --file_path "${_opts[FROM_FILE_PATH]}") &&
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
        log_error "${FUNCNAME[0]}: étape #$_step (${_steps[$_step]}) en erreur"
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
            delimiter:COMMA;
            load_mode:OVERWRITE_DATA;
            rowid:yes' \
        --args_p '
            tag:delimiter@1N,table_columns@1N,load_mode@1N,rowid@bool
        ' \
        --pow_argv _opts "$@" || return $?

    local -i _step=0
    local -a _steps=(
        'FILE_PATH existence'
        'FILE_PATH nom'
        'FILE_PATH extension'
        'TMPFILE init'
        'POW_ARGV arguments vides'
        EXCEL_TO_CSV
        IMPORT_CSV_FILE
        'TMPFILE effacement'
    )
    local _tmpfile _sheet _list _limit _from _to

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv context' &&
    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]} ; declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    [ -f "${_opts[FILE_PATH]}" ] &&
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
    {
        [[ ${_debug_steps[context]:-1} -ne 0 ]] || {
            echo "sheet=($_sheet)"
            echo "columns=${_list}"
            echo "limit=${_limit}"
            echo "from_number=${_from}"
            echo "to_number=${_to}"
            [[ ${_debug_bps[context]} -ne 0 ]] || read
        }
    } &&
    _step+=1 &&
    excel_to_csv \
        --from_file_path "${_opts[FILE_PATH]}" \
        --to_file_path "$_tmpfile" \
        --delimiter "${_opts[DELIMITER]}" \
        $_sheet &&
    _step+=1 &&
    import_csv_file \
        --file_path "$_tmpfile" \
        --schema_name "${_opts[SCHEMA_NAME]}" \
        --table_name "${_opts[TABLE_NAME]}" \
        --delimiter "${_opts[DELIMITER]}" \
        --load_mode "${_opts[LOAD_MODE]}" \
        --table_columns "${_opts[TABLE_COLUMNS]}" \
        --rowid "${_opts[ROWID]}" \
        $_list \
        $_limit \
        $_from \
        $_to &&
    _step+=1 &&
    rm "$_tmpfile" || {
        log_error "${FUNCNAME[0]}: étape #$_step (${_steps[$_step]}) en erreur"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# import GEO (as shapefile, ...)
import_geo_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin absolu vers le fichier à traiter;
            schema_name:Nom du schema cible;
            table_name:Nom de la table cible;
            layers:Nom de(s) couche(s) GPKG à créer (liste séparée avec espace si multiple) ;
            password:Mot de passe;
            load_mode:Mode de chargement des données;
            encoding:Encodage de caractères;
            from_srid:Identifiant du système de projection des objets géographiques;
            to_srid:Identifiant du système de reprojection des objets géographiques;
            geometry_type:Type des objets geographiques;
            spatial_index:Indique si il faut créer un index géographique;
            limit:Limiter à n enregistrements;
            rowid:Générer un identifiant unique rowid' \
        --args_m 'file_path;table_name' \
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
        --args_p '
            tag:load_mode@1N,encoding@1N,geometry_type@1N,spatial_index@bool,rowid@bool,limit@int
        ' \
        --pow_argv _opts "$@" || return $?

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

    local -i _step=0
    local -a _steps=(
        'FILE_PATH existence'
        'FILE_PATH nom'
        'FILE_PATH extension'
        'OGR2OGR mode'
        TABLE_NAME
        'FILE_EXTENSION filtre'
        'TMPFILE log'
        'OGR2OGR arguments'
        'FILE_EXTENSION mif'
        'ENCODING init'
        PASSWD
        OGR2OGR
        'ENCODING reset'
        'RETURN CODE'
        ROWID
        'TMPFILE archivage'
    )
    local _load_mode_ogr2ogr _logfile _ogr_args _layer_creation_options _rc

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv' &&
    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]} ; declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    [ -f "${_opts[FILE_PATH]}" ] &&
    _step+=1 &&
    _opts[FILE_NAME]=$(get_file_name --file_path "${_opts[FILE_PATH]}") &&
    _step+=1 &&
    _opts[FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FILE_PATH]}") &&
    _step+=1 &&
    case "${_opts[LOAD_MODE]}" in
    OVERWRITE_DATA|OVERWRITE_TABLE)
        _load_mode_ogr2ogr=overwrite
        ;;
    APPEND)
        _load_mode_ogr2ogr=append
        ;;
    esac &&
    _step+=1 &&
    {
        # eventually no table name! w/ GPKG, many layers can be loaded together
        [ "${_opts[FILE_EXTENSION]}" = gpkg ] || {
            [ -n "${_opts[TABLE_NAME]}" ] || {
                execute_query \
                    --name LABEL_TO_CODE \
                    --query "SELECT public.label_to_code('${_opts[FILE_NAME]}')" \
                    --with_log no \
                    --return _opts[TABLE_NAME]
            }
        }
    } &&
    _step+=1 &&
    {
        # FIXME: try to run each word (shp, ...) as command!
        #[[ ! ${_opts[FILE_EXTENSION]} =~ shp|mif|dbf|json|gpkg ]] && {
        echo ${_opts[FILE_EXTENSION]} | grep --perl-regexp --silent 'shp|mif|dbf|json|gpkg' || {
            log_error "Le Fichier ${_opts[FILE_PATH]} n'a pas une extension gérée (shp,mif,dbf,json,gpkg)"
            false
        }
    } &&
    _step+=1 &&
    get_tmp_file --tmpfile _logfile --create yes --tmpext log &&
    _step+=1 &&
    # ogr2ogr arguments
    # http://www.bostongis.com/PrinterFriendly.aspx?content_name=ogr_cheatsheet
    # -t_srs srs_def : Reproject/transform to this SRS on output
    # -s_srs srs_def : Override source SRS
    {
        [ -z "${_opts[TO_SRID]}" ] || _ogr_args+=" -t_srs ${_opts[TO_SRID]}"
        [ -z "${_opts[FROM_SRID]}" ] || _ogr_args+=" -s_srs ${_opts[FROM_SRID]}"
        [ -z "${_opts[LIMIT]}" ] || _ogr_args+=" -limit ${_opts[LIMIT]}"
        [ -z "${_opts[TABLE_NAME]}" ] || _ogr_args+=" -nln ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}"

        _layer_creation_options='-lco GEOMETRY_NAME=geom'
        [ "$spatial_index" = yes ] || _layer_creation_options+=' -lco SPATIAL_INDEX=no'
        if [ "${_opts[FILE_EXTENSION]}" = gpkg ]; then
            #_layer_creation_options+=' -lco FID=fid'
            [ -z "${_opts[LAYERS]}" ] || _layer_creation_options+=" ${_opts[LAYERS]}"
        else
            _layer_creation_options+=' -lco FID=rowid'
        fi
    }
    _step+=1 &&
    {
        [ "${_opts[FILE_EXTENSION]}" != mif ] || {
            # NOTE: remains original dbf (not one this created by ogr2ogr), so use temporary directory
            local _mif_dir=$(dirname "${_opts[FILE_PATH]}")
            local _mif_to_shp_dir=$_mif_dir/mif_to_shp
            local _log_mif_to_shp="$POW_DIR_TMP/mif_to_shp_${_opts[FILE_NAME]}.log"

            mkdir --parents $_mif_to_shp_dir &&
            ogr2ogr \
                -f 'ESRI Shapefile' \
                $_mif_to_shp_dir \
                ${_opts[FILE_PATH]} > "$_log_mif_to_shp" 2>&1

            if [ $? -eq 0 ] && [ -n "$(grep --max-count 1 ERROR $_log_mif_to_shp)" ]; then
                log_error "Erreur lors de la conversion de ${_opts[FILE_NAME]} en shapefile, voir $_log_mif_to_shp"
                false
            else
                archive_file "$_log_mif_to_shp" &&
                log_info "Conversion avec succès de ${_opts[FILE_NAME]} en shapefile" &&
                # NOTE: copy only new files (so origin dbf is not replaced)
                mv --no-clobber $_mif_to_shp_dir/* $_mif_dir/ &&
                rm --recursive $_mif_to_shp_dir &&
                _opts[FILE_PATH]="$_mif_dir/${_opts[FILE_NAME]}.shp"
            fi
        }
    } &&
    _step+=1 &&
    {
        [ -z "${_opts[ENCODING]}" ] || {
            _PGCLIENTENCODING_SAVE=$PGCLIENTENCODING
            export PGCLIENTENCODING=${_opts[ENCODING]}
        }
    } &&
    _step+=1 &&
    {
        [ -n "${_opts[PASSWORD]}" ] || {
            local _passwd
            get_pg_passwd --user_name $POW_PG_USERNAME --password _passwd &&
            _opts[PASSWORD]=$_passwd || {
                log_error "Erreur de récupération du mot de passe (user=$POW_PG_USERNAME)"
                false
            }
        }
    } &&
    _step+=1 &&
    {
        ogr2ogr \
            -f "PostgreSQL" \
            PG:"host=$POW_PG_HOST user=$POW_PG_USERNAME dbname=$POW_PG_DBNAME password=${_opts[PASSWORD]}" \
            "${_opts[FILE_PATH]}" \
            -$_load_mode_ogr2ogr \
            -nlt ${_opts[GEOMETRY_TYPE]} \
            $_ogr_args \
            $_layer_creation_options 2> "$_logfile"
        _rc=$?
    } &&
    _step+=1 &&
    # restore previous encoding
    {
        [ -z "${_opts[ENCODING]}" ] || PGCLIENTENCODING=$_PGCLIENTENCODING_SAVE
    } &&
    # returns OK even if encoding error, so search for ERROR
    _step+=1 &&
    {
        ([ $_rc -eq 0 ] && [ -z "$(grep --max-count 1 ERROR $_logfile)" ]) || {
            log_error "Erreur lors de l'import de ${_opts[FILE_NAME]}, voir $_logfile"
            false
        }
    } &&
    _step+=1 &&
    {
        [ "${_opts[FILE_EXTENSION]}" = gpkg ] || {
            [ "${_opts[ROWID]}" = yes ] || {
                execute_query \
                    --name DROP_COLUMN_ROWID \
                    --query "ALTER TABLE ${_opts[TABLE_NAME]} DROP COLUMN IF EXISTS rowid"
            }
        }
    } &&
    _step+=1 &&
    archive_file "$_logfile" || {
        log_error "${FUNCNAME[0]}: étape #$_step (${_steps[$_step]}) en erreur"
        return $ERROR_CODE
    }

    log_info "Import avec succès de ${_opts[FILE_NAME]}"
    return $SUCCESS_CODE
}

# import file into DB
import_file() {
    local -A _opts &&
    pow_argv \
        --args_n '
            file_path:Chemin absolu vers le fichier à importer;
            schema_name:Nom du schema cible;
            table_name:Nom de la table cible;
            load_mode:Mode de chargement des données;
            import_options:Options d import du fichier spécifiques à son format;
            limit:Limiter a n enregistrements;
            rowid:Générer un identifiant unique rowid' \
        --args_m 'file_path' \
        --args_v '
            load_mode:OVERWRITE_DATA|OVERWRITE_TABLE|APPEND;
            rowid:yes|no' \
        --args_d '
            schema_name:'$POW_PG_DEFAULT_SCHEMA';
            load_mode:OVERWRITE_DATA;
            rowid:yes' \
        --args_p '
            tag:load_mode@1N,limit@int,rowid@bool
        ' \
        --pow_argv _opts "$@" || return $?

    local -i _step=0
    local -a _steps=(
        'FILE_PATH existence'
        'FILE_PATH nom'
        'FILE_PATH extension'
        'ARCHIVE traitement'
        'TABLE_NAME init'
        'IMPORT_OPTIONS init'
        'LIMIT init'
        'TYPE_IMPORT MIME'
        'TYPE_IMPORT FILE_EXTENSION'
        IMPORT_FILE
        'ARCHIVE purge'
    )
    local -a _list_options=()
    local _extract_dir _import_options _limit _type_import _table_name

    # DEBUG steps
    local -A _debug_steps _debug_bps
    get_env_debug \
        ${FUNCNAME[0]} \
        _debug_steps \
        _debug_bps \
        'argv table context' &&
    {
        [[ ${_debug_steps[argv]:-1} -ne 0 ]] || {
            echo ${FUNCNAME[0]} ; declare -p _opts
            [[ ${_debug_bps[argv]} -ne 0 ]] || read
        }
    } &&
    [ -f "${_opts[FILE_PATH]}" ] &&
    _step+=1 &&
    _opts[FILE_NAME]=$(get_file_name --file_path "${_opts[FILE_PATH]}") &&
    _step+=1 &&
    _opts[FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FILE_PATH]}") &&
    _step+=1 &&
    {
        (! is_archive --archive_path "${_opts[FILE_PATH]}") || {
            local _files _error=yes _msg

            _extract_dir="$POW_DIR_TMP/${_opts[FILE_NAME]}" &&
            rm --recursive --force "$_extract_dir" &&
            mkdir "$_extract_dir" &&
            extract_archive \
                --archive_path "${_opts[FILE_PATH]}" \
                --extract_path "$_extract_dir" &&
            _files=($(ls -1 "$_extract_dir"/*)) &&
            case ${#_files[@]} in
            0)
                _msg="Auncun fichier trouvé dans l'archive ${_opts[FILE_PATH]}, un attendu"
                ;;
            1)
                _error=no
                ;;
            *)
                _msg="Plusieurs fichiers trouvés dans l'archive ${_opts[FILE_PATH]}, un seul attendu"
                ;;
            esac &&
            {
                if [ "$_error" = yes ]; then
                    log_error "$_msg"
                    false
                else
                    _opts[FILE_PATH]=${_files[0]} &&
                    _opts[FILE_NAME]=$(get_file_name --file_path "${_opts[FILE_PATH]}") &&
                    _opts[FILE_EXTENSION]=$(get_file_extension --file_path "${_opts[FILE_PATH]}")
                fi
            }
        }
    } &&
    _step+=1 &&
    {
        [ -n "${_opts[TABLE_NAME]}" ] || {
            execute_query \
                --name LABEL_TO_CODE \
                --query "SELECT public.label_to_code('${_opts[FILE_NAME]}')" \
                --with_log no \
                --return _table_name &&
            _opts[TABLE_NAME]=$_table_name
        }
    } &&
    {
        [[ ${_debug_steps[table]:-1} -ne 0 ]] || {
            echo "table_name=(${_opts[TABLE_NAME]})"
            [[ ${_debug_bps[table]} -ne 0 ]] || read
        }
    } &&
    # options
    _step+=1 &&
    {
        [ -z "${_opts[IMPORT_OPTIONS]}" ] || {
            local _option _key _value
            local _prefix
            IFS=';' read -ra _list_options <<< "${_opts[IMPORT_OPTIONS]}"
            for _option in "${_list_options[@]}"; do
                _key=$(echo $_option | grep --only-matching '^[^:]*')
                _value=$(echo $_option | grep --only-matching '[^:]*$')
                if [ $(expr length $_key) -eq 1 ]; then
                    _prefix='-'
                else
                    _prefix='--'
                fi
                if [ -n "${_import_options}" ]; then
                    _prefix=" ${_prefix}"
                fi
                _import_options+="${_prefix}${_key} ${_value}"
            done
        }
    } &&
    # limit
    _step+=1 &&
    {
        [ -z "${_opts[LIMIT]}" ] || _limit="--limit ${_opts[LIMIT]}"
    } &&
    _step+=1 &&
    {
        local _mime=$(get_file_mimetype --file_path "${_opts[FILE_PATH]}")
        case "$_mime" in
        text/plain|text/csv|text/x-csv)
            _type_import=CSV
            ;;
        application/vnd.openxmlformats-officedocument.spreadsheetml.sheet|application/vnd.ms-excel|application/vnd.oasis.opendocument.spreadsheet)
            _type_import=SPREADSHEET
            ;;
        application/*dbf*|application/octet-stream)
            local _type_file=$(file "${_opts[FILE_PATH]}" | cut --delimiter : --fields 2)
            _type_file=${_type_file,,}
            [[ $_type_file =~ esri[[:space:]]shapefile|dbase|json ]] && _type_import=GEO
            ;;
        application/*json*)
            _type_import=JSON
            ;;
        esac
    } &&
    _step+=1 &&
    {
        [ -n "$_type_import" ] || {
            case "${_opts[FILE_EXTENSION]},,}" in
            txt|[cdt]sv)    _type_import=CSV            ;;
            shp|dbf)        _type_import=GEO            ;;
            json)           _type_import=JSON           ;;
            xls|xlsx|ods)   _type_import=SPREADSHEET    ;;
            esac
        }
    } &&
    {
        [[ ${_debug_steps[context]:-1} -ne 0 ]] || {
            echo "type_import=($_type_import)"
            echo "options=${_import_options}"
            echo "limit=${_opts[LIMIT]}"
            echo "rowid=${_opts[ROWID]}"
            [[ ${_debug_bps[context]} -ne 0 ]] || read
        }
    } &&
    _step+=1 &&
    case "$_type_import" in
    CSV)
        import_csv_file \
            --file_path "${_opts[FILE_PATH]}" \
            --schema_name "${_opts[SCHEMA_NAME]}" \
            --table_name "${_opts[TABLE_NAME]}" \
            --load_mode "${_opts[LOAD_MODE]}" \
            --rowid "${_opts[ROWID]}" \
            $_limit \
            $_import_options
        ;;
    SPREADSHEET)
        import_excel_file \
            --file_path "${_opts[FILE_PATH]}" \
            --schema_name "${_opts[SCHEMA_NAME]}" \
            --table_name "${_opts[TABLE_NAME]}" \
            --load_mode "${_opts[LOAD_MODE]}" \
            --rowid "${_opts[ROWID]}" \
            $_limit \
            $_import_options
        ;;
    GEO)
        import_geo_file \
            --file_path "${_opts[FILE_PATH]}" \
            --schema_name "${_opts[SCHEMA_NAME]}" \
            --table_name "${_opts[TABLE_NAME]}" \
            --load_mode "${_opts[LOAD_MODE]}" \
            --rowid "${_opts[ROWID]}" \
            $_limit \
            $_import_options
        ;;
    JSON)
        local _i _column_name
        local -a _opt
        local -A _json_options
        #declare -p _list_options
        {
            [[ ${#_list_options[@]} -eq 0 ]] || {
                for ((_i=0; _i<${#_list_options[@]}; _i++)); do
                    IFS='=' read -ra _opt <<< ${_list_options[$_i]}
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
                    --query "
                        SELECT get_table_columns('${_opts[SCHEMA_NAME]}', '${_opts[TABLE_NAME]}')
                    " \
                    --return _columns_str &&
                array_sql_to_bash --array_sql "$_columns_str" --array_bash _columns_array &&
                {
                    [[ ${#_columns_array[@]} -eq 1 ]] || {
                        log_error "Table de chargement JSON (${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}) ne doit avoir qu'une colonne de type JSON!"
                        false
                    }
                } &&
                _column_name=${_columns_array[0]}
            }
        } &&
        {
            [ "${_opts[LOAD_MODE]}" = APPEND ] || {
                execute_query \
                    --name TABLE_TRUNCATE \
                    --query "TRUNCATE TABLE ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]}"
            }
        } &&
        {
            # fixed name log would be overwriten when session of multiple files, need to be saved
            jq --raw-output --compact-output '.' < "${_opts[FILE_PATH]}" | (execute_query \
                --name LOAD_JSON \
                --query "COPY ${_opts[SCHEMA_NAME]}.${_opts[TABLE_NAME]} (${_column_name}) FROM STDIN" || {
                    backup_file_as_uniq --path "$POW_DIR_ARCHIVE/LOAD_JSON-error.log"
                    false
                })
        }
        ;;
    *)
        log_error "Le fichier ${_opts[FILE_PATH]} ne peut pas être traité (extension non gérée)!"
        false
        ;;
    esac &&
    # purge
    _step+=1 &&
    {
        [ -z "$_extract_dir" ] || {
            rm --recursive --force "$_extract_dir"
        }
    } || {
        log_error "${FUNCNAME[0]}: étape #$_step (${_steps[$_step]}) en erreur"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}
