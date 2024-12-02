#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BAL addresses

on_import_error() {
    bash_args \
        --args_p '
            vars:Entité des variables globales
        ' \
        --args_o '
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars

    # import created?
    [ "$POW_DEBUG" = yes ] && { echo "io_history_id=${_vars_ref[IO_ID]}"; }
    [ -n "${_vars_ref[IO_ID]}" ] && io_history_end_ko --id ${_vars_ref[IO_ID]}

    log_error "Erreur import BAL (${_vars_ref[IO_NAME]#*_})"
    exit $ERROR_CODE
}

# load BAL data (summary or one municipality)
bal_load() {
    bash_args \
        --args_p '
            vars:Entité des variables globales
        ' \
        --args_o '
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars
    local _query

    case "${_vars_ref[MUNICIPALITY_CODE]}" in
    ALL)
        _vars_ref[IO_NAME]=BAL_SUMMARY
        _vars_ref[URL_DATA]='api/communes-summary.csv'
        _query="SELECT COUNT(DISTINCT co_insee_commune) FROM fr.laposte_address_area WHERE fl_active"
        ;;
    *)
        _vars_ref[IO_NAME]=BAL_${_vars_ref[MUNICIPALITY_CODE]}
        _vars_ref[URL_DATA]='lookup/'BAL_${_vars_ref[MUNICIPALITY_CODE]}
        _query="
            SELECT areas + streets + housenumbers_auth
            FROM fr.bal_municipality WHERE code='${_vars_ref[MUNICIPALITY_CODE]}'"
        ;;
    esac

    io_todo_import \
        --force ${_vars_ref[FORCE]} \
        --name ${_vars_ref[IO_NAME]} \
        --date_end "${_vars_ref[IO_END]}"
    case $? in
    $POW_IO_SUCCESSFUL)
        return $SUCCESS_CODE
        ;;
    $POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
        on_import_error --vars _vars_ref
        ;;
    esac

    log_info "Import BAL (${_vars_ref[IO_NAME]#*_})" &&
    _vars_ref[TABLE_NAME]=tmp_${_vars_ref[IO_NAME],,} &&
    {
        _vars_ref[FILE_NAME]=$(basename "${_vars_ref[URL]}/${_vars_ref[URL_DATA]}") &&
        {
            [ "${_vars_ref[MUNICIPALITY_CODE]}" != ALL ] && {
                _vars_ref[FILE_NAME]+=.json &&
                execute_query \
                    --name BAL_TABLE_CREATE \
                    --query "
                        CREATE TABLE IF NOT EXISTS fr.{$_vars_ref[TABLE_NAME]} (data JSON)
                    " &&
                execute_query \
                    --name BAL_LAST_IO \
                    --query "SELECT (get_last_io('${_vars_ref[IO_NAME]}')).date_data_end" \
                    --psql_arguments 'tuples-only:pset=format=unaligned' \
                    --return _vars_ref[IO_BEGIN]
            } || true
        }
    } &&
    execute_query \
        --name BAL_IO_ROWS \
        --query "$_query" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _vars_ref[IO_ROWS] &&
    {
        [ -z "${_vars_ref[IO_ROWS]}" ] && {
            log_error "Import préalable de l'ensemble des Communes (--municipality ALL)"
            false
        } || true
    } &&
    io_history_begin \
        --name "${_vars_ref[IO_NAME]}" \
        --date_begin "${_vars_ref[IO_BEGIN]:-1970-01-01}" \
        --date_end "${_vars_ref[IO_END]}" \
        --nrows_todo ${_vars_ref[IO_ROWS]:-1} \
        --id _vars_ref[IO_ID] &&
    io_download_file \
        --url "${_vars_ref[URL]}/${_vars_ref[URL_DATA]}" \
        --overwrite yes \
        --output_directory "$POW_DIR_IMPORT" \
        --output_file "${_vars_ref[FILE_NAME]}" &&
    import_file \
        --file_path "$POW_DIR_IMPORT/${_vars_ref[FILE_NAME]}" \
        --table_name ${_vars_ref[TABLE_NAME]} \
        --load_mode OVERWRITE_DATA || on_import_error --vars _vars_ref

    return $SUCCESS_CODE
}

# integration BAL data (summary or one municipality)
bal_integration() {
    bash_args \
        --args_p '
            vars:Entité des variables globales
        ' \
        --args_o '
            vars
        ' \
        "$@" || return $ERROR_CODE

    local -n _vars_ref=$get_arg_vars

    table_exists --schema_name fr --table_name "${_vars_ref[TABLE_NAME]}" &&
    {
        case "${_vars_ref[MUNICIPALITY_CODE]}" in
        ALL)
            execute_query \
                --name BAL_INTEGRATION_${_vars_ref[IO_NAME]#*_} \
                --query "
                    SELECT drop_table_indexes('fr', 'bal_municipality');
                    TRUNCATE TABLE fr.bal_municipality;
                    INSERT INTO fr.bal_municipality(
                        code,
                        name,
                        population,
                        areas,
                        streets,
                        housenumbers,
                        housenumbers_auth,
                        last_update
                        )
                    SELECT
                        code_commune,
                        nom_commune,
                        population,
                        nb_lieux_dits,
                        nb_voies,
                        nb_numeros_certifies
                        composed_at
                    FROM
                        fr.tmp_bal_summary
                        ;
                    CALL fr.set_bal_municipality_index();
                    DROP TABLE fr.tmp_bal_summary;
                " &&
            io_history_end_ok \
                --nrows_processed '
                    (SELECT COUNT(*) FROM fr.bal_municipality)
                    ' \
                --id ${_vars_ref[IO_ID]} &&
            vacuum \
                --schema_name fr \
                --table_name bal_municipality \
                --mode ANALYZE
            ;;
        *)
            ;;
        esac
    }

    return $SUCCESS_CODE
}

bal_list_municipalities() {
    bash_args \
        --args_p '
            order:Ordre de sélection des communes;
            list:Liste résultat
        ' \
        --args_o '
            order;
            list
        ' \
        "$@" || return $ERROR_CODE}

    local -n _list_ref=$get_arg_list

    return $SUCCESS_CODE
}

# is code OK ?
valid_municipality_code() {
    bash_args \
        --args_p '
            municipality:Code Commune
        ' \
        --args_o '
            municipality
        ' \
        "$@" || return $ERROR_CODE

    local _valid

    execute_query \
        --name BAL_MUNICIPALITY_OK \
        --query "
            SELECT EXISTS(
                SELECT 1 FROM fr.laposte_address_area
                WHERE co_insee_commune = COALESCE('$get_arg_municipality', '99999') AND fl_active
            )" \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _valid || return $ERROR_CODE

    [ "$_valid" = f ] && {
        log_error "code Commune '$get_arg_municipality' non valide!"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

bash_args \
    --args_p '
        municipality:Code Commune INSEE à traiter (ou ALL pour télécharger la liste complète);
        limit:Limiter à n communes;
        stop_time:Heure d arrêt du traitement (format: hh:mm:ss);
        force:Forcer le traitement même si celui-ci a déjà été fait;
        verbose:Ajouter des détails sur les traitements
    ' \
    --args_o '
        municipality
    ' \
    --args_v '
        force:yes|no;
        verbose:yes|no
    ' \
    --args_d '
        force:no;
        limit:30;
        stop_time:0;
        verbose:no
    ' \
    "$@" || exit $ERROR_CODE

declare -A bal_vars=(
    [MUNICIPALITY_CODE]="${get_arg_municipality^^}"
    [URL]='https://plateforme.adresse.data.gouv.fr'
    [URL_DATA]=
    [IO_NAME]=
    [IO_ID]=
    [IO_BEGIN]=
    [IO_END]="$(date +'%F')"
    [IO_ROWS]=0
    [FILE_NAME]=
    [TABLE_NAME]=
    [LIMIT]=$get_arg_limit
    [STOP_TIME]=$get_arg_stop_time
    [FORCE]=$get_arg_force
    [VERBOSE]=$get_arg_verbose
)
declare -a bal_codes=()

set_env --schema_name fr &&
{
    [ "${bal_vars[MUNICIPALITY_CODE]}" = ALL ] && {
        bal_load --vars bal_vars &&
        bal_integration --vars bal_vars || exit $ERROR_CODE
    } || {
        bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
    }
} &&

_count=0
for ((_i=0; _i<${#bal_codes[@]}; _i++)); do
    _count=$((_count++))
    [[ ${bal_vars[LIMIT]} -gt 0 ]] &&
    [[ $_count -gt ${bal_vars[LIMIT]} ]] && {
        log_info "Limite '${bal_vars[LIMIT]}' atteinte: fin de traitement"
        exit $SUCCESS_CODE
    }

    valid_municipality_code --municipality "${bal_codes[$_i]}" || {
        log_error "commune BAL '${bal_codes[$_i]}' non valide!"
        continue
    }

    bal_vars[MUNICIPALITY_CODE]=${bal_codes[$_i]} &&
    bal_load --vars bal_vars &&
    bal_integration --vars bal_vars || exit $ERROR_CODE
done

exit $SUCCESS_CODE
