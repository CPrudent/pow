#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # import BAL addresses

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

    # is code OK ?
    execute_query \
        --name OK_MUNICIPALITY_CODE \
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

on_import_error() {
    bash_args \
        --args_p '
            vars:Entité des variables globales;
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
        verbose:no
    ' \
    "$@" || exit $ERROR_CODE

declare -A bal_vars=(
    [MUNICIPALITY_CODE]="${get_arg_municipality^^}"
    [URL]='https://plateforme.adresse.data.gouv.fr'
    [URL_DATA]=
    [IO_NAME]=
    [IO_ID]=
    [IO_BEGIN]='1970-01-01'
    [IO_END]="$(date +'%F %T.%N')"
    [FILE_NAME]=
    [TABLE_NAME]=
    [LIMIT]=$get_arg_limit
    [STOP_TIME]=$get_arg_stop_time
    [FORCE]=$get_arg_force
    [VERBOSE]=$get_arg_verbose
)
declare -a bal_codes

set_env --schema_name fr
case "${bal_vars[MUNICIPALITY_CODE]}" in
ALL)
    bal_vars[IO_NAME]=BAL_SUMMARY
    bal_vars[URL_DATA]='api/communes-summary.csv'
    ;;
*)
    valid_municipality_code --municipality "${bal_vars[MUNICIPALITY_CODE]}" || exit $ERROR_CODE
    bal_vars[IO_NAME]=BAL_${bal_vars[MUNICIPALITY_CODE]}
    bal_vars[URL_DATA]='lookup/'BAL_${bal_vars[MUNICIPALITY_CODE]}
    bal_codes[0]=${bal_vars[MUNICIPALITY_CODE]}
    ;;
esac

io_todo_import \
    --force ${bal_vars[FORCE]} \
    --name ${bal_vars[IO_NAME]} \
    --date_end "${bal_vars[IO_END]}"
case $? in
$POW_IO_SUCCESSFUL)
    exit $SUCCESS_CODE
    ;;
$POW_IO_IN_PROGRESS | $POW_IO_ERROR | $ERROR_CODE)
    on_import_error --vars bal_vars
    ;;
esac

log_info "Import BAL (${bal_vars[IO_NAME]#*_})" &&
bal_vars[TABLE_NAME]=tmp_${bal_vars[IO_NAME],,} &&
{
    bal_vars[FILE_NAME]=$(basename "${bal_vars[URL]}/${bal_vars[URL_DATA]}")
    [ -z "$(get_file_extension --file_path "${bal_vars[FILE_NAME]}")" ] &&
    bal_vars[FILE_NAME]+=.json || true
} &&
io_history_begin \
    --name "${bal_vars[IO_NAME]}" \
    --date_begin "${bal_vars[IO_BEGIN]}" \
    --date_end "${bal_vars[IO_END]}" \
    --nrows_todo 35000 \
    --id bal_vars[IO_ID] &&
io_download_file \
    --url "${bal_vars[URL]}/${bal_vars[URL_DATA]}" \
    --overwrite yes \
    --output_directory "$POW_DIR_IMPORT" \
    --output_file "${bal_vars[FILE_NAME]}" &&
import_file \
    --file_path "$POW_DIR_IMPORT/${bal_vars[FILE_NAME]}" \
    --table_name ${bal_vars[TABLE_NAME]} \
    --load_mode OVERWRITE_DATA &&

    || on_import_error --vars bal_vars


exit $SUCCESS_CODE
