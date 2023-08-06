#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # update territories w/ altitude (if available)

bash_args \
    --args_p '
        force:Forcer le traitement même si celui-ci a déjà été fait;
        use_cache:Utiliser les données présentes dans le cache
    ' \
    --args_v '
        force:yes|no;
        use_cache:yes|no
    ' \
    --args_d '
        force:no;
        use_cache:yes
    ' \
    "$@" || exit $ERROR_CODE

set_env --schema_name fr &&
execute_query \
    --name "PREPARE_TERRITORY_ALTITUDE" \
    --query "
        CREATE TABLE IF NOT EXISTS fr.municipality_altitude AS
            WITH
            municipality_namesake AS (
                SELECT
                    name
                FROM
                    public.territory
                WHERE
                    country = 'FR'
                    AND
                    level = 'COM'
                GROUP BY
                    name
                HAVING
                    COUNT(*) > 1
            )
            SELECT
                t.code, t.name municipality
                , CASE WHEN mns.name IS NULL THEN NULL::VARCHAR
                ELSE
                    d.name
                END department
                , CASE WHEN g.name IS NULL THEN NULL::VARCHAR
                ELSE
                    CONCAT(REGEXP_REPLACE(REGEXP_REPLACE(t.name, '^[^0-9]*', ''), ' A', '_a'), '_de_', g.name)
                END district
                , NULL::INT z_min, NULL::INT z_max
                , FALSE done
            FROM public.territory t
                LEFT OUTER JOIN municipality_namesake mns ON t.name = mns.name
                CROSS JOIN get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'COM', t.code), 'DEP')) d
                LEFT OUTER JOIN get_territory_from_query(get_query_territory_extended_to_level('fr', get_query_territory('fr', 'COM', t.code), 'COM_GLOBALE_ARM')) g ON TRUE
            WHERE t.country = 'FR' AND t.level = 'COM'
        " &&
_territory_list=$POW_DIR_TMP/territory_altitude.txt &&
execute_query \
    --name "TODO_TERRITORY_ALTITUDE" \
    --query "
        COPY (
            SELECT
                code
                , municipality
                , department
                , district
            FROM
                fr.municipality_altitude
            WHERE
                NOT done
        ) TO STDOUT WITH (DELIMITER E':', FORMAT CSV, HEADER FALSE, ENCODING UTF8)
    " \
    --output $_territory_list || exit $ERROR_CODE

_territory_cache="$POW_DIR_COMMON_GLOBAL_SCHEMA/wikipedia"
mkdir -p "$_territory_cache"
while IFS=: read _code _name _department _district; do
    [ -n "$_district" ] && _url=$_district || _url=$_name
    [ -n "$_department" ] && _url+="_($_department)"
    _url='https://fr.wikipedia.org/wiki/'${_url}
    _file=$(basename $_url)
    ([ "$get_arg_use_cache" = no ] || [ ! -s "$_territory_cache/$_file" ]) && {
        curl --output "$_territory_cache/$_file" $_url || {
            _error=$?
            log_error "téléchargement $_file en erreur ($_error)"
            continue
        }
    }
    [ -s "$_territory_cache/$_file" ] && {
        # delete optional HTML numerical code (&#160; for &nbsp;)
        # see: https://www.leptidigital.fr/productivite/caracteres-speciaux-html-2-19297/
        _min=$(sed --expression 's/&#[0-9]*;//g' "$_territory_cache/$_file" | grep --only-matching --perl-regexp 'Min\.[ ]*[ 0-9]*' | grep --only-matching --perl-regexp '[ 0-9]*')
        _max=$(sed --expression 's/&#[0-9]*;//g' "$_territory_cache/$_file" | grep --only-matching --perl-regexp 'Max\.[ ]*[ 0-9]*' | grep --only-matching --perl-regexp '[ 0-9]*')
        # delete potential space
        _min=${_min// }
        _max=${_max// }
        echo "$_file ($_code) min=$_min max=$_max"
        execute_query \
            --name "UDPATE_TERRITORY_ALTITUDE" \
            --query "
                UPDATE fr.municipality_altitude SET
                    z_min = ${_min:-NULL::INT}
                    , z_max = ${_max:-NULL::INT}
                    , done = TRUE
                WHERE
                    code = '$_code'
            " || {
            log_error "Mise à jour $_code en erreur"
        }
    }
done < $_territory_list

execute_query \
    --name "IS_OK_TERRITORY_ALTITUDE" \
    --query "SELECT EXISTS(SELECT 1 FROM fr.municipality_altitude WHERE NOT done)" \
    --psql_arguments 'tuples-only:pset=format=unaligned' \
    --return _territory_ko || exit $ERROR_CODE
is_yes --var _territory_ko && {
    log_error "Mise à jour Altitudes des communes en erreur!"
    exit $ERROR_CODE
} || {
    set_env --schema_name public &&
    execute_query \
        --name "SET_MUNICIPALITY_ALTITUDE" \
        --query "
            UPDATE public.territory t SET
                z_min = ma.z_min
                , z_max = ma.z_max
            FROM fr.municipality_altitude ma
            WHERE
                t.country = 'FR' AND t.level = 'COM' AND t.code = ma.code
        " || exit $ERROR_CODE
}
rm $_territory_list
log_info "Mise à jour Altitudes des communes avec succès"

exit $SUCCESS_CODE
