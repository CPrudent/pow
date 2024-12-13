    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define STD

# best practices: see https://gist.github.com/outro56/4a2403ae8fefdeb832a5
set -o pipefail

    #
    # log
    #

set_log_active() {
    POW_LOG_ACTIVE=$1
}

set_log_echo() {
	POW_LOG_ECHO=$1
}

[ ! -f "$POW_DIR_LOG/$POW_LOG_FILE" ] && set_log_active no
[ -z "$POW_LOG_ACTIVE" ] && set_log_active yes
[ -z "$POW_LOG_ECHO" ] && set_log_echo yes

log() {
    local _severity=${1:-info}
    local _message=$(echo $2 | tr \| \_)
    local _state=$3
    local _command="$(realpath $0)"
    local _log_entry="$(date --utc +%FT%TZ)|$1|$$|${USER}|$_command|$_message"
    ([ "$POW_LOG_ECHO" = yes ] || [ "$_severity" = error ]) && echo $_log_entry
    [ "$POW_LOG_ACTIVE" = yes ] && echo $_log_entry >> $POW_DIR_LOG/$POW_LOG_FILE

    return $SUCCESS_CODE
}

log_info() {
    log info "$1" "$2"

    return $?
}

log_error() {
    log error "$1" "$2"

    return $?
}

    #
    # general
    #

# print expression and wait ENTER
breakpoint() {
    echo "###BREAK($1)"
    read
}

# expect expression
# in: $1=(argc, isnum, env, file) as (#args, is numeric, variable is defined, file exists)
expect() {
    local _ctrl _var

    case $1 in

        # expect argc func $# nr_args
    argc)
        if [ $3 -ne $4 ]; then
            log_error "$2: argument ($3/$4)"
            return $ERROR_CODE
        fi
        ;;

        # expect isnum <data>
    isnum)
        expect argc isnum $# 2 || return $ERROR_CODE

            # Search for none numeric character (return null if not found)
        _ctrl=$(expr "$2" : '.*\([^0-9]\).*' \| "")
        if [ ! -z "$_ctrl" ]; then
            log_error "variable non numerique ($2)"
            return $ERROR_CODE
        fi
        ;;

        # expect env <var>
    env)
        expect argc env $# 2 || return $ERROR_CODE

        _var=$(eval echo \${$2})
        [ -z "$_var" ] && {
            log_error "manque variable ($2)"
            return $ERROR_CODE
        }
        ;;

        # expect file <path>
    file)
        expect argc env $# 2 || return $ERROR_CODE

        [ ! -f "$2" ] && {
            log_error "manque fichier ($2)"
            return $ERROR_CODE
        }
        ;;

    esac

    return $SUCCESS_CODE
}

# build index on array w/ an associative index array (key=value, value=position)
array_index() {
    bash_args \
        --args_p '
            array:Tableau;
            index:Index du tableau
        ' \
        --args_o '
            array;
            index
        ' \
        "$@" || return $ERROR_CODE

    local -n _array_ref=$get_arg_array
    local -n _index_ref=${get_arg_index}
    local _i

    for _i in "${!_array_ref[@]}"; do
        _index_ref["${_array_ref[$_i]}"]=$_i
    done

    return $SUCCESS_CODE
}

# item in array
# https://stackoverflow.com/questions/8082947/how-to-pass-an-array-to-a-bash-function
# optional 3rd argument gives ID of searched item, as: in_array ARRAY STR_TO_SEARCH ID
# FIX due to error!
# another solution w/ print
# https://stackoverflow.com/questions/3685970/check-if-a-bash-array-contains-a-value
in_array() {
    bash_args \
        --args_p '
            array:Tableau;
            item:Elèment recherché;
            search:Recherche Clé/Valeur;
            position:Position de l élément trouvé
        ' \
        --args_o '
            array;
            item
        ' \
        --args_v '
            search:KEY|VALUE
        ' \
        --args_d '
            search:VALUE
        ' \
        "$@" || return $ERROR_CODE

    local -n _array_ref=$get_arg_array
    local _rc

    [ ${#_array_ref[@]} -eq 0 ] && return 1
    case "$get_arg_search" in
    VALUE)
        _pos=$(printf '%s\n' "${_array_ref[@]}" | grep --fixed-strings --line-number --line-regexp -- "$get_arg_item")
        ;;
    KEY)
        _pos=$(printf '%s\n' "${!_array_ref[@]}" | grep --fixed-strings --line-number --line-regexp -- "$get_arg_item")
        ;;
    esac
    _rc=$?
    # returning position ?
    [ -n "$get_arg_position" ] && {
        local -n _pos_ref=$get_arg_position
        # exists item ?
        [ $_rc -eq 0 ] && {
            _pos_ref=${_pos%:*}
            # decrement because grep is 1-base
            ((--_pos_ref))
            return 0
        } || {
            _pos_ref=-1
        }
    }

    # found
    [ $_rc -eq 0 ] && return 0
    # not found
    return 1
}

# FIX ME: syntax error near unexpected token `('
# clone_array() {
#     bash_args \
#         --args_p '
#             from_array:Tableau à cloner;
#             to_array:Tableau cloné
#         ' \
#         --args_o '
#             from_array;
#             to_array
#         ' \
#         "$@" || return $ERROR_CODE
#
#     local -n _array_ref=$get_arg_to_array
#     local _tmp=$(declare -p $get_arg_from_array)
#
#     echo "$_tmp"
#     eval "${_tmp/${get_arg_from_array}=/${_array_ref}=}" &&
#     declare -p ${_array_ref}
#
#     return $SUCCESS_CODE
# }

# eval duration of treatment (w/ its beginning)
get_elapsed_time() {
    bash_args \
        --args_p '
            start:Horodatage du début de traitement;
            result:Durée calculée
        ' \
        --args_o '
            start;
            result
        ' \
        "$@" || return $ERROR_CODE

    local -n _result_ref=$get_arg_result
    local _end=$(date +%s)
    _result_ref="$((($_end-$get_arg_start)/3600))h:$((($_end-$get_arg_start)%3600/60))m:$((($_end-$get_arg_start)%60))s"

    return $SUCCESS_CODE
}

# check mandatory root
is_user_root() {
    [ "$USER" != root ] && {
        log_error "Ce script est à exécuter par l'utilisateur root"
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

# check user exists
user_exists() {
    getent passwd $1 > /dev/null 2&>1
    return $?
}

# OK if var contains a Yes value
is_yes() {
    bash_args \
        --args_p '
            var:Variable à tester
        ' \
        --args_o '
            var
        ' \
        "$@" || return $ERROR_CODE

    local -n _var_ref=$get_arg_var

    [[ $_var_ref =~ ^(yes|YES|y|Y|oui|OUI|o|O|ok|OK|t|T|true|TRUE)$ ]] && return $SUCCESS_CODE
    return $ERROR_CODE
}

# define delimiter w/o worry of bash_args!
set_delimiter() {
    bash_args \
        --args_p '
            delimiter_code:Code délimiteur;
            delimiter_value:Valeur délimiteur' \
        --args_o '
            delimiter_code;
            delimiter_value' \
        "$@" || return $ERROR_CODE

    local -n _delimiter_ref=$get_arg_delimiter_value
    # https://linuxhint.com/associative_array_bash/
    [ ${POW_DELIMITER[$get_arg_delimiter_code]+_} ] && {
        _delimiter_ref=${POW_DELIMITER[$get_arg_delimiter_code]}
    } || {
        return $ERROR_CODE
    }

    return $SUCCESS_CODE
}

    #
    # command line
    #

# getopts improved (w/ list of values, default value, ...)
# voir wiki https://wiki.net.extra.laposte.fr/confluence/pages/viewpage.action?pageId=824282297
bash_args() {
    local get_arg_help=
    local get_arg_interactif=
    #a voir : local get_arg_verbose=

    local tmp_arg_name
    local tmp_arg_value

    local get_arg_args_p
    local tmp_liste_args_p=()
    local tmp_arg_p
    local tmp_arg_p_name
    local tmp_arg_p_libelle
    local tmp_arg_p_value
    local tmp_arg_p_help
    local tmp_arg_p_ok

    local get_arg_args_o=
    local tmp_liste_args_o=()
    local tmp_liste_args_o_or
    local tmp_arg_o
    local tmp_arg_o_or_name
    local tmp_arg_o_ok

    local get_arg_args_v=
    local tmp_liste_args_v=()
    local tmp_arg_v
    local tmp_arg_v_name
    local tmp_arg_v_values
    local tmp_liste_arg_v_values
    local tmp_arg_v_value
    local tmp_arg_v_ok

    local get_arg_args_d=
    local tmp_liste_args_d=()
    local tmp_arg_d
    local tmp_arg_d_name
    local tmp_arg_d_value

    # tableau associatif local des {clé/valeur} des arguments passés
    declare -A _argv

    while echo "$1" | grep -q ^--; do
        tmp_arg_name=$(echo "$1" | sed 's/^--//')
        #echo "argument $tmp_arg_name"
        tmp_arg_value=
        #eval get_arg_$tmp_arg_name=$tmp_arg_value

        shift
        while echo $(eval echo \"$1\") | grep -q ^[^--]; do
            #echo "valeur $1"
            if [ -z "$tmp_arg_value" ]; then
                tmp_arg_value="$1"
            else
                tmp_arg_value="$tmp_arg_value $1"
            fi
            shift
        done
        if [ -z "$1" ]; then
            #si l'argument est sans valeur / valeur vide, alors on passe à l'argument suivant
            shift
        fi
        #PROVOQUE UN COMPORTEMENT INNATENDU (cf set_pg_cluster, à considérer sur tous les rappels de fonctions repassant tous les paramètres)
        #si l'argument est présent, mais sans valeur, on considère que c'est un indicateur type OUI/NON et que sa précense vaut OUI(YES/Y)
        if [ -z "$tmp_arg_value" ]; then
            # le retour des paramètres dans un tableau nécessite le nom de ce tableau
            if [ "$tmp_arg_name" = bash_args_argv ]; then
                >&2 echo "L'argument $tmp_arg_name nécessite une valeur! : --$tmp_arg_name <argv>"
                return 1
            fi
            # sans valeur, l'argument est considéré comme booléen (Y/N)
            if [ "$tmp_arg_name" = 'help' ] || [ "$tmp_arg_name" = 'interactif' ]; then
                tmp_arg_value='Y'
            fi
        fi
        #echo "$tmp_arg_name = $tmp_arg_value"
        #On fait en sorte de ne pas évaluer les variables utilisées dans la valeur en préfixant les $ par un \
        #car sinon pose problème pour la définition des valeurs par défaut avec l'utilisation de variables faisant référence à d'autres paramètres
        #et on entoure la valeur par des doubles côtes pour permettre que la valeur contienne une simple cote
        tmp_arg_value=${tmp_arg_value//$/\\$}
        #protection des doubles cotes
        tmp_arg_value=${tmp_arg_value//\"/\\\"}
        eval get_arg_$tmp_arg_name=\"$tmp_arg_value\"
        #anciennement :
            #eval get_arg_$tmp_arg_name=\'$tmp_arg_value\'

        # range les arguments dans le tableau associatif local
        _argv+=([$tmp_arg_name]=$tmp_arg_value)

        if [ -z "$get_arg_args_p" ]; then
            echo "Veuillez définir les argument possibles en premier paramètres avec args_p, exemple : --args_p 'nom_argument1:libelle;nom_argument2:libelle'"
            return 1
        elif [ "$tmp_arg_name" = 'args_p' ]; then
            IFS=';' read -ra tmp_liste_args_p <<< "${get_arg_args_p}"
            for tmp_arg_p in "${tmp_liste_args_p[@]}"; do
                tmp_arg_p_name=$(echo $tmp_arg_p | grep -o '^[^:]*')
                #echo "Init argument $tmp_arg_name2 à vide"
                eval get_arg_$tmp_arg_p_name=''
            done
        elif [ "$tmp_arg_name" != 'help' ] && [ "$tmp_arg_name" != 'interactif' ] && [ "$tmp_arg_name" != 'args_o' ] && [ "$tmp_arg_name" != 'args_v' ] && [ "$tmp_arg_name" != 'args_d' ]; then
            tmp_arg_p_ok='N'
            for tmp_arg_p in "${tmp_liste_args_p[@]}"; do
                tmp_arg_p_name=$(echo $tmp_arg_p | grep -o '^[^:]*')
                if [ "$tmp_arg_name" = "$tmp_arg_p_name" -o "$tmp_arg_name" = bash_args_argv ]; then
                    tmp_arg_p_ok='Y'
                    break
                fi
            done
            if [ "$tmp_arg_p_ok" = 'N' ]; then
                >&2 echo "L'argument $tmp_arg_name ne fait pas partie des arguments possibles"
                return 1
            fi
        fi
    done

    if [ -n "$get_arg_args_d" ]; then
        IFS=';' read -ra tmp_liste_args_d <<< "${get_arg_args_d}"
    fi
    if [ -n "$get_arg_args_v" ]; then
        IFS=';' read -ra tmp_liste_args_v <<< "${get_arg_args_v}"
    fi
    if [ -n "$get_arg_args_o" ]; then
        IFS=';' read -ra tmp_liste_args_o <<< "${get_arg_args_o}"
    fi

    if [ -n "$get_arg_help" ] || [ -n "$get_arg_interactif" ]; then
        for tmp_arg_p in "${tmp_liste_args_p[@]}"; do
            tmp_arg_p_name=$(echo $tmp_arg_p | grep -o '^[^:]*')
            tmp_arg_p_libelle=$(echo $tmp_arg_p | grep -o '[^:]*$')
            tmp_arg_p_help="$tmp_arg_p_name : $tmp_arg_p_libelle"

            for tmp_arg_o in "${tmp_liste_args_o[@]}"; do
                tmp_arg_o_ok='N'
                tmp_arg_o_name=$(echo $tmp_arg_o | grep -o '^[^:]*')
                if [ "$tmp_arg_o_name" = "$tmp_arg_p_name" ]; then
                    tmp_arg_o_value=$(echo $tmp_arg_o | grep -o '[^:]*$')
                    tmp_arg_p_help="$tmp_arg_p_help, obligatoire"
                    tmp_arg_o_ok='O'
                    break
                fi
            done
            if [ "$tmp_arg_o_ok" = 'N' ]; then
                tmp_arg_p_help="$tmp_arg_p_help, facultatif"
            fi

            for tmp_arg_v in "${tmp_liste_args_v[@]}"; do
                tmp_arg_v_name=$(echo $tmp_arg_v | grep -o '^[^:]*')
                if [ "$tmp_arg_v_name" = "$tmp_arg_p_name" ]; then
                    tmp_arg_v_value=$(echo $tmp_arg_v | grep -o '[^:]*$')
                    tmp_arg_p_help="$tmp_arg_p_help, valeurs possibles : $tmp_arg_v_value"
                    break
                fi
            done

            for tmp_arg_d in "${tmp_liste_args_d[@]}"; do
                tmp_arg_d_name=$(echo $tmp_arg_d | grep -o '^[^:]*')
                if [ "$tmp_arg_d_name" = "$tmp_arg_p_name" ]; then
                    tmp_arg_d_value=$(echo $tmp_arg_d | grep -o '[^:]*$')
                    if [ -n "$get_arg_interactif" ]; then
                        eval tmp_arg_d_value=\"$tmp_arg_d_value\"
                    fi
                    tmp_arg_p_help="$tmp_arg_p_help, valeur par défaut : $tmp_arg_d_value"
                    break
                fi
            done

            if [ -n "$get_arg_interactif" ]; then
                bash_question --question "$tmp_arg_p_help"
                eval get_arg_$tmp_arg_p_name=\"$get_reponse\"
            else
                echo "$tmp_arg_p_help"
            fi
        done
        if [ -n "$get_arg_help" ]; then
            # script.sh --help : arrêter le traitement du script, une fois l'aide affichée
            # je ne comprends pas comment s'arrête : fonction --help avec un return 0
            return 1
        fi
    fi

    #application si besoin des valeurs par défaut
    for tmp_arg_d in "${tmp_liste_args_d[@]}"; do
        tmp_arg_d_name=$(echo $tmp_arg_d | grep -o '^[^:]*')
        tmp_arg_d_value=$(echo $tmp_arg_d | grep -o '[^:]*$')
        eval tmp_arg_value=\"\$get_arg_$tmp_arg_d_name\"
        #echo "DEFAULT($tmp_arg_d_name)=($tmp_arg_value)"
        if [ -z "$tmp_arg_value" ]; then
            eval get_arg_$tmp_arg_d_name=\"$tmp_arg_d_value\"
            #eval tmp_arg_d_value=\"$tmp_arg_d_value\"
            #echo "Valeur par défaut $tmp_arg_d_value pour $tmp_arg_d_name"
            _argv+=([$tmp_arg_d_name]=$tmp_arg_d_value)
        fi
    done

    #controle des arguments obligatoires
    for tmp_arg_o in "${tmp_liste_args_o[@]}"; do
        tmp_arg_o_ok='N'
        #prise en compte d'une éventuelle condition de type OU
        IFS='|' read -ra tmp_liste_args_o_or <<< "${tmp_arg_o}"
        for tmp_arg_o_or_name in "${tmp_liste_args_o_or[@]}"; do
            eval tmp_arg_value=\"\$get_arg_$tmp_arg_o_or_name\"
            #si la valeur de l'argument est renseignée, la condition est remplie
            if [ -n "$tmp_arg_value" ]; then
                tmp_arg_o_ok='O'
                break
            fi
        done
        if [ "$tmp_arg_o_ok" != 'O' ]; then
            >&2 echo "La condition d'argument obligatoire $tmp_arg_o n'est pas remplie, astuce : utilisez l'option --help pour l'aide ou --interactif pour une utilisation interactive"
            return 1
        fi
    done

    #controle des valeurs possibles
    for tmp_arg_v in "${tmp_liste_args_v[@]}"; do
        tmp_arg_v_name=$(echo $tmp_arg_v | grep -o '^[^:]*')
        tmp_arg_v_values=$(echo $tmp_arg_v | grep -o '[^:]*$')
        #si l'argument est renseigné
        eval tmp_arg_value=\"\$get_arg_$tmp_arg_v_name\"
        if [ -n "$tmp_arg_value" ]; then
            #on vérifie que sa valeur fait partie des valeurs possibles
            tmp_arg_v_ok='N'
            IFS='|' read -ra tmp_liste_arg_v_values <<< "${tmp_arg_v_values}"
            for tmp_arg_v_value in "${tmp_liste_arg_v_values[@]}"; do
                #echo "Valeur possible pour $tmp_arg_v_name : $tmp_arg_v_value"
                if [ "$tmp_arg_v_value" = "$tmp_arg_value" ]; then
                    tmp_arg_v_ok='Y'
                    break
                fi
            done
            if [ "$tmp_arg_v_ok" = 'N' ]; then
                >&2 echo "La valeur de $tmp_arg_v_name ($tmp_arg_value) ne fait pas partie des valeurs possibles ($tmp_arg_v_values), astuce : utilisez l'option --help pour l'aide ou --interactif pour une utilisation interactive"
                return 1
            fi
        fi
    done

    # retour des paramètres {clé/valeur} au programme appelant
    [[ " ${!_argv[@]} " =~ " bash_args_argv " ]] && {
        local -n _argv_ref=${_argv[bash_args_argv]}
        _argv_ref=()
        #echo "reset argv: #${#_argv_ref[@]}"
        for _key in ${!_argv[@]}; do
            [[ "$_key" =~ bash_args_argv|args_(p|o|v|d) ]] && continue
            #echo '+'$_key
            _argv_ref[$_key]=${_argv[$_key]}
        done
    }

    return 0
}

    #
    # file
    #

# compare
is_different() {
    bash_args --args_p '
            dir_a:Dossier à comparer;
            dir_b:Autre dossier à comparer à dir_a;
            file_name:Nom ou masque optionnel de sélection des fichiers ou dossiers à comparer;
            verbose:Mode verbeux (Y/N)' \
        --args_o 'dir_a;dir_b' \
        "$@" || return 1

    local dir_a="$get_arg_dir_a"
    local dir_b="$get_arg_dir_b"
    local file_name="$get_arg_file_name"
    if [ -z "$file_name" ]; then
        file_name='*'
    fi
    local mode_verbeux="$get_arg_verbose"
    if [ -z "$mode_verbeux" ]; then
        mode_verbeux='N'
    fi

    local tmp_chemin_fichier_ou_dossier
    local tmp_nom_fichier_ou_dossier

    if [ "$mode_verbeux" = 'Y' ]; then
        echo "Lecture de $dir_a/$file_name"
    fi
    for tmp_chemin_fichier_ou_dossier in "$dir_a/"$file_name
    do
        #si c'est un fichier
        if [ -f "$tmp_chemin_fichier_ou_dossier" ]; then
            tmp_nom_fichier_ou_dossier=$(basename "$tmp_chemin_fichier_ou_dossier")
            if [ ! -f "$dir_b/$tmp_nom_fichier_ou_dossier" ]; then
                echo "Fichier $dir_a/$tmp_nom_fichier_ou_dossier non présent dans $dir_b"
                return $SUCCESS_CODE
            fi
            if file_is_binary "$tmp_chemin_fichier_ou_dossier"; then
                file_size_a=$(wc -c "$dir_a/$tmp_nom_fichier_ou_dossier" | cut -d' ' -f1)
                file_size_b=$(wc -c "$dir_b/$tmp_nom_fichier_ou_dossier" | cut -d' ' -f1)
                if [ "$file_size_a" != "$file_size_b" ]; then
                    echo "Fichier binaire $dir_a/$tmp_nom_fichier_ou_dossier de taille différente dans $dir_b"
                    return $SUCCESS_CODE
                elif [ "$mode_verbeux" = 'Y' ]; then
                    echo "Fichier binaire $dir_a/$tmp_nom_fichier_ou_dossier de taille identique dans $dir_b"
                fi
            else
                modification=$(git diff "$dir_a/$tmp_nom_fichier_ou_dossier" "$dir_b/$tmp_nom_fichier_ou_dossier")
                if [ "$modification" ]; then
                    echo "Fichier texte $dir_a/$tmp_nom_fichier_ou_dossier différent dans $dir_b"
                    return $SUCCESS_CODE
                elif [ "$mode_verbeux" = 'Y' ]; then
                    echo "Fichier texte $dir_a/$tmp_nom_fichier_ou_dossier identique dans $dir_b"
                fi
            fi
        #sinon, si c'est un dossier
        elif [ -d "$tmp_chemin_fichier_ou_dossier" ]; then
            tmp_nom_fichier_ou_dossier=$(basename "$tmp_chemin_fichier_ou_dossier")
            if [ ! -d "$dir_b/$tmp_nom_fichier_ou_dossier" ]; then
                echo "Dossier $dir_a/$tmp_nom_fichier_ou_dossier non présent dans $dir_b"
                return $SUCCESS_CODE
            else
                if [ "$mode_verbeux" = 'Y' ]; then
                    echo "Dossier $dir_a/$tmp_nom_fichier_ou_dossier présent dans $dir_b"
                fi
                est_different --dir_a "$dir_a/$tmp_nom_fichier_ou_dossier" --dir_b "$dir_b/$tmp_nom_fichier_ou_dossier" --file_name "$file_name" --verbose "$mode_verbeux" &&
                return $SUCCESS_CODE
            fi
        fi
    done

    if [ "$mode_verbeux" = 'Y' ]; then
        echo "Lecture de $dir_b/$file_name"
    fi
    for tmp_chemin_fichier_ou_dossier in "$dir_b/"$file_name
    do
        if [ -f "$tmp_chemin_fichier_ou_dossier" ]; then
            tmp_nom_fichier_ou_dossier=$(basename "$tmp_chemin_fichier_ou_dossier")
            if [ ! -f "$dir_a/$tmp_nom_fichier_ou_dossier" ]; then
                echo "Fichier $dir_b/$tmp_nom_fichier_ou_dossier non présent dans $dir_a"
                return $SUCCESS_CODE
            fi
        #sinon, si c'est un dossier
        elif [ -d "$tmp_chemin_fichier_ou_dossier" ]; then
            tmp_nom_fichier_ou_dossier=$(basename "$tmp_chemin_fichier_ou_dossier")
            if [ ! -d "$dir_a/$tmp_nom_fichier_ou_dossier" ]; then
                echo "Dossier $dir_b/$tmp_nom_fichier_ou_dossier non présent dans $dir_a"
                return $SUCCESS_CODE
            fi
        fi
    done

    return $ERROR_CODE
}

# extension of file
get_file_extension() {
    bash_args \
        --args_p 'file_path:Nom du fichier' \
        --args_o 'file_path' \
        "$@" || return $ERROR_CODE

    local _file_extension="${get_arg_file_path##*.}"
    echo "${_file_extension,,}"
    return $SUCCESS_CODE
}

# basename of file (w/o extension)
get_file_name() {
    bash_args \
        --args_p 'file_path:Nom du fichier' \
        --args_o 'file_path' \
        "$@" || return $ERROR_CODE

    local _file_name=$(basename -- "$get_arg_file_path")
    echo "${_file_name%%.*}"
    return $SUCCESS_CODE
}

# get MIME's type of file
get_file_mimetype() {
    file --mime-type "$1" | sed 's/.*: //'
}

# known if file is binary
file_is_binary() {
    file --brief --dereference --mime "$1" | grep --silent 'charset=binary'
}

# get number of rows
get_file_nrows() {
    expect argc $0 $# 2  &&
    expect file "$1"     || return $ERROR_CODE

    local -n _nr=$2
    _nr=$(wc --lines "$1" | cut --delimiter ' ' --fields 1)

    return $SUCCESS_CODE
}

# backup file (with uniq extension as: .backup.#)
backup_file_as_uniq() {
    bash_args \
        --args_p 'path:nom complet' \
        --args_o 'path' \
        "$@" || return $ERROR_CODE

    local _suffix=1

    while [ -f "${get_arg_path}.backup.${_suffix}" ]; do
        ((_suffix++))
    done
    cp "${get_arg_path}" "${get_arg_path}.backup.${_suffix}"
    return $?
}

# get temporary file (w/ uniq name)
get_tmp_file() {
    bash_args \
        --args_p '
            tmpfile:Nom de la variable dans laquelles est retourné le chemin du fichier temporaire demandé;
            tmpdir:Dossier temporaire dans lequel le fichier temporaire est demandé;
            tmpext:Extension du fichier temporaire;
            chmod:Permissions (rwx) à donner à ce fichier;
            create:Créer le fichier temporaire' \
        --args_o '
            tmpfile' \
        --args_v '
            create:no|yes' \
        --args_d '
            tmpdir:'$POW_DIR_TMP';
            tmpext:tmp;
            chmod:666;
            create:no' \
        "$@" || return $ERROR_CODE

    local _tmp_pow=$(mktemp --tmpdir="$get_arg_tmpdir" pow_XXXXX.$get_arg_tmpext)
    local -n _tmp_ref=$get_arg_tmpfile
    [ "$get_arg_create" = no ] && rm --force "$_tmp_pow" || chmod $get_arg_chmod "$_tmp_pow"
    _tmp_ref="$_tmp_pow"
    return $SUCCESS_CODE
}

# sync to wait for file
wait_for_file() {
    bash_args	\
        --args_p '
            file_path:chemin complet vers le fichier attendu;
            wait_file_minute:combien de temps en minutes faut-il attendre que le fichier soit présent ?;
            max_age_file_minute:quel age maximum en minutes doit avoir le fichier ?
        ' \
        --args_o 'file_path' \
        --args_d 'wait_file_minute:0;max_age_file_minute:0' \
        "$@" || return $ERROR_CODE

    local file_path=$get_arg_file_path
    local wait_file_minute=$get_arg_wait_file_minute
    local max_age_file_minute=$get_arg_max_age_file_minute

    # waiting delay
    # AND
    #   file not available
    #   OR
    #   present file is too old
    while [ $wait_file_minute -gt 0 ] && { [ ! -f $file_path ] || ([ $max_age_file_minute -gt 0 ] && [ $(find $file_path -mmin +$max_age_file_minute) ]) }; do
        echo "En attente du fichier $file_path (temps restant : $wait_file_minute minutes, age maximum du fichier : $max_age_file_minute minutes, fichier présent mais trop ancien : $([ -f $file_path ] && echo 'oui' || echo 'non'))"
        sleep 60
        ((wait_file_minute--))
    done

    if [ -f "$file_path" ]; then
        # older ?
        [ $max_age_file_minute -gt 0 ] && [ $(find $file_path -mmin +$max_age_file_minute) ] && log_error "Le fichier $file_path est présent mais trop ancien et l'éventuel temps d'attente est dépassé" && return $ERROR_CODE

        # not currently growing?
        file_size_before=$(stat --printf="%s" $file_path)
        sleep 5
        file_size_after=$(stat --printf="%s" $file_path)
        while [ "$file_size_before" != "$file_size_after" ]; do
            echo "La taille du fichier $file_path a changée, attente de 5 secondes supplémentaires"
            file_size_before=$file_size_after
            sleep 5
            file_size_after=$(stat --printf="%s" $file_path)
        done
    else
        echo "Le fichier $file_path n'est pas présent et l'éventuel temps d'attente est dépassé" && return $ERROR_CODE
    fi

    return $SUCCESS_CODE
}

    #
    # archive
    #

is_archive() {
    bash_args \
        --args_p "
            archive_path:chemin complet de l'archive;
            type_archive:obtenir le type de l'archive (MIME)
        " \
        --args_o 'archive_path' \
        "$@" || return $ERROR_CODE

    expect file "$get_arg_archive_path" || return $ERROR_CODE
    [ -n "$get_arg_type_archive" ] && local -n _type_ref=$get_arg_type_archive
    # TODO: (to add 7z and rar) apt install p7zip-full p7zip-rar
	[[ $(file --mime-type "$get_arg_archive_path") =~ application/(zip|gzip|x-bzip2|x-7z-compressed) ]] && {
        _type_ref=${BASH_REMATCH[1]}
        return $SUCCESS_CODE
    }
    return $ERROR_CODE
}

# extract data from archive (zip, gz, ...)
extract_archive() {
    bash_args \
        --args_p "
            archive_path:chemin complet de l'archive;
            extract_path:chemin complet du résultat de l'extraction de l'archive (STDOUT pour écran)
        " \
        --args_o '
            archive_path;
            extract_path
        ' \
        "$@" || return $ERROR_CODE

    local _start=$(date +%s)
    local _archive_name=$(basename "$get_arg_archive_path")
    local _log_tmp_path="$POW_DIR_TMP/extract_$_archive_name.log"
    local _log_archive_path="$POW_DIR_ARCHIVE/extract_$_archive_name.log"

    local _type_archive
    is_archive --archive_path "$get_arg_archive_path" --type_archive _type_archive || {
        log_error "${FUNCNAME[1]}: le fichier $get_arg_archive_path n'est pas une archive"
        return $ERROR_CODE
    }
    case $_type_archive in
    zip)
        if [ "$get_arg_extract_path" = STDOUT ]; then
            # -p : extract files to pipe (stdout)
            unzip -p "$get_arg_archive_path" 2> $_log_tmp_path
        else
            # -o : overwrite files WITHOUT prompting
            # -d : extract files into dir
            unzip -o "$get_arg_archive_path" -d "$get_arg_extract_path" > $_log_tmp_path 2>&1
        fi
        ;;
    gzip)
        if [ "$get_arg_extract_path" = STDOUT ]; then
            gunzip --stdout "$get_arg_archive_path" 2> $_log_tmp_path
        else
            gunzip --stdout "$get_arg_archive_path" > "$get_arg_extract_path/${_archive_name%.*}"
        fi
        ;;
    x-bzip2)
        if [ "$get_arg_extract_path" = STDOUT ]; then
            bunzip2 --stdout "$get_arg_archive_path" 2> $_log_tmp_path
        else
            bunzip2 --stdout "$get_arg_archive_path" > "$get_arg_extract_path/${_archive_name%.*}"
        fi
        ;;
    x-7z-compressed)
        if [ "$get_arg_extract_path" = STDOUT ]; then
            log_error "Mode d'extraction sur le type d'archive .7z non pris en charge pour le moment"
            return $ERROR_CODE
        else
            7z x "$get_arg_archive_path" -o"$get_arg_extract_path" -y > $_log_tmp_path 2>&1
        fi
        ;;
    *)
        log_error "${FUNCNAME[1]}: format d'archive ($_type_archive) non pris en charge"
        return $ERROR_CODE
        ;;
    esac

    # https://stackoverflow.com/questions/19120263/why-exit-code-141-with-grep-q
    [[ ! $? =~ 0|141 ]] && {
        archive_file "$_log_tmp_path"
        log_error "${FUNCNAME[1]}: erreur lors de l'extraction de l'archive $_archive_name, veuillez consulter $_log_archive_path"
        return $ERROR_CODE
    } || {
        local _last
        get_elapsed_time --start $_start --result _last
        log_info "Extraction avec succès de l'archive $_archive_name en $_last"
    }

    return $SUCCESS_CODE
}

create_archive() {
    bash_args \
        --args_p '
            type_archive:Type archive demandée;
            output:Chemin complet archive générée;
            input:Données à archiver
        ' \
        --args_o 'output;input' \
        --args_v 'type_archive:zip|gzip|x-bzip2' \
        --args_d 'type_archive:gzip' \
        "$@" || return $ERROR_CODE

    case "$get_arg_type_archive" in
    zip)
        zip --filesync --recurse-paths --junk-paths "$get_arg_output" "$get_arg_input"
        ;;
    gzip)
        gzip --recursive --stdout "$get_arg_input" > "$get_arg_output"
        ;;
    x-bzip2)
        bzip2 --stdout "$get_arg_input" > "$get_arg_output"
        ;;
    esac

    return $?
}

    #
    # mail
    #

# send mail
#
# message from a file
#  send_mail --subject 'test MAIL body as file' --body ./test_send_mail.txt --attachment "data.txt.gz,full2part.txt.gz" --to <mail>
# message from command line
#  send_mail --subject 'test MAIL body as text' --body 'test envoi message' --attachment "data.txt.gz,full2part.txt.gz" --to <mail>
#
# argument --encoding to convert (UTF8 to LATIN1 (Windows))
#
# see:
#  https://stackoverflow.com/questions/5395082/how-to-send-html-body-email-with-multiple-text-attachments-using-sendmail
#
send_mail() {
    bash_args \
        --args_p '
            subject:Objet du message;
            body:Texte du message;
            to:Destinataire(s);
            cc:Copie(s);
            attachment:Liste des fichiers à insérer en pièce jointe (séparés par une virgule, sans espace);
            encoding:Changer le jeu de caractères (la source étant en UTF8);
            format:Format du texte;
            compress:Compression piéce jointe (si pas déjà fait);
            tool:outil de codage;
            debug:Activer le mode debug;
            verbose:Activer le mode bavard' \
        --args_o '
            subject;
            body;
            to' \
        --args_v '
            encoding:ISO8859-1|WINDOWS-1252|LATIN1;
            format:PLAIN|HTML;
            compress:no|yes;
            tool:uuencode|base64;
            debug:no|yes;
            verbose:no|yes' \
        --args_d '
            format:PLAIN;
            compress:no;
            tool:base64;
            debug:no;
            verbose:no' \
        "$@" || return $ERROR_CODE

    # texte du message
    local _body
    [ -f "$get_arg_body" ] && _body=$(< "$get_arg_body") || {
        [ -n "$get_arg_body" ] && _body="$get_arg_body" || {
            log_error 'manque texte du message!'
            return $ERROR_CODE
        }
    }
    # encoding
    [ -n "$get_arg_encoding" ] && {
        _body=$(echo "$_body" | iconv --from-code UTF8 --to-code $get_arg_encoding)
        [ $? -ne 0 ] && {
            log_error "conversion texte du message (UTF8 vers $get_arg_encoding)!"
            return $ERROR_CODE
        }
    }
    # préparation message (avec éventuelle(s) pj(s))
    local _msg=$POW_DIR_TMP/mail_$$.msg
    {
        echo "To: ${get_arg_to}"
        [ -n "${get_arg_cc}" ] && echo "Cc: ${get_arg_cc}"
        echo "Subject: ${get_arg_subject}"
        echo 'MIME-Version: 1.0'
        [ "$get_arg_debug" = yes ] && [ ! -z "$get_arg_attachment" ] && echo '###DEBUG: avec pj' > /dev/stderr
        [ ! -z "$get_arg_attachment" ] && {
            [ "$get_arg_debug" = yes ] && echo '###DEBUG: ajout BOUNDARY' > /dev/stderr
            echo 'Content-Type: multipart/mixed; boundary="###BOUNDARY"'
            echo '--###BOUNDARY'
        }
        echo 'Content-Type: text/'${get_arg_format,,}
        # semble être indispensable, cet écho! en mode PLAIN en tout cas...
        echo
        echo "$_body"

        [ ! -z "$get_arg_attachment" ] && {
            [ "$get_arg_debug" = yes ] && echo '###DEBUG: ajout PJ(s)' > /dev/stderr
            local _tmplist='' _file _todo _mime _list=(${get_arg_attachment//,/ }) _i
            for ((_i=0; _i<${#_list[*]}; _i++)); do
            #FIX souci bash_args avec valeurs multiples séparées par un espace, 'file1 file2'
            #for _file in $get_arg_attachment; do
                _file=${_list[$_i]}
                [ ! -s "$_file" ] && continue
                [ "$get_arg_debug" = yes ] && echo '###DEBUG: ajout PJ:'$_file > /dev/stderr
                _todo="$_file"
                if ! is_archive "$_file" ; then
                    [ "$get_arg_compress" = yes ] && {
                        _todo+='.gz'
                        gzip --force --stdout "$_file" > "$_todo"
                        [ -n "$_tmplist" ] && _tmplist+=' '
                        # liste pièce(s) jointe(s)
                        _tmplist+="'$_todo'"
                    }
                fi

                _mime=$(get_file_mimetype "$_todo")

                echo '--###BOUNDARY'
                echo 'Content-Type: '$_mime
                echo 'Content-Transfer-Encoding: '$get_arg_tool
                echo 'Content-Disposition: attachment; filename="'$(basename "$_todo")'"'

                # semble être indispensable, cet écho!
                echo
                # encodage des données (base64 par défaut)
                case $get_arg_tool in
                uuencode)
                    uuencode "$_todo" "$_todo"
                    ;;
                base64)
                    base64 "$_todo"
                    ;;
                esac
                echo
            done
            # fermeture des pj, avec dernier --
            echo '--###BOUNDARY--'
        }
    } > $_msg

    case "$get_arg_debug" in
    yes)
        cat $_msg
        ;;
    no)
        cat $_msg | sendmail -t
        ;;
    esac

    local _rc=$?
    [ -n "$_tmplist" ] && rm $_tmplist
    [ "$get_arg_debug" = no ] && [ -f $_msg ] && rm $_msg
    return $_rc
}
