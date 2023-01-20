    #--------------------------------------------------------------------------
    # synopsis
    #--
    # define STD

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
    local severity=${1:-info}
    local message=$(echo $2 | tr \| \_)
    local status=$3
    local cmd_name="$(realpath $0)"
    local log_entry="$(date --utc +%FT%TZ)|$1|$$|${USER}|$cmd_name|$message"
    [ "$POW_LOG_ECHO" = yes ] && echo $log_entry
    [ "$POW_LOG_ACTIVE" = yes ] && echo $log_entry >> $POW_DIR_LOG/$POW_LOG_FILE

    return $SUCCESS_CODE
}

log_info() {
    log "info" "$1" "$2"

    return $?
}

log_error() {
    log "error" "$1" "$2"

    return $?
}

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
        if [ -z "$tmp_arg_value" ]; then
            eval get_arg_$tmp_arg_d_name=\"$tmp_arg_d_value\"
            #eval tmp_arg_d_value=\"$tmp_arg_d_value\"
            #echo "Valeur par défaut $tmp_arg_d_value pour $tmp_arg_d_name"
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
                break;
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

    ###
    # récupérer type MIME d'un fichier
    #
get_mimetype() {
    file --mime-type "$1" | sed 's/.*: //'
}

file_is_binary() {
    file -bL --mime "$1" | grep -q 'charset=binary'
    return $?
}

    ###
    # envoi mail
    #
    # exemples:
    #  texte du message contenu dans un fichier
    #  send_mail --subject 'test MAIL body as file' --body ./test_send_mail.txt --attachment "data.txt.gz,full2part.txt.gz" --to christophe.prudent@laposte.fr
    #  texte du message en option de la ligne de commande
    #  send_mail --subject 'test MAIL body as text' --body 'test envoi message' --attachment "data.txt.gz,full2part.txt.gz" --to christophe.prudent@laposte.fr
    #
    #  l'argument d'encodage (--encoding) permet de changer le jeu de caractères (UTF8 vers ?), à priori LATIN1 (Windows)
    #
    # liens utiles
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
    local _msg=$dir_tmp/mail_$$.msg
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

                _mime=$(get_mimetype "$_todo")

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

    ###
    # print expression and wait ENTER
    #
breakpoint() {
    echo "###BREAK($1)"
    read
}

    ###
    # expect expression
    #  in: $1=(argc, isnum, env, file) as (#args, is numeric, variable is defined, file exists)
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

    ###
    # get nrows of file
    #
file_get_nrows() {
    expect argc $0 $# 2  &&
    expect file "$1"     || return $ERROR_CODE

    local -n _nr=$2
    _nr=$(wc -l $1 | cut -d ' ' -f 1)

    return $SUCCESS_CODE
}

    ###
    # backup file (with uniq extension as: .backup.#)
    #
backup_file_as_uniq() {
    bash_args \
        --args_p 'path:nom complet' \
        --args_o 'path' \
        "$@" || return $ERROR_CODE

    local _suffix=1

    while [ -f "${get_arg_path}.backup.${_suffix}" ]; do
        ((_suffix++))
    done
    cp ${get_arg_path} ${get_arg_path}.backup.${_suffix}
    return $?
}

    ###
    # get temporary file (w/ uniq name)
    #
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

    local _tmp_pow=$(mktemp --tmpdir=$get_arg_tmpdir pow_XXXXX.$get_arg_tmpext)
    typeset -n _tmp_ref=$get_arg_tmpfile
    [ $get_arg_create = 'no' ] && rm --force $_tmp_pow || chmod $get_arg_chmod $_tmp_pow
    _tmp_ref=$_tmp_pow
    return $SUCCESS_CODE
}

    ###
    # item in array
    # https://stackoverflow.com/questions/8082947/how-to-pass-an-array-to-a-bash-function
    # optional 3rd argument gives ID of searched item, as: in_array ARRAY STR_TO_SEARCH ID
    #
    # another solution w/ print
    # https://stackoverflow.com/questions/3685970/check-if-a-bash-array-contains-a-value
in_array() {
    local _ref=$1[@]
    local _array=("${!_ref}")
    local _rc=1 _i _return_id=0
    [ $# -eq 3 ] && {
        _return_id=1
        local -n _id_ref=$3
        [ "$3" = _i ] && log_error "retour indice vers _i (en conflit avec local _i) : changer le nom"
    }
    for ((_i=0; _i < ${#_array[@]}; _i++)); do
        #echo "$_i: ${_array[$_i]}"
        [ "${_array[$_i]}" = "$2" ] && {
            _rc=0
            break
        }
    done
    [ $_return_id -eq 1 ] && [ $_i -lt ${#_array[@]} ] && _id_ref=$_i
    return $_rc
}
