#!/bin/bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # execute script(s) defined into install.d

source $POW_DIR_ROOT/lib/libenv.sh || exit ${ERROR_CODE:-3}

bash_args \
    --args_p '
        filter:filtre (REGEX) des scripts à exécuter;
        check:contrôle installation
    ' \
    --args_v '
        check:no|yes
    ' \
    --args_d '
        check:yes
    ' \
    "$@" || exit $ERROR_CODE

    # check installation
[ "$get_arg_check" = yes ] && {
    expect env POW_DIR_ROOT && {
        [ -d "$POW_DIR_ROOT" ] || {
            echo "manque dossier $POW_DIR_ROOT"
            false
        }
    } &&
    expect env POW_DIR_DATA && {
        [ -d "$POW_DIR_DATA" ] || {
            echo "manque dossier $POW_DIR_DATA"
            false
        }
    } &&
    expect env POW_DIR_LOG  && {
        [ -d "$POW_DIR_LOG" ] || {
            echo "manque dossier $POW_DIR_LOG"
            false
        }
    } &&
    expect env POW_USER && {
        user_exists $POW_USER || {
            echo "manque utilisateur $POW_USER"
            false
        }
    } || exit $ERROR_CODE
}

    # execute scripts
for _script in $POW_DIR_ROOT/bin/admin/install.d/*; do
    [ -n "$get_arg_filter" ] &&
    [[ ! $_script =~ $get_arg_filter ]] &&
    continue

    # execute it
    $_script
done

exit $SUCCESS_CODE
