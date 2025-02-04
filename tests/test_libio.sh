#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # tests libio

declare -A test_libio
pow_argv \
    --args_n '
        test:Préciser le test à réaliser;
        force:Forcer le traitement même si celui-ci a déjà été fait
    ' \
    --args_m '
        test
    ' \
    --args_v '
        test:NEWER_DATE|NEWER_TIME;
        force:yes|no
    ' \
    --args_d '
        force:no
    ' \
    --pow_argv test_libio "$@" || exit $ERROR_CODE

set_log_echo no
case "${test_libio[TEST]}" in
NEWER_DATE)
    get_tmp_file --tmpfile _tmp1 --create yes
    _epoch1=$(stat --format '%Y' "$_tmp1")
    ls -l $_tmp1
    echo "epoch1=$_epoch1"
    touch -m -t 202501311003.15 $_tmp1
    _epoch1=$(stat --format '%Y' "$_tmp1")
    ls -l $_tmp1
    echo "epoch1=$_epoch1 (after touch)"

    _epoch2=$(date '+%s' --date "2025-01-31 04:07:29")
    _delta=$((_epoch1 - _epoch2))
    echo -n "epoch1=$_epoch1 epoch2=$_epoch2 delta=$_delta "
    [[ $_delta -lt 0 ]] && echo 'NEWER (yes)' || echo 'NEWER (no)'

    _epoch2=$(date '+%s' --date "2025-01-31 11:07:29")
    _delta=$((_epoch1 - _epoch2))
    echo -n "epoch1=$_epoch1 epoch2=$_epoch2 delta=$_delta "
    [[ $_delta -lt 0 ]] && echo 'NEWER (yes)' || echo 'NEWER (no)'

    execute_query \
        --name BAL_44109_DATE \
        --query "
            SELECT last_update
            FROM fr.bal_municipality
            WHERE code = '44109'
        " \
        --psql_arguments 'tuples-only:pset=format=unaligned' \
        --return _date
    [ -n "$_date" ] && {
        _epoch2=$(date '+%s' --date "$_date")
        echo "epoch2=$_epoch2 "
    }

    rm $_tmp1
    ;;
NEWER_TIME)
    ;;
esac || exit $ERROR_CODE

exit $SUCCESS_CODE
