#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build territories (for all available countries)

pow_argv \
    --args_n '
        force:Forcer le traitement même si celui-ci a déjà été fait;
        except_country:RE pour écarter certains pays;
        only_country:RE pour traiter certains pays;
        public:Publication
    ' \
    --args_v '
        force:yes|no;
        public:yes|no
    ' \
    --args_d '
        force:no;
        public:yes
    ' \
    --args_p '
        tag:force@bool,public@bool
    ' \
    "$@" || exit $?

for _dir in $(ls -1d "$POW_DIR_ROOT/bin/"* | xargs --max-args 1 basename); do
    # system
    [[ "$_dir" =~ admin|public ]] && continue
    # only country?
    [ -n "${POW_ARGV[ONLY_COUNTRY]}" ] && [[ ! $_dir =~ ${POW_ARGV[ONLY_COUNTRY]} ]] && continue
    # except country?
    [ -n "${POW_ARGV[EXCEPT_COUNTRY]}" ] && [[ $_dir =~ ${POW_ARGV[EXCEPT_COUNTRY]} ]] && continue

    [ -x "$POW_DIR_ROOT/bin/$_dir"/territory.sh ] && "$POW_DIR_ROOT/bin/$_dir"/territory.sh
done

[ "${POW_ARGV[PUBLIC]}" = yes ] &&  {
    log_info "Publication des territoires"
    set_env --schema_name public &&
    execute_query \
        --name SET_TERRITORY \
        --query "SELECT public.set_territory('${POW_ARGV[FORCE]}'='yes')" || exit $ERROR_CODE
}

exit $SUCCESS_CODE
