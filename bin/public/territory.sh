#!/usr/bin/env bash

    #--------------------------------------------------------------------------
    # synopsis
    #--
    # build territories (for all available countries)

bash_args \
    --args_p '
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
    "$@" || exit $?

force="$get_arg_force"

for _dir in $(ls -1d "$POW_DIR_ROOT/bin/"* | xargs --max-args 1 basename); do
    # system
    [[ "$_dir" =~ admin|public ]] && continue
    # only country?
    [ -n "$get_arg_only_country" ] && [[ ! $_dir =~ $get_arg_only_country ]] && continue
    # except country?
    [ -n "$get_arg_except_country" ] && [[ $_dir =~ $get_arg_except_country ]] && continue

    [ -x "$POW_DIR_ROOT/bin/$_dir"/territory.sh ] && "$POW_DIR_ROOT/bin/$_dir"/territory.sh
done

[ "$get_arg_public" = yes ] &&  {
    log_info "Publication des territoires"
    set_env --schema_name public &&
    execute_query \
        --name SET_TERRITORY \
        --query "SELECT public.set_territory('$force'='yes')" || exit $ERROR_CODE
}

exit $SUCCESS_CODE
