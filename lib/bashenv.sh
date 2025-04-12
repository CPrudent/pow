    #--------------------------------------------------------------------------
    # synopsis
    #--
    # BASH environment

    # HELP
    # https://unix.stackexchange.com/questions/393091/unable-to-use-an-array-as-environment-variable

# global config
# remember that BASH can't export array!
declare -A POW_CONF=(
    [JAVA_HOME]=/usr/lib/jvm/default-java
    [PG_DBNAME]=pow
    [PG_VERSION]=16
    [POSTGIS_VERSION]=3
    [PG_HOST]=localhost
    [PG_PORT]=5432
)

# delimiters (worry w/ bash_args eval!)
declare -A POW_DELIMITER=(
    [COLON]=':'
    [COMMA]=','
    [PIPE]='|'
    [SEMICOLON]=';'
    [TAB]='\t'
)

# defaults used by pow_argv()
declare -A POW_ARGV
declare -A POW_ARGV_PROPERTIES=(
    [CASE]=UPPER
    [RESET]=yes
)
declare -i POW_ARGC
