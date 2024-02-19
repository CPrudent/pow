    #--------------------------------------------------------------------------
    # synopsis
    #--
    # BASH environment

# global config
# remember that BASH can't export array!
# https://unix.stackexchange.com/questions/393091/unable-to-use-an-array-as-environment-variable
declare -A POW_CONF=(
    [JAVA_HOME]=/usr/lib/jvm/default-java
    [PG_DBNAME]=pow
    [PG_VERSION]=15
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
