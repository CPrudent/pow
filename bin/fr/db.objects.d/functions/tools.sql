/***
 * add FR-TOOLS facilities
 */

/* NOTE
get value of parameters (threshold, ratio, ...) as real
w/o parameters, take value from db
    defaults are defined as global variables (fr.<category>.<level>.<key>)
else
    HSTORE parameters to custom properties (<level>_<key>)
    '"STREET_OCCURS" => 3'::HSTORE
 */
SELECT drop_all_functions_if_exists('fr', 'get_parameter_value');
CREATE OR REPLACE FUNCTION fr.get_parameter_value(
    parameters IN HSTORE
    , category IN VARCHAR
    , level IN VARCHAR
    , key IN VARCHAR
    , value OUT REAL
)
AS
$func$
DECLARE
    _property VARCHAR;
    _value TEXT;
BEGIN
    /* NOTE
    HSTORE property as LEVEL_KEY => VALUE
     */
    _property := CONCAT_WS('_'
        , UPPER(level)
        , UPPER(key)
    );
    IF parameters IS NOT NULL AND parameters ? _property THEN
        value := (parameters -> _property)::REAL;
    ELSE
        /* NOTE
        get from global variables (defined in constant.sql)
         */
        _property := CONCAT_WS('.'
            , 'fr'
            , LOWER(category)
            , LOWER(level)
            , LOWER(key)
        );
        _value := (SELECT (CURRENT_SETTING(_property)));
        IF _value IS NULL THEN
            RAISE NOTICE ' global % NULL?', _property;
        END IF;
        IF LENGTH(TRIM(_value)) > 0 THEN
            value := (TRIM(_value))::REAL;
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;
