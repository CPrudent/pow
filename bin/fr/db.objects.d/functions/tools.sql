/***
 * add FR-TOOLS facilities
 */

-- get value of parameters (threshold, ratio, ...)
SELECT drop_all_functions_if_exists('fr', 'get_parameter_value');
CREATE OR REPLACE FUNCTION fr.get_parameter_value(
        /* NOTE
        HSTORE parameter to custom properties, as:
        '"STREET_OCCURS" => 3'::HSTORE
        defaults are defined as global variables, view constant.sql
         */
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
    IF parameters IS NULL THEN
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
    ELSE
        /* NOTE
        HSTORE property as LEVEL_KEY => VALUE
         */
        _property := CONCAT_WS('_'
            , UPPER(level)
            , UPPER(key)
        );
        IF parameters ? _property THEN
            value := (parameters -> _property)::REAL;
        ELSE
            RAISE NOTICE ' pas de propriété % ?', _property;
        END IF;
    END IF;
END
$func$ LANGUAGE plpgsql;
