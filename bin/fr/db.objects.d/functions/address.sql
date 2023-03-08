/***
 * add FR-ADDRESS facilities
 */

-- deduce department code from municipality code
SELECT public.drop_all_functions_if_exists('fr', 'get_department_code_from_municipality_code');
CREATE OR REPLACE FUNCTION fr.get_department_code_from_municipality_code(
    municipality_code CHARACTER(5)
)
RETURNS CHARACTER VARYING(3)
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE
        -- DOM + (98) = POLYNESIE
        WHEN LEFT(municipality_code, 2) IN ('97', '98') THEN LEFT(municipality_code, 3)
        -- FRANCE métropolitaine + (99) = MONACO
        ELSE LEFT(municipality_code, 2)
        END;
END
$func$ LANGUAGE plpgsql;
