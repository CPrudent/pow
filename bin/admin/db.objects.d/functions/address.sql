/***
 * add ADDRESS facilities
 */

-- deduce department code from district code
SELECT public.drop_all_functions_if_exists('public', 'get_department_code_from_district_code');
CREATE OR REPLACE FUNCTION public.get_department_code_from_district_code(
    district_code CHARACTER(5)
    )
RETURNS CHARACTER VARYING(3)
IMMUTABLE
AS
$func$
BEGIN
    -- LAPOSTE/RAN db
    RETURN CASE
        -- DOM + (98) = POLYNESIE
        WHEN LEFT(district_code, 2) IN ('97', '98') THEN LEFT(district_code, 3)
        -- FRANCE métropolitaine + (99) = MONACO
        ELSE LEFT(district_code, 2)
        END;
END
$func$ LANGUAGE plpgsql;

-- clean address label (upcase, no special chars, only alphanum)
SELECT public.drop_all_functions_if_exists('public', 'clean_address_label');
CREATE OR REPLACE FUNCTION clean_address_label(
    label_in CHARACTER VARYING
    )
RETURNS CHARACTER VARYING AS
$func$
BEGIN
	label_in := TRANSLATE(UPPER(label_in),'ÀÁÂÃÄÅÇÊÉÈËÌÍÎÏÌÑÒÓÔÕÖÙÚÛÜ','AAAAAACEEEEIIIIINOOOOOUUUU');
	label_in := REPLACE(label_in,'Œ','OE');
	label_in := REPLACE(label_in,'Æ','AE');
	--exemples : '"’-&°
	label_in := TRIM(REGEXP_REPLACE(label_in,'[^A-Z0-9]+',' ','g'));
	return label_in;
END
$func$ LANGUAGE plpgsql;

-- transform label to code
SELECT public.drop_all_functions_if_exists('public', 'label_to_code');
CREATE OR REPLACE FUNCTION label_to_code(
    label_in CHARACTER VARYING
    )
RETURNS CHARACTER VARYING AS
$func$
DECLARE
BEGIN
	return LOWER(REPLACE(clean_address_label(label_in),' ','_'));
END
$func$ LANGUAGE plpgsql;

-- greatest gap between serie of number(s)
SELECT drop_all_functions_if_exists('public','get_greatest_gap');
CREATE OR REPLACE FUNCTION public.get_greatest_gap(
    in_ar_suite_entiers IN INTEGER[]
    )
RETURNS INTEGER AS
$func$
DECLARE
    _max_gap INTEGER;
    _number INTEGER;
    _previous_number INTEGER;
BEGIN
    FOREACH _number IN ARRAY in_ar_suite_entiers LOOP
        -- not the first && not null
        IF _previous_number IS NOT NULL AND _number IS NOT NULL THEN
            IF _max_gap IS NULL OR (_number - _previous_number) > _max_gap THEN
                _max_gap := _number - _previous_number;
            END IF;
        END IF;
        _previous_number := _number;
    END LOOP;
    RETURN _max_gap;
END
$func$ LANGUAGE plpgsql;
