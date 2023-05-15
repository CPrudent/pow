/***
 * add ADDRESS facilities
 */

-- clean address label (upcase, no special chars, only alphanum)
SELECT public.drop_all_functions_if_exists('public', 'clean_address_label');
CREATE OR REPLACE FUNCTION clean_address_label(
    address_label CHARACTER VARYING
)
RETURNS CHARACTER VARYING AS
$func$
BEGIN
    address_label := TRANSLATE(UPPER(address_label), 'ÀÁÂÃÄÅÇÊÉÈËÌÍÎÏÌÑÒÓÔÕÖÙÚÛÜÝŸ', 'AAAAAACEEEEIIIIINOOOOOUUUUYY');
    address_label := REPLACE(address_label, 'Œ', 'OE');
    address_label := REPLACE(address_label, 'Æ', 'AE');
    --exemples : '"’-&°
    address_label := TRIM(REGEXP_REPLACE(address_label, '[^A-Z0-9]+', ' ', 'g'));
    RETURN address_label;
END
$func$ LANGUAGE plpgsql;

-- transform label to code
SELECT public.drop_all_functions_if_exists('public', 'label_to_code');
CREATE OR REPLACE FUNCTION label_to_code(
    address_label CHARACTER VARYING
    )
RETURNS CHARACTER VARYING AS
$func$
DECLARE
BEGIN
	RETURN LOWER(REPLACE(clean_address_label(address_label), ' ', '_'));
END
$func$ LANGUAGE plpgsql;

-- greatest gap into serie of number(s)
SELECT drop_all_functions_if_exists('public', 'get_greatest_gap');
CREATE OR REPLACE FUNCTION public.get_greatest_gap(
    _numbers INTEGER[]
    )
RETURNS INTEGER AS
$func$
DECLARE
    _max_gap INTEGER;
    _number INTEGER;
    _previous_number INTEGER;
BEGIN
    FOREACH _number IN ARRAY _numbers LOOP
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
