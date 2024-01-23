/***
 * add ADDRESS facilities
 */

-- delete (or test) "bad" space(s) in name
SELECT public.drop_all_functions_if_exists('public', 'bad_space_in_name');
CREATE OR REPLACE FUNCTION public.bad_space_in_name(
    name INOUT VARCHAR
    , test_only IN BOOLEAN DEFAULT FALSE
    , to_fix OUT BOOLEAN
)
AS
$func$
BEGIN
    -- heading or trailing space(s)
    IF (name ~ '^ +' OR name ~ ' +$') THEN
        IF NOT test_only THEN
            name := TRIM(name);
        ELSE
            to_fix := TRUE;
            RETURN;
        END IF;
    END IF;
    -- multiple spaces
    IF (name ~ '[ ]{2,}') THEN
        IF NOT test_only THEN
            name := REGEXP_REPLACE(name, '[ ]{2,}', ' ', 'g');
        ELSE
            to_fix := TRUE;
            RETURN;
        END IF;
    END IF;
    IF NOT test_only THEN
        to_fix := FALSE;
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT CONCAT('"', (SELECT name FROM public.bad_space_in_name(' WITH SPACE AT BEGIN')), '"');
SELECT CONCAT('"', (SELECT name FROM public.bad_space_in_name('WITH SPACE AT END ')), '"');
SELECT CONCAT('"', (SELECT name FROM public.bad_space_in_name(' WITH   MULTIPLE    SPACES  ')), '"');
 */

-- clean address label (upcase, no special chars, only alphanum)
SELECT public.drop_all_functions_if_exists('public', 'clean_address_label');
CREATE OR REPLACE FUNCTION clean_address_label(
    name INOUT VARCHAR
)
AS
$func$
BEGIN
    IF (SELECT to_fix FROM bad_space_in_name(name, test_only => TRUE)) THEN
        name := (SELECT bs.name FROM bad_space_in_name(name) bs);
    END IF;

    name := TRANSLATE(
        UPPER(name)
        , 'ÀÁÂÃÄÅÇÊÉÈËÌÍÎÏÌÑÒÓÔÕÖÙÚÛÜÝŸ'
        , 'AAAAAACEEEEIIIIINOOOOOUUUUYY'
    );
    name := REPLACE(name, 'Œ', 'OE');
    name := REPLACE(name, 'Æ', 'AE');
    --exemples : '"’-&°
    name := TRIM(REGEXP_REPLACE(name, '[^A-Z0-9]+', ' ', 'g'));
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT CONCAT('"', clean_address_label('ÀÁÂÃÄÅÇÊÉÈËÌÍÎÏÌÑÒÓÔÕÖÙÚÛÜÝŸ'), '"');
SELECT CONCAT('"', clean_address_label('ÀÁÂÃÄÅ  Ç  ÊÉÈË  ÌÍÎÏÌ Ñ ÒÓÔÕÖ ÙÚÛÜ  ÝŸ'), '"');
SELECT CONCAT('"', clean_address_label('  Œ '), '"');
SELECT CONCAT('"', clean_address_label(' Æ  '), '"');
SELECT CONCAT('"', clean_address_label('( HELLO WORLD !)'), '"');
 */

-- transform label to code
SELECT public.drop_all_functions_if_exists('public', 'label_to_code');
CREATE OR REPLACE FUNCTION label_to_code(
    name INOUT VARCHAR
    )
AS
$func$
DECLARE
BEGIN
    name := LOWER(REPLACE(clean_address_label(name), ' ', '_'));
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT label_to_code('ÀÁÂÃÄÅÇÊÉÈËÌÍÎÏÌÑÒÓÔÕÖÙÚÛÜÝŸ');
SELECT label_to_code('ÀÁÂÃÄÅ  Ç  ÊÉÈË  ÌÍÎÏÌ Ñ ÒÓÔÕÖ ÙÚÛÜ  ÝŸ');
SELECT label_to_code('  Œ ');
SELECT label_to_code(' Æ  ');
SELECT label_to_code('( HELLO WORLD !)');
 */

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
