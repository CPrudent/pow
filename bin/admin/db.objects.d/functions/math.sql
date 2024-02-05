/***
 * add MATH facilities
 */

-- generate all subsets of set
-- https://math.stackexchange.com/questions/349220/is-there-an-algorithm-to-find-all-subsets-of-a-set
SELECT public.drop_all_functions_if_exists('public', 'subsets');
CREATE OR REPLACE FUNCTION public.subsets(
    set IN VARCHAR[]
    , n IN INT
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , subsets OUT VARCHAR[]
)
AS
$func$
DECLARE
    _flags INT[] := ARRAY_FILL(0, ARRAY[n]);
    _pos INT;
    _subset VARCHAR[];
BEGIN
    WHILE TRUE
    LOOP
        _subset := '{}'::VARCHAR[];
        _pos := 0;
        WHILE _pos < n
        LOOP
            IF _flags[_pos +1] = 1 THEN
                _subset := ARRAY_APPEND(_subset, set[_pos +1]);
            END IF;
            _pos := _pos +1;
        END LOOP;

        IF raise_notice THEN RAISE NOTICE '{%}', ARRAY_TO_STRING(_subset, ', '); END IF;
        subsets := ARRAY_APPEND(subsets, ARRAY_TO_STRING(_subset, ','));

        _pos := 0;
        WHILE _pos < n
        LOOP
            IF _flags[_pos +1] = 0 THEN
                _flags[_pos +1] := 1;
                EXIT;
            END IF;

            _flags[_pos +1] := 0;
            _pos := _pos +1;
        END LOOP;

        IF _pos = n THEN
            EXIT;
        END IF;
    END LOOP;
END
$func$ LANGUAGE plpgsql;
