/***
 * add RANDOM facilities
 */

-- obtains random number into range
SELECT public.drop_all_functions_if_exists('public', 'random_between');
CREATE OR REPLACE FUNCTION random_between(
    low INT
    , high INT
    )
RETURNS INT AS
$$
BEGIN
    RETURN FLOOR(random() * (high - low + 1) + low);
END;
$$ LANGUAGE 'plpgsql' STRICT;

-- obtains n random numbers into range
CREATE OR REPLACE FUNCTION random_between(
    n INT
    , low INT
    , high INT
    )
RETURNS INT[] AS
$$
DECLARE
	_list INT[];
	_item INT;
BEGIN
    WHILE (COALESCE(ARRAY_LENGTH(_list, 1), 0) < n)
    LOOP
        SELECT random_between(low, high) INTO _item;
        IF _list IS NULL THEN
            _list := ARRAY[_item];
        ELSE
            IF NOT _item = ANY(_list) THEN
                _list := ARRAY_APPEND(_list, _item);
            END IF;
        END IF;
        --RAISE NOTICE '_item=% len=% array=%', _item, COALESCE(ARRAY_LENGTH(_list, 1), 0), _list;
    END LOOP;

    RETURN _list;
END;
$$ LANGUAGE 'plpgsql' STRICT;

/* TESTS
SELECT random_between(1, 100) -> 87
SELECT random_between(1, 100) -> 3

SELECT UNNEST(random_between(10, 1, 100)) ->
13
71
48
40
80
43
68
69
53
19
 */
											   
