/***
 * add functionnalities to array management
 */

DROP AGGREGATE IF EXISTS public.array_agg_distinct(anyelement) CASCADE;

-- add new item in an array (if not already present)
SELECT public.drop_all_functions_if_exists('public', 'array_append_if_not_exists');
CREATE OR REPLACE FUNCTION public.array_append_if_not_exists(
    array_in IN anyarray
    , item IN anyelement
    )
RETURNS anyarray LANGUAGE plpgsql IMMUTABLE STRICT AS
$$
BEGIN
    IF item IS NOT NULL AND NOT ARRAY[item] <@ array_in THEN
        RETURN array_append(array_in, item);
    ELSE
        RETURN array_in;
    END IF;
END
$$;

/* TESTS
SELECT array_append_if_not_exists(ARRAY[1,2,3,4], 4) -> "{1,2,3,4}"
SELECT array_append_if_not_exists(ARRAY[1,2,3,4], 5) -> "{1,2,3,4,5}"
 */

-- return array w/ distincts values
SELECT public.drop_all_functions_if_exists('public', 'array_distinct');
CREATE OR REPLACE FUNCTION public.array_distinct(
    array_in IN anyarray
    , remove_nulls BOOLEAN DEFAULT TRUE
    )
RETURNS ANYARRAY AS 
$$
DECLARE
    _distinct_values array_in%TYPE := '{}';
BEGIN
    SELECT ARRAY_AGG(distinct_value)
    INTO _distinct_values
    FROM (
        SELECT DISTINCT valeur AS distinct_value
        FROM UNNEST(array_in) AS val
        WHERE (
            NOT remove_nulls
            OR
            val IS NOT NULL
        )
    ) AS t;
    RETURN _distinct_values;
END
$$ LANGUAGE plpgsql;

/* TESTS
SELECT array_distinct(ARRAY[1,1,2,3,4]) -> "{1,2,3,4}"
 */

-- shift(s) array
SELECT public.drop_all_functions_if_exists('public', 'array_shift');
CREATE OR REPLACE FUNCTION public.array_shift(
    array_in IN anyarray
    , nvalues_to_shift IN INTEGER DEFAULT 1
    )
RETURNS VARCHAR[] AS
$$
DECLARE
	out_array array_in%TYPE;
BEGIN
	WITH list AS (SELECT val FROM UNNEST(array_in) AS val OFFSET nvalues_to_shift)
	SELECT ARRAY_AGG(list.val) INTO out_array
	FROM list;
	
	RETURN out_array;
END
$$ LANGUAGE plpgsql;

/* TESTS
SELECT array_shift(ARRAY[1,2,3,4]) --> "{2,3,4}"
SELECT array_shift(ARRAY[1,2,3,4], 3) --> "{4}"
SELECT array_shift(ARRAY[1,2,3,4], 5) --> NULL
 */

CREATE AGGREGATE public.array_agg_distinct(
    -- the function seems not be called for NULL values
    sfunc    = public.array_append_if_not_exists,
    basetype = anyelement,
    stype    = anyarray,
    initcond = '{}'
);

/* Alternative : delete multiple at the end
DROP AGGREGATE IF EXISTS public.array_agg_distinct(anyelement) CASCADE;
CREATE AGGREGATE public.array_agg_distinct(
    sfunc    = array_append,
    basetype = anyelement,
    stype    = anyarray,
    initcond = '{}',
    finalfunc = array_distinct
);
 */

/* TESTS
WITH tests AS (
    SELECT NULL::INTEGER AS val
    UNION ALL
    SELECT * FROM generate_series(1,10)
    UNION ALL
    SELECT * FROM generate_series(5,15)
    UNION ALL
    SELECT NULL::INTEGER
)
SELECT array_agg_distinct(val) FROM tests
;

--> window
WITH tests AS (
    SELECT NULL::INTEGER AS val
    UNION ALL
    SELECT * FROM generate_series(1,10)
    UNION ALL
    SELECT * FROM generate_series(5,15)
    UNION ALL
    SELECT NULL::INTEGER
)
SELECT array_agg_distinct(val) OVER () FROM tests;
 */

/*
remove part of array
limit: returned array has not always the same order
see: https://stackoverflow.com/questions/49626115/postgresql-array-remove-for-elements-from-select

extension 'intarray' nice but only for int-arrays
 */
SELECT drop_all_functions_if_exists('public', 'array_remove');

CREATE OR REPLACE FUNCTION array_remove(
    ANYARRAY
    , ANYARRAY
    )
RETURNS ANYARRAY AS
$$
	SELECT ARRAY(SELECT UNNEST($1) EXCEPT SELECT UNNEST($2))
$$ LANGUAGE SQL;

/* TESTS
SELECT array_remove(ARRAY[1, 3, 5, 7], ARRAY[1,7]) -> {3,5}
SELECT array_remove(ARRAY[1, 3, 5, 7], ARRAY[2,7]) -> {5,1,3}
SELECT array_remove(ARRAY[1, 3, 3, 7], ARRAY[3,8,null]) -> {7,1}
 */
