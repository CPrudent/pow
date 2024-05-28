/***
 * add ARRAY facilities
 */

DROP AGGREGATE IF EXISTS public.array_agg_distinct(ANYELEMENT) CASCADE;

-- add new item in an array (if not already present)
SELECT public.drop_all_functions_if_exists('public', 'array_append_if_not_exists');
CREATE OR REPLACE FUNCTION public.array_append_if_not_exists(
    array_in ANYARRAY,
    item ANYELEMENT
)
RETURNS ANYARRAY LANGUAGE plpgsql IMMUTABLE STRICT AS
$$
BEGIN
    IF item IS NOT NULL AND NOT ARRAY[item] <@ array_in THEN
        RETURN ARRAY_APPEND(array_in, item);
    ELSE
        RETURN array_in;
    END IF;
END
$$;

/* TEST
SELECT array_append_if_not_exists(ARRAY[1,2,3,4], 4) -> "{1,2,3,4}"
SELECT array_append_if_not_exists(ARRAY[1,2,3,4], 5) -> "{1,2,3,4,5}"
 */

-- return array w/ distincts values
SELECT public.drop_all_functions_if_exists('public', 'array_distinct');
CREATE OR REPLACE FUNCTION public.array_distinct(
    array_in ANYARRAY,
    remove_nulls BOOLEAN DEFAULT TRUE
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

/* TEST
SELECT array_distinct(ARRAY[1,1,2,3,4]) -> "{1,2,3,4}"
 */

-- shift(s) array
SELECT public.drop_all_functions_if_exists('public', 'array_shift');
CREATE OR REPLACE FUNCTION public.array_shift(
    array_in ANYARRAY,
    nvalues_to_shift INTEGER DEFAULT 1
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

/* TEST
SELECT array_shift(ARRAY[1,2,3,4]) --> "{2,3,4}"
SELECT array_shift(ARRAY[1,2,3,4], 3) --> "{4}"
SELECT array_shift(ARRAY[1,2,3,4], 5) --> NULL
 */

CREATE AGGREGATE public.array_agg_distinct(
    -- the function seems not be called for NULL values
    sfunc    = public.array_append_if_not_exists,
    basetype = ANYELEMENT,
    stype    = ANYARRAY,
    initcond = '{}'
);

/* Alternative : delete multiple at the end
DROP AGGREGATE IF EXISTS public.array_agg_distinct(ANYELEMENT) CASCADE;
CREATE AGGREGATE public.array_agg_distinct(
    sfunc    = ARRAY_APPEND,
    basetype = ANYELEMENT,
    stype    = ANYARRAY,
    initcond = '{}',
    finalfunc = array_distinct
);
 */

/* TEST
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
    ANYARRAY,
    ANYARRAY
)
RETURNS ANYARRAY AS
$$
	SELECT ARRAY(SELECT UNNEST($1) EXCEPT SELECT UNNEST($2))
$$ LANGUAGE SQL;

/* TEST
SELECT array_remove(ARRAY[1, 3, 5, 7], ARRAY[1,7]) -> {3,5}
SELECT array_remove(ARRAY[1, 3, 5, 7], ARRAY[2,7]) -> {5,1,3}
SELECT array_remove(ARRAY[1, 3, 3, 7], ARRAY[3,8,null]) -> {7,1}
 */

-- https://gist.github.com/ryanguill/6c0e82dc7dee9d025bd27ad2abc274b9
SELECT drop_all_functions_if_exists('public', 'array_merge');
/*
CREATE OR REPLACE FUNCTION array_merge(
    a1 ANYARRAY,
    a2 ANYARRAY
)
RETURNS ANYARRAY AS
$$
    SELECT ARRAY_AGG(x ORDER BY x)
    FROM (
        SELECT DISTINCT UNNEST($1 || $2) AS x
    ) s;
$$ LANGUAGE SQL STRICT;
 */
/* TEST
but:
SELECT array_merge(NULL::INT[], ARRAY[2, 4, 8]) -> NULL
 */
CREATE OR REPLACE FUNCTION array_merge(
    a1 ANYARRAY,
    a2 ANYARRAY
)
RETURNS ANYARRAY AS
$$
DECLARE
    _array VARCHAR[];
BEGIN
    SELECT ARRAY_AGG(x ORDER BY x)
    INTO _array::ANYARRAY
    FROM (
        SELECT DISTINCT
            UNNEST(
                CASE
                WHEN COALESCE(CARDINALITY(a1), 0) > 0 AND COALESCE(CARDINALITY(a2), 0) > 0 THEN a1 || a2
                WHEN COALESCE(CARDINALITY(a1), 0) = 0 AND COALESCE(CARDINALITY(a2), 0) > 0 THEN a2
                WHEN COALESCE(CARDINALITY(a1), 0) > 0 AND COALESCE(CARDINALITY(a2), 0) = 0 THEN a1
                ELSE NULL
                END
            ) AS x
    ) t;

    RETURN _array;
END
$$ LANGUAGE plpgsql;

/* TEST
SELECT array_merge(ARRAY[1, 3, 5, 7], ARRAY[2,7]) -> {1,2,3,5,7}
SELECT array_merge(ARRAY[1, 3, 5], ARRAY[2, 4, 8]) -> {1,2,3,4,5,8}
SELECT array_merge(ARRAY[]::INT[], ARRAY[2, 4, 8]) -> {2,4,8}
SELECT array_merge(NULL::INT[], ARRAY[2, 4, 8]) -> {2,4,8}
SELECT array_merge(ARRAY[2, 4, 8], NULL) -> {2,4,8}
 */

-- concat items of array[from_ .. to_] to string (w/ separator)
SELECT drop_all_functions_if_exists('public', 'items_of_array_to_string');
CREATE OR REPLACE FUNCTION public.items_of_array_to_string(
    elements ANYARRAY,
    separator VARCHAR DEFAULT ' ',
    from_ INT DEFAULT NULL,
    to_ INT DEFAULT NULL
)
RETURNS VARCHAR AS
$func$
DECLARE
    _items INT := COALESCE(to_, ARRAY_UPPER(elements, 1));
    _i INT;
    _string VARCHAR;
BEGIN
    FOR _i IN COALESCE(from_, ARRAY_LOWER(elements, 1)) .. _items
    LOOP
        IF elements[_i] IS NOT NULL THEN
            IF _string IS NOT NULL THEN
                _string := CONCAT(_string, separator, elements[_i]);
            ELSE
                _string := elements[_i];
            END IF;
        END IF;
    END LOOP;

    RETURN _string;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT items_of_array_to_string(
    elements => '{1,2,3}'::VARCHAR[],
    from_ => 1,
    to_ => 2
) => '1 2'
SELECT items_of_array_to_string(
    elements => '{1,2,3}'::VARCHAR[],
    from_ => 1,
    --to_ => 2
) => '1 2 3'
SELECT items_of_array_to_string(
    elements => '{1,2,3}'::VARCHAR[],
    from_ => 1,
    to_ => 0
) => NULL
 */
