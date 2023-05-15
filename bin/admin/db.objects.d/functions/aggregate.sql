/***
 * add AGG facilities
 */

-- see: https://wiki.postgresql.org/wiki/First/last_(aggregate)

-- create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION public.first_agg(
    ANYELEMENT
    , ANYELEMENT
    )
RETURNS ANYELEMENT LANGUAGE SQL IMMUTABLE STRICT AS
$$
    SELECT $1;
$$;
 
-- and then wrap an aggregate around it
DROP AGGREGATE IF EXISTS public.FIRST(ANYELEMENT) CASCADE;
CREATE AGGREGATE public.FIRST(
    sfunc    = public.first_agg,
    basetype = ANYELEMENT,
    stype    = ANYELEMENT
);
 
-- create a function that always returns the last non-NULL item
CREATE OR REPLACE FUNCTION public.last_agg(ANYELEMENT, ANYELEMENT)
RETURNS ANYELEMENT LANGUAGE SQL IMMUTABLE STRICT AS
$$
    SELECT $2;
$$;
 
-- and then wrap an aggregate around it
DROP AGGREGATE IF EXISTS public.LAST(ANYELEMENT) CASCADE;
CREATE AGGREGATE public.LAST(
    sfunc    = public.last_agg,
    basetype = ANYELEMENT,
    stype    = ANYELEMENT
);

-- uniq aggregate
SELECT public.drop_all_functions_if_exists('public', 'null_if_not_equal');
CREATE OR REPLACE FUNCTION public.null_if_not_equal(
    val_a ANYELEMENT
    , val_b ANYELEMENT
    )
RETURNS ANYELEMENT LANGUAGE plpgsql IMMUTABLE /*STRICT*/ AS
$$
BEGIN
    --RAISE NOTICE '% = % ?', val_a, val_b;
    -- optimisation: stop once NULL found
    IF val_a IS NULL OR val_b IS NULL THEN RETURN NULL; END IF;
    -- no comparison for first call
    IF val_a = 'INIT_VALUE' THEN RETURN val_b; END IF;
    IF val_a = val_b THEN
        RETURN val_a;
    ELSE
        RETURN NULL;
    END IF;
END
$$;

DROP AGGREGATE IF EXISTS public.unique_agg(ANYELEMENT) CASCADE;
CREATE AGGREGATE public.unique_agg(
    -- force replace NULL before: NULLIF(UNIQUE_AGG(COALESCE(ma_colonne_varchar,'NULL')),'NULL')
    sfunc = public.null_if_not_equal
    ,basetype = ANYELEMENT
    ,stype = ANYELEMENT
    ,initcond = 'INIT_VALUE'
);

-- exists aggregate
DROP AGGREGATE IF EXISTS public.exists_agg(bool) CASCADE;
SELECT public.drop_all_functions_if_exists('public', 'exists_agg_fn');
CREATE OR REPLACE FUNCTION public.exists_agg_fn(
    was_true BOOLEAN
    , is_true BOOLEAN
    )
RETURNS BOOLEAN AS
$$
BEGIN
    IF was_true THEN
        RETURN TRUE;
    ELSE
        RETURN COALESCE(is_true, FALSE);
    END IF;
END
$$ LANGUAGE plpgsql;

CREATE AGGREGATE public.exists_agg(
    sfunc    = public.exists_agg_fn,
    basetype = bool,
    stype    = bool
);
