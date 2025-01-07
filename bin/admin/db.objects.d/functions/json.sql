/***
 * add JSON facilities
 */

-- merge JSONB into a JSONB data
-- https://stackoverflow.com/questions/30101603/merging-concatenating-jsonb-columns-in-query
SELECT public.drop_all_functions_if_exists('public', 'jsonb_merge');
CREATE OR REPLACE FUNCTION jsonb_merge(
    json_a JSONB,
    json_b JSONB
)
RETURNS JSONB AS
$func$
    SELECT
        CASE JSONB_TYPEOF(json_a)
        WHEN 'object' THEN
            CASE JSONB_TYPEOF(json_b)
            WHEN 'object' THEN (
                SELECT JSONB_OBJECT_AGG(
                    k,
                    CASE
                    WHEN e2.v IS NULL THEN e1.v
                    WHEN e1.v IS NULL THEN e2.v
                    WHEN e1.v = e2.v THEN e1.v
                    ELSE jsonb_merge(e1.v, e2.v)
                    END
                )
                FROM JSONB_EACH(json_a) e1(k, v)
                    FULL OUTER JOIN JSONB_EACH(json_b) e2(k, v) USING (k)
            )
            ELSE json_b
            END
        WHEN 'array' THEN json_a || json_b
        ELSE json_b
        END
    ;
$func$ LANGUAGE sql;

/* TEST

SELECT jsonb_merge(
    '{"a":{"a":"a_a_value1", "b":"b_b_value1", "c":"c_c_value1"}, "b":"b_value_1"}'::JSONB
    ,'{"a":{"a":"a_a_value2", "d":"d_d_value2"}, "b":"b_value_2"}'::JSONB
)

SELECT jsonb_merge(
    '{"integration": {"streets": 0, "housenumbers": 0, "fixes": [{"name":"SPACE_IN_CODE", "housenumbers": 0}]}}'::JSONB,
    '{"integration": {"fixes": [{"name":"CONVERT_ATTRIBUTES"}]}}'::JSONB
)
->
{"integration": {"fixes": [{"name": "SPACE_IN_CODE", "housenumbers": 0}, {"name": "CONVERT_ATTRIBUTES"}], "streets": 0, "housenumbers": 0}}"
 */
