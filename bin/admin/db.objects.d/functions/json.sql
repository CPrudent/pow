/***
 * add JSON facilities
 */

-- merge field into a JSON data
SELECT public.drop_all_functions_if_exists('public', 'jsonb_merge');
CREATE OR REPLACE FUNCTION jsonb_merge(
    json_a JSONB
    , json_b JSONB
    , on_key_exists VARCHAR DEFAULT 'B_REPLACE_A'   -- A_REPLACE_B, CONCAT
    )
RETURNS JSONB AS
$func$
DECLARE
	_json_merged JSONB;
BEGIN
    SELECT
        JSONB_OBJECT_AGG(
            COALESCE(ka, kb)
            , CASE
                WHEN va IS NULL THEN vb
                WHEN vb IS NULL THEN va
                WHEN JSONB_TYPEOF(va) = 'object' AND JSONB_TYPEOF(vb) = 'object' THEN jsonb_merge(va, vb)
                ELSE
                    CASE on_key_exists
                        WHEN 'CONCAT' THEN va || vb
                        WHEN 'A_REPLACE_B' THEN va
                        WHEN 'B_REPLACE_A' THEN vb
                    END
            END
        )
    INTO _json_merged
    FROM JSONB_EACH(json_a) e1(ka, va)
        FULL OUTER JOIN JSONB_EACH(json_b) e2(kb, vb) ON ka = kb;
    RETURN _json_merged;
END
$func$ LANGUAGE plpgsql;

/* TESTS
SELECT jsonb_merge(
'{"a":{"a":"a_a_value1", "b":"b_b_value1", "c":"c_c_value1"}, "b":"b_value_1"}'::JSONB
,'{"a":{"a":"a_a_value2", "d":"d_d_value2"}, "b":"b_value_2"}'::JSONB
);
 */
