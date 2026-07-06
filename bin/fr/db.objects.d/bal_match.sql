/***
 * FR: BAL match
 */

-- clean BAL match result(s) w/ old and/or updated addresses
SELECT public.drop_all_functions_if_exists('fr', 'bal_match_clean');
CREATE OR REPLACE FUNCTION fr.bal_match_clean(
    code IN VARCHAR,                -- municipality
    todo IN INT,                    -- what is to do (field of bits)
    simulation IN BOOLEAN DEFAULT FALSE,
    counters OUT INT[]              -- result counters {+,-,!}
)
AS
$func$
DECLARE
    _requests       INT[];
    _n              INT;
    _i              INT;
    _j              INT;
    _nrows          INT;
BEGIN
    counters := ARRAY_FILL(0, ARRAY[2]);

    SELECT
        ARRAY_AGG(id ORDER BY date_create DESC)
    INTO
        _requests
    FROM
        fr.address_match_request
    WHERE
        source_name ~ CONCAT('BAL_', code)
    ;

    _n := COALESCE(CARDINALITY(_requests), 0);
    CALL public.log_info(
        FORMAT('Suppression BAL_%s (Rapprochement(s) %s)', code, _requests)
    );
    FOR _i IN 1 .. _n
    LOOP
        -- clean old addresses
        IF todo & 1 = 1 THEN
            IF NOT simulation THEN
                DELETE FROM fr.address_match_result mr
                WHERE
                    mr.id_request = _requests[_i]
                    AND
                    (
                        (
                            (mr.standardized_address).level = 'HOUSENUMBER'
                            AND
                            NOT EXISTS(
                                SELECT 1
                                FROM fr.bal_housenumber b
                                WHERE b.code = (mr.standardized_address).id
                            )
                        )
                        OR
                        (
                            (mr.standardized_address).level = 'STREET'
                            AND
                            NOT EXISTS(
                                SELECT 1
                                FROM fr.bal_street b
                                WHERE b.code = (mr.standardized_address).id
                            )
                        )
                    )
                ;
            ELSE
                PERFORM * FROM fr.address_match_result mr
                WHERE
                    mr.id_request = _requests[_i]
                    AND
                    (
                        (
                            (mr.standardized_address).level = 'HOUSENUMBER'
                            AND
                            NOT EXISTS(
                                SELECT 1
                                FROM fr.bal_housenumber b
                                WHERE b.code = (mr.standardized_address).id
                            )
                        )
                        OR
                        (
                            (mr.standardized_address).level = 'STREET'
                            AND
                            NOT EXISTS(
                                SELECT 1
                                FROM fr.bal_street b
                                WHERE b.code = (mr.standardized_address).id
                            )
                        )
                    )
                ;
            END IF;
            GET DIAGNOSTICS _nrows = ROW_COUNT;
            counters[1] := counters[1] + _nrows;
            CALL public.log_info(
                FORMAT('Suppression BAL_%s (Rapprochement %s): #%s addresse(s) effacée(s)',
                    code,
                    _requests[_i],
                    _nrows
                )
            );
        END IF;

        -- clean updated addresses (in previous match(s))
        IF todo & 2 = 2 THEN
            CONTINUE WHEN _i = 1;

            FOR _j IN _i .. _n
            LOOP
                IF NOT simulation THEN
                    DELETE FROM fr.address_match_result mr2
                    USING fr.address_match_result mr1
                    WHERE
                        mr1.id_request = _requests[_i -1]
                        AND
                        mr2.id_request = _requests[_j]
                        AND
                        (mr2.standardized_address).id = (mr1.standardized_address).id
                    ;
                ELSE
                    PERFORM * FROM fr.address_match_result mr2, fr.address_match_result mr1
                    WHERE
                        mr1.id_request = _requests[_i -1]
                        AND
                        mr2.id_request = _requests[_j]
                        AND
                        (mr2.standardized_address).id = (mr1.standardized_address).id
                    ;
                END IF;
                GET DIAGNOSTICS _nrows = ROW_COUNT;
                counters[2] := counters[2] + _nrows;
                CALL public.log_info(
                    FORMAT('Suppression BAL_%s (Rapprochement %s/%s): #%s addresse(s) modifiée(s)',
                        code,
                        _requests[_i -1],
                        _requests[_j],
                        _nrows
                    )
                );
            END LOOP;
        END IF;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

/*
 * TEST
 *
SELECT * FROM fr.bal_match_clean(code => '55093', todo => 3, simulation => true)

NOTICE:  14:53:02.961 Suppression BAL_55093 (Rapprochement(s) {25513,25509,8697})
NOTICE:  14:53:02.962 Suppression BAL_55093 (Rapprochement 25513): #0 addresse(s) effacée(s)
NOTICE:  14:53:02.962 Suppression BAL_55093 (Rapprochement 25509): #0 addresse(s) effacée(s)
NOTICE:  14:53:02.962 Suppression BAL_55093 (Rapprochement 25513/25509): #0 addresse(s) modifiée(s)
NOTICE:  14:53:02.962 Suppression BAL_55093 (Rapprochement 25513/8697): #3 addresse(s) modifiée(s)
NOTICE:  14:53:02.964 Suppression BAL_55093 (Rapprochement 8697): #1 addresse(s) effacée(s)
NOTICE:  14:53:02.964 Suppression BAL_55093 (Rapprochement 25509/8697): #0 addresse(s) modifiée(s)
 */
