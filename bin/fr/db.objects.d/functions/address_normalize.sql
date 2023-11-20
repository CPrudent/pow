/***
 * add FR-ADDRESS facilities (normalized label, following AFNOR NF Z 10-011 (1/2013))
 */

SELECT public.drop_all_functions_if_exists('fr', 'normalize_municipality_name');
CREATE OR REPLACE FUNCTION fr.normalize_municipality_name(
    code VARCHAR
    , name VARCHAR
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _name VARCHAR;
    _name_normalized VARCHAR;
    _words TEXT[];
    _words_normalized VARCHAR[];
    _word_end VARCHAR;
BEGIN
    -- deal w/ exceptions
    SELECT value
    INTO _name_normalized
    FROM fr.constant
    WHERE
        usecase = 'LAPOSTE_MUNICIPALITY_EXCEPTION'
        AND
        key = code
        ;
    IF FOUND THEN
        return _name_normalized;
    END IF;

    -- only upper and not special characters
    _name := clean_address_label(name);

    -- replace (SAINT|SAINTE)
    IF _name LIKE '% SAINT' OR _name LIKE '% SAINTE' THEN
        -- exception if it's the name itself
        return _name;
    END IF;
    -- as starting word
    IF _name LIKE 'SAINT %' THEN
        _name := CONCAT('ST ', SUBSTR(_name, 7));
    ELSIF _name LIKE 'SAINTE %' THEN
        _name := CONCAT('STE ', SUBSTR(_name, 8));
    END IF;
    -- else anywhere (but at the end)
    return REPLACE(REPLACE(_name, ' SAINTE ', ' STE '), ' SAINT ', ' ST ');

    /* NOTE
     avoid REGEX because too expansive! in run-time
     */
    _words := REGEXP_SPLIT_TO_ARRAY(_name, '\s+');
    FOR _i IN 1..ARRAY_LENGTH(_words, 1) LOOP
        IF _words[_i] ~* '^SAINT[E]?$' THEN
            -- exception if it's the name itself
            IF _i = 2 THEN
                IF _words[_i -1] ~* '^(LE|LA)$' THEN
                    _words_normalized := ARRAY_APPEND(_words_normalized, _words[_i]);
                    CONTINUE;
                END IF;
            END IF;
            _word_end := (REGEXP_MATCH(_words[_i], 'SAINT([E]?)', 'i'))[1];
            _words_normalized := ARRAY_APPEND(_words_normalized, CONCAT('ST', UPPER(_word_end)));
            CONTINUE;
        END IF;
        _words_normalized := ARRAY_APPEND(_words_normalized, _words[_i]);
    END LOOP;

    return ARRAY_TO_STRING(_words_normalized, ' ');
END
$func$ LANGUAGE plpgsql;

/* TEST
-- municipality differences
SELECT *
FROM (
    SELECT
        za.co_insee_commune AS municipality_code
        , c.nom AS name
        , fr.normalize_municipality_name(c.insee_com, c.nom) AS name_normalized
        , za.lb_ach_nn AS name_normalized_laposte
    FROM
        fr.laposte_zone_address za
            JOIN fr.ign_municipality c ON za.co_insee_commune = c.insee_com
    WHERE
        za.fl_active
        AND
        za.lb_l5_nn IS NULL
    ) t
WHERE
    name_normalized != name_normalized_laposte
ORDER BY
    1
    ;
 */
