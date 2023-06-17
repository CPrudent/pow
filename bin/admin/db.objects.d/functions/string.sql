/***
 * add STRING facilities
 */

SELECT public.drop_all_functions_if_exists('public', 'alias_words');
CREATE OR REPLACE FUNCTION public.alias_words(
    words VARCHAR
    , separator VARCHAR
    , alias_name VARCHAR
)
RETURNS VARCHAR AS
$func$
DECLARE
    _word VARCHAR;
    _separator_out VARCHAR := REGEXP_REPLACE(separator, '\[.*\]\*', '');
    _aliased_word VARCHAR;
    _aliased_words VARCHAR;
BEGIN
    --RAISE NOTICE 'out: %', _separator_out;
    FOREACH _word IN ARRAY REGEXP_SPLIT_TO_ARRAY(words, separator) LOOP
        _aliased_word := CONCAT(alias_name, '.', _word);
        IF _aliased_words IS NULL THEN
            _aliased_words := _aliased_word;
        ELSE
            _aliased_words := CONCAT(_aliased_words, _separator_out, _aliased_word);
        END IF;
    END LOOP;
    RETURN _aliased_words;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT alias_words('id1, id2, id3', ',[ ]*', 'c');
 */
