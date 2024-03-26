/***
 * add FR-ADDRESS facilities (normalized label, following AFNOR NF Z 10-011 (1/2013))
 */

/* NOTE
LAPOSTE
street descriptor items
 A article
 C number
 E reserved word
 N name
 P firstname
 T title
 V type
+
complement descriptor items
 G group 3
 H group 2
 I group 1
 */

-- is number (date, roman, arabic)
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_number');
CREATE OR REPLACE FUNCTION fr.is_normalized_number(
    word VARCHAR
    , only_digit VARCHAR DEFAULT 'ALL'   -- ARABIC|COMPLEMENT|DATE|HOUSENUMBER|ROAD_NETWORK|ROMAN
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_number BOOLEAN;
    _only VARCHAR[];
    _i INT;
    _re VARCHAR;
BEGIN
    IF LENGTH(word) = 0 THEN RETURN FALSE; END IF;

    IF only_digit = 'ALL' THEN
        _only := '{ARABIC,COMPLEMENT,DATE,HOUSENUMBER,ROAD_NETWORK,ROMAN}'::VARCHAR[];
    ELSE
        _only := STRING_TO_ARRAY(only_digit, ',');
    END IF;

    -- roman number
    -- https://www.geeksforgeeks.org/validating-roman-numerals-using-regular-expression/

    FOR _i IN 1 .. ARRAY_LENGTH(_only, 1)
    LOOP
        IF UPPER(_only[_i]) = 'HOUSENUMBER' THEN
            SELECT
                ARRAY_TO_STRING(
                    ARRAY_AGG(name ORDER BY CASE
                        -- A .. Z
                        WHEN LENGTH(name) = 1   THEN ASCII(name) - ASCII('A') + 1
                        -- EXT before LETTER (BIS before B)
                        WHEN name = 'BIS'       THEN 2 - .1
                        WHEN name = 'TER'       THEN 3 - .1
                        WHEN name = 'QUATER'    THEN 4 - .1
                        WHEN name = 'QUINQUIES' THEN 5 - .1
                        WHEN name = 'SEXTO'     THEN 6 - .1
                        END
                    )
                    , '|'
                )
            INTO
                _re
            FROM
                fr.laposte_address_keyword
            WHERE
                "group" = 'EXT'
                ;
        END IF;

        _is_number := CASE
            WHEN UPPER(_only[_i]) = 'ARABIC' THEN (word ~ '^[0-9]+$')
            /* NOTE
            building number: as B10, B10E
            from-to: 1A10
             */
            WHEN UPPER(_only[_i]) = 'COMPLEMENT' THEN (word ~ '^([A-Z]{1,2}[0-9]+([A-Z])?|[0-9]+A[0-9]+)$')
            WHEN UPPER(_only[_i]) = 'DATE' THEN
                (
                    -- UK dates
                    word ~ '^([2-9][0-9]*)?(1ST|2ND|3RD|([4-9]|[1-9]+[0-9]*0?)TH)$'
                )
                OR
                (
                    -- FR dates
                    word ~ '^(1(ERE?)?|([2-9][0-9]*|1[0-9]+)*I?(E|EME)?)$'
                )
            WHEN UPPER(_only[_i]) = 'HOUSENUMBER' THEN (word ~ CONCAT('^[0-9]+(', _re, ')$'))
            WHEN UPPER(_only[_i]) = 'ROMAN' THEN (word ~ '^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')
            WHEN UPPER(_only[_i]) = 'ROAD_NETWORK' THEN (word ~ '^(A|B|CD?|CR|D|E?V|GR?|N|R|RD|RN|S|T|VC)?([0-9]+(E[0-9]*)?|E[0-9]*)$')
        END IF;
        IF _is_number THEN
            RETURN TRUE;
        END IF;
    END LOOP;

    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

-- is article
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_article');
CREATE OR REPLACE FUNCTION fr.is_normalized_article(
    word VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _articles TEXT[] := '{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}'::TEXT[];
    _is_article BOOLEAN := FALSE;
BEGIN
    IF word = ANY(_articles) THEN
        _is_article := TRUE;
    END IF;
    RETURN _is_article;
END
$func$ LANGUAGE plpgsql;

-- is reserved word
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_reserved_word');
CREATE OR REPLACE FUNCTION fr.is_normalized_reserved_word(
    word VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_reserved BOOLEAN := FALSE;
BEGIN
    IF word ~ '^(INFERIEUR|SUPERIEUR|PROLONGE)E?S?$' THEN
        _is_reserved := TRUE;
    END IF;
    RETURN _is_reserved;
END
$func$ LANGUAGE plpgsql;

-- is firstname
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_firstname');
CREATE OR REPLACE FUNCTION fr.is_normalized_firstname(
    word VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_firstname BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1
        FROM fr.constant c JOIN fr.laposte_address_street_word_descriptor w ON c.key = w.word
        WHERE
            c.usecase = 'LAPOSTE_STREET_FIRSTNAME'
            AND
            w.word = is_normalized_firstname.word
            AND
            -- at least 5%, others are ignored
            (	as_fname >= (
                    COALESCE(as_name, 0)
                    + COALESCE(as_reserved, 0)
                    + COALESCE(as_article, 0)
                    + COALESCE(as_number, 0)
                    + COALESCE(as_title, 0)
                    + COALESCE(as_type, 0)
                ) * 0.05
            )

    )
    INTO _is_firstname
    ;

    RETURN _is_firstname;
END
$func$ LANGUAGE plpgsql;

-- is title
SELECT public.drop_all_functions_if_exists('fr', 'is_normalized_title');
CREATE OR REPLACE FUNCTION fr.is_normalized_title(
    word VARCHAR
    , groups VARCHAR DEFAULT 'ALL'   -- TITLE|TYPE|EXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _is_title BOOLEAN;
    _groups VARCHAR[];
BEGIN
    IF groups = 'ALL' THEN
        _groups := '{TITLE,TYPE,EXT}'::VARCHAR[];
    ELSE
        _groups := STRING_TO_ARRAY(groups, ',');
    END IF;

    SELECT EXISTS(
        SELECT 1
        FROM fr.laposte_address_keyword k
        WHERE
            ARRAY_POSITION(_groups, k.group) > 0
            AND
            k.name = is_normalized_title.word
    )
    INTO _is_title
    ;

    RETURN _is_title;
END
$func$ LANGUAGE plpgsql;

-- abbreviate holy word(s)
SELECT drop_all_functions_if_exists('fr', 'normalize_abbreviate_holy');
CREATE OR REPLACE FUNCTION fr.normalize_abbreviate_holy(
    name IN VARCHAR
)
RETURNS VARCHAR AS
$func$
BEGIN
    -- replace (SAINT|SAINTE)
    IF name LIKE '% SAINT' OR name LIKE '% SAINTE' THEN
        -- exception if it's the name itself
        RETURN name;
    END IF;
    -- as starting word
    IF name LIKE 'SAINT %' THEN
        name := CONCAT('ST ', SUBSTR(name, 7));
    ELSIF name LIKE 'SAINTE %' THEN
        name := CONCAT('STE ', SUBSTR(name, 8));
    END IF;
    -- else anywhere (but at the end)
    RETURN REPLACE(REPLACE(name, ' SAINTE ', ' STE '), ' SAINT ', ' ST ');

    /* NOTE
DECLARE
    _name VARCHAR;
    _name_normalized VARCHAR;
    _words TEXT[];
    _words_normalized VARCHAR[];
    _word_end VARCHAR;

     avoid REGEX because too expansive! in run-time
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
    RETURN ARRAY_TO_STRING(_words_normalized, ' ');
     */
END
$func$ LANGUAGE plpgsql;

-- abbreviate keyword
SELECT drop_all_functions_if_exists('fr', 'normalize_abbreviate_keyword');
CREATE OR REPLACE FUNCTION fr.normalize_abbreviate_keyword(
    name IN VARCHAR
    , groups IN VARCHAR DEFAULT 'ALL'
    , name_abbreviated OUT VARCHAR
)
AS
$func$
DECLARE
    _groups VARCHAR[];
BEGIN
    IF groups = 'ALL' THEN
        _groups := '{TITLE,TYPE,EXT,NAME}'::VARCHAR[];
    ELSE
        _groups := STRING_TO_ARRAY(groups, ',');
    END IF;

    SELECT
        k.name_abbreviated
    INTO
        normalize_abbreviate_keyword.name_abbreviated
    FROM
        fr.laposte_address_keyword k
    WHERE
        k.name = normalize_abbreviate_keyword.name
        AND
        k.name_abbreviated IS NOT NULL
        -- among group(s)
        AND (
            ARRAY_POSITION(_groups, k.group) > 0
        )
    ;
END
$func$ LANGUAGE plpgsql;

-- normalize name of municipality
SELECT public.drop_all_functions_if_exists('fr', 'normalize_municipality_name');
CREATE OR REPLACE FUNCTION fr.normalize_municipality_name(
    code VARCHAR
    , name VARCHAR
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _name_normalized VARCHAR;
    _name VARCHAR;
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
        RETURN _name_normalized;
    END IF;

    -- only upper and not special characters
    _name := clean_address_label(name);
    -- replace (SAINT|SAINTE)
    RETURN fr.normalize_abbreviate_holy(_name);
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
        fr.laposte_address_area za
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

-- order changes by heuristic method
SELECT public.drop_all_functions_if_exists('fr', 'order_changes');
SELECT public.drop_all_functions_if_exists('fr', 'normalize_order_changes');
CREATE OR REPLACE FUNCTION fr.normalize_order_changes(
    len IN INT
    , nchanges IN INT
    , changes IN VARCHAR[]
    , earns IN INT[]
    , positions IN INT[]
    , words IN VARCHAR[]
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , simulation IN BOOLEAN DEFAULT FALSE
    , heuristic_method IN VARCHAR DEFAULT 'MEmCMN'
    , ordered_changes OUT VARCHAR
)
AS
$func$
DECLARE
    _query_select TEXT;
    _query_columns TEXT;
    _query_orderby TEXT;
    _query TEXT;
    _subsets VARCHAR[];
    _ordered_changes VARCHAR[];
    _earns INT[];
    _nrows INT;
    _i INT;
BEGIN
    IF raise_notice THEN RAISE NOTICE 'C=% E=% #=% len=%', changes, earns, nchanges, len; END IF;

    /* NOTE
    heuristic methods
        DBE:    descending bigger earn
        MNmC:   maximize name (nearest 32) & minimize change(s)
        MEmC:   maximize earning w/ min change(s)
        MEmCMN: maximize earning w/ min change(s) maximize name (nearest 32)
     */

    IF heuristic_method = 'DBE' THEN
        ordered_changes :=
            ARRAY(
                SELECT
                    c.change
                FROM
                    UNNEST(changes) WITH ORDINALITY AS c(change, i)
                        JOIN UNNEST(earns) WITH ORDINALITY AS e(earn, i) ON c.i = e.i
                ORDER BY
                    e.earn DESC
            )
        ;
    ELSE
        -- all subsets
        _subsets := (
            SELECT subsets FROM subsets(
                set => changes
                , n => nchanges
            )
        );
        IF raise_notice THEN
            RAISE NOTICE '#subsets=%', ARRAY_LENGTH(_subsets, 1);
            RAISE NOTICE 'subsets=%', _subsets;
        END IF;

        /* NOTE
        step 1 to find all solutions w/ NN restriction, so lt 32
        step 2 better solution from all, but gt 32
         */
        FOR _i IN 1 .. 2
        LOOP
            IF simulation THEN _query_columns := ', $4 len, es.*'; END IF;
            _query_select := CONCAT('
                WITH
                subsets AS (
                    SELECT
                        i
                        , subsets
                    FROM
                        UNNEST($1) WITH ORDINALITY AS s(subsets, i)
                )
                , subsets_as_items AS (
                    SELECT
                        i
                        , UNNEST(STRING_TO_ARRAY(subsets, '','')) item
                    FROM
                        subsets
                )
                , change_and_earn AS (
                    SELECT
                        c.change
                        , e.earn
                        , w.word
                    FROM
                        UNNEST($2) WITH ORDINALITY AS c(change, i)
                            JOIN UNNEST($3) WITH ORDINALITY AS e(earn, i) ON c.i = e.i
                            JOIN UNNEST($5) WITH ORDINALITY AS p(position, i) ON c.i = p.i
                            JOIN UNNEST($6) WITH ORDINALITY AS w(word, i) ON w.i = p.position
                )
                , earn_by_subset AS (
                    SELECT
                        s.i
                        , SUM(ce.earn) earn
                        , COUNT(*) nchanges
                        , SUM(CASE WHEN ce.change ~ ''^V'' THEN 1 ELSE 0 END) n_v
                        , SUM(CASE WHEN ce.change ~ ''^T'' THEN 1 ELSE 0 END) n_t
                        , SUM(CASE WHEN ce.change ~ ''^P'' THEN 1 ELSE 0 END) n_p
                        , SUM(CASE WHEN ce.change ~ ''^N'' THEN 1 ELSE 0 END) n_n
                        , SUM(CASE WHEN ce.change ~ ''^E'' THEN 1 ELSE 0 END) n_e
                        , SUM(CASE WHEN ce.change ~ ''^A'' THEN 1 ELSE 0 END) n_a
                        , SUM(CASE WHEN w.word IS NOT NULL THEN w.rank_0 ELSE 0 END) nranks
                    FROM
                        subsets_as_items s
                            JOIN change_and_earn ce ON s.item = ce.change
                            LEFT OUTER JOIN fr.laposte_address_street_word_descriptor w ON w.word = ce.word
                    GROUP BY
                        s.i
                )
                SELECT
                    STRING_TO_ARRAY(s.subsets, '','') subsets
                    ', _query_columns, '
                FROM
                    subsets s
                        JOIN earn_by_subset es ON s.i = es.i
                '
                , CASE WHEN _i = 1 THEN
                    '
                    WHERE
                        -- normalized name
                        ($4 - es.earn) <= 32
                    '
                END
            );
            IF simulation THEN
                _query := CONCAT('
                    DROP TABLE IF EXISTS fr.tmp_order_changes;
                    CREATE TABLE fr.tmp_order_changes AS
                    '
                    , _query_select
                );
                EXECUTE _query
                    USING _subsets, changes, earns, len, positions, words
                    ;
            END IF;

            _query_orderby := CASE
                WHEN _i = 1 THEN
                    CASE
                    WHEN heuristic_method = 'MNmC' THEN
                        '
                        -- nearest max size
                        (32 - ($4 - earn))
                        -- favour P,A against V,T
                        , (n_v + n_t + (n_p * 2) + (n_a * 3)) DESC
                        -- least change(s)
                        , (nchanges)
                        -- respect ascending order of changes (A1 before A2)
                        , subsets
                        '
                    WHEN heuristic_method = 'MEmC' THEN
                        /* NOTE
                        reminder: the lower is rank, the greater word is usual
                        avoid unusual title (rank_0 at 1001 only for lt 200 occurs)
                        example: AVENUE DE LA GRANDE CHARMILLE DU PARC
                        w/ title CHARMILLE (abbr CHI not really readable!)
                        => normalized: AV LA GRANDE CHARMILLE DU PARC
                        */
                        '
                        CASE WHEN n_t > 0 AND nranks > 1001 THEN 1
                        ELSE
                            earn::NUMERIC / nchanges
                        END DESC
                        -- favour P,A against V,T
                        , (n_v + n_t + (n_p * 2) + (n_a * 3)) DESC
                        -- respect ascending order of changes (A1 before A2)
                        , subsets
                        '
                    WHEN heuristic_method = 'MEmCMN' THEN
                        '
                        CASE WHEN n_t > 0 AND nranks > 1001 THEN 1
                        ELSE
                            CASE
                            WHEN ((32 - ($4 - earn)) * nchanges) > 0 THEN
                                (earn::NUMERIC / nchanges) / ((32 - ($4 - earn)) * nchanges)
                            ELSE
                                (earn::NUMERIC / nchanges)
                            END
                        END * (
                            -- minimize if NAME are abbreviated
                            CASE WHEN n_n > 0 THEN 0.95
                            ELSE 1
                            END
                        ) DESC
                        '
                    END
                ELSE
                    '
                    earn DESC
                    '
                END
            ;

            IF simulation THEN
                _query := '
                    SELECT subsets
                    FROM fr.tmp_order_changes
                    '
                ;
            ELSE
                _query := _query_select;
            END IF;

            EXECUTE CONCAT(_query, ' ORDER BY ', _query_orderby, ' LIMIT 1')
                INTO ordered_changes
                USING _subsets, changes, earns, len, positions, words
                ;
            GET DIAGNOSTICS _nrows = ROW_COUNT;

            -- with solution ?
            IF _nrows > 0 THEN
                EXIT;
            END IF;
        END LOOP;
    END IF;
END
$func$ LANGUAGE plpgsql;

-- normalize name of street
SELECT public.drop_all_functions_if_exists('fr', 'normalize_street_name');
CREATE OR REPLACE FUNCTION fr.normalize_street_name(
    name IN VARCHAR
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , simulation IN BOOLEAN DEFAULT FALSE
    , heuristic_method IN VARCHAR DEFAULT 'MEmCMN'
    , nwords OUT INT
    , as_words OUT INT[]
    , name_as_words OUT TEXT[]
    , name_abbreviated_as_words OUT TEXT[]
    , descriptors_as_words OUT TEXT[]
    , name_normalized_as_words OUT TEXT[]
    , descriptors_normalized_as_words OUT TEXT[]
)
AS
$func$
DECLARE
    _name VARCHAR;
    _len_normalized INT;
    _words_todo TEXT[];
    _descriptors VARCHAR;
    _descriptor VARCHAR;
    _i INT;
    _j INT;
    _changes VARCHAR[];
    _nchanges INT := 0;
    _earn_changes INT[];
    _position_changes INT[];
    _ordered_changes VARCHAR[];
    _nordered_changes INT;
    _positions INT[];
    _ITEMS_DESCRIPTOR INT := 6;
    _POSITION_DESCRIPTOR_A INT := 1;
    _POSITION_DESCRIPTOR_E INT := 2;
    _POSITION_DESCRIPTOR_N INT := 3;
    _POSITION_DESCRIPTOR_P INT := 4;
    _POSITION_DESCRIPTOR_T INT := 5;
    _POSITION_DESCRIPTOR_V INT := 6;
    _position INT;
    _word INT;
    _abbr_t_to_1 BOOLEAN;
BEGIN
    -- only upper and not special characters
    _name := clean_address_label(name);
    IF raise_notice THEN RAISE NOTICE 'N=% #=%', _name, LENGTH(_name); END IF;

    -- descriptors, words (by descriptor)
    SELECT
        ds.descriptors
        , ds.words_by_descriptor
        , ds.words_abbreviated_by_descriptor
        , ds.words_todo_by_descriptor
        , ds.as_words
    INTO
        _descriptors
        , name_as_words
        , name_abbreviated_as_words
        , _words_todo
        , normalize_street_name.as_words
    FROM
        fr.get_descriptors_of_street(
            name => _name
            , with_abbreviation => TRUE
        ) ds;
    nwords := ARRAY_LENGTH(name_as_words, 1);
    -- descriptors_as_words as array
    SELECT
        as_array
    INTO
        descriptors_as_words
    FROM
        fr.split_descriptors_as_array(
            descriptors => _descriptors
            , words => name_as_words
            , nwords => nwords
        ) da;

    _len_normalized := (
        SELECT SUM(LENGTH(w)) FROM UNNEST(name_as_words) w
    ) + (nwords -1);
    IF raise_notice THEN RAISE NOTICE 'NN=% #=%', name_as_words, _len_normalized; END IF;
    -- already OK ?
    IF _len_normalized <= 32 THEN
        RETURN;
    END IF;
    name_normalized_as_words := name_as_words;
    descriptors_normalized_as_words := descriptors_as_words;

    -- eval changes and their earning size
    _positions := ARRAY_FILL(0, ARRAY[_ITEMS_DESCRIPTOR]);
    FOR _i IN 1 .. nwords
    LOOP
        _position := CASE
            WHEN descriptors_as_words[_i] ~ 'A' THEN _POSITION_DESCRIPTOR_A
            WHEN descriptors_as_words[_i] ~ 'V' THEN _POSITION_DESCRIPTOR_V
            WHEN descriptors_as_words[_i] ~ 'P' THEN
                CASE
                -- exception if
                --  previous word is holy
                WHEN _i > 1 AND name_as_words[_i -1] ~ '^(ST|STE|SAINT|SAINTE)$' THEN 0
                --  next is number
                WHEN _i < nwords AND descriptors_as_words[_i +1] ~ 'C' THEN 0
                ELSE _POSITION_DESCRIPTOR_P
                END
            ELSE
                CASE
                -- only if abbreviatable
                WHEN name_abbreviated_as_words[_i] IS NULL THEN 0
                ELSE
                    CASE
                    /* NOTE
                    example: ANCIENNE CARRAIRE DES TROUPEAUX D ARLES PROLONGEE
                    w/o E-abbreviation : {ANCIENNE,C,NULL,T,NULL,ARLES,PROLONGEE}
                     */
                    WHEN descriptors_as_words[_i] ~ 'E' THEN _POSITION_DESCRIPTOR_E
                    WHEN descriptors_as_words[_i] ~ 'N' THEN _POSITION_DESCRIPTOR_N
                    WHEN descriptors_as_words[_i] ~ 'T' THEN _POSITION_DESCRIPTOR_T
                    ELSE 0
                    END
                END
            END
        ;
        IF _position > 0 THEN
            _positions[_position] := _positions[_position] +1;
            _changes := ARRAY_APPEND(_changes, CONCAT(descriptors_as_words[_i], _positions[_position]));
            _nchanges := _nchanges +1;

            -- earn of change
            _earn_changes[_nchanges] := CASE
                -- delete article (count space separator, +1)
                WHEN descriptors_as_words[_i] ~ 'A' THEN LENGTH(name_as_words[_i]) +1
                -- remain 1st letter only
                WHEN descriptors_as_words[_i] ~ 'P' THEN LENGTH(name_as_words[_i]) -1
                -- replace w/ abbreviation (if defined)
                ELSE LENGTH(name_as_words[_i]) - LENGTH(COALESCE(name_abbreviated_as_words[_i], name_as_words[_i]))
                END
            ;
            -- position of change (i-th word)
            _position_changes[_nchanges] := _i;
        END IF;
    END LOOP;

    -- at least 1 change
    IF _nchanges > 0 THEN
        -- search for better solution (subset) of all change(s)
        _ordered_changes := (
            SELECT fr.normalize_order_changes(
                len => _len_normalized
                , nchanges => _nchanges
                , changes => _changes
                , earns => _earn_changes
                , positions => _position_changes
                , words => name_normalized_as_words
                , raise_notice => raise_notice
                , simulation => simulation
                , heuristic_method => heuristic_method
            )
        );

        IF raise_notice THEN RAISE NOTICE 'C=%, P=%, G=%, O=%', _changes, _position_changes, _earn_changes, _ordered_changes; END IF;
        IF simulation THEN RETURN; END IF;

        -- apply solution
        _nordered_changes := ARRAY_LENGTH(_ordered_changes, 1);
        FOR _i IN 1 .. _nordered_changes
        LOOP
            _position := ARRAY_POSITION(_changes, _ordered_changes[_i]);
            _word := _position_changes[_position];
            _descriptor := SUBSTR(_ordered_changes[_i], 1, 1);

            IF _descriptor IS NULL THEN
                RAISE 'changement %/% non trouvé!', _i, _nordered_changes;
            END IF;
            IF raise_notice THEN RAISE NOTICE 'changement %/% : % (w=%)', _i, _nordered_changes, _descriptor, _word; END IF;

            IF _descriptor = 'A' THEN
                name_normalized_as_words[_word] := NULL;
                descriptors_normalized_as_words[_word] := NULL;
                _len_normalized := _len_normalized - _earn_changes[_position];
            ELSIF _descriptor = 'P' THEN
                name_normalized_as_words[_word] := SUBSTR(name_normalized_as_words[_word], 1, 1);
                _len_normalized := _len_normalized - _earn_changes[_position];
            ELSE
                name_normalized_as_words[_word] := name_abbreviated_as_words[_word];
                descriptors_normalized_as_words[_word] := REPEAT(_descriptor, count_words(name_abbreviated_as_words[_word]));
                _len_normalized := _len_normalized - _earn_changes[_position];
            END IF;

            IF _len_normalized <= 32 THEN
                EXIT;
            END IF;
        END LOOP;
    END IF;

    -- not normalized ? try to abbreviate name(s) to just initials (1-letter)
    IF _len_normalized > 32 THEN
        -- exists ST|STE w/ firstname as following word (so not abbreviated) ?
        _j := COALESCE(ARRAY_POSITION(name_as_words, 'SAINT'), ARRAY_POSITION(name_as_words, 'SAINTE'));
        IF (_j IS NOT NULL
            AND
            _j < nwords
            AND
            descriptors_as_words[_j +1] ~ 'P'
            AND
            (_len_normalized - (LENGTH(name_as_words[_j +1]) - 1)) <= 32
        ) THEN
            _j := _j +1;
        ELSE
            -- exists ROAD NETWORK words
            _j := COALESCE(
                ARRAY_POSITION(name_as_words, 'NATIONALE')
                , ARRAY_POSITION(name_as_words, 'DEPARTEMENTALE')
                , ARRAY_POSITION(name_as_words, 'COMMUNALE')
                , ARRAY_POSITION(name_as_words, 'RURALE')
            );
            IF NOT (_j IS NOT NULL
                AND
                (_len_normalized - (LENGTH(name_as_words[_j]) - 1)) <= 32
            ) THEN
                _abbr_t_to_1 := FALSE;
                -- all article(s) deleted and all words already abbreviated
                IF ((CARDINALITY(ARRAY_POSITIONS(name_normalized_as_words, NULL)) = CARDINALITY(ARRAY_POSITIONS(descriptors_as_words, 'A')))
                    AND
                    (name_normalized_as_words @> ARRAY_REMOVE(name_abbreviated_as_words, NULL))
                ) THEN
                    /* NOTE
                    example: CHEMIN VICINAL VOIE COMMUNALE 5 LESIGNY A BRIE COMTE ROBERT
                     */
                    -- abbreviate title to 1-letter ...
                    _abbr_t_to_1 := TRUE;
                    -- ... starting at 2nd word (if type) else 1st
                    _j := CASE
                        WHEN descriptors_as_words[1] ~ 'V' THEN 2
                        ELSE 1
                        END
                    ;
                ELSE
                    -- abbreviate (asc order) either name or fname (except 1st)
                    _j := CASE
                        WHEN descriptors_as_words[1] = 'N' THEN 2
                        ELSE 1
                        END
                    ;
                END IF;
            END IF;
        END IF;
        FOR _i IN _j .. nwords
        LOOP
            _descriptor := CASE
                WHEN _i = _j OR _i = (nwords -1) THEN 'N|P'
                ELSE 'N'
                END
            ;
            IF ((descriptors_as_words[_i] ~ _descriptor)
                OR
                ((descriptors_as_words[_i] ~ 'T') AND _abbr_t_to_1)
                AND
                (name_normalized_as_words[_i] IS NOT NULL)
            ) THEN
                _len_normalized := _len_normalized - (LENGTH(name_normalized_as_words[_i]) -1);
                name_normalized_as_words[_i] := SUBSTR(name_normalized_as_words[_i], 1, 1);
                IF _len_normalized <= 32 THEN
                    EXIT;
                END IF;
            END IF;
        END LOOP;
    END IF;

    IF _len_normalized <= 32 THEN
        -- try to restore type (if abbreviated and NN is possible w/o)
        IF (descriptors_as_words[1] ~ 'V|T'
            AND
            (_len_normalized + (LENGTH(name_as_words[1]) - LENGTH(name_abbreviated_as_words[1]))) <= 32
        ) THEN
            name_normalized_as_words[1] := name_as_words[1];
        END IF;
    ELSE
        RAISE NOTICE 'pas de normalisation (%) : NN=% #=%', name, name_normalized_as_words, _len_normalized;
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
view test_normalize.sh : option NAME_DIFF, NAME_LIST, NAME_CASE
 */

-- get normalized (name, descriptors) as words from normalized name w/ (name, descriptors) as words (of complete name)
SELECT drop_all_functions_if_exists('fr', 'normalize_name_get_as_words');
CREATE OR REPLACE FUNCTION fr.normalize_name_get_as_words(
    name_normalized IN VARCHAR
    , name_as_words IN TEXT[]
    , name_abbreviated_as_words IN TEXT[]
    , descriptors_as_words IN TEXT[]
    , nwords IN INT
    , raise_notice IN BOOLEAN DEFAULT FALSE
    , name_normalized_as_words OUT TEXT[]
    , descriptors_normalized_as_words OUT TEXT[]
)
AS
$func$
DECLARE
    _i INT;
    _j INT := 1;
    _nwords INT;
    _name_abbreviated VARCHAR;
BEGIN
    -- through complete name
    FOR _i IN 1 .. nwords
    LOOP
        _nwords := count_words(name_as_words[_i]);
        IF extract_words(
            str => name_normalized
            , n => _nwords
            , from_ => _j) = name_as_words[_i] THEN
            -- same word(s), within the meaning of descriptor
            name_normalized_as_words[_i] := name_as_words[_i];
            descriptors_normalized_as_words[_i] := descriptors_as_words[_i];
        ELSE
            _name_abbreviated := CASE
                WHEN descriptors_as_words[_i] ~ 'V|T' THEN
                    name_abbreviated_as_words[_i]
                WHEN descriptors_as_words[_i] ~ 'N|P' THEN
                    SUBSTR(name_as_words[_i], 1, 1)
                ELSE NULL
                END
            ;
            _nwords := count_words(_name_abbreviated);
            /* NOTE
            use similarity because of abbreviation error!
             */
            IF (_name_abbreviated IS NOT NULL
                AND
                extract_words(
                    str => name_normalized
                    , n => _nwords
                    , from_ => _j) % _name_abbreviated
            ) THEN
                -- abbreviated word(s)
                name_normalized_as_words[_i] := _name_abbreviated;
                descriptors_normalized_as_words[_i] := REPEAT(SUBSTR(descriptors_as_words[_i], 1, 1), _nwords);
            ELSE
                -- deleted article
                _nwords := 0;
                name_normalized_as_words[_i] := NULL;
                descriptors_normalized_as_words[_i] := NULL;
            END IF;
        END IF;
        _j := _j + _nwords;
    END LOOP;
END
$func$ LANGUAGE plpgsql;

/* TEST
view test_normalize.sh : option NAME_DIFF
 */

-- normalize one address
SELECT drop_all_functions_if_exists('fr', 'standardize_address');
CREATE OR REPLACE FUNCTION fr.standardize_address(
    address IN RECORD                   -- address to standardize
    , columns_map IN HSTORE             -- mapping address(client)/address(reference)
)
RETURNS fr.standardized_address AS
$func$
DECLARE
    _standardized_address fr.standardized_address;
    _column_map VARCHAR[];
    _geom GEOMETRY;
    _geom_x DOUBLE PRECISION;
    _geom_y DOUBLE PRECISION;
    _geom_srid SMALLINT;
    _geom_srid_default SMALLINT := 2154;
    _cadastre_parcel_number VARCHAR;
    _cadastre_parcel_section VARCHAR;
    _cadastre_parcel_prefix CHAR(3);
    _street_type_is_abbreviated BOOLEAN;
    _exists BOOLEAN;
    _timestamp TIMESTAMP := clock_timestamp();
BEGIN
    FOREACH _column_map SLICE 1 IN ARRAY %# columns_map LOOP
        _column_map[2] := CONCAT('$1.', _column_map[2]);
        BEGIN
            CASE _column_map[1]
                WHEN 'id' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.id
                        USING address;
                WHEN 'complement' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.complement
                        USING address;
                    _standardized_address.complement := NULLIF(TRIM(public.clean_address_label(_standardized_address.complement)), '');
                WHEN 'housenumber' THEN
                    EXECUTE CONCAT('SELECT NULLIF(TRIM(', _column_map[2], '::TEXT), '''')::INTEGER')
                        INTO _standardized_address.housenumber
                        USING address;
                    IF (
                        (_standardized_address.housenumber::VARCHAR !~ '^[0-9]+$')
                        OR
                        (_standardized_address.housenumber = 0)
                    ) THEN
                        RAISE NOTICE 'Numéro de voie ignoré car invalide : %', _standardized_address.housenumber;
                        _standardized_address.housenumber := NULL;
                    END IF;
                WHEN 'extension' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.extension
                        USING address;
                    _standardized_address.extension := NULLIF(TRIM(public.clean_address_label(_standardized_address.extension)), '');
                WHEN 'street' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.street
                        USING address;

                    SELECT
                        ARRAY_TO_STRING(COALESCE(name_normalized_as_words, name_as_words), ' ')
                        , ARRAY_TO_STRING(COALESCE(descriptors_normalized_as_words, descriptors_as_words), '')
                        , CASE
                            WHEN name_normalized_as_words IS NULL THEN as_words
                            ELSE fr.get_as_words_from_splited_value(
                                property_as_words => descriptors_normalized_as_words
                            )
                            END
                    INTO
                        _standardized_address.street
                        , _standardized_address.descriptors
                        , _standardized_address.as_words
                    FROM
                        fr.normalize_street_name(
                            name => _standardized_address.street
                        )
                    ;
                WHEN 'municipality_code' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.municipality_code
                        USING address;

                    SELECT EXISTS(
                        SELECT 1 FROM fr.laposte_address_area
                        WHERE co_insee_commune = _standardized_address.municipality_code
                        AND fl_active)
                    INTO _exists;
                    IF NOT _exists THEN
                        RAISE NOTICE 'Code INSEE commune ignoré car invalide : %', _standardized_address.municipality_code;
                        _standardized_address.municipality_code := NULL;
                    END IF;
                WHEN 'postcode' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.postcode
                        USING address;
                    SELECT EXISTS(
                        SELECT 1 FROM fr.laposte_address_area
                        WHERE co_postal = _standardized_address.postcode
                        AND fl_active)
                    INTO _exists;
                    IF NOT _exists THEN
                        RAISE NOTICE 'Code Postal commune ignoré car invalide : %', _standardized_address.postcode;
                        _standardized_address.postcode := NULL;
                    END IF;
                WHEN 'municipality_name' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.municipality_name
                        USING address;
                -- TODO à intégrer dans lb_ligneX
                -- mention CEDEX OU libellé Ancienne Commune OU les 2 accollées
                -- RE=^((BP|CS|CE|CP) *[0-9]+)? *([A-Z ]+)?$
                WHEN 'municipality_old_name' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _standardized_address.municipality_old_name
                        USING address;

                WHEN 'geo_xy' THEN
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',1)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',2)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                WHEN 'geo_latlon' THEN
                    -- latitude = Y, longitude = X
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',2)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',1)::VARCHAR, '','' ,''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                    _geom_srid_default := 4326;
                WHEN 'geo_x' THEN
                    EXECUTE CONCAT('SELECT REPLACE(', _column_map[2], '::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                WHEN 'geo_y' THEN
                    EXECUTE CONCAT('SELECT REPLACE(', _column_map[2], '::VARCHAR, '','' ,''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                WHEN 'geo_srid' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2], '::SMALLINT')
                        INTO _geom_srid
                        USING address;
                WHEN 'geo_wkt' THEN
                    EXECUTE CONCAT('SELECT ST_PointFromText(', _column_map[2], ')')
                        INTO _geom
                        USING address;
                WHEN 'geo_json' THEN
                    EXECUTE CONCAT('SELECT ST_GeomFromGeoJSON(', _column_map[2], ')')
                        INTO _geom
                        USING address;
                WHEN 'geo' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _geom
                        USING address;

                WHEN 'cadastre_parcel_number' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2], '::INTEGER::VARCHAR')
                        INTO _cadastre_parcel_number
                        USING address;
                WHEN 'cadastre_parcel_section' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _cadastre_parcel_section
                        USING address;
                    --On enlève les éventuel 0 préfixant l'identifiant de section cadastrale
                    --Alternative : ne prendre que les lettre alphabéthiques ?
                    _cadastre_parcel_section := REPLACE(_cadastre_parcel_section, '0', '');
                WHEN 'cadastre_parcel_prefix' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _cadastre_parcel_prefix
                        USING address;
            ELSE
                RAISE NOTICE 'Attribut % ignoré car inconnu', _column_map[1];
            END CASE;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Attribut % ignoré car provoquant une erreur à l''évaluation de % : %', _column_map[1], _column_map[2], SQLERRM;
        END;
    END LOOP;

    IF _standardized_address.id IS NULL THEN
        RAISE 'Vous devez spécifier un code identifiant de l''adresse';
    END IF;

    _standardized_address.municipality_name := fr.normalize_municipality_name(
        code => _standardized_address.municipality_code
        , name => _standardized_address.municipality_name
    );
    IF _standardized_address.municipality_old_name IS NOT NULL THEN
        _standardized_address.municipality_old_name := fr.normalize_municipality_name(
            name => _standardized_address.municipality_old_name
        );
    END IF;
    IF _standardized_address.municipality_code IS NULL AND _standardized_address.municipality_name IS NOT NULL THEN
        BEGIN
            SELECT DISTINCT co_insee_commune
            INTO _standardized_address.municipality_code
            FROM fr.laposte_address_area
            WHERE lb_ach_nn = _standardized_address.municipality_name AND fl_active;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Déduction code Commune à partir du nom % provoquant une erreur : %', _standardized_address.municipality_name,  SQLERRM;
        END;
    END IF;
    IF _standardized_address.municipality_name IS NULL AND _standardized_address.municipality_code IS NOT NULL THEN
        BEGIN
            SELECT DISTINCT lb_ach_nn
            INTO _standardized_address.municipality_name
            FROM fr.laposte_address_area
            WHERE co_insee_commune = _standardized_address.municipality_code AND fl_active;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Déduction libellé Commune à partir du code % provoquant une erreur : %', _standardized_address.municipality_code,  SQLERRM;
        END;
    END IF;

    IF _geom IS NULL
    AND _geom_x IS NOT NULL
    AND _geom_y IS NOT NULL THEN
        _geom := ST_MakePoint(_geom_x,_geom_y);
    END IF;

    IF _geom IS NOT NULL THEN
        IF ST_SRID(_geom) = 0 THEN
            _geom := ST_SetSRID(_geom, COALESCE(_geom_srid, _geom_srid_default));
        END IF;
        IF NOT public.is_valid_geometry_in_SRID_bounds(_geom) THEN
            RAISE NOTICE 'Coordonnées en dehors des limites du système de projection : %, SRID %', ST_AsText(_geom), ST_SRID(_geom);
        ELSE
            _standardized_address.geom := ST_Transform(_geom, 3857);
        END IF;
    END IF;

    _standardized_address.level :=
    CASE
        WHEN _standardized_address.complement IS NOT NULL THEN 'COMPLEMENT'
        WHEN _standardized_address.housenumber IS NOT NULL THEN 'HOUSENUMBER'
        WHEN _standardized_address.street IS NOT NULL THEN 'STREET'
        WHEN _standardized_address.municipality_code IS NOT NULL THEN 'AREA'
    END;

    IF (_standardized_address.postcode IS NOT NULL
        OR _standardized_address.municipality_code IS NOT NULL
        OR _standardized_address.municipality_old_name IS NOT NULL
        OR _standardized_address.municipality_name IS NOT NULL
    ) THEN
        _standardized_address.match_code_area := fr.get_match_code(
            level => 'AREA'
            , standardized_address => _standardized_address
        );
        IF _standardized_address.street IS NOT NULL THEN
            _standardized_address.match_code_street := fr.get_match_code(
                level => 'STREET'
                , standardized_address => _standardized_address
            );
            IF _standardized_address.housenumber IS NOT NULL THEN
                _standardized_address.match_code_housenumber := fr.get_match_code(
                    level => 'HOUSENUMBER'
                    , standardized_address => _standardized_address
                );
            END IF;
            IF _standardized_address.complement IS NOT NULL THEN
                _standardized_address.match_code_complement := fr.get_match_code(
                    level => 'COMPLEMENT'
                    , standardized_address => _standardized_address
                );
            END IF;
        END IF;
    END IF;

    IF _standardized_address.street IS NOT NULL THEN
        _standardized_address.words := STRING_TO_ARRAY(_standardized_address.street, ' ');
    END IF;

    /*
    -- calcul mot directeur, si absent
    IF _standardized_address.lb_voie_mot_directeur IS NULL AND _standardized_address.street IS NOT NULL THEN
        _standardized_address.lb_voie_mot_directeur := getVoieMotDirecteur(_standardized_address.street);
    END IF;
     */

    _standardized_address.elapsed_time := clock_timestamp() - _timestamp;
    RETURN _standardized_address;
END
$func$ LANGUAGE plpgsql;
