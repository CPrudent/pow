/***
 * FR-CONSTANTS
 */

CREATE TABLE IF NOT EXISTS fr.constant (
    usecase CHARACTER VARYING NOT NULL
    , key VARCHAR NOT NULL
    , value VARCHAR
);

DO $$
BEGIN
    IF column_exists('fr', 'constant', 'list') THEN
        ALTER TABLE fr.constant RENAME COLUMN "list" TO usecase;
        DROP INDEX IF EXISTS ix_constant_list_key;
    END IF;

    IF table_exists('fr', 'laposte_street_type') AND NOT table_exists('fr', 'laposte_address_street_type') THEN
        ALTER TABLE fr.laposte_street_type RENAME TO laposte_address_street_type;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_constant_index');
CREATE OR REPLACE PROCEDURE fr.set_constant_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_constant_usecase_key ON fr.constant (usecase, key);
END
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of types
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_type');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_type()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'TYPE';
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated)
        SELECT DISTINCT
            'TYPE'
            , lb_type
            , lb_type_abrege
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        ;
    WITH
    first_word_of_type AS (
        SELECT
            name
            , CASE
            WHEN POSITION(' ' IN name) = 0 THEN NULL
            ELSE SUBSTR(name, 1, POSITION(' ' IN name) -1)
            END first_word
        FROM fr.laposte_address_street_keyword
        WHERE "group" = 'TYPE'
    )
    , occurs_type AS (
        SELECT
            lb_type name
            , COUNT(*) occurs
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        GROUP BY lb_type
    )
    UPDATE fr.laposte_address_street_keyword st SET
        first_word = fw.first_word
        , occurs = ot.occurs
        FROM
            first_word_of_type fw
            , occurs_type ot
        WHERE
            "group" = 'TYPE'
            AND
            st.name = fw.name
            AND
            st.name = ot.name
        ;
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of firstnames
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_firstname');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_firstname()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des prénoms dans le nom des voies');

    CALL public.log_info(' Purge');
    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.constant (
        SELECT DISTINCT
            'LAPOSTE_STREET_FIRSTNAME'
            , mots.mot
        FROM fr.laposte_address_street AS voie_ran
        INNER JOIN LATERAL UNNEST(REGEXP_SPLIT_TO_ARRAY(voie_ran.lb_voie, '\s+'))
            WITH ORDINALITY AS mots(mot, ordre)
            ON TRUE
        INNER JOIN LATERAL UNNEST(STRING_TO_ARRAY(voie_ran.lb_desc, NULL))
            WITH ORDINALITY AS descripteurs(descripteur, ordre)
            ON mots.ordre = descripteurs.ordre AND descripteurs.descripteur = 'P'
        WHERE
            LENGTH(mots.mot) > 1
            AND
            -- not article!
            NOT fr.is_normalized_article(mots.mot)
            AND
            -- fault!
            NOT mots.mot = ANY('{GAY,FLEUR}')
    );
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Prénoms: ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE extension (of housenumber), w/ abbreviated value
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_extension_of_housenumber');
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_ext');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_ext()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_housenumber') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'EXT';
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated, first_word)
        SELECT DISTINCT 'EXT', lb_ext, lb_abr_nn, NULL
        FROM fr.laposte_address_housenumber
        WHERE fl_active AND lb_ext IS NOT NULL
        ;

    WITH
    ext_occurs AS (
        SELECT lb_ext, COUNT(*) n FROM fr.laposte_address_housenumber
        WHERE fl_active AND lb_ext IS NOT NULL
        GROUP BY lb_ext
    )
    UPDATE fr.laposte_address_street_keyword k SET
        occurs = o.n
        FROM ext_occurs o
        WHERE
            k.group = 'EXT'
            AND
            k.name = o.lb_ext
            ;
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE titles
-- Query returned successfully in 1 min 24 secs.
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_titles');
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_title');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_title()
AS
$proc$
DECLARE
    _set RECORD;
    _words TEXT[];
    _descriptors TEXT[];
    _words_normalized TEXT[];
    _descriptors_normalized TEXT[];
    _abbr_i INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_uniq')
        AND NOT table_exists('fr', 'laposte_address_street_word') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    DROP TABLE IF EXISTS fr.tmp_address_street_title;
    CREATE UNLOGGED TABLE fr.tmp_address_street_title (
        title VARCHAR NOT NULL
    );
    DROP TABLE IF EXISTS fr.tmp_address_street_title_abbr;
    CREATE UNLOGGED TABLE fr.tmp_address_street_title_abbr (
        title VARCHAR NOT NULL
        , title_abbreviated VARCHAR
    );

    TRUNCATE TABLE fr.tmp_address_street_title;
    FOR _set IN (
        WITH
        with_title AS (
        SELECT
            name
            , name_normalized
            , descriptors
        FROM
            fr.laposte_address_street_uniq us
        WHERE
            POSITION('T' IN descriptors) > 0
        )
        SELECT DISTINCT
            UNNEST(t.titles) title
        FROM
            with_title wt
            , fr.get_titles_from_name(
                name => wt.name
                , descriptor => wt.descriptors
            ) t
    )
    LOOP
        INSERT INTO fr.tmp_address_street_title(title) VALUES(_set.title);
    END LOOP;

    TRUNCATE TABLE fr.tmp_address_street_title_abbr;
    FOR _set IN (
        SELECT
            t.title
            , wt.name
            , wt.name_normalized
            , wt.descriptors
        FROM
            fr.laposte_address_street_uniq wt
            , fr.tmp_address_street_title t
            , fr.get_titles_from_name(
                name => wt.name
                , descriptor => wt.descriptors
            ) tw
        WHERE
            -- name w/ this title
            POSITION(REPEAT('T', count_words(t.title)) IN wt.descriptors) > 0
            AND
            tw.titles @> ARRAY[t.title]::TEXT[]
            AND
            -- w/ normalization
            wt.name_normalized IS NOT NULL
            AND
            -- w/ abbreviation
            POSITION(CONCAT(t.title, ' ') IN wt.name_normalized) = 0
    )
    LOOP
        SELECT words, descriptors
        INTO _words_normalized, _descriptors_normalized
        FROM
            fr.split_name_of_street_as_descriptor(
                name => _set.name_normalized
                , descriptor => _set.descriptors
                , split_only => 'T'
                , is_normalized => TRUE
            )
        ;
        SELECT words, descriptors
        INTO _words, _descriptors
        FROM
            fr.split_name_of_street_as_descriptor(
                name => _set.name
                , descriptor => _set.descriptors
                , split_only => 'T'
            )
        ;

        _abbr_i := ARRAY_POSITION(_words, _set.title);
        RAISE NOTICE 'title=% abbr=% (at %)', _set.title, _words_normalized[_abbr_i], _abbr_i;
        INSERT INTO fr.tmp_address_street_title_abbr
            VALUES (_set.title, _words_normalized[_abbr_i])
        ;
    END LOOP;

    -- populate titles
    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'TITLE';
    INSERT INTO fr.laposte_address_street_keyword("group", name)
        SELECT 'TITLE', title FROM fr.tmp_address_street_title
        ;

    -- delete kw, if exists other w/ same abbr (or w/o, never abbreviated)
    WITH
    title_abbr AS (
        SELECT title, FIRST(title_abbreviated) abbr
        FROM fr.tmp_address_street_title_abbr
        WHERE count_words(title) = 1
        GROUP BY title
        HAVING COUNT(DISTINCT title_abbreviated) <= 1
        UNION
        SELECT title, NULL abbr
        FROM fr.tmp_address_street_title t
        WHERE count_words(title) = 1
        AND NOT EXISTS(
            SELECT 1 FROM fr.tmp_address_street_title_abbr ta WHERE ta.title = t.title
        )
    )
    , other_abbr AS (
        SELECT
            ok.name
            --, ok.name_abbreviated
            --, ta.abbr
        FROM
            fr.laposte_address_street_keyword ok
                JOIN title_abbr ta ON ok.name = ta.title
        WHERE
            "group" != 'TITLE'
            AND
            (
                name_abbreviated = ta.abbr
                OR
                ta.abbr IS NULL
            )
    )
    --SELECT * FROM other_abbr ORDER BY 1
    DELETE FROM fr.laposte_address_street_keyword kt
        USING other_abbr ko
        WHERE kt.group = 'TITLE' AND kt.name = ko.name
        ;

    -- delete no titles
    DELETE FROM fr.laposte_address_street_keyword kt
        WHERE kt.group = 'TITLE' AND
            name ~ '^BIS '
        ;

    -- update occurs
    WITH
    title_occurs AS (
        SELECT
            kt.name
            , COUNT(*) occurs
        FROM
            fr.laposte_address_street_keyword kt
            , fr.laposte_address_street las
            , fr.get_titles_from_name(
                name => las.lb_voie
                , descriptor => las.lb_desc
            ) t
        WHERE
            kt.group = 'TITLE'
            AND
            las.fl_active
            AND
            POSITION('T' IN las.lb_desc) > 0
            AND
            t.titles @> ARRAY[kt.name]::TEXT[]
        GROUP BY
            kt.name
    )
    --SELECT * FROM title_occurs ORDER BY 1
    UPDATE fr.laposte_address_street_keyword kt SET
        occurs = sto.occurs
        FROM title_occurs sto
        WHERE
            kt.group = 'TITLE'
            AND
            kt.name = sto.name
        ;

    -- delete no titles
    WITH
    title_with_uniq_occur AS (
        SELECT
            name
            , REGEXP_SPLIT_TO_ARRAY(name, '\s+') as_words
            , count_words(name) n_words
        FROM fr.laposte_address_street_keyword
        WHERE "group" = 'TITLE'
        AND occurs = 1
        -- #338
        AND count_words(name) > 1
    )
    --SELECT * FROM title_with_uniq_occur
    , split_as_word AS (
        SELECT
            o1.name
            , u.word
            , u.i
            , o1.as_words
            , o1.n_words
        FROM
            title_with_uniq_occur o1
                INNER JOIN LATERAL UNNEST(as_words) WITH ORDINALITY AS u(word, i) ON TRUE
    )
    --SELECT * FROM all_word_as_title ORDER BY 1, 3
    , is_keyword AS (
        SELECT
            name
            , ARRAY_AGG((
                SELECT k.group
                FROM fr.laposte_address_street_keyword k
                WHERE k.name = sw.word ORDER BY occurs DESC LIMIT 1
                )
            ) as_kw
        FROM
            split_as_word sw
        GROUP BY
            name
    )
    , composed_words AS (
        SELECT
            o1.*
            , kw.as_kw
        FROM
            title_with_uniq_occur o1
                JOIN is_keyword kw ON o1.name = kw.name
        WHERE
            o1.n_words = ARRAY_LENGTH(kw.as_kw, 1)
    )
    --SELECT * FROM composed_words
    DELETE FROM fr.laposte_address_street_keyword kt
        USING composed_words cw
        WHERE kt.group = 'TITLE' AND kt.name = cw.name
        ;

    WITH
    no_title AS (
        SELECT w.word
        FROM
            fr.laposte_address_street_keyword k
                JOIN fr.laposte_address_street_word w ON k.name = w.word
        WHERE
            k.group = 'TITLE'
            AND
            -- at least 5%, others are ignored
            (	as_title < (
                    COALESCE(as_name, 0)
                    + COALESCE(as_reserved, 0)
                    + COALESCE(as_article, 0)
                    + COALESCE(as_number, 0)
                    + COALESCE(as_fname, 0)
                ) * 0.05
            )
    )
    --SELECT * FROM no_title ORDER BY 1
    DELETE FROM fr.laposte_address_street_keyword kt
        USING no_title nt
        WHERE kt.group = 'TITLE' AND kt.name = nt.word
        ;

    -- update abbreviation (one-word title only)
    WITH
    title_abbr AS (
        SELECT title, FIRST(title_abbreviated) abbr
        FROM fr.tmp_address_street_title_abbr
        WHERE count_words(title) = 1
        GROUP BY title
        HAVING COUNT(DISTINCT title_abbreviated) = 1
    )
    UPDATE fr.laposte_address_street_keyword kt SET
        name_abbreviated = ta.abbr
        FROM title_abbr ta
        WHERE
            kt.group = 'TITLE'
            AND
            kt.name = ta.title
        ;
    -- update missing abbreviation (n-words)
    WITH
    correction_abbr AS (
        SELECT *
        FROM (
            VALUES
            ('ANCIENNE ROUTE', 'ANCI ROUTE', 'ANCIENNE', 1)
            , ('NOTRE DAME', 'ND', 'NOTRE', 1)
            , ('LIEUTENANT DE VAISSEAU', 'LTDV', 'LIEUTENANT', 1)
        ) AS t(name, name_abbreviated, first_word, occurs)
    )
    UPDATE fr.laposte_address_street_keyword kt SET
        name_abbreviated = ca.name_abbreviated
        FROM correction_abbr ca
        WHERE
            kt.group = 'TITLE'
            AND
            kt.name = ca.name
        ;

    -- update first word
    UPDATE fr.laposte_address_street_keyword kt SET
        first_word = (REGEXP_MATCH(kt.name, '^\S+'))[1]
        WHERE
            kt.group = 'TITLE'
            AND
            count_words(kt.name) > 1
        ;
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE municipality : list of normalized label exceptions
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_municipality_normalized_label_exception');
CREATE OR REPLACE PROCEDURE fr.set_laposte_municipality_normalized_label_exception()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_area') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_MUNICIPALITY_EXCEPTION';
    INSERT INTO fr.constant (
        SELECT
            'LAPOSTE_MUNICIPALITY_EXCEPTION'
            , t.*
        FROM (
            SELECT
                co_insee_commune
                , lb_ach_nn
            FROM fr.laposte_address_area
            WHERE
                fl_active
                AND
                -- difference normalized label w/ delivery one : exception!
                (
                    (lb_nn != lb_ach_nn)
                    OR
                    -- w/o ST|STE : delete article(s)
                    ((LENGTH(lb_in_ext_loc) > 32) AND (lb_in_ext_loc !~ '\mSAINT[E]?\M'))
                )
                AND
                lb_l5_nn IS NULL

            UNION

            SELECT
                co_insee_commune
                , lb_ach_nn
            FROM
                fr.laposte_address_area
                    JOIN fr.insee_municipality
                        ON co_insee_commune = codgeo
            WHERE
                -- some municipality w/ () in its name
                -- ex: 16052 Bors (Canton de Charente-Sud)
                POSITION('(' IN libgeo) > 0

            ORDER BY
                1
        ) t
        WHERE
            -- except municipalities w/ districts (Lyon, Marseille et Paris) and (Polynésie, Nouvelle Calédonie)
            co_insee_commune !~ '^(98|693|751|132)'
    );
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_territory_overseas');
CREATE OR REPLACE PROCEDURE fr.set_territory_overseas()
AS
$proc$
BEGIN
    DELETE FROM fr.constant WHERE usecase = 'TERRITORY_OVERSEAS_NAME';
    INSERT INTO fr.constant (usecase, key, value) VALUES
          ('TERRITORY_OVERSEAS_NAME', '97501', 'Miquelon-Langlade')
        , ('TERRITORY_OVERSEAS_NAME', '97502', 'Saint-Pierre')
        , ('TERRITORY_OVERSEAS_NAME', '97701', 'Saint-Barthélemy')
        , ('TERRITORY_OVERSEAS_NAME', '97801', 'Saint-Martin')
        , ('TERRITORY_OVERSEAS_NAME', '98714', 'Bora-Bora')
        , ('TERRITORY_OVERSEAS_NAME', '98718', 'Fatu-Hiva')
        , ('TERRITORY_OVERSEAS_NAME', '98723', 'Hiva-Oa')
        , ('TERRITORY_OVERSEAS_NAME', '98729', 'Moorea-Maiao')
        , ('TERRITORY_OVERSEAS_NAME', '98731', 'Nuku-Hiva')
        , ('TERRITORY_OVERSEAS_NAME', '98747', 'Taiarapu-Est')
        , ('TERRITORY_OVERSEAS_NAME', '98748', 'Taiarapu-Ouest')
        , ('TERRITORY_OVERSEAS_NAME', '98756', 'Ua-Huka')
        , ('TERRITORY_OVERSEAS_NAME', '98757', 'Ua-Pou')
        , ('TERRITORY_OVERSEAS_NAME', '98801', 'Bélep')
        , ('TERRITORY_OVERSEAS_NAME', '98805', 'Dumbéa')
        , ('TERRITORY_OVERSEAS_NAME', '98807', 'Hienghène')
        , ('TERRITORY_OVERSEAS_NAME', '98808', 'Houaïlou')
        , ('TERRITORY_OVERSEAS_NAME', '98809', 'Île des Pins')
        , ('TERRITORY_OVERSEAS_NAME', '98810', 'Kaala-Gomen')
        , ('TERRITORY_OVERSEAS_NAME', '98811', 'Koné')
        , ('TERRITORY_OVERSEAS_NAME', '98815', 'Maré')
        , ('TERRITORY_OVERSEAS_NAME', '98817', 'Mont-Dore')
        , ('TERRITORY_OVERSEAS_NAME', '98819', 'Ouégoa')
        , ('TERRITORY_OVERSEAS_NAME', '98820', 'Ouvéa')
        , ('TERRITORY_OVERSEAS_NAME', '98821', 'Païta')
        , ('TERRITORY_OVERSEAS_NAME', '98822', 'Poindimié')
        , ('TERRITORY_OVERSEAS_NAME', '98823', 'Ponérihouen')
        , ('TERRITORY_OVERSEAS_NAME', '98824', 'Pouébo')
        , ('TERRITORY_OVERSEAS_NAME', '98828', 'Sarraméa')
        , ('TERRITORY_OVERSEAS_NAME', '98832', 'Yaté')

        , ('TERRITORY_OVERSEAS_NAME', '9871', 'Îles Marquises')
        , ('TERRITORY_OVERSEAS_NAME', '9872', 'Îles Tuamotu-Gambier')
        , ('TERRITORY_OVERSEAS_NAME', '9873', 'Îles du Vent')
        , ('TERRITORY_OVERSEAS_NAME', '9874', 'Îles Sous-le-Vent')
        , ('TERRITORY_OVERSEAS_NAME', '9875', 'Îles Australes')
        , ('TERRITORY_OVERSEAS_NAME', '9881', 'Province Sud')
        , ('TERRITORY_OVERSEAS_NAME', '9882', 'Province Nord')
        , ('TERRITORY_OVERSEAS_NAME', '9883', 'Îles Loyauté')

        , ('TERRITORY_OVERSEAS_NAME', '975', 'Saint-Pierre-et-Miquelon')
        , ('TERRITORY_OVERSEAS_NAME', '977', 'Saint-Barthélemy')
        , ('TERRITORY_OVERSEAS_NAME', '978', 'Saint-Martin')
        , ('TERRITORY_OVERSEAS_NAME', '986', 'Wallis et Futuna')
        , ('TERRITORY_OVERSEAS_NAME', '987', 'Polynésie française')
        , ('TERRITORY_OVERSEAS_NAME', '988', 'Nouvelle Calédonie')
        , ('TERRITORY_OVERSEAS_NAME', '989', 'Île de Clipperton')

        , ('TERRITORY_OVERSEAS_NAME', '97', 'Îles en Atlantique')
        , ('TERRITORY_OVERSEAS_NAME', '98', 'Îles en Pacifique')
    ;

    DELETE FROM fr.constant WHERE usecase = 'TERRITORY_OVERSEAS_RELATION';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        --9871 Îles Marquises
          ('TERRITORY_OVERSEAS_RELATION', '9871', '98718')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98723')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98731')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98746')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98756')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98757')
        --9872 Îles Tuamotu-Gambier
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98711')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98713')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98716')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98717')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98719')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98720')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98721')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98726')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98727')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98730')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98732')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98737')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98740')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98742')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98749')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98751')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98755')
        --9873 Îles du Vent
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98729')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98712')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98715')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98722')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98725')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98733')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98734')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98735')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98736')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98738')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98747')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98748')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98752')
        --9874 Îles Sous-le-Vent
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98714')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98724')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98728')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98745')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98750')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98754')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98758')
        --9875 Îles Australes
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98739')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98741')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98743')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98744')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98753')

        --9881 Province Sud
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98829')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98832')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98809')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98817')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98818')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98805')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98821')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98802')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98813')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98828')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98806')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98816')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98803')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98827') -- 'SUD'
        --9882 Province Nord
        --, ('TERRITORY_OVERSEAS_RELATION', '9882', '98827') -- 'NORD' !
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98825')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98811')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98831')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98810')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98812')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98826')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98801')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98819')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98824')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98807')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98830')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98822')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98823')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98808')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98833')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98804')
        --9883 Îles Loyauté
        , ('TERRITORY_OVERSEAS_RELATION', '9883', '98820')
        , ('TERRITORY_OVERSEAS_RELATION', '9883', '98814')
        , ('TERRITORY_OVERSEAS_RELATION', '9883', '98815')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_correction_list');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_correction_list()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_CORRECTION';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        ('LAPOSTE_ADDRESS_CORRECTION', 'TOO_SPACE', '1')
        , ('LAPOSTE_ADDRESS_CORRECTION', 'COMPLEMENT_WITH_STREET_ERROR', '2')
    ;
END;
$proc$ LANGUAGE plpgsql;
