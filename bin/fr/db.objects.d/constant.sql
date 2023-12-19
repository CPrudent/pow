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

-- build LAPOSTE street : list of types
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_street_type');
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_type');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_type()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DROP TABLE IF EXISTS fr.laposte_address_street_type;
    CREATE TABLE fr.laposte_address_street_type AS
        SELECT DISTINCT
            lb_type AS type
            , NULL::VARCHAR AS type_abbreviated
            , NULL::VARCHAR AS first_word
            , NULL::INT AS same_first_word
            , NULL::INT AS occurs
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        ;
    --SELECT * FROM fr.laposte_address_street_type ORDER BY first_word, type;
    UPDATE fr.laposte_address_street_type st SET
        type_abbreviated = s.lb_type_abrege
        FROM fr.laposte_address_street s
        WHERE
            s.lb_type = st.type
            ;
    WITH
    first_word_of_type AS (
        SELECT type, (REGEXP_MATCHES(type, '([^ ]*)'))[1] first_word
        FROM fr.laposte_address_street_type
    )
    , occurs_type AS (
        SELECT
            lb_type type
            , COUNT(*) occurs
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        GROUP BY
            lb_type
    )
    UPDATE fr.laposte_address_street_type st SET
        first_word = fw.first_word
        , occurs = ot.occurs
        FROM
            first_word_of_type fw
            , occurs_type ot
        WHERE
            st.type = fw.type
            AND
            st.type = ot.type
        ;
    WITH
    first_word_of_type AS (
        SELECT first_word, COUNT(*) same_first_word
        FROM fr.laposte_address_street_type
        GROUP BY first_word
    )
    UPDATE fr.laposte_address_street_type st SET
        same_first_word = fw.same_first_word
        FROM first_word_of_type fw
        WHERE
            st.first_word = fw.first_word
        ;
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of firstnames
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_street_firstname');
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_firstname');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_firstname()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME';
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
            NOT mots.mot = ANY('{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}')
    );
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE extension (of housenumber), w/ abbreviated value
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_extension_of_housenumber');
CREATE OR REPLACE PROCEDURE fr.set_laposte_extension_of_housenumber()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_housenumber') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_EXTENSION_HOUSENUMBER';
    INSERT INTO fr.constant (
        SELECT
            'LAPOSTE_EXTENSION_HOUSENUMBER'
            , t.*
        FROM (
            SELECT lb_ext, lb_abr_nn FROM (
            SELECT DISTINCT lb_ext, lb_abr_nn FROM fr.laposte_address_housenumber
            WHERE fl_active AND lb_ext IS NOT NULL ) t
            ORDER BY
                CASE
                -- A .. Z
                WHEN LENGTH(lb_ext) = 1   THEN ASCII(lb_ext) - ASCII('A') + 1
                -- EXT before LETTER (BIS before B)
                WHEN lb_ext = 'BIS'       THEN 2 - .1
                WHEN lb_ext = 'TER'       THEN 3 - .1
                WHEN lb_ext = 'QUATER'    THEN 4 - .1
                WHEN lb_ext = 'QUINQUIES' THEN 5 - .1
                WHEN lb_ext = 'SEXTO'     THEN 6 - .1
                END
        ) t
    );
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

CREATE INDEX IF NOT EXISTS ix_constant_usecase_key ON fr.constant (usecase, key);
