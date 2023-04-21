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
END $$;

-- build LAPOSTE municipality : list of normalized label exceptions
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_municipality_normalized_label_exception');
CREATE OR REPLACE PROCEDURE fr.set_laposte_municipality_normalized_label_exception()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_zone_address') THEN
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
            FROM fr.laposte_zone_address
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
                fr.laposte_zone_address
                    JOIN fr.insee_administrative_cutting_municipality_and_district
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
CREATE OR REPLACE PROCEDURE fr.set_laposte_street_type()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DROP TABLE IF EXISTS fr.laposte_street_type;
    CREATE TABLE fr.laposte_street_type AS
        SELECT DISTINCT
            lb_type AS type
            , NULL::VARCHAR AS type_abbreviated
            , NULL::VARCHAR AS first_word
            , NULL::INT AS same_first_word
            , NULL::INT AS occurs
        FROM fr.laposte_street
        WHERE lb_type IS NOT NULL
        ;
    --SELECT * FROM fr.laposte_street_type ORDER BY first_word, type;
    UPDATE fr.laposte_street_type st SET
        type_abbreviated = s.lb_type_abrege
        FROM fr.laposte_street s
        WHERE
            s.lb_type = st.type
            ;
    WITH
    first_word_of_type AS (
        SELECT type, (REGEXP_MATCHES(type, '([^ ]*)'))[1] first_word
        FROM fr.laposte_street_type
    )
    , occurs_type AS (
        SELECT
            lb_type type
            , COUNT(*) occurs
        FROM fr.laposte_street
        WHERE lb_type IS NOT NULL
        GROUP BY
            lb_type
    )
    UPDATE fr.laposte_street_type st SET
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
        FROM fr.laposte_street_type
        GROUP BY first_word
    )
    UPDATE fr.laposte_street_type st SET
        same_first_word = fw.same_first_word
        FROM first_word_of_type fw
        WHERE
            st.first_word = fw.first_word
        ;
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of firstnames
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_street_firstname');
CREATE OR REPLACE PROCEDURE fr.set_laposte_street_firstname()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME';
    INSERT INTO fr.constant (
        SELECT DISTINCT
            'LAPOSTE_STREET_FIRSTNAME'
            , mots.mot
        FROM fr.laposte_street AS voie_ran
        INNER JOIN LATERAL UNNEST(REGEXP_SPLIT_TO_ARRAY(voie_ran.lb_voie, '\s+'))
            WITH ORDINALITY AS mots(mot, ordre)
            ON TRUE
        INNER JOIN LATERAL UNNEST(STRING_TO_ARRAY(voie_ran.lb_desc, NULL))
            WITH ORDINALITY AS descripteurs(descripteur, ordre)
            ON mots.ordre = descripteurs.ordre AND descripteurs.descripteur = 'P'
        WHERE
            LENGTH(mots.mot) > 1
    );
END;
$proc$ LANGUAGE plpgsql;

CREATE INDEX IF NOT EXISTS ix_constant_usecase_key ON fr.constant (usecase, key);
