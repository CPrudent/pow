/***
 * FR-ADDRESS management
 */

/* NOTE
PART 1
 update dictionaries (street, housenumber and complement), logging address_history
PART 2
 update address (w/ links to dictionaries), by descending order
 update cross reference (to put LAPOSTE id, well-known as CEA)
PART 3
 update XY
 */

/* NOTE
no delete applied on dictionaries. It will be necessary to known usage of this item (notion of counts of reference: equal to 1, ok to delete else no!)
a specific job could be built to purge item from dictionary, which not have usage!
 */

-- push properties of street dictionary (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_dictionary_street_to_public');
CREATE OR REPLACE PROCEDURE fr.push_dictionary_street_to_public(
    force BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
    _nb_rows INT[];
BEGIN
    CALL public.log_info('Mise à jour du dictionnaire des Voies');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_street_changes;
    CREATE TEMPORARY TABLE tmp_fr_street_changes AS (
        WITH
        street_public AS (
            SELECT
                name
                , name_normalized
                , typeof
                , descriptors
            FROM
                public.address_street d
                    JOIN public.address a ON d.id = a.id_street
                    JOIN public.territory t ON t.id = a.id_territory
            WHERE
                t.country = 'FR'
        )
        , street_fr AS (
            /* NOTE
            367 faults (lb_type NULL) on restored LAPOSTE data (of 12/2022)
            e.g.
            RUE LOUIS BOREL
            RESIDENCE DES AJONCS
             */
            SELECT
                lb_voie name
                , MIN(lb_voie_normalise) name_normalized
                , ARRAY_AGG(DISTINCT lb_type) typeof
                -- ignore different descriptors if typeof is null
                , ARRAY_AGG(DISTINCT CASE WHEN lb_type IS NULL THEN NULL ELSE lb_desc END) descriptors
            FROM fr.laposte_street
            WHERE fl_active
            GROUP BY
                lb_voie
        )
        , changes AS (
            (
                SELECT '-' change, name FROM street_public
                EXCEPT
                SELECT '-', name FROM street_fr
            )
            UNION
            (
                SELECT '+', name FROM street_fr
                EXCEPT
                SELECT '+', name FROM street_public
            )
            UNION
            SELECT '!', street_public.name
            FROM street_public
                JOIN street_fr ON street_public.name = street_fr.name
            WHERE
                (street_public.name_normalized IS DISTINCT FROM street_fr.name_normalized)
                OR
                (street_public.typeof IS DISTINCT FROM street_fr.typeof[1])
                OR
                (street_public.descriptors IS DISTINCT FROM street_fr.descriptors[1])
        )

        -- insert/update
        SELECT
            c.change
            , street_fr.name
            , street_fr.name_normalized
            , street_fr.typeof
            , street_fr.descriptors
        FROM
            changes c
                JOIN street_fr ON c.name = street_fr.name
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete
        SELECT
            c.change
            , street_public.name
            , street_public.name_normalized
            , street_public.typeof[1] typeof
            , street_public.descriptors[1] descriptors
        FROM
            changes c
                JOIN street_public ON c.name = street_public.name
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    _nb_rows[1] := COUNT(*) FROM tmp_fr_street_changes WHERE change = '+';
    _nb_rows[2] := COUNT(*) FROM tmp_fr_street_changes WHERE change = '!';
    _nb_rows[3] := COUNT(*) FROM tmp_fr_street_changes WHERE change = '-';
    CALL public.log_info(CONCAT('Total: ', _nrows_affected, ' (+: ', _nb_rows[1], ', !: ', _nb_rows[2], ', -: ', _nb_rows[3], ')'));

    --CALL public.log_info('Historique des modifications/suppressions');
    CALL public.log_info('Historique des modifications');
    INSERT INTO public.address_history (
            id
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            a.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'STREET'
            , ROW_TO_JSON(d.*)::JSONB
        FROM
            tmp_fr_street_changes c
                JOIN public.address_street d ON d.name = c.name
                JOIN public.address a ON a.id_street = d.id
                JOIN public.territory t ON t.id = a.id_territory
        --WHERE c.change = ANY('{-,!}') AND t.country = 'FR'
        WHERE c.change = '!' AND t.country = 'FR'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des ajouts');
    INSERT INTO public.address_street (
            name
            , name_normalized
            , typeof
            , descriptors
        )
        SELECT
            c.name
            , c.name_normalized
            , c.typeof[1]
            , c.descriptors[1]
        FROM
            tmp_fr_street_changes c
        WHERE
            c.change = '+'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des modifications');
    UPDATE public.address_street SET
            name_normalized = c.name_normalized
            , typeof = c.typeof[1]
            , descriptors = c.descriptors[1]
        FROM
            tmp_fr_street_changes c
        WHERE
            c.change = '!'
            AND
            address_street.name = c.name
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    /* no delete!
    CALL public.log_info('Mise à jour des suppressions');
    DELETE FROM public.address_street d
    USING tmp_fr_street_changes c
    WHERE
        c.change = '-'
        AND
        d.name = c.name
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));
     */

    IF drop_temporary THEN
        DROP TABLE IF EXISTS tmp_fr_street_changes;
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- push properties of housenumber dictionary (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_dictionary_housenumber_to_public');
CREATE OR REPLACE PROCEDURE fr.push_dictionary_housenumber_to_public(
    force BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
    _nb_rows INT[];
BEGIN
    CALL public.log_info('Mise à jour du dictionnaire des Numéros');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_housenumber_changes;
    CREATE TEMPORARY TABLE tmp_fr_housenumber_changes AS (
        WITH
        housenumber_public AS (
            SELECT
                number
                , extension
            FROM
                public.address_housenumber d
                    JOIN public.address a ON d.id = a.id_housenumber
                    JOIN public.territory t ON t.id = a.id_territory
            WHERE
                t.country = 'FR'
        )
        , housenumber_fr AS (
            SELECT DISTINCT
                no_voie number
                , lb_ext extension
            FROM fr.laposte_housenumber
            WHERE fl_active
        )
        , changes AS (
            (
                SELECT '-' change, number, extension FROM housenumber_public
                EXCEPT
                SELECT '-', number, extension FROM housenumber_fr
            )
            UNION
            (
                SELECT '+', number, extension FROM housenumber_fr
                EXCEPT
                SELECT '+', number, extension FROM housenumber_public
            )
        )

        -- insert
        SELECT
            c.change
            , housenumber_fr.number
            , housenumber_fr.extension
        FROM
            changes c
                JOIN housenumber_fr ON
                (c.number, COALESCE(c.extension, 'NULL')) = (housenumber_fr.number, COALESCE(housenumber_fr.extension, 'NULL'))
        WHERE
            c.change = '+'

        UNION

        -- delete
        SELECT
            c.change
            , housenumber_public.number
            , housenumber_public.extension
        FROM
            changes c
                JOIN housenumber_public ON
                (c.number, COALESCE(c.extension, 'NULL')) = (housenumber_public.number, COALESCE(housenumber_public.extension, 'NULL'))
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    _nb_rows[1] := COUNT(*) FROM tmp_fr_housenumber_changes WHERE change = '+';
    _nb_rows[2] := 0;
    _nb_rows[3] := COUNT(*) FROM tmp_fr_housenumber_changes WHERE change = '-';
    CALL public.log_info(CONCAT('Total: ', _nrows_affected, ' (+: ', _nb_rows[1], ', !: ', _nb_rows[2], ', -: ', _nb_rows[3], ')'));

    /*
    CALL public.log_info('Historique des suppressions');
    INSERT INTO public.address_history (
            id
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            a.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'HOUSENUMBER'
            , ROW_TO_JSON(d.*)::JSONB
        FROM
            tmp_fr_housenumber_changes c
                JOIN public.address_housenumber d ON
                    (d.number, COALESCE(d.extension, 'NULL') = (c.number, COALESCE(c.extension, 'NULL'))
                JOIN public.address a ON a.id_housenumber = d.id
                JOIN public.territory t ON t.id = a.id_territory
        WHERE c.change = '-' AND t.country = 'FR'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));
     */

    CALL public.log_info('Mise à jour des ajouts');
    INSERT INTO public.address_housenumber (
            number
            , extension
        )
        SELECT
            c.number
            , c.extension
        FROM
            tmp_fr_housenumber_changes c
        WHERE
            c.change = '+'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    /*
    CALL public.log_info('Mise à jour des suppressions');
    DELETE FROM public.address_housenumber d
    USING tmp_fr_housenumber_changes c
    WHERE
        c.change = '-'
        AND
        (d.number, COALESCE(d.extension, 'NULL') = (c.number, COALESCE(c.extension, 'NULL'))
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));
     */

    IF drop_temporary THEN
        DROP TABLE IF EXISTS tmp_fr_housenumber_changes;
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- push properties of complement dictionary (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_dictionary_complement_to_public');
CREATE OR REPLACE PROCEDURE fr.push_dictionary_complement_to_public(
    force BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
    _nb_rows INT[];
BEGIN
    CALL public.log_info('Mise à jour du dictionnaire des L3');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_complement_changes;
    CREATE TEMPORARY TABLE tmp_fr_complement_changes AS (
        WITH
        complement_public AS (
            SELECT
                name
                , name_normalized
            FROM
                public.address_complement d
                    JOIN public.address a ON d.id = a.id_complement
                    JOIN public.territory t ON t.id = a.id_territory
            WHERE
                t.country = 'FR'
        )
        , complement_fr AS (
            SELECT DISTINCT
                CONCAT_WS(' '
                    , lb_type_groupe1_l3
                    , lb_groupe1
                    , lb_type_groupe2_l3
                    , lb_groupe2
                    , lb_type_groupe3_l3
                    , lb_groupe3
                ) name
                , lb_standard_nn name_normalized
            FROM fr.laposte_complement
            WHERE fl_active
        )
        , changes AS (
            (
                SELECT '-' change, name FROM complement_public
                EXCEPT
                SELECT '-', name FROM complement_fr
            )
            UNION
            (
                SELECT '+', name FROM complement_fr
                EXCEPT
                SELECT '+', name FROM complement_public
            )
            UNION
            SELECT '!', complement_public.name
            FROM complement_public
                JOIN complement_fr ON complement_public.name = complement_fr.name
            WHERE
                (complement_public.name_normalized IS DISTINCT FROM complement_fr.name_normalized)
        )

        -- insert/update
        SELECT
            c.change
            , complement_fr.name
            , complement_fr.name_normalized
        FROM
            changes c
                JOIN complement_fr ON c.name = complement_fr.name
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete
        SELECT
            c.change
            , complement_public.name
            , complement_public.name_normalized
        FROM
            changes c
                JOIN complement_public ON c.name = complement_public.name
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    _nb_rows[1] := COUNT(*) FROM tmp_fr_complement_changes WHERE change = '+';
    _nb_rows[2] := COUNT(*) FROM tmp_fr_complement_changes WHERE change = '!';
    _nb_rows[3] := COUNT(*) FROM tmp_fr_complement_changes WHERE change = '-';
    CALL public.log_info(CONCAT('Total: ', _nrows_affected, ' (+: ', _nb_rows[1], ', !: ', _nb_rows[2], ', -: ', _nb_rows[3], ')'));

    --CALL public.log_info('Historique des modifications/suppressions');
    CALL public.log_info('Historique des modifications');
    INSERT INTO public.address_history (
            id
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            a.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'COMPLEMENT'
            , ROW_TO_JSON(d.*)::JSONB
        FROM
            tmp_fr_complement_changes c
                JOIN public.address_complement d ON d.name = c.name
                JOIN public.address a ON a.id_complement = d.id
                JOIN public.territory t ON t.id = a.id_territory
        --WHERE c.change = ANY('{-,!}') AND t.country = 'FR'
        WHERE c.change = '!' AND t.country = 'FR'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des ajouts');
    INSERT INTO public.address_complement (
            name
            , name_normalized
        )
        SELECT
            c.name
            , c.name_normalized
        FROM
            tmp_fr_complement_changes c
        WHERE
            c.change = '+'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des modifications');
    UPDATE public.address_complement SET
            name_normalized = c.name_normalized
        FROM
            tmp_fr_complement_changes c
        WHERE
            c.change = '!'
            AND
            address_complement.name = c.name
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    /* no delete!
    CALL public.log_info('Mise à jour des suppressions');
    DELETE FROM public.address_complement d
    USING tmp_fr_complement_changes c
    WHERE
        c.change = '-'
        AND
        d.name = c.name
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));
     */

    IF drop_temporary THEN
        DROP TABLE IF EXISTS tmp_fr_complement_changes;
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- push links of address (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_address_links_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_links_to_public(
    force BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
BEGIN
    CALL public.log_info('Mise à jour des Adresses');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_address_changes;
    CREATE TEMPORARY TABLE tmp_fr_address_changes AS (
        WITH
        address_public AS (
            SELECT
                cr1.id_source code_address
                , CASE
                    WHEN id_complement IS NOT NULL THEN 'L3'
                    WHEN id_housenumber IS NOT NULL AND id_complement IS NULL THEN 'NUMERO'
                    ELSE 'VOIE'
                END level
                , cr2.id_source code_parent
                , t.code code_territory
                , cr3.id_source code_street
                , cr4.id_source code_housenumber
                , cr5.id_source code_complement
                /*
                , a.id id_address
                , a.id_parent
                , a.id_territory
                , a.id_street
                , a.id_housenumber
                , a.id_complement
                 */
            FROM
                public.address a
                    JOIN public.territory t ON t.id = a.id_territory
                    JOIN public.address_cross_reference cr1 ON cr1.id_address = a.id AND cr1.source = 'LAPOSTE'
                    LEFT OUTER JOIN public.address_cross_reference cr2 ON cr2.id_address = a.id_parent AND cr2.source = 'LAPOSTE'
                    JOIN public.address_cross_reference cr3 ON cr3.id_address = a.id_street AND cr3.source = 'LAPOSTE'
                    LEFT OUTER JOIN public.address_cross_reference cr4 ON cr4.id_address = a.id_housenumber AND cr4.source = 'LAPOSTE'
                    LEFT OUTER JOIN public.address_cross_reference cr5 ON cr5.id_address = a.id_complement AND cr5.source = 'LAPOSTE'
            WHERE
                t.country = 'FR'
        )
        , address_fr AS (
            SELECT
                co_cea_determinant code_address
                , co_niveau level
                , CASE WHEN co_niveau = 'VOIE' THEN NULL ELSE co_cea_parent END code_parent
                , co_cea_za code_territory
                , co_cea_voie code_street
                , co_cea_numero code_housenumber
                , co_cea_l3 code_complement
                /*
                , cr1.id_address
                , cr2.id_address id_parent
                , t.id id_territory
                , cr3.id_address id_street
                , cr4.id_address id_housenumber
                , cr5.id_address id_complement
                 */
            FROM fr.laposte_address
                /*
                JOIN public.territory t ON t.code = co_cea_za AND t.level = 'ZA' AND country = 'FR'
                LEFT OUTER JOIN public.address_cross_reference cr1 ON cr1.id_source = co_cea_determinant AND cr1.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address_cross_reference cr2 ON cr2.id_source = CASE WHEN co_niveau = 'VOIE' THEN NULL ELSE co_cea_parent END AND cr2.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address_cross_reference cr3 ON cr3.id_source = co_cea_voie AND cr3.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address_cross_reference cr4 ON cr4.id_source = co_cea_numero AND cr4.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address_cross_reference cr5 ON cr5.id_source = co_cea_l3 AND cr5.source = 'LAPOSTE'
                 */
            WHERE fl_active AND co_cea_voie IS NOT NULL
        )
        , changes AS (
            (
                SELECT '-' change, code_address FROM address_public
                EXCEPT
                SELECT '-', code_address FROM address_fr
            )
            UNION
            (
                SELECT '+', code_address FROM address_fr
                EXCEPT
                SELECT '+', code_address FROM address_public
            )
            UNION
            SELECT '!', address_public.code_address
            FROM address_public
                JOIN address_fr ON address_public.code_address = address_fr.code_address
            WHERE
                (address_public.code_parent IS DISTINCT FROM address_fr.code_parent)
                OR
                (address_public.code_territory IS DISTINCT FROM address_fr.code_territory)
                OR
                (address_public.code_street IS DISTINCT FROM address_fr.code_street)
                OR
                (address_public.code_housenumber IS DISTINCT FROM address_fr.code_housenumber)
                OR
                (address_public.code_complement IS DISTINCT FROM address_fr.code_complement)
        )

        -- insert/update
        SELECT
            c.change
            , address_fr.level
            , c.code_address
            , address_fr.code_parent
            , address_fr.code_territory
            , address_fr.code_street
            , address_fr.code_housenumber
            , address_fr.code_complement
            /*
            , address_fr.id_address
            , address_fr.id_parent
            , address_fr.id_territory
            , address_fr.id_street
            , address_fr.id_housenumber
            , address_fr.id_complement
             */
        FROM
            changes c
                JOIN address_fr ON c.code_address = address_fr.code_address
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete
        SELECT
            c.change
            , address_public.level
            , c.code_address
            , address_public.code_parent
            , address_public.code_territory
            , address_public.code_street
            , address_public.code_housenumber
            , address_public.code_complement
            /*
            , address_public.id_address
            , address_public.id_parent
            , address_public.id_territory
            , address_public.id_street
            , address_public.id_housenumber
            , address_public.id_complement
             */
        FROM
            changes c
                JOIN address_public ON c.code_address = address_public.code_address
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Indexation des changements');
    CREATE INDEX IF NOT EXISTS ix_tmp_fr_address_changes ON tmp_fr_address_changes(change, level);

    CALL public.log_info('Historique des modifications/suppressions');
    INSERT INTO public.address_history (
            id
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            a.id_address
            , TIMEOFDAY()::DATE
            , c.change
            , 'ADDRESS'
            , ROW_TO_JSON(a.*)::JSONB
        FROM
            tmp_fr_address_changes c
                JOIN public.address_cross_reference cr ON cr.id_source = c.code_address AND cr.source = 'LAPOSTE'
                JOIN public.address a ON a.id_address = cr.id_address
        WHERE c.change = ANY('{-,!}')
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    /* NOTE
    have to add addresses in descending order, because of parent id
     */
    CALL public.log_info('Mise à jour des ajouts');

    -- STREET
    CALL public.log_info('Préparation');
    DROP TABLE IF EXISTS tmp_fr_address_news;
    CREATE TEMPORARY TABLE tmp_fr_address_news AS
        SELECT * FROM public.address WITH NO DATA;
    ALTER TABLE tmp_fr_address_news ADD COLUMN code_address VARCHAR;
    INSERT INTO tmp_fr_address_news (
            id_territory
            , id_street
            , code_address
        )
        SELECT
            t.id
            , s2.id
            , c.code_address
        FROM
            tmp_fr_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND country = 'FR'
                JOIN fr.laposte_street s1 ON s1.co_cea = c.code_street
                JOIN public.address_street s2 ON s2.name = s1.lb_voie
        WHERE
            change = '+'
            AND
            level = 'VOIE'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('VOIE: ', _nrows_affected));

    CALL public.log_info('Insertion');
    INSERT INTO public.address (
            id_territory
            , id_street
        )
        SELECT
            id_territory
            , id_street
        FROM
            tmp_fr_address_news
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('VOIE: ', _nrows_affected));

    CALL public.log_info('Références');
    INSERT INTO public.address_cross_reference (
            id_address
            , source
            , id_source
        )
        SELECT
            a.id
            , 'LAPOSTE'
            , n.code_address
        FROM
            tmp_fr_address_news n
                JOIN public.address a ON a.id_territory = n.id_territory AND a.id_street = n.id_street
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('VOIE: ', _nrows_affected));

    -- HOUSENUMBER
    CALL public.log_info('Préparation');
    TRUNCATE TABLE tmp_fr_address_news;
    INSERT INTO tmp_fr_address_news (
            id_parent
            , id_territory
            , id_street
            , id_housenumber
            , code_address
        )
        SELECT
            a.id
            , t.id
            , a.id_street
            , hn2.id
            , c.code_address
        FROM
            tmp_fr_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND country = 'FR'
                JOIN public.address_cross_reference cr ON cr.id_source = c.code_street AND cr.source = 'LAPOSTE'
                JOIN public.address a ON a.id = cr.id_address
                JOIN fr.laposte_housenumber hn1 ON hn1.co_cea = c.code_housenumber
                JOIN public.address_housenumber hn2 ON (hn2.number, COALESCE(hn2.extension, 'NULL')) = (hn1.no_voie, COALESCE(hn1.lb_ext, 'NULL'))
        WHERE
            change = '+'
            AND
            level = 'NUMERO'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('NUMERO: ', _nrows_affected));

    CALL public.log_info('Insertion');
    INSERT INTO public.address (
            id_parent
            , id_territory
            , id_street
            , id_housenumber
        )
        SELECT
            id_parent
            , id_territory
            , id_street
            , id_housenumber
        FROM
            tmp_fr_address_news
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('NUMERO: ', _nrows_affected));

    CALL public.log_info('Références');
    INSERT INTO public.address_cross_reference (
            id_address
            , source
            , id_source
        )
        SELECT
            a.id
            , 'LAPOSTE'
            , n.code_address
        FROM
            tmp_fr_address_news n
                JOIN public.address a ON a.id_territory = n.id_territory AND a.id_street = n.id_street AND a.id_housenumber = n.id_housenumber
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('NUMERO: ', _nrows_affected));

    -- COMPLEMENT
    CALL public.log_info('Préparation');
    TRUNCATE TABLE tmp_fr_address_news;
    INSERT INTO tmp_fr_address_news (
            id_parent
            , id_territory
            , id_street
            , id_housenumber
            , id_complement
            , code_address
        )
        SELECT
            a.id
            , t.id
            , a.id_street
            , a.id_housenumber
            , c2.id
            , c.code_address
        FROM
            tmp_fr_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND country = 'FR'
                JOIN public.address_cross_reference cr ON cr.id_source = COALESCE(c.code_housenumber, c.code_street) AND cr.source = 'LAPOSTE'
                JOIN public.address a ON a.id = cr.id_address
                JOIN fr.laposte_complement c1 ON c1.co_cea = c.code_complement
                JOIN public.address_complement c2 ON c2.name = CONCAT_WS(' '
                    , c1.lb_type_groupe1_l3
                    , c1.lb_groupe1
                    , c1.lb_type_groupe2_l3
                    , c1.lb_groupe2
                    , c1.lb_type_groupe3_l3
                    , c1.lb_groupe3
                )
        WHERE
            change = '+'
            AND
            level = 'L3'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('L3: ', _nrows_affected));

    CALL public.log_info('Insertion');
    INSERT INTO public.address (
            id_parent
            , id_territory
            , id_street
            , id_housenumber
            , id_complement
        )
        SELECT
            id_parent
            , id_territory
            , id_street
            , id_housenumber
            , id_complement
        FROM
            tmp_fr_address_news
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('L3: ', _nrows_affected));

    CALL public.log_info('Références');
    INSERT INTO public.address_cross_reference (
            id_address
            , source
            , id_source
        )
        SELECT
            a.id
            , 'LAPOSTE'
            , n.code_address
        FROM
            tmp_fr_address_news n
                JOIN public.address a ON
                    a.id_territory = n.id_territory
                    AND
                    a.id_street = n.id_street
                    AND (
                            (
                                (n.id_housenumber IS NOT NULL)
                                AND
                                (a.id_housenumber = n.id_housenumber)
                            )
                            OR
                            (n.id_housenumber IS NULL)
                    )
                    AND
                    a.id_complement = n.id_complement
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('L3: ', _nrows_affected));

    CALL public.log_info('Mise à jour des modifications');
    WITH
    address_updates AS (
        SELECT
            cr1.id_address
            , cr2.id_address id_parent
            , t.id_territory
            , a3.id_street
            , a4.id_housenumber
            , a5.id_complement
        FROM
            tmp_fr_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND country = 'FR'
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address_cross_reference cr2 ON cr2.id_source = c.code_parent AND cr2.source = 'LAPOSTE'
                JOIN public.address_cross_reference cr3 ON cr3.id_source = c.code_street AND cr3.source = 'LAPOSTE'
                JOIN public.address a3 ON a3.id_address = cr3.id_address
                LEFT OUTER JOIN public.address_cross_reference cr4 ON cr4.id_source = c.code_housenumber AND cr4.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address a4 ON a4.id_address = cr4.id_address
                LEFT OUTER JOIN public.address_cross_reference cr5 ON cr5.id_source = c.code_complement AND cr5.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address a5 ON a5.id_address = cr5.id_address
        WHERE
            change = '!'
    )
    UPDATE public.address a SET
            id_parent = u.id_parent
            , id_territory = u.id_territory
            , id_street = u.id_street
            , id_housenumber = u.id_housenumber
            , id_complement = u.id_complement
        FROM address_updates u
        WHERE
            a.id_address = u.id_address
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des suppressions');
    WITH
    address_deletes AS (
        SELECT
            cr1.id_address
        FROM
            tmp_fr_address_changes c
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
        WHERE
            change = '-'
    )
    DELETE FROM public.address a
        USING address_deletes d
        WHERE
            a.id_address = d.id_address
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    IF drop_temporary THEN
        DROP TABLE IF EXISTS tmp_fr_address_changes;
        DROP TABLE IF EXISTS tmp_fr_address_news;
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- push properties of address xy (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_address_xy_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_xy_to_public(
    force BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
BEGIN
    CALL public.log_info('Mise à jour des XY des Adresses');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_xy_changes;
    CREATE TEMPORARY TABLE tmp_fr_xy_changes AS (
        WITH
        xy_public AS (
            SELECT
                cr.id_source code_address
                , xy.kind
                , xy.geom
            FROM
                public.address_xy xy
                    JOIN public.address_cross_reference cr ON cr.id_address = xy.id_address AND cr.source = 'LAPOSTE'
            WHERE
                xy.source = 'LAPOSTE'
        )
        , xy_fr AS (
            SELECT
                co_cea
                , CASE no_type_localisation
                    WHEN '1' THEN 'MUNICIPALITY_CENTER'
                    WHEN '2' THEN 'TOWN_HALL'
                    WHEN '3' THEN 'AREA'
                    WHEN '4' THEN 'STREET_CENTER'
                    WHEN '5' THEN 'STREET_SECTION_CENTER'
                    WHEN '6' THEN 'STREET_SECTION'
                    WHEN '7' THEN 'PARCEL'
                    WHEN '8' THEN 'ENTRANCE'
                    ELSE          'UNKNOWN'
                END no_type_localisation
                , gm_coord
            FROM fr.laposte_xy
            WHERE
                gm_coord IS NOT NULL
        )
        , changes AS (
            (
                SELECT '-' change, code_address FROM xy_public
                EXCEPT
                SELECT '-', co_cea FROM xy_fr
            )
            UNION
            (
                SELECT '+', co_cea FROM xy_fr
                EXCEPT
                SELECT '+', code_address FROM xy_public
            )
            UNION
            SELECT '!', xy_public.code_address
            FROM xy_public
                JOIN xy_fr ON xy_public.code_address = xy_fr.co_cea
            WHERE
                (xy_public.kind IS DISTINCT FROM xy_fr.no_type_localisation)
                OR
                (NOT ST_Equals(xy_public.geom, xy_fr.gm_coord))
        )

        -- insert/update addresses
        SELECT
            c.change
            , c.code_address
            , xy_fr.no_type_localisation kind
            , xy_fr.gm_coord geom
        FROM
            changes c
                JOIN xy_fr ON c.code_address = xy_fr.co_cea
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete old addresses
        SELECT
            c.change
            , c.code_address
            , xy_public.kind
            , xy_public.geom
        FROM
            changes c
                JOIN xy_public ON c.code_address = xy_public.code_address
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des ajouts/modifications');
    INSERT INTO public.address_xy (
            id_address
            , kind
            , source
            , geom
        )
        SELECT
            cr.id_address
            , c.kind
            , 'LAPOSTE'
            , c.geom
        FROM
            tmp_fr_xy_changes c
                JOIN public.address_cross_reference cr ON cr.id_source = xy.code_address AND cr.source = 'LAPOSTE'
        WHERE
            c.change = ANY('{+,!}')
    ON CONFLICT(id_address, kind, source) DO UPDATE
        SET
            geom = EXCLUDED.geom
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    CALL public.log_info('Mise à jour des suppressions');
    WITH
    xy_deletes AS (
        SELECT
            cr1.id_address
        FROM
            tmp_fr_xy_changes c
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
        WHERE
            change = '-'
    )
    DELETE FROM public.address_xy xy
        USING xy_deletes d
        WHERE
            xy.id_address = d.id_address
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));

    IF drop_temporary THEN
        DROP TABLE IF EXISTS tmp_fr_xy_changes;
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- push address (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_address_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_to_public(
    force BOOLEAN DEFAULT FALSE
)
AS
$proc$
BEGIN
    -- dictionaries (items of an address)
    CALL fr.push_dictionary_street_to_public(force);
    CALL fr.push_dictionary_housenumber_to_public(force);
    CALL fr.push_dictionary_complement_to_public(force);
    -- addresses (w/ cross reference to store LAPOSTE id, as CEA)
    CALL fr.push_address_links_to_public(force);
    -- XY
    CALL fr.push_address_xy_to_public(force);
END
$proc$ LANGUAGE plpgsql;
