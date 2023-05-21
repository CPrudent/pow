/***
 * FR-ADDRESS management
 */

/* NOTE
PART 1
 update dictionaries (street, housenumber and complement), logging address_history (UPDATE/DELETE)
PART 2
 update address (w/ links to dictionaries)
PART 3
 update cross reference (to put LAPOSTE id, well-known as CEA)
 update XY
 */

-- push properties of address street (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_address_street_properties_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_street_properties_to_public(
    force BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _nrows_affected INT;
BEGIN
    CALL public.log_info('Mise à jour du dictionnaire des Voies');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_address_street_changes;
    CREATE TEMPORARY TABLE tmp_fr_address_street_changes AS (
        WITH
        address_street_public AS (
            SELECT
                s.name
                , s.name_normalized
                , s.typeof
                , s.descriptors
            FROM
                public.address_street s
                    JOIN public.address a ON a.id_street = s.id
                    JOIN public.territory t ON t.id = a.id_territory
            WHERE
                t.country = 'FR'
        )
        , address_street_fr AS (
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
                SELECT '-' change, name FROM address_street_public
                EXCEPT
                SELECT '-', name FROM address_street_fr
            )
            UNION
            (
                SELECT '+', name FROM address_street_fr
                EXCEPT
                SELECT '+', name FROM address_street_public
            )
            UNION
            SELECT '!', address_street_public.name
            FROM address_street_public
                JOIN address_street_fr ON address_street_public.name = address_street_fr.name
            WHERE
                (address_street_public.name_normalized IS DISTINCT FROM address_street_fr.name_normalized)
                OR
                (address_street_public.typeof IS DISTINCT FROM address_street_fr.typeof[1])
                OR
                (address_street_public.descriptors IS DISTINCT FROM address_street_fr.descriptors[1])
        )

        -- insert/update addresses
        SELECT
            c.change
            , address_street_fr.name
            , address_street_fr.name_normalized
            , address_street_fr.typeof
            , address_street_fr.descriptors
        FROM
            changes c
                JOIN address_street_fr ON c.name = address_street_fr.name
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete old territories
        SELECT
            c.change
            , address_street_public.name
            , address_street_public.name_normalized
            , address_street_public.typeof[1] typeof
            , address_street_public.descriptors[1] descriptors
        FROM
            changes c
                JOIN address_street_public ON c.name = address_street_public.name
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('CHANGE: ', _nrows_affected));

    CALL public.log_info('Historique des modifications/suppressions');
    INSERT INTO public.address_history (
            id
            , date_change
            , change
            , kind
            , values
        )
        SELECT
            s.id
            , TIMEOFDAY()::DATE
            , c.change
            , 'STREET'
            , ROW_TO_JSON(s.*)::JSONB
        FROM
            tmp_fr_address_street_changes c
                JOIN public.address_street s ON s.name = c.name
                JOIN public.address a ON a.id_street = s.id
                JOIN public.territory t ON t.id = a.id_territory
        WHERE c.change = ANY('{-,!}') AND t.country = 'FR'
        ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('CHANGE: ', _nrows_affected));

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
            tmp_fr_address_street_changes c
        WHERE
            c.change = '+'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('INSERT: ', _nrows_affected));

    CALL public.log_info('Mise à jour des modifications');
    UPDATE public.address_street SET
            name = c.name
            , name_normalized = c.name_normalized
            , typeof = c.typeof[1]
            , descriptors = c.descriptors[1]
        FROM
            tmp_fr_address_street_changes c
        WHERE
            c.change = '!'
            AND
            address_street.name = c.name
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('UPDATE: ', _nrows_affected));

    CALL public.log_info('Mise à jour des suppressions');
    DELETE FROM public.address_street s
    USING tmp_fr_address_street_changes c, public.address a, public.territory t
    WHERE
        c.change = '-'
        AND
        a.id_street = s.id
        AND
        t.id = a.id_territory
        AND
        t.country = 'FR'
        AND
        s.name = c.name
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('DELETE: ', _nrows_affected));
END
$proc$ LANGUAGE plpgsql;

-- push links of address (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_address_links_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_links_to_public(
    force BOOLEAN DEFAULT FALSE
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
                , cr2.id_source code_parent
                , cr3.id_source code_street
                , cr4.id_source code_housenumber
                , cr5.id_source code_complement
                , t.code code_territory
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
                co_cea_determinant
                , CASE WHEN co_niveau = 'VOIE' THEN NULL ELSE co_cea_parent END co_cea_parent
                , co_cea_voie
                , co_cea_numero
                , co_cea_l3
                , co_cea_za
            FROM fr.laposte_address
            WHERE a.fl_active AND co_cea_voie IS NOT NULL
        )
        , changes AS (
            (
                SELECT '-' change, code_address FROM address_public
                EXCEPT
                SELECT '-', co_cea_determinant FROM address_fr
            )
            UNION
            (
                SELECT '+', co_cea_determinant FROM address_fr
                EXCEPT
                SELECT '+', code_address FROM address_public
            )
            UNION
            SELECT '!', address_public.code_address
            FROM address_public
                JOIN address_fr ON address_public.code_address = address_fr.co_cea_determinant
            WHERE
                (address_public.code_parent IS DISTINCT FROM address_fr.co_cea_parent)
                OR
                (address_public.code_street IS DISTINCT FROM address_fr.co_cea_voie)
                OR
                (address_public.code_housenumber IS DISTINCT FROM address_fr.co_cea_numero)
                OR
                (address_public.code_complement IS DISTINCT FROM address_fr.co_cea_l3)
                OR
                (address_public.code_territory IS DISTINCT FROM address_fr.co_cea_za)
        )

        -- insert/update addresses
        SELECT
            c.change
            , c.code_address
            , address_fr.co_cea_parent code_parent
            , address_fr.co_cea_voie code_street
            , address_fr.co_cea_numero code_housenumber
            , address_fr.co_cea_l3 code_complement
            , address_fr.co_cea_za code_territory
        FROM
            changes c
                JOIN address_fr ON c.code_address = address_fr.co_cea_determinant
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete old addresses
        SELECT
            c.change
            , c.code_address
            , address_public.code_parent
            , address_public.code_street
            , address_public.code_housenumber
            , address_public.code_complement
            , address_public.code_territory
        FROM
            changes c
                JOIN address_public ON c.code_address = address_public.code_address
        WHERE
            c.change = '-'
    )
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('CHANGE: ', _nrows_affected));

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
    CALL fr.push_address_street_properties_to_public(force);
    CALL fr.push_address_housenumber_properties_to_public(force);
    CALL fr.push_address_complement_properties_to_public(force);

    CALL fr.push_address_links_to_public(force);
END
$proc$ LANGUAGE plpgsql;
