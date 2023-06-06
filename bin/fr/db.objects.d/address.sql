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

/* NOTE
can't commit dictionary, while address not inserting
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
                d.name
                , d.name_normalized
                , d.typeof
                , d.descriptors
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
                -- trick to ignore different descriptors if typeof is null (by order)
                , ARRAY_AGG(DISTINCT lb_desc ORDER BY lb_desc DESC) descriptors
            FROM fr.laposte_street
            -- hors MONACO
            WHERE fl_active AND co_insee_commune != '99138'
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
            , street_fr.typeof[1] typeof
            , street_fr.descriptors[1] descriptors
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
            , street_public.typeof
            , street_public.descriptors
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
            , c.typeof
            , c.descriptors
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
            , typeof = c.typeof
            , descriptors = c.descriptors
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
                d.name
                , d.name_normalized
            FROM
                public.address_complement d
                    JOIN public.address a ON d.id = a.id_complement
                    JOIN public.territory t ON t.id = a.id_territory
            WHERE
                t.country = 'FR'
        )
        , complement_fr AS (
            /* NOTE
            7 faults due to name_normalized!
            BATIMENT A RESIDENCE BELLEVUE	    {BATIMENT A RESIDENCE BELLEVUE,BATIMENT A RESIDENCE VILLA BELLEVUE}
            BATIMENT A RESIDENCE MONTMORENCY	{BAT A RESIDENCE MONTMORENCY,BATIMENT A RESIDENCE MONTMORENCY}
            BATIMENT A RESIDENCE VILLA ROSA	    {BAT A RESIDENCE VILLA ROSA,BATIMENT A RESIDENCE VILLA ROSA}
            BATIMENT B RESIDENCE BELLEVUE	    {BATIMENT B RESIDENCE BELLEVUE,BATIMENT B RESIDENCE VILLA BELLEVUE}
            BATIMENT B RESIDENCE MONTMORENCY	{BAT B RESIDENCE MONTMORENCY,BATIMENT B RESIDENCE MONTMORENCY}
            BATIMENT B RESIDENCE VILLA ROSA	    {BAT B RESIDENCE VILLA ROSA,BATIMENT B RESIDENCE VILLA ROSA}
            BATIMENT C RESIDENCE MONTMORENCY	{BAT C RESIDENCE MONTMORENCY,BATIMENT C RESIDENCE MONTMORENCY}
             */
            SELECT
                name
                -- trick to ignore normalized faults, fortunaly not needed!
                , CASE
                    -- have to be normalize?
                    WHEN LENGTH(name) > 38 THEN name_normalized[1]
                    ELSE name
                END name_normalized
            FROM (
                SELECT
                    CONCAT_WS(' '
                        , lb_type_groupe1_l3
                        , lb_groupe1
                        , lb_type_groupe2_l3
                        , lb_groupe2
                        , lb_type_groupe3_l3
                        , lb_groupe3
                    ) name
                    , ARRAY_AGG(DISTINCT lb_standard_nn /*ORDER BY lb_standard_nn*/) name_normalized
                FROM fr.laposte_complement
                WHERE fl_active
                GROUP BY
                    CONCAT_WS(' '
                        , lb_type_groupe1_l3
                        , lb_groupe1
                        , lb_type_groupe2_l3
                        , lb_groupe2
                        , lb_type_groupe3_l3
                        , lb_groupe3
                    )
            ) t
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
    _address RECORD;
    _id INT;
BEGIN
    CALL public.log_info('Mise à jour des Adresses');

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS fr.tmp_address_changes;
    CREATE UNLOGGED TABLE fr.tmp_address_changes AS (
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
            FROM fr.laposte_address a
                JOIN fr.laposte_zone_address za ON za.co_cea = a.co_cea_za
            -- hors ZA
            -- hors MONACO
            WHERE a.fl_active AND co_cea_voie IS NOT NULL AND za.co_insee_commune != '99138'
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
            , s.id id_street
        FROM
            changes c
                JOIN address_fr ON c.code_address = address_fr.code_address
                JOIN LATERAL (
                    SELECT
                        s1.co_cea
                        , s2.id
                    FROM
                        fr.laposte_street s1
                            JOIN public.address_street s2 ON s2.name = s1.lb_voie
                    WHERE
                        s1.co_cea = address_fr.code_street
                ) s ON TRUE
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
            , NULL::INT
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
    CREATE INDEX IF NOT EXISTS ix_tmp_fr_address_changes ON fr.tmp_address_changes(change, level);

    CALL public.log_info('Historique des modifications/suppressions');
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
            , 'ADDRESS'
            , ROW_TO_JSON(a.*)::JSONB
        FROM
            fr.tmp_address_changes c
                JOIN public.address_cross_reference cr ON cr.id_source = c.code_address AND cr.source = 'LAPOSTE'
                JOIN public.address a ON a.id = cr.id_address
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
    DROP TABLE IF EXISTS fr.tmp_address_news;
    CREATE UNLOGGED TABLE fr.tmp_address_news AS
        SELECT * FROM public.address WITH NO DATA;
    ALTER TABLE fr.tmp_address_news ADD COLUMN code_address VARCHAR;
    ALTER TABLE fr.tmp_address_news DROP COLUMN id;
    INSERT INTO fr.tmp_address_news (
            id_territory
            , id_street
            , code_address
        )
        SELECT
            t.id
            , c.id_street
            , c.code_address
        FROM
            fr.tmp_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND t.country = 'FR'
                /*
                JOIN fr.laposte_street s1 ON s1.co_cea = c.code_street
                JOIN public.address_street s2 ON s2.name = s1.lb_voie
                 */
        WHERE
            c.change = '+'
            AND
            c.level = 'VOIE'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('VOIE: ', _nrows_affected));

    /* NOTE
    be careful about street w/ same name (and same territory) !
    have to insert row per row (to obtain uniq id address)
     */
    CALL public.log_info('Insertion/Références');
    _nrows_affected := 0;
    FOR _address IN (
        SELECT * FROM fr.tmp_address_news
    )
    LOOP
        INSERT INTO public.address (
                id_territory
                , id_street
            )
        VALUES (
            _address.id_territory
            , _address.id_street
        )
        RETURNING id INTO _id;

        INSERT INTO public.address_cross_reference (
                id_address
                , source
                , id_source
            )
        VALUES (
            _id
            , 'LAPOSTE'
            , _address.code_address
        );

        _nrows_affected := _nrows_affected +1;
        IF _nrows_affected % 1000 = 0 THEN
            CALL public.log_info(CONCAT('VOIE: ', _nrows_affected));
        END IF;
    END LOOP;
    CALL public.log_info(CONCAT('VOIE: ', _nrows_affected));
    COMMIT;

    -- HOUSENUMBER
    CALL public.log_info('Préparation');
    TRUNCATE TABLE fr.tmp_address_news;
    INSERT INTO fr.tmp_address_news (
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
            fr.tmp_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND t.country = 'FR'
                JOIN public.address_cross_reference cr ON cr.id_source = c.code_street AND cr.source = 'LAPOSTE'
                JOIN public.address a ON a.id = cr.id_address
                JOIN fr.laposte_housenumber hn1 ON hn1.co_cea = c.code_housenumber
                JOIN public.address_housenumber hn2 ON (hn2.number, COALESCE(hn2.extension, 'NULL')) = (hn1.no_voie, COALESCE(hn1.lb_ext, 'NULL'))
        WHERE
            c.change = '+'
            AND
            c.level = 'NUMERO'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('NUMERO: ', _nrows_affected));

    /* NOTE
    be careful about same housenumber on street w/ same name (and same territory) !
    likewise, have to insert row per row (to obtain uniq id address)
    e.g. 2 housenumbers : 35 RUE DE L EGLISE 30190 SAINTE ANASTASIE {30228222LN, 30228222LH}
     */
    CALL public.log_info('Insertion/Références');
    _nrows_affected := 0;
    FOR _address IN (
        SELECT * FROM fr.tmp_address_news
    )
    LOOP
        INSERT INTO public.address (
                id_parent
                , id_territory
                , id_street
                , id_housenumber
            )
        VALUES (
            _address.id_parent
            , _address.id_territory
            , _address.id_street
            , _address.id_housenumber
        )
        RETURNING id INTO _id;

        INSERT INTO public.address_cross_reference (
                id_address
                , source
                , id_source
            )
        VALUES (
            _id
            , 'LAPOSTE'
            , _address.code_address
        );

        _nrows_affected := _nrows_affected +1;
        IF _nrows_affected % 10000 = 0 THEN
            CALL public.log_info(CONCAT('NUMERO: ', _nrows_affected));
        END IF;
    END LOOP;
    CALL public.log_info(CONCAT('NUMERO: ', _nrows_affected));
    COMMIT;

    -- COMPLEMENT
    CALL public.log_info('Préparation');
    TRUNCATE TABLE fr.tmp_address_news;
    INSERT INTO fr.tmp_address_news (
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
            fr.tmp_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND t.country = 'FR'
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
            c.change = '+'
            AND
            c.level = 'L3'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('L3: ', _nrows_affected));

    /* NOTE
    eventualy for same complement ?
    likewise, have to insert row per row (to obtain id address)
     */
    CALL public.log_info('Insertion/Références');
    _nrows_affected := 0;
    FOR _address IN (
        SELECT * FROM fr.tmp_address_news
    )
    LOOP
        INSERT INTO public.address (
                id_parent
                , id_territory
                , id_street
                , id_housenumber
                , id_complement
            )
        VALUES (
            _address.id_parent
            , _address.id_territory
            , _address.id_street
            , _address.id_housenumber
            , _address.id_complement
        )
        RETURNING id INTO _id;

        INSERT INTO public.address_cross_reference (
                id_address
                , source
                , id_source
            )
        VALUES (
            _id
            , 'LAPOSTE'
            , _address.code_address
        );

        _nrows_affected := _nrows_affected +1;
        IF _nrows_affected % 1000 = 0 THEN
            CALL public.log_info(CONCAT('L3: ', _nrows_affected));
        END IF;
    END LOOP;
    CALL public.log_info(CONCAT('L3: ', _nrows_affected));
    COMMIT;

    CALL public.log_info('Mise à jour des modifications');
    WITH
    address_updates AS (
        SELECT
            cr1.id_address
            , cr2.id_address id_parent
            , t.id id_territory
            , a3.id_street
            , a4.id_housenumber
            , a5.id_complement
        FROM
            fr.tmp_address_changes c
                JOIN public.territory t ON t.code = c.code_territory AND t.level = 'ZA' AND t.country = 'FR'
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address_cross_reference cr2 ON cr2.id_source = c.code_parent AND cr2.source = 'LAPOSTE'
                JOIN public.address_cross_reference cr3 ON cr3.id_source = c.code_street AND cr3.source = 'LAPOSTE'
                JOIN public.address a3 ON a3.id = cr3.id_address
                LEFT OUTER JOIN public.address_cross_reference cr4 ON cr4.id_source = c.code_housenumber AND cr4.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address a4 ON a4.id = cr4.id_address
                LEFT OUTER JOIN public.address_cross_reference cr5 ON cr5.id_source = c.code_complement AND cr5.source = 'LAPOSTE'
                LEFT OUTER JOIN public.address a5 ON a5.id = cr5.id_address
        WHERE
            c.change = '!'
    )
    UPDATE public.address a SET
            id_parent = u.id_parent
            , id_territory = u.id_territory
            , id_street = u.id_street
            , id_housenumber = u.id_housenumber
            , id_complement = u.id_complement
        FROM address_updates u
        WHERE
            a.id = u.id_address
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));
    COMMIT;

    CALL public.log_info('Mise à jour des suppressions');
    WITH
    address_deletes AS (
        SELECT
            cr1.id_address
        FROM
            fr.tmp_address_changes c
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
        WHERE
            c.change = '-'
    )
    DELETE FROM public.address a
        USING address_deletes d
        WHERE
            a.id = d.id_address
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));
    COMMIT;

    IF drop_temporary THEN
        DROP TABLE IF EXISTS fr.tmp_address_changes;
        DROP TABLE IF EXISTS fr.tmp_address_news;
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
            /* TODO
            filter only {street, housenumber, complement} gemoetries
            LAPOSTE/RAN doesn't supply geometry for complement
             */
            SELECT
                co_cea code_address
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
                END kind
                , gm_coord geom
            FROM fr.laposte_xy
            WHERE
                gm_coord IS NOT NULL
        )
        , changes AS (
            (
                SELECT '-' change, code_address FROM xy_public
                EXCEPT
                SELECT '-', code_address FROM xy_fr
            )
            UNION
            (
                SELECT '+', code_address FROM xy_fr
                EXCEPT
                SELECT '+', code_address FROM xy_public
            )
            UNION
            SELECT '!', xy_public.code_address
            FROM xy_public
                JOIN xy_fr ON xy_public.code_address = xy_fr.code_address
            WHERE
                (xy_public.kind IS DISTINCT FROM xy_fr.kind)
                OR
                (NOT ST_Equals(xy_public.geom, xy_fr.geom))
        )

        -- insert/update addresses
        SELECT
            c.change
            , c.code_address
            , xy_fr.kind
            , xy_fr.geom
        FROM
            changes c
                JOIN xy_fr ON c.code_address = xy_fr.code_address
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
            , c.kind
        FROM
            tmp_fr_xy_changes c
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
        WHERE
            c.change = '-'
    )
    DELETE FROM public.address_xy xy
        USING xy_deletes d
        WHERE
            xy.id_address = d.id_address
            AND
            xy.kind = c.kind
            AND
            xy.source = 'LAPOSTE'
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
    , drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
BEGIN
    -- dictionaries (items of an address)
    CALL fr.push_dictionary_street_to_public(force, drop_temporary);
    CALL fr.push_dictionary_housenumber_to_public(force, drop_temporary);
    CALL fr.push_dictionary_complement_to_public(force, drop_temporary);
    -- addresses (w/ cross reference to store LAPOSTE id, as CEA)
    CALL fr.push_address_links_to_public(force, drop_temporary);
    -- XY
    CALL fr.push_address_xy_to_public(force, drop_temporary);
END
$proc$ LANGUAGE plpgsql;
