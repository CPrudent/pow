/***
 * FR-ADDRESS management
 */

/* NOTE
PART 1
 update dictionaries (street, housenumber and complement), logging address_history
PART 2
 update by descending order
    address (w/ links to dictionaries)
    cross reference (to put LAPOSTE id, well-known as CEA)
PART 3
 update XY
 */

/* NOTE
no delete applied on dictionaries. It will be necessary to known usage of this item (notion of counts of reference: equal to 1, ok to delete else no!)
a specific job could be built to purge item from dictionary, which not have usage!
 */

/* NOTE
can't commit dictionary, while address not inserted
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

    /* TODO
    be careful, if run again this procedure, but not the address part
    dictionary 'public.address_street' will be inserted twice!
    a solution is to drop it before (and so reset sequence too), but not compliant
    w/ many countries!
     */

    CALL public.log_info('Préparation des changements');
    DROP TABLE IF EXISTS tmp_fr_street_changes;
    CREATE TEMPORARY TABLE tmp_fr_street_changes AS (
        WITH
        public_street AS (
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
        , fr_street AS (
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
            -- except MONACO
            WHERE fl_active AND co_insee_commune != '99138'
            GROUP BY
                lb_voie
        )
        , changes AS (
            (
                SELECT '-' change, name FROM public_street
                EXCEPT
                SELECT '-', name FROM fr_street
            )
            UNION
            (
                SELECT '+', name FROM fr_street
                EXCEPT
                SELECT '+', name FROM public_street
            )
            UNION
            SELECT '!', public_street.name
            FROM public_street
                JOIN fr_street ON public_street.name = fr_street.name
            WHERE
                (public_street.name_normalized IS DISTINCT FROM fr_street.name_normalized)
                OR
                (public_street.typeof IS DISTINCT FROM fr_street.typeof[1])
                OR
                (public_street.descriptors IS DISTINCT FROM fr_street.descriptors[1])
        )

        -- insert/update
        SELECT
            c.change
            , fr_street.name
            , fr_street.name_normalized
            , fr_street.typeof[1] typeof
            , fr_street.descriptors[1] descriptors
        FROM
            changes c
                JOIN fr_street ON c.name = fr_street.name
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete
        SELECT
            c.change
            , public_street.name
            , public_street.name_normalized
            , public_street.typeof
            , public_street.descriptors
        FROM
            changes c
                JOIN public_street ON c.name = public_street.name
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
        public_housenumber AS (
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
        , fr_housenumber AS (
            SELECT DISTINCT
                no_voie number
                , lb_ext extension
            FROM fr.laposte_housenumber
            WHERE fl_active
        )
        , changes AS (
            (
                SELECT '-' change, number, extension FROM public_housenumber
                EXCEPT
                SELECT '-', number, extension FROM fr_housenumber
            )
            UNION
            (
                SELECT '+', number, extension FROM fr_housenumber
                EXCEPT
                SELECT '+', number, extension FROM public_housenumber
            )
        )

        -- insert
        SELECT
            c.change
            , fr_housenumber.number
            , fr_housenumber.extension
        FROM
            changes c
                JOIN fr_housenumber ON
                (c.number, COALESCE(c.extension, 'NULL')) = (fr_housenumber.number, COALESCE(fr_housenumber.extension, 'NULL'))
        WHERE
            c.change = '+'

        UNION

        -- delete
        SELECT
            c.change
            , public_housenumber.number
            , public_housenumber.extension
        FROM
            changes c
                JOIN public_housenumber ON
                (c.number, COALESCE(c.extension, 'NULL')) = (public_housenumber.number, COALESCE(public_housenumber.extension, 'NULL'))
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
        public_complement AS (
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
        , fr_complement AS (
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
                SELECT '-' change, name FROM public_complement
                EXCEPT
                SELECT '-', name FROM fr_complement
            )
            UNION
            (
                SELECT '+', name FROM fr_complement
                EXCEPT
                SELECT '+', name FROM public_complement
            )
            UNION
            SELECT '!', public_complement.name
            FROM public_complement
                JOIN fr_complement ON public_complement.name = fr_complement.name
            WHERE
                (public_complement.name_normalized IS DISTINCT FROM fr_complement.name_normalized)
        )

        -- insert/update
        SELECT
            c.change
            , fr_complement.name
            , fr_complement.name_normalized
        FROM
            changes c
                JOIN fr_complement ON c.name = fr_complement.name
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete
        SELECT
            c.change
            , public_complement.name
            , public_complement.name_normalized
        FROM
            changes c
                JOIN public_complement ON c.name = public_complement.name
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

-- address element
SELECT drop_all_functions_if_exists('fr', 'push_address_element_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_element_to_public(
    element VARCHAR
    , table_name_to VARCHAR
    , table_name_from VARCHAR
    , simulation BOOLEAN DEFAULT FALSE
    , notice_counter INT DEFAULT 100
)
AS
$proc$
DECLARE
    _query TEXT;
    _columns_insert VARCHAR :=
        CASE element
        WHEN 'VOIE' THEN
            'id_territory'
        WHEN 'NUMERO' THEN
            'id_parent, id_territory, id_street'
        WHEN 'L3' THEN
            'id_parent, id_territory, id_street, id_housenumber'
        END;
    _columns_select VARCHAR :=
        CASE element
        WHEN 'VOIE' THEN
            't.id'
        WHEN 'NUMERO' THEN
            'a.id, t.id, a.id_street'
        WHEN 'L3' THEN
            'a.id, t.id, a.id_street, a.id_housenumber'
        END;
    _column_id VARCHAR :=
        CASE element
        WHEN 'VOIE' THEN
            'id_street'
        WHEN 'NUMERO' THEN
            'id_housenumber'
        WHEN 'L3' THEN
            'id_complement'
        END;
    _columns_id VARCHAR := 'id_territory, id_street';
    _columns_id2 VARCHAR;
    _columns_id_aliased VARCHAR;
    _columns_id2_aliased VARCHAR;
    _columns_id_array TEXT[];
    _columns_id_values TEXT;
    _source_parent VARCHAR :=
        CASE element
        WHEN 'VOIE' THEN
            NULL
        WHEN 'NUMERO' THEN
            'c.code_street'
        WHEN 'L3' THEN
            'COALESCE(c.code_housenumber, c.code_street)'
        END;
    _join_dictionary VARCHAR :=
        CASE element
        WHEN 'VOIE' THEN
            '
                s2.id
            FROM fr.laposte_street s1, public.address_street s2
            WHERE
                s1.co_cea = n.code_address
                AND
                s2.name = s1.lb_voie
            '
        WHEN 'NUMERO' THEN
            '
                hn2.id
            FROM fr.laposte_housenumber hn1, public.address_housenumber hn2
            WHERE
                hn1.co_cea = n.code_address
                AND
                (hn2.number, COALESCE(hn2.extension, ''NULL'')) = (hn1.no_voie, COALESCE(hn1.lb_ext, ''NULL''))
            '
        WHEN 'L3' THEN
            '
                c2.id
            FROM fr.laposte_complement c1, public.address_complement c2
            WHERE
                c1.co_cea = n.code_address
                AND
                c2.name = CONCAT_WS('' ''
                    , c1.lb_type_groupe1_l3
                    , c1.lb_groupe1
                    , c1.lb_type_groupe2_l3
                    , c1.lb_groupe2
                    , c1.lb_type_groupe3_l3
                    , c1.lb_groupe3
                )
            '
        END;
    _nrows_affected INT;
    _mode_multiple VARCHAR;
    _nrows INT;
    _i INT;
    _address RECORD;
    _kv RECORD;
    _id INT;
BEGIN
    _columns_insert := CONCAT(_columns_insert, ', code_address');
    _columns_select := CONCAT(_columns_select, ', c.code_address');
    IF element != 'VOIE' THEN
        _columns_id2 := _columns_id;
        _columns_id := CONCAT(_columns_id, ', id_housenumber');
        IF element != 'NUMERO' THEN
            _columns_id := CONCAT(_columns_id, ', id_complement');
            _columns_id2 := CONCAT(_columns_id2, ', id_complement');
        END IF;
    END IF;

    -- prepare addresses of element
    CALL public.log_info(CONCAT(element, ': Préparation'));
    _query := CONCAT(
        'INSERT INTO ', table_name_to, ' ('
        , _columns_insert
        , ')
        SELECT '
        , _columns_select
        , ' FROM ', table_name_from , ' c
            JOIN public.territory t ON t.code = c.code_territory AND t.level = ''ZA'' AND t.country = ''FR''
        '
    );
    -- get IDs from address
    IF _source_parent IS NOT NULL THEN
        _query := CONCAT(_query
            , 'JOIN public.address_cross_reference cr ON cr.id_source = '
            , _source_parent, ' AND cr.source = ''LAPOSTE''
                JOIN public.address a ON a.id = cr.id_address
            '
        );
    END IF;
    -- filter creation of element
    _query := CONCAT(_query
        , ' WHERE
            c.change = ''+''
            AND
            c.level = $1'
    );

    IF simulation THEN
        RAISE NOTICE 'query: %', _query;
    ELSE
        /* NOTE
        https://dba.stackexchange.com/questions/276680/passing-parameters-for-dynamic-sql-in-a-function
        _query := 'TRUNCATE TABLE $1;'; EXECUTE _query USING _table; don't work! only DML
         */
        EXECUTE FORMAT('TRUNCATE TABLE %s', table_name_to);

        EXECUTE _query USING element;
        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT(element, ': ', _nrows_affected));
    END IF;

    CALL public.log_info(CONCAT(element, ': Préparation (ID dictionnaire)'));
    _query := CONCAT(
        'UPDATE ', table_name_to, ' n SET '
        , _column_id
        , ' = '
        , _join_dictionary
    );
    IF simulation THEN
        RAISE NOTICE 'query: %', _query;
    ELSE
        EXECUTE _query;
        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT(element, ': ', _nrows_affected));
    END IF;

    /* NOTE
    detect multiple address (w/ all same ID), according to state of address
    if first time (no address of element) then find them into initial set (to add)
    else, search for equals w/ existing addresses
     */
    CALL public.log_info(CONCAT(element, ': Préparation (multiples)'));
    _query := CONCAT(
        'SELECT COUNT(*) FROM public.address a
        JOIN public.territory t ON t.id = a.id_territory
        WHERE t.country = ''FR''
        AND a.', _column_id, ' IS NOT NULL'
    );
    EXECUTE _query INTO _nrows;
    _columns_id_aliased := alias_words(_columns_id, ',[ ]*', 'n');
    -- address already initiated ?
    IF _nrows > 0 THEN
        _mode_multiple := 'DELTA';
        _query := CONCAT(
            'UPDATE ', table_name_to, ' n
                SET uniq = FALSE
            FROM
                public.address a
            WHERE (', _columns_id_aliased, ')
                IS NOT DISTINCT FROM
                (', alias_words(_columns_id, ',[ ]*', 'a'), ')'
        );
    ELSE
        _mode_multiple := 'INIT';
        _query := CONCAT(
            'WITH
            namesake_addresses AS (
                SELECT ', _columns_id, '
                FROM ', table_name_to, '
                GROUP BY ', _columns_id, '
                HAVING COUNT(*) > 1
            )
            UPDATE ', table_name_to, ' n
                SET uniq = FALSE
            FROM namesake_addresses na
            WHERE
                (', _columns_id_aliased, ')
                IS NOT DISTINCT FROM
                (', alias_words(_columns_id, ',[ ]*', 'na'), ')'
        );
    END IF;

    IF simulation THEN
        RAISE NOTICE 'query: %', _query;
    ELSE
        EXECUTE _query;
        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT(element, ': ', _nrows_affected, ' (mode ', _mode_multiple, ')'));
    END IF;

    /* NOTE
    be careful about element w/ same IDs (and same territory), namesake!
    so insert new addresses in 2 parts:
    1/ uniq
    2/ multiple
       have to insert row per row (to obtain uniq id address)

    multiples (street: 123, housenumber: 345, complement: 621)
    248 streets (w/ same name & territory)
        <CHEMIN DE MOLAND>      010722249B
        <PLACE DE LA FONTAINE>  0402322266

    691 housenumbers (w/ same number/extension, street & territory)
        <35> RUE DE L EGLISE 30190 SAINTE ANASTASIE           {30228222LN, 30228222LH}

    1307 complements (w/ same name, [housenumber], street & territory)
        <ENTREE 7 RESIDENCE ARDENNES>       2   RUE DU MARECHAL JOFFRE          5901722NTP
        <BATIMENT 1 RESIDENCE DE FRANCE>        RUE DU GENERAL DE GAULLE        9401922AGL
            remark: this last example has another occurence, on housenumber 26!
     */

    -- Part/1 uniq address
    CALL public.log_info(CONCAT(element, ': Adresses (uniques)'));
    _columns_id_aliased := alias_words(_columns_id, ',[ ]*', 'n');
    _query := CONCAT(
        'INSERT INTO public.address ( '
        , CONCAT_WS(',', CASE WHEN element != 'VOIE' THEN 'id_parent' END, _columns_id)
        , ' ) SELECT '
        , CONCAT_WS(',', CASE WHEN element != 'VOIE' THEN 'n.id_parent' END, _columns_id_aliased)
        , ' FROM ', table_name_to, ' n
        WHERE
            n.uniq'
    );
    IF simulation THEN
        RAISE NOTICE 'query: %', _query;
    ELSE
        EXECUTE _query;
        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT(element, ': ', _nrows_affected));
    END IF;

    CALL public.log_info(CONCAT(element, ': Références (uniques)'));
    /* NOTE
    joining address w/ 'IS NOT DISTINCT FROM' degrades performance, back to equal!
    BUT
    have to accommodate usecase of complement's parent (housenumber OR street)
    can't write (ids) = (ids) due to NULL housenumber possibility
     */
    _query := CONCAT(
        'INSERT INTO public.address_cross_reference (
            id_address
            , source
            , id_source
        )
        SELECT
            a.id
            , ''LAPOSTE''
            , n.code_address
        FROM ', table_name_to, ' n
            JOIN public.address a ON
                (', _columns_id_aliased, ')
                =
                (', alias_words(_columns_id, ',[ ]*', 'a'), ')
        WHERE
            n.uniq'
    );
    IF simulation THEN
        RAISE NOTICE 'query: %', _query;
    ELSE
        EXECUTE _query;
        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT(element, CASE WHEN element = 'L3' THEN '(NUMERO): ' ELSE ': ' END, _nrows_affected));
    END IF;

    IF element = 'L3' THEN
        _columns_id2_aliased := alias_words(_columns_id2, ',[ ]*', 'n');
        _query := CONCAT(
            'INSERT INTO public.address_cross_reference (
                id_address
                , source
                , id_source
            )
            SELECT
                a.id
                , ''LAPOSTE''
                , n.code_address
            FROM ', table_name_to, ' n
                JOIN public.address a ON
                    (', _columns_id2_aliased, ')
                    =
                    (', alias_words(_columns_id2, ',[ ]*', 'a'), ')
            WHERE
                n.uniq
                AND
                n.id_housenumber IS NULL'
        );
        IF simulation THEN
            RAISE NOTICE 'query: %', _query;
        ELSE
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
            CALL public.log_info(CONCAT(element, '(VOIE): ', _nrows_affected));
        END IF;
    END IF;

    -- Part/2 multiple address
    CALL public.log_info(CONCAT(element, ': Adresses/Références (multiples)'));
    _nrows_affected := 0;
    _columns_id_array := REGEXP_SPLIT_TO_ARRAY(
        CONCAT_WS(', ', CASE WHEN element != 'VOIE' THEN 'id_parent' END, _columns_id)
        , ',[ ]*'
    );
    -- https://stackoverflow.com/questions/20965882/for-loop-with-dynamic-table-name-in-postgresql-9-1
    FOR _address IN EXECUTE FORMAT('SELECT * FROM %s WHERE NOT uniq', table_name_to)
    LOOP
        -- https://dba.stackexchange.com/questions/52826/insert-values-from-a-record-variable-into-a-table
        _columns_id_values := NULL;
        FOR _i IN 1..ARRAY_LENGTH(_columns_id_array, 1)
        LOOP
            FOR _kv IN SELECT * FROM EACH(HSTORE(_address)) LOOP
                IF _kv.key != _columns_id_array[_i] THEN CONTINUE; END IF;
                IF _columns_id_array @> ARRAY[_kv.key] THEN
                    _columns_id_values := CONCAT_WS(','
                        , _columns_id_values
                        , quote_nullable(_kv.value)
                    );
                    EXIT;
                END IF;
            END LOOP;
         END LOOP;

        _query := CONCAT(
            'INSERT INTO public.address ('
            , CONCAT_WS(',', CASE WHEN element != 'VOIE' THEN 'id_parent' END, _columns_id)
            , ') VALUES (', _columns_id_values, ')
            RETURNING id'
        );
        IF simulation THEN
            RAISE NOTICE 'query: %', _query;
        ELSE
            EXECUTE _query INTO _id;

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
            IF _nrows_affected % notice_counter = 0 THEN
                CALL public.log_info(CONCAT(element, ': ', _nrows_affected));
            END IF;
        END IF;
    END LOOP;
    CALL public.log_info(CONCAT(element, ': ', _nrows_affected));
END
$proc$ LANGUAGE plpgsql;

-- push elements of address (as changes) to public
SELECT drop_all_functions_if_exists('fr', 'push_address_elements_to_public');
CREATE OR REPLACE PROCEDURE fr.push_address_elements_to_public(
    force BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
    , part_todo INT DEFAULT 1 | 2 | 4 | 8 | 16
)
AS
$proc$
DECLARE
    _elements VARCHAR[] := ARRAY['VOIE', 'NUMERO', 'L3'];
    _element VARCHAR;
    _nrows_affected INT;
BEGIN
    CALL public.log_info('Mise à jour des Adresses');

    IF part_todo & 1 = 1 THEN
        CALL public.log_info('Préparation des changements');
        DROP TABLE IF EXISTS fr.tmp_address_changes;
        CREATE UNLOGGED TABLE fr.tmp_address_changes AS (
            WITH
            public_address AS (
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
            , fr_address AS (
                SELECT
                    a.co_cea_determinant code_address
                    , a.co_niveau level
                    , CASE WHEN a.co_niveau = 'VOIE' THEN NULL ELSE a.co_cea_parent END code_parent
                    , a.co_cea_za code_territory
                    , a.co_cea_voie code_street
                    , a.co_cea_numero code_housenumber
                    , a.co_cea_l3 code_complement
                FROM fr.laposte_address a
                    JOIN fr.laposte_zone_address za ON za.co_cea = a.co_cea_za
                WHERE
                    a.fl_active
                    AND
                    -- w/o ZA (CEA-voie defined)
                    /* NOTE
                    62 faults in LAPOSTE/RAN data, as address w/o street occurence!
                    to avoid them, add a join
                    */
                    EXISTS (
                        SELECT 1 FROM fr.laposte_street s WHERE s.co_cea = a.co_cea_voie
                    )
                    AND
                    -- except MONACO
                    za.co_insee_commune != '99138'
            )
            , changes AS (
                (
                    SELECT '-' change, code_address FROM public_address
                    EXCEPT
                    SELECT '-', code_address FROM fr_address
                )
                UNION
                (
                    SELECT '+', code_address FROM fr_address
                    EXCEPT
                    SELECT '+', code_address FROM public_address
                )
                UNION
                SELECT '!', public_address.code_address
                FROM public_address
                    JOIN fr_address ON public_address.code_address = fr_address.code_address
                WHERE
                    (public_address.code_parent IS DISTINCT FROM fr_address.code_parent)
                    OR
                    (public_address.code_territory IS DISTINCT FROM fr_address.code_territory)
                    OR
                    (public_address.code_street IS DISTINCT FROM fr_address.code_street)
                    OR
                    (public_address.code_housenumber IS DISTINCT FROM fr_address.code_housenumber)
                    OR
                    (public_address.code_complement IS DISTINCT FROM fr_address.code_complement)
            )

            -- insert/update
            SELECT
                c.change
                , fr_address.level
                , c.code_address
                , fr_address.code_parent
                , fr_address.code_territory
                , fr_address.code_street
                , fr_address.code_housenumber
                , fr_address.code_complement
            FROM
                changes c
                    JOIN fr_address ON c.code_address = fr_address.code_address
            WHERE
                c.change = ANY('{+,!}')

            UNION

            -- delete
            SELECT
                c.change
                , public_address.level
                , c.code_address
                , public_address.code_parent
                , public_address.code_territory
                , public_address.code_street
                , public_address.code_housenumber
                , public_address.code_complement
            FROM
                changes c
                    JOIN public_address ON c.code_address = public_address.code_address
            WHERE
                c.change = '-'
        )
        ;
        GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
        CALL public.log_info(CONCAT('Total: ', _nrows_affected));
        ALTER TABLE fr.tmp_address_changes SET (autovacuum_enabled = FALSE);

        CALL public.log_info('Indexation des changements');
        CREATE INDEX IF NOT EXISTS ix_tmp_fr_address_changes ON fr.tmp_address_changes(change, level);
    END IF;

    IF part_todo & 2 = 2 THEN
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
    END IF;

    IF part_todo & 4 = 4 THEN
        /* NOTE
        have to add addresses in descending order, because of parent id
        */
        CALL public.log_info('Mise à jour des ajouts');
        DROP TABLE IF EXISTS fr.tmp_address_news;
        CREATE UNLOGGED TABLE fr.tmp_address_news AS
            SELECT * FROM public.address WITH NO DATA;
        ALTER TABLE fr.tmp_address_news
            ADD COLUMN code_address VARCHAR
            , ADD COLUMN uniq BOOLEAN DEFAULT TRUE;
        ALTER TABLE fr.tmp_address_news DROP COLUMN id;
        ALTER TABLE fr.tmp_address_news SET (autovacuum_enabled = FALSE);

        -- dictionaries (items of an address) and addresses
        FOREACH _element IN ARRAY _elements
        LOOP
            --RAISE NOTICE 'element: %', _element;
            EXECUTE FORMAT('CALL fr.push_dictionary_%s_to_public($1, $2)'
                , CASE _element
                WHEN 'VOIE' THEN 'street'
                WHEN 'NUMERO' THEN 'housenumber'
                WHEN 'L3' THEN 'complement'
                END
                )
                USING force, drop_temporary;
            CALL fr.push_address_element_to_public(
                element => _element
                , table_name_to => 'fr.tmp_address_news'
                , table_name_from => 'fr.tmp_address_changes'
            );
        END LOOP;
    END IF;

    IF part_todo & 8 = 8 THEN
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
    END IF;

    IF part_todo & 16 = 16 THEN
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
    END IF;

    IF drop_temporary THEN
        DROP TABLE IF EXISTS fr.tmp_address_changes;
        DROP TABLE IF EXISTS fr.tmp_address_news;
        DROP TABLE IF EXISTS fr.tmp_address_news_m;
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
    DROP TABLE IF EXISTS fr.tmp_xy_changes;
    CREATE UNLOGGED TABLE fr.tmp_xy_changes AS (
        WITH
        public_xy AS (
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
        , fr_xy AS (
            /* TODO
            filter only {street, housenumber, complement} gemoetries
            LAPOSTE/RAN doesn't supply geometry for complement (and no more for ZA)
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
                SELECT '-' change, code_address FROM public_xy
                EXCEPT
                SELECT '-', code_address FROM fr_xy
            )
            UNION
            (
                SELECT '+', code_address FROM fr_xy
                EXCEPT
                SELECT '+', code_address FROM public_xy
            )
            UNION
            SELECT '!', public_xy.code_address
            FROM public_xy
                JOIN fr_xy ON public_xy.code_address = fr_xy.code_address
            WHERE
                (public_xy.kind IS DISTINCT FROM fr_xy.kind)
                OR
                (NOT ST_Equals(public_xy.geom, fr_xy.geom))
        )

        -- insert/update addresses
        SELECT
            c.change
            , c.code_address
            , fr_xy.kind
            , fr_xy.geom
        FROM
            changes c
                JOIN fr_xy ON c.code_address = fr_xy.code_address
        WHERE
            c.change = ANY('{+,!}')

        UNION

        -- delete old addresses
        SELECT
            c.change
            , c.code_address
            , public_xy.kind
            , public_xy.geom
        FROM
            changes c
                JOIN public_xy ON c.code_address = public_xy.code_address
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
            fr.tmp_xy_changes c
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
            fr.tmp_xy_changes c
                JOIN public.address_cross_reference cr1 ON cr1.id_source = c.code_address AND cr1.source = 'LAPOSTE'
        WHERE
            c.change = '-'
    )
    DELETE FROM public.address_xy xy
        USING xy_deletes d
        WHERE
            xy.id_address = d.id_address
            AND
            xy.kind = d.kind
            AND
            xy.source = 'LAPOSTE'
    ;
    GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
    CALL public.log_info(CONCAT('Total: ', _nrows_affected));
    --COMMIT;

    IF drop_temporary THEN
        DROP TABLE IF EXISTS fr.tmp_xy_changes;
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
    -- addresses (w/ cross reference to store LAPOSTE id, as CEA)
    CALL fr.push_address_elements_to_public(force, drop_temporary);
    -- XY
    CALL fr.push_address_xy_to_public(force, drop_temporary);
END
$proc$ LANGUAGE plpgsql;
