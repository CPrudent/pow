/***
 * add IO
 */

CREATE TABLE IF NOT EXISTS public.io_list (
    id SERIAL NOT NULL,
    name VARCHAR NOT NULL
);

-- create IO list indexes
SELECT drop_all_functions_if_exists('public', 'set_io_list_index');
CREATE OR REPLACE PROCEDURE public.set_io_list_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS uix_io_list_id ON public.io_list(id);
    CREATE UNIQUE INDEX IF NOT EXISTS uix_io_list_name ON public.io_list(name);
END
$proc$ LANGUAGE plpgsql;

-- add IO if not exists
SELECT public.drop_all_functions_if_exists('public', 'io_add_if_not_exists');
CREATE OR REPLACE PROCEDURE public.io_add_if_not_exists(
    name VARCHAR
)
AS
$proc$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.io_list l WHERE l.name = io_add_if_not_exists.name LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES (io_add_if_not_exists.name);
    END IF;
END
$proc$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS public.io_relation (
    id INT NOT NULL,
    id_child INT NULL
);

SELECT drop_all_functions_if_exists('public', 'set_io_relation_index');
CREATE OR REPLACE PROCEDURE public.set_io_relation_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS uix_io_relation_ids ON public.io_relation(id, id_child);
END
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_add_relation_if_not_exists');
CREATE OR REPLACE PROCEDURE public.io_add_relation_if_not_exists(
    id1 INT,
    id2 INT
)
AS
$proc$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.io_relation WHERE id = id1 AND id_child = id2 LIMIT 1) THEN
        INSERT INTO public.io_relation(id, id_child) VALUES (id1, id2);
    END IF;
END
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_get_subscript_from_array_by_name');
SELECT public.drop_all_functions_if_exists('public', 'io_get_id_from_array_by_name');
CREATE OR REPLACE FUNCTION public.io_get_id_from_array_by_name(
    from_array public.io_list[],
    name VARCHAR
)
RETURNS INT AS
$func$
DECLARE
    _id INT := 0;
    _i INT;
BEGIN
    IF ARRAY_UPPER(from_array, 1) IS NULL THEN RETURN 0; END IF;

    FOR _i IN 1 .. ARRAY_UPPER(from_array, 1) LOOP
        IF from_array[_i].name = name THEN
            _id := from_array[_i].id;
            EXIT;
        END IF;
    END LOOP;

    RETURN _id;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_get_subscript_from_array_by_name');
CREATE OR REPLACE FUNCTION public.io_get_subscript_from_array_by_name(
    from_array public.io_history[],
    name VARCHAR
)
RETURNS INT AS
$func$
DECLARE
    _id INT := 0;
    _i INT;
BEGIN
    IF ARRAY_UPPER(from_array, 1) IS NULL THEN RETURN 0; END IF;

    FOR _i IN 1 .. ARRAY_UPPER(from_array, 1) LOOP
        IF from_array[_i].name = name THEN
            _id := _i;
            EXIT;
        END IF;
    END LOOP;

    RETURN _id;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_has_relation');
CREATE OR REPLACE FUNCTION public.io_has_relation(
    name VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query TEXT;
    _has BOOLEAN;
BEGIN
    _query := FORMAT('SELECT EXISTS(
        SELECT 1 FROM public.io_relation
        WHERE id = (SELECT id FROM public.io_list WHERE name = ''%s''))', name);

    EXECUTE _query INTO _has;
    RETURN _has;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_with_difference_query');
CREATE OR REPLACE FUNCTION public.io_with_difference_query(
    name VARCHAR
)
RETURNS TEXT AS
$func$
DECLARE
    _query TEXT;
    _last_io TIMESTAMP;
BEGIN
    IF name = 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY' THEN
        _last_io := (public.get_last_io(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT')).date_data_end;
    ELSIF name = 'FR-TERRITORY-IGN-EVENT' THEN
        _last_io := (public.get_last_io(name => 'FR-TERRITORY-IGN')).date_data_end;
    ELSIF name = 'FR-TERRITORY-INSEE-EVENT' THEN
        _last_io := (public.get_last_io(name => 'FR-TERRITORY-INSEE')).date_data_end;
    ELSIF name = 'FR-TERRITORY-LAPOSTE-AREA-EVENT' THEN
        _last_io := (public.get_last_io(name => 'FR-TERRITORY-LAPOSTE-AREA')).date_data_end;
    END IF;

    _query := CASE name
        WHEN 'FR-TERRITORY-IGN-MUNICIPALITY' THEN
            '
                (
                    SELECT
                        insee_com AS codgeo,
                        nom AS libgeo
                    FROM
                        fr.ign_municipality
                    WHERE
                        insee_com NOT IN (''75056'', ''13055'', ''69123'')
                    UNION
                    SELECT
                        insee_arm,
                        nom
                    FROM
                        fr.ign_municipal_district
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        codgeo,
                        libgeo
                    FROM
                        fr.territory
                    WHERE
                        nivgeo = ''COM''
                        AND
                        codgeo !~ ''^(98|97[578])''
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                (
                    x.codgeo IS NULL
                    OR
                    t.codgeo IS NULL
                    OR
                    x.libgeo IS DISTINCT FROM t.libgeo
                )
            '
        WHEN 'FR-TERRITORY-IGN-MUNICIPALITY-POPULATION' THEN
            '
                (
                    SELECT
                        insee_com AS codgeo,
                        population
                    FROM
                        fr.ign_municipality
                    WHERE
                        insee_com NOT IN (''75056'', ''13055'', ''69123'')
                    UNION
                    SELECT
                        insee_arm,
                        population
                    FROM
                        fr.ign_municipal_district
                ) x

                JOIN

                (
                    SELECT
                        codgeo,
                        population
                    FROM
                        fr.territory
                    WHERE
                        nivgeo = ''COM''
                        AND
                        codgeo !~ ''^(98|97[578])''
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                x.population != t.population
            '
        WHEN 'FR-TERRITORY-IGN-GEOMETRY' THEN
            '
                (
                    SELECT
                        insee_com AS codgeo,
                        geom
                    FROM
                        fr.ign_municipality
                    WHERE
                        insee_com NOT IN (''75056'', ''13055'', ''69123'')
                    UNION
                    SELECT
                        insee_arm,
                        geom
                    FROM
                        fr.ign_municipal_district
                ) x

                JOIN

                (
                    SELECT
                        codgeo,
                        gm_contour_natif
                    FROM
                        fr.territory
                    WHERE
                        nivgeo = ''COM''
                        AND
                        codgeo !~ ''^(98|97[578])''
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                NOT ST_Equals_with_Threshold(x.geom, t.gm_contour_natif)
            '
        WHEN 'FR-TERRITORY-IGN-EVENT' THEN
            CONCAT(
                '
                    (
                        SELECT
                            insee_com
                        FROM
                            fr.ign_municipality
                        WHERE
                            insee_com NOT IN (''75056'', ''13055'', ''69123'')
                        UNION
                        SELECT
                            insee_arm
                        FROM
                            fr.ign_municipal_district
                    ) ign
                        CROSS JOIN fr.get_municipality_to_date(
                            code => ign.insee_com,
                            code_previous => ign.insee_com,
                            date_geography_from => ''',
                _last_io,
                '''::DATE,
                            with_deleted => TRUE,
                            check_exists => FALSE
                        ) to_now
                WHERE
                    to_now.date_geography != ''',
                _last_io,
                '''::DATE
                '
            )
        WHEN 'FR-TERRITORY-INSEE-MUNICIPALITY' THEN
            '
                (
                    SELECT
                        codgeo
                    FROM
                        fr.insee_municipality
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        codgeo
                    FROM
                        fr.territory
                    WHERE
                        nivgeo = ''COM''
                        AND
                        codgeo !~ ''^(98|97[578])''
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                (
                    x.codgeo IS NULL
                    OR
                    t.codgeo IS NULL
                )
            '
        WHEN 'FR-TERRITORY-INSEE-SUPRA' THEN
            '
                (
                    SELECT
                        nivgeo,
                        codgeo,
                        libgeo
                    FROM
                        fr.insee_supra
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        nivgeo,
                        codgeo,
                        libgeo
                    FROM
                        fr.territory
                    WHERE
                        nivgeo ~ ''COM_GLOBALE_ARM|ARR|CV|DEP|REG''
                        AND
                        codgeo !~ ''^(98|97[578])''
                        AND
                        codgeo != ''97''
                ) t

                ON (x.nivgeo, x.codgeo) = (t.nivgeo, t.codgeo)
            WHERE
                (
                    x.codgeo IS NULL
                    OR
                    t.codgeo IS NULL
                    OR
                    x.libgeo IS DISTINCT FROM t.libgeo
                )
            '
        WHEN 'FR-TERRITORY-INSEE-EVENT' THEN
            CONCAT(
                '
                    fr.insee_municipality insee
                        CROSS JOIN fr.get_municipality_to_date(
                            code => insee.codgeo,
                            code_previous => insee.codgeo,
                            date_geography_from => ''',
                _last_io,
                '''::DATE,
                            with_deleted => TRUE,
                            check_exists => FALSE
                        ) to_now
                WHERE
                    to_now.date_geography != ''',
                _last_io,
                '''::DATE
                '
            )
        WHEN 'FR-TERRITORY-BANATIC-LIST' THEN
            '
                (
                    SELECT
                        siren_epci codgeo,
                        nom_complet libgeo,
                        nature_juridique typgeo
                    FROM
                        fr.gouv_epci
                    WHERE
                        nature_juridique IN (''MET69'', ''CC'', ''CA'', ''METRO'', ''CU'')
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        codgeo,
                        libgeo,
                        typgeo
                    FROM
                        fr.territory
                    WHERE
                        nivgeo = ''EPCI''
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                (
                    x.codgeo IS NULL
                    OR
                    t.codgeo IS NULL
                    OR
                    x.libgeo IS DISTINCT FROM t.libgeo
                )
            '
        WHEN 'FR-TERRITORY-BANATIC-SET' THEN
            -- w/ district and w/o global municipality
            '
                (
                    SELECT
                        em.siren codgeo_epci,
                        m.codgeo codgeo_com
                    FROM
                        fr.gouv_epci_municipality em
                            JOIN fr.insee_municipality m ON em.insee = COALESCE(m.com, m.codgeo)
                    WHERE
                        em.nature_juridique IN (''MET69'', ''CC'', ''CA'', ''METRO'', ''CU'')
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        t.codgeo_epci_parent code_epci,
                        t.codgeo code_com
                    FROM
                        fr.territory t
                    WHERE
                        t.nivgeo = ''COM''
                        AND
                        -- exclude 9[789] and 4 islands (last ones)
                        --t.codgeo !~ ''^(97[578]|9[89]|29083|29155|22016|85113)''
                        t.codgeo_epci_parent IS NOT NULL
                ) t

                ON (x.codgeo_epci, x.codgeo_com) = (t.code_epci, t.code_com)
            WHERE
                (
                    (x.codgeo_epci IS NULL OR x.codgeo_com IS NULL)
                    OR
                    (t.code_epci IS NULL OR t.code_com IS NULL)
                )
            '
        WHEN 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL' THEN
            '
                (
                    SELECT
                        co_cea codgeo
                    FROM
                        fr.laposte_address_area
                    WHERE
                        fl_active
                        AND
                        -- exclude MONACO
                        co_insee_commune !~ ''^99''
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        codgeo
                    FROM
                        fr.territory
                    WHERE
                        nivgeo = ''ZA''
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                (
                    x.codgeo IS NULL
                    OR
                    t.codgeo IS NULL
                )
            '
        WHEN 'FR-TERRITORY-LAPOSTE-AREA-UPD' THEN
            '
                (
                    SELECT
                        co_cea codgeo,
                        co_insee_commune codgeo_com,
                        CASE WHEN co_insee_commune ~ ''^98[78]'' AND lb_l5_nn IS NOT NULL THEN lb_ach_nn ELSE lb_l5_nn END libgeo_l5,
                        CASE WHEN co_insee_commune ~ ''^98[78]'' THEN COALESCE(lb_l5_nn, lb_ach_nn) ELSE lb_ach_nn END libgeo_l6,
                        co_postal
                    FROM
                        fr.laposte_address_area
                    WHERE
                        fl_active
                        AND
                        -- exclude MONACO
                        co_insee_commune !~ ''^99''
                ) x

                JOIN

                (
                    WITH
                    l5_cp_l6 AS (
                        SELECT
                            codgeo codgeo_za,
                            codgeo_com_parent codgeo_com,
                            codgeo_cp_parent codgeo_cp,
                            STRING_TO_ARRAY(libgeo, ''-'') libs
                        FROM
                            fr.territory
                        WHERE
                            nivgeo = ''ZA''
                    )
                    SELECT
                        codgeo_za,
                        codgeo_com,
                        codgeo_cp,
                        CASE ARRAY_LENGTH(libs, 1)
                            WHEN 2 THEN libs[2]
                            WHEN 3 THEN libs[3]
                            END libgeo_l6,
                        CASE ARRAY_LENGTH(libs, 1)
                            WHEN 2 THEN NULL
                            WHEN 3 THEN libs[1]
                            END libgeo_l5
                    FROM
                        l5_cp_l6
                ) t

                ON x.codgeo = t.codgeo_za
            WHERE
                (
                    x.codgeo_com IS DISTINCT FROM t.codgeo_com
                    OR
                    x.co_postal IS DISTINCT FROM t.codgeo_cp
                    OR
                    x.libgeo_l6 IS DISTINCT FROM t.libgeo_l6
                    OR
                    x.libgeo_l5 IS DISTINCT FROM t.libgeo_l5
                )
            '
        WHEN 'FR-TERRITORY-LAPOSTE-AREA-EVENT' THEN
            CONCAT(
                '
                    fr.laposte_address_area area
                        CROSS JOIN fr.get_municipality_to_date(
                            code => area.co_insee_commune,
                            code_previous => COALESCE(area.co_insee_commune_precedente, area.co_insee_commune),
                            date_geography_from => ''',
                _last_io,
                '''::DATE,
                            with_deleted => TRUE,
                            check_exists => FALSE
                        ) to_now
                WHERE
                    area.fl_active
                    AND
                    to_now.date_geography != ''',
                _last_io,
                '''::DATE
                '
            )
        /* NOTE never difference!
                moreover it has relation, so depends of prerequisites
        WHEN 'FR-TERRITORY-LAPOSTE-SUPRA' THEN
            '
                (
                    SELECT
                        codgeo,
                        NULLIF(codgeo_pdc_ppdc_parent, codgeo) codgeo_pdc_ppdc_parent,
                        NULLIF(codgeo_ppdc_pdc_parent, codgeo) codgeo_ppdc_pdc_parent,
                        CASE WHEN nivgeo = ''DEX'' THEN codgeo ELSE codgeo_dex_parent END codgeo_dex_parent
                    FROM
                        fr.territory_laposte
                    WHERE
                        -- fix bug (see #45 3rd usecase)
                        (
                            nivgeo = ''CP''
                            AND
                            codgeo !~ ''^9[78]''
                        )
                        OR	(
                            nivgeo = ''PDC_PPDC''
                            AND
                            NOT codgeo = ANY(''{A19500,A19490,A19497,A19503,A19492,A75042,A19494}'')
                        )
                        OR (
                            nivgeo = ''PPDC_PDC''
                            AND
                            NOT codgeo = ANY(''{A75042}'')
                        )
                        OR (
                            nivgeo = ''DEX''
                        )
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        codgeo,
                        NULLIF(codgeo_pdc_ppdc_parent, codgeo) codgeo_pdc_ppdc_parent,
                        NULLIF(codgeo_ppdc_pdc_parent, codgeo) codgeo_ppdc_pdc_parent,
                        CASE WHEN nivgeo = ''DEX'' THEN codgeo ELSE codgeo_dex_parent END codgeo_dex_parent
                    FROM
                        fr.territory
                    WHERE
                        -- fix bug (see #45 3rd usecase)
                        ((
                                nivgeo = ''CP''
                                AND
                                codgeo !~ ''^9[78]''
                                -- fix bug (see #45 4th usecase)
                                AND
                                COALESCE(codgeo_pdc_ppdc_parent, codgeo_ppdc_pdc_parent, codgeo_dex_parent) IS NOT NULL
                            )
                            OR (
                                nivgeo = ''PDC_PPDC''
                                AND
                                NOT codgeo = ANY(''{A19500,A19490,A19497,A19503,A19492,A75042,A19494}'')
                            )
                            OR (
                                nivgeo = ''PPDC_PDC''
                                AND
                                NOT codgeo = ANY(''{A75042}'')
                            )
                            OR (
                                nivgeo = ''DEX''
                            )
                        )
                ) t

                ON x.codgeo = t.codgeo
            WHERE
                -- level
                (
                    x.codgeo IS NULL
                    OR
                    t.codgeo IS NULL
                )
                -- SUPRA
                OR
                (
                    x.codgeo_pdc_ppdc_parent IS DISTINCT FROM t.codgeo_pdc_ppdc_parent
                    OR
                    x.codgeo_ppdc_pdc_parent IS DISTINCT FROM t.codgeo_ppdc_pdc_parent
                    OR
                    x.codgeo_dex_parent IS DISTINCT FROM t.codgeo_dex_parent
                )
            '
         */
        WHEN 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY' THEN
            CONCAT(
                '
                    fr.delivery_point_view p
                        JOIN fr.territory t ON p.co_adr_za = t.codgeo
                WHERE
                    t.nivgeo = ''ZA''
                    AND
                    -- new point from last IO
                    p.pdi_dt_modification > ''',
                _last_io,
                '''::TIMESTAMP
                    AND
                    -- valid point
                    p.fl_active AND p.fl_diffusable AND p.pdi_etat = 1 AND p.pdi_visible
                    -- at least street-center (=4)
                    AND p.pdi_no_type_localisation_coord >= 4
                    -- any valid point apart from existing geometry ?
                    -- w/ DIMM : intersect(interior point, exterior area) = point (dim=0)
                    AND ST_Relate(ST_Transform(p.pdi_coord, 4326), t.gm_contour, ''**0******'')
                '
            )
    END CASE;

    RETURN _query;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_with_difference_exists');
CREATE OR REPLACE FUNCTION public.io_with_difference_exists(
    name VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query TEXT;
    _with BOOLEAN;
BEGIN
    _query := public.io_with_difference_query(name);
    IF _query IS NOT NULL THEN
        _query := CONCAT(
            '
            SELECT EXISTS(
                SELECT 1
                FROM
            ',
            _query,
            ')'
        );
        EXECUTE _query INTO _with;
    ELSE
        -- always true (if this IO is more recent)
        _with := TRUE;
    END IF;

    RETURN _with;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'io_with_difference_count');
CREATE OR REPLACE FUNCTION public.io_with_difference_count(
    name VARCHAR
)
RETURNS INT AS
$func$
DECLARE
    _query TEXT;
    _count INT;
BEGIN
    _query := public.io_with_difference_query(name);
    IF _query IS NOT NULL THEN
        _query := CONCAT(
            '
            SELECT COUNT(*)
            FROM
            ',
            _query
        );
        EXECUTE _query INTO _count;
    ELSE
        _count := 0;
    END IF;

    RETURN _count;
END
$func$ LANGUAGE plpgsql;

-- is IO to do ?
SELECT public.drop_all_functions_if_exists('public', 'io_is_todo');
CREATE OR REPLACE FUNCTION public.io_is_todo(
    name VARCHAR
)
RETURNS HSTORE AS
$func$
DECLARE
    _todo BOOLEAN;
    _result VARCHAR;
    _error_message VARCHAR := CONCAT('IO ', io_is_todo.name, ' non valide');
    _io_history public.io_history;
    _io_currents public.io_history[];
    _io_lasts public.io_history[];
    _io_depends VARCHAR[];
    _io_missing VARCHAR[];
    _io_more_recents BOOLEAN[];
    _io_with_differences BOOLEAN[];
    _i INT;
    _j INT;
    _k INT;
    _has_relation BOOLEAN;
    _more_recent BOOLEAN;
    _with_difference BOOLEAN;
    _relation HSTORE;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.io_list l WHERE l.name = io_is_todo.name) THEN
        RAISE '% (pas défini)', _error_message;
    END IF;

    /*
     IO_EXIST: IO already done
     IO_MORE_RECENT: at least one depended IO more recent
     IO_WITH_DIFFERENCE: with difference compared with previous result
        IO condition allows comparison (not depended IO to process)
     */
    _result := NULL;
    _io_history := (SELECT get_last_io(io_is_todo.name));
    _todo := (_io_history IS NULL);

    -- depended IO
    _io_depends := ARRAY(
        SELECT
            (SELECT l.name FROM public.io_list l WHERE l.id = r.id_child) name
        FROM
            public.io_relation r
        WHERE
            id = (SELECT id FROM public.io_list l WHERE l.name = io_is_todo.name)
    );

    IF _io_depends IS NULL THEN
        RAISE '% (pas de dépendance)', _error_message;
    ELSE
        -- last history of depended IO
        _io_lasts := ARRAY(
            SELECT
                io_history
            FROM
                public.io_history
            WHERE
                id = ANY(
                    SELECT h.id
                    FROM
                        (SELECT UNNEST(_io_depends) name) l
                            JOIN get_last_io(l.name) h ON h.name = l.name
                )
        );
    END IF;

    -- current history of depended IO
    _io_currents := ARRAY(
        SELECT
            io_history
        FROM
            public.io_history
        WHERE
            id = ANY(
                SELECT io.id::TEXT::INT id
                FROM (
                    SELECT value id
                    FROM JSON_EACH((SELECT (get_last_io(io_is_todo.name)).attributes::JSON))
                ) io
            )
    );

    _result := CONCAT_WS(',',
        _result,
        FORMAT('"%s"=>"%s"', 'DEPENDS', ARRAY_TO_STRING(_io_depends, ':'))
    );

    _io_more_recents := ARRAY[]::BOOLEAN[];
    _io_with_differences := ARRAY[]::BOOLEAN[];
    FOR _i IN 1 .. ARRAY_UPPER(_io_depends, 1) LOOP
        _j := public.io_get_subscript_from_array_by_name(_io_lasts, _io_depends[_i]);
        _k := public.io_get_subscript_from_array_by_name(_io_currents, _io_depends[_i]);
        _has_relation := public.io_has_relation(name => _io_depends[_i]);
        IF NOT _has_relation THEN
            _with_difference := FALSE;
            -- no history (1st time, IO condition) ?
            IF _k = 0 THEN
                -- eval difference to known if todo
                _more_recent := TRUE;
            ELSE
                _more_recent := (_io_lasts[_j].date_data_end > _io_currents[_k].date_data_end);
            END IF;
        ELSE
            _relation := public.io_is_todo(name => _io_depends[_i]);
            _more_recent := _relation->'TODO';
            _with_difference := _more_recent;
        END IF;
        _io_more_recents := ARRAY_APPEND(_io_more_recents, _more_recent);

        IF _io_more_recents[_i] AND NOT _has_relation AND NOT _with_difference THEN
            _with_difference := public.io_with_difference_exists(name => _io_depends[_i]);
        END IF;
        _io_with_differences := ARRAY_APPEND(_io_with_differences, _with_difference);

        IF NOT _todo AND _io_more_recents[_i] AND _io_with_differences[_i] THEN
            _todo := TRUE;
        END IF;

        IF NOT _has_relation THEN
            _result := CONCAT_WS(',',
                _result,
                FORMAT('"%s"=>%s',
                    CONCAT(_io_depends[_i], '_t'),
                    _io_more_recents[_i] AND _io_with_differences[_i]
                )/*,
                FORMAT('"%s"=>%s',
                    CONCAT(_io_depends[_i], '_i'),
                    COALESCE(_io_lasts[_j].id, 0)
                )*/
            );
        ELSE
            _result := CONCAT_WS(',',
                _result,
                ((_relation - ARRAY['TODO', 'DEPENDS']) || HSTORE(CONCAT(_io_depends[_i], '_t'), _relation->'TODO') || HSTORE(CONCAT(_io_depends[_i], '_d'), _relation->'DEPENDS'))::TEXT
            );
        END IF;

        -- last history, if defined (IO condition not exists in history, so id=0)
        _result := CONCAT_WS(',',
            _result,
            FORMAT('"%s"=>%s',
                CONCAT(_io_depends[_i], '_i'),
                CASE WHEN (_j > 0) THEN _io_lasts[_j].id ELSE 0 END
            )
        );
    END LOOP;

    _result := CONCAT_WS(',',
        _result,
        FORMAT('"TODO"=>%s', _todo)
    );

    RETURN _result::HSTORE;
END
$func$ LANGUAGE plpgsql;

DO $INIT$
DECLARE
    _schema_name VARCHAR;
    _procedure_name VARCHAR := 'set_io';
    _query TEXT;
BEGIN
    -- for each country
    FOR _schema_name IN (
        SELECT schema_name FROM information_schema.schemata
        WHERE
            schema_name ~ '^..$'
    )
    LOOP
        -- initialize IOs (list and relation)
        IF procedure_exists(_schema_name, _procedure_name) THEN
            _query := CONCAT(
                'CALL ',
                _schema_name,
                '.',
                _procedure_name,
                '()'
            );

            EXECUTE _query;
        END IF;
    END LOOP;

    CALL public.set_io_list_index();
    CALL public.set_io_relation_index();
END $INIT$;
