/***
 * add IO
 */

CREATE TABLE IF NOT EXISTS public.io_list (
    id SERIAL NOT NULL
    , name VARCHAR NOT NULL
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
    id INT NOT NULL
    , id_child INT NULL
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
    id1 INT
    , id2 INT
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
    from_array public.io_list[]
    , name VARCHAR
)
RETURNS INT AS
$func$
DECLARE
    _id INT := 0;
    _i INT;
BEGIN
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
    from_array public.io_history[]
    , name VARCHAR
)
RETURNS INT AS
$func$
DECLARE
    _id INT := 0;
    _i INT;
BEGIN
    FOR _i IN 1 .. ARRAY_UPPER(from_array, 1) LOOP
        IF from_array[_i].co_type = name THEN
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

SELECT public.drop_all_functions_if_exists('public', 'io_with_difference');
CREATE OR REPLACE FUNCTION public.io_with_difference(
    name VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query TEXT;
    _with BOOLEAN;
    _last_io TIMESTAMP;
BEGIN
    IF name = ANY('{FR-TERRITORY-LAPOSTE-GEOMETRY,FR-MUNICIPALITY-INSEE-EVENT}') THEN
        _last_io := (public.get_last_io(type_in => name)).dt_data_end;
    END IF;

    _query := CASE name
        WHEN 'FR-TERRITORY-IGN' THEN
            '
                (
                    SELECT
                        insee_com AS codgeo
                        , nom AS libgeo
                        , geom
                    FROM
                        fr.admin_express_commune
                    WHERE
                        insee_com NOT IN (''75056'', ''13055'', ''69123'')
                    UNION
                    SELECT
                        arm.insee_arm
                        , arm.nom
                        , arm.geom
                    FROM
                        fr.admin_express_arrondissement_municipal AS arm
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        code
                        , name
                        , geom_native
                    FROM
                        public.territory
                    WHERE
                        country = ''FR''
                        AND
                        level = ''COM''
                        AND
                        code !~ ''^(98|97[578])''
                ) t

                ON x.codgeo = t.code
            WHERE
                --  municipality
                (
                    x.codgeo IS NULL
                    OR
                    t.code IS NULL
                    OR
                    x.libgeo IS DISTINCT FROM t.name
                )
                --  geometry
                OR
                NOT ST_Equals(x.geom, t.geom_native)
            '
        WHEN 'FR-TERRITORY-INSEE' THEN
            '
                (
                    SELECT
                        codgeo
                        , libgeo
                        , cv
                        , arr
                        , dep
                        , reg
                    FROM
                        fr.insee_administrative_cutting_municipality_and_district
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        code
                        , name
                        , (get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''COM'', code), ''CV''))).code code_cv
                        , (get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''COM'', code), ''ARR''))).code code_arr
                        , (get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''COM'', code), ''DEP''))).code code_dep
                        , (get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''COM'', code), ''REG''))).code code_reg
                    FROM
                        public.territory
                    WHERE
                        country = ''FR''
                        AND
                        level = ''COM''
                        AND
                        code !~ ''^(98|97[578])''
                ) t

                ON x.codgeo = t.code
            WHERE
                --  municipality
                (
                    x.codgeo IS NULL
                    OR
                    t.code IS NULL
                )
                -- SUPRA
                OR
                (
                    x.cv IS DISTINCT FROM t.code_cv
                    OR
                    x.arr IS DISTINCT FROM t.code_arr
                    OR
                    x.dep IS DISTINCT FROM t.code_dep
                    OR
                    x.reg IS DISTINCT FROM t.code_reg
                )
            '
        WHEN 'FR-TERRITORY-BANATIC' THEN
            '
                (
                    SELECT
                        n_siren codgeo
                        , nom_du_groupement libgeo
                        , nature_juridique typgeo
                    FROM
                        fr.banatic_listof_epci
                    WHERE
                        nature_juridique IN (''MET69'', ''CC'', ''CA'', ''METRO'', ''CU'')
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        code
                        , name
                        , attributs->''TYPE''
                    FROM
                        public.territory
                    WHERE
                        country = ''FR''
                        AND
                        level = ''EPCI''
                ) t

                ON x.codgeo = t.code
            WHERE
                --  EPCI
                (
                    x.codgeo IS NULL
                    OR
                    t.code IS NULL
                    OR
                    x.libgeo IS DISTINCT FROM t.name
                )
            UNION
            SELECT 1
            FROM
                (
                    SELECT
                        s.n_siren codgeo_epci
                        , (get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''COM_GLOBALE_ARM'', l.insee), ''COM''))).code codgeo_com
                    FROM
                        fr.banatic_setof_epci s
                            JOIN fr.banatic_siren_insee l ON s.siren_membre = l.siren
                    WHERE
                        s.nature_juridique IN (''MET69'', ''CC'', ''CA'', ''METRO'', ''CU'')
                        AND
                        EXISTS(
                            SELECT 1
                            FROM public.territory
                            WHERE country = ''FR'' AND level = ''COM_GLOBALE_ARM'' AND code = l.insee
                        )
                    UNION
                    SELECT
                        s.n_siren codgeo_epci
                        , l.insee codgeo_com
                    FROM
                        fr.banatic_setof_epci s
                            JOIN fr.banatic_siren_insee l ON s.siren_membre = l.siren
                    WHERE
                        s.nature_juridique IN (''MET69'', ''CC'', ''CA'', ''METRO'', ''CU'')
                        AND
                        NOT EXISTS(
                            SELECT 1
                            FROM public.territory
                            WHERE country = ''FR'' AND level = ''COM_GLOBALE_ARM'' AND code = l.insee
                        )
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        t.code code_epci
                        , c.code code_com
                    FROM
                        public.territory t
                            CROSS JOIN get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''EPCI'', t.code), ''COM'')) c
                    WHERE
                        t.country = ''FR''
                        AND
                        t.level = ''EPCI''
                ) t

                ON (x.codgeo_epci, x.codgeo_com) = (t.code_epci, t.code_com)
            WHERE
                -- links
                (
                    (x.codgeo_epci IS NULL OR x.codgeo_com IS NULL)
                    OR
                    (t.code_epci IS NULL OR t.code_com IS NULL)
                )
            '
        WHEN 'FR-TERRITORY-LAPOSTE-AREA' THEN
            '
                (
                    SELECT
                        co_cea codgeo
                        , co_insee_commune codgeo_com
                        , CASE WHEN co_insee_commune ~ ''^98[78]'' THEN lb_ach_nn ELSE lb_l5_nn END libgeo_l5
                        , CASE WHEN co_insee_commune ~ ''^98[78]'' THEN lb_l5_nn ELSE lb_ach_nn END libgeo_l6
                        , co_postal
                    FROM
                        fr.laposte_zone_address
                    WHERE
                        fl_active
                        AND
                        -- exclude MONACO, and trick for bug #45
                        co_insee_commune !~ ''^9[89]''
                ) x

                FULL OUTER JOIN

                (
                    SELECT
                        za.code
                        , com.code code_com
                        , za.attributs->''L5_NORM'' name_l5
                        , com.attributs->''L6_NORM'' name_l6
                        , cp.code code_cp
                    FROM
                        public.territory za
                            CROSS JOIN get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''ZA'', za.code), ''COM'')) com
                            CROSS JOIN get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', ''ZA'', za.code), ''CP'')) cp
                    WHERE
                        za.country = ''FR''
                        AND
                        za.level = ''ZA''
                        AND
                        -- fix bug (see #45 2nd usecase)
                        com.code !~ ''^98''
                ) t

                ON x.codgeo = t.code
            WHERE
                -- ZA
                (
                    x.codgeo IS NULL
                    OR
                    t.code IS NULL
                    OR
                    x.codgeo_com IS DISTINCT FROM t.code_com
                    OR
                    x.co_postal IS DISTINCT FROM t.code_cp
                    OR
                    x.libgeo_l6 IS DISTINCT FROM t.name_l6
                    OR
                    x.libgeo_l5 IS DISTINCT FROM t.name_l5
                )
            '
        WHEN 'FR-TERRITORY-LAPOSTE-SUPRA' THEN
            '
                (
                    SELECT
                        codgeo
                        , NULLIF(codgeo_pdc_ppdc_parent, codgeo) codgeo_pdc_ppdc_parent
                        , NULLIF(codgeo_ppdc_pdc_parent, codgeo) codgeo_ppdc_pdc_parent
                        , CASE WHEN nivgeo = ''DEX'' THEN codgeo ELSE codgeo_dex_parent END codgeo_dex_parent
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
                        t.code
                        , NULLIF(pdc_ppdc.code, t.code) code_pdc_ppdc
                        , NULLIF(ppdc_pdc.code, t.code) code_ppdc_pdc
                        , (get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', t.level, t.code), ''DEX''))).code code_dex
                    FROM
                        public.territory t
                            LEFT OUTER JOIN get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', t.level, t.code), ''PDC_PPDC'')) pdc_ppdc ON public.is_level_below(''fr'', t.level, ''PDC_PPDC'')
                            LEFT OUTER JOIN get_territory_from_query(get_query_territory_extended_to_level(''fr'', get_query_territory(''fr'', t.level, t.code), ''PPDC_PDC'')) ppdc_pdc ON public.is_level_below(''fr'', t.level, ''PPDC_PDC'')
                    WHERE
                        t.country = ''FR''
                        AND ((
                                t.level = ''CP''
                                AND
                                -- fix bug (see #45 3rd usecase)
                                t.code !~ ''^9[78]''
                            )
                            OR (
                                t.level = ANY(''{PDC_PPDC,PPDC_PDC,DEX}'')
                            )
                        )
                ) t

                ON x.codgeo = t.code
            WHERE
                -- level
                (
                    x.codgeo IS NULL
                    OR
                    t.code IS NULL
                )
                -- SUPRA
                OR
                (
                    x.codgeo_pdc_ppdc_parent IS DISTINCT FROM t.code_pdc_ppdc
                    OR
                    x.codgeo_ppdc_pdc_parent IS DISTINCT FROM t.code_ppdc_pdc
                    OR
                    x.codgeo_dex_parent IS DISTINCT FROM t.code_dex
                )
            '
        WHEN 'FR-TERRITORY-LAPOSTE-GEOMETRY' THEN
            CONCAT(
                '
                    fr.delivery_point_view p
                        JOIN public.territory t ON p.co_adr_za = t.code
                WHERE
                    t.country = ''FR'' AND t.level = ''ZA''
                    AND
                    -- new point from previous IO
                    p.pdi_dt_modification > '''
                , _last_io
                , '''::TIMESTAMP
                    AND
                    -- valid point
                    p.fl_active AND p.fl_diffusable AND p.pdi_etat = 1 AND p.pdi_visible
                    -- at least street-center (=4)
                    AND p.pdi_no_type_localisation_coord >= 4
                    -- any valid point apart from existing geometry ?
                    AND ST_Relate(ST_Transform(p.pdi_coord, 4326), t.geom_world, ''**0******'')
                '
            )
        WHEN 'FR-MUNICIPALITY-INSEE-EVENT' THEN
            CONCAT(
                '
                    fr.insee_municipality_event
                WHERE
                    date_eff > '''
                , _last_io
                , '''::DATE
                '
            )
        -- always true if this IO is more recent
        ELSE
            '
            pg_tables
            '
    END CASE;

    IF _query IS NULL THEN RETURN FALSE; END IF;
    _query := CONCAT(
        '
        SELECT EXISTS(
            SELECT 1
            FROM
        '
        , _query
        , ')'
    );
    EXECUTE _query INTO _with;
    RETURN _with;
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
    _todo BOOLEAN := FALSE;
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
    _has_relation BOOLEAN;
    _more_recent BOOLEAN;
    _with_difference BOOLEAN;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.io_list l WHERE l.name = io_is_todo.name) THEN
        RAISE '% (pas défini)', _error_message;
    END IF;

    /*
     IO_EXIST: integration already done, and all depended IO exist
     IO_MORE_RECENT: at least one depended IO more recent
     IO_WITH_DIFFERENCE: with difference compared with previous result
     */
    _result := NULL;
    _io_history := (SELECT get_last_io(io_is_todo.name));
    IF _io_history IS NULL THEN
        _todo := TRUE;
    ELSE
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
                                JOIN get_last_io(l.name) h ON h.co_type = l.name
                    )
            );

            IF ARRAY_UPPER(_io_lasts, 1) IS NULL THEN
                RAISE '% (manque historique des dépendances)', _error_message;
            END IF;

            -- missing IO ?
            _io_missing := ARRAY_CAT(_io_depends, NULL);
            FOR _i IN 1 .. ARRAY_UPPER(_io_depends, 1) LOOP
                FOR _j IN 1 .. ARRAY_UPPER(_io_lasts, 1) LOOP
                    IF _io_lasts[_j].co_type = _io_depends[_i] THEN
                        _io_missing := ARRAY_REMOVE(_io_missing, _io_depends[_i]);
                        EXIT;
                    END IF;
                END LOOP;
            END LOOP;
            IF ARRAY_LENGTH(_io_missing, 1) > 0 THEN
                RAISE '% (manque %)', _error_message, ARRAY_TO_STRING(_io_missing, ', ');
            END IF;
        END IF;
    END IF;

    IF NOT _todo THEN
        -- current history of depended IO
        _io_currents := ARRAY(
            SELECT
                io_history
            FROM
                public.io_history
            WHERE
                id = ANY(
                    SELECT io_json.value::TEXT::INT id
                    FROM (
                        SELECT value ios
                        FROM JSON_EACH((SELECT (get_last_io(io_is_todo.name)).infos_data::JSON))
                    ) io
                        CROSS JOIN JSON_EACH(io.ios) io_json
                )
        );

        _io_more_recents := ARRAY[]::BOOLEAN[];
        _io_with_differences := ARRAY[]::BOOLEAN[];
        FOR _i IN 1 .. ARRAY_UPPER(_io_depends, 1) LOOP
            _has_relation := public.io_has_relation(name => _io_depends[_i]);
            IF NOT _has_relation THEN
                _more_recent := (
                    _io_lasts[public.io_get_subscript_from_array_by_name(_io_lasts, _io_depends[_i])].dt_data_end >
                    _io_currents[public.io_get_subscript_from_array_by_name(_io_currents, _io_depends[_i])].dt_data_end
                );
                _with_difference := FALSE;
            ELSE
                _more_recent := (public.io_is_todo(name => _io_depends[_i]))->'TODO';
                _with_difference := _more_recent;
            END IF;
            _io_more_recents := ARRAY_APPEND(_io_more_recents, _more_recent);

            IF _io_more_recents[_i] AND NOT _has_relation THEN
                _with_difference := public.io_with_difference(name => _io_depends[_i]);
            END IF;
            _io_with_differences := ARRAY_APPEND(_io_with_differences, _with_difference);

            IF NOT _todo AND _io_more_recents[_i] AND _io_with_differences[_i] THEN
                _todo := TRUE;
            END IF;

            _result := CONCAT_WS(','
                , _result
                , FORMAT('"%s"=>%s', _io_depends[_i], _io_more_recents[_i] AND _io_with_differences[_i])
            );
        END LOOP;
    END IF;

    _result := CONCAT_WS(','
        , _result
        , FORMAT('"TODO"=>%s', _todo)
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
        IF procedure_exists(_schema_name, _procedure_name) THEN
            _query := CONCAT(
                'CALL '
                , _schema_name
                , '.'
                , _procedure_name
                , '()'
            );

            EXECUTE _query;
        END IF;
    END LOOP;

    CALL public.set_io_list_index();
    CALL public.set_io_relation_index();
END $INIT$;
