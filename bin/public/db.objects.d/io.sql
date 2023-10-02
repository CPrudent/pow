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
BEGIN
    _query := CASE name
        WHEN 'FR-TERRITORY-IGN' THEN
            '
            (
                SELECT
                    insee_com AS codgeo
                    , nom AS libgeo
                    , geom
                    , population
                FROM
                    fr.admin_express_commune
                WHERE
                    insee_com NOT IN (''75056'', ''13055'', ''69123'')
                UNION
                SELECT
                    arm.insee_arm
                    , arm.nom
                    , arm.geom
                    , population
                FROM
                    fr.admin_express_arrondissement_municipal AS arm
            ) x

            FULL OUTER JOIN

            (
                SELECT
                    code
                    , name
                    , geom_native
                    , population
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
            NULL
        WHEN 'FR-TERRITORY-BANATIC' THEN
            NULL
        WHEN 'FR-TERRITORY-LAPOSTE' THEN
            NULL
        ELSE
            NULL
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
    , simulation BOOLEAN DEFAULT FALSE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _todo BOOLEAN := FALSE;
    _error_message VARCHAR := CONCAT('IO ', io_is_todo.name, '% non valide');
    _io_history public.io_history;
    _io_currents public.io_history[];
    _io_lasts public.io_history[];
    _io_depends VARCHAR[];
    _io_missing VARCHAR[];
    _io_more_recents BOOLEAN[];
    _io_with_differences BOOLEAN[];
    _i INT;
    _j INT;
    _with_difference BOOLEAN;
BEGIN
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = io_is_todo.name) THEN
        RAISE '% (pas défini)', _error_message;
    END IF;

    /*
     IO_EXIST: integration already done, and all depended IO exist
     IO_MORE_RECENT: at least one depended IO more recent
     IO_WITH_DIFFERENCE: with difference compared with previous result
     */
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
                id = (SELECT id FROM public.io_list WHERE name = io_is_todo.name)
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
                            (SELECT UNNEST(_io_list) name) l
                                JOIN get_last_io(l.name) h ON h.co_type = l.name
                    )
            );

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
            WITH
            io_each AS (
                SELECT value ios FROM JSON_EACH((SELECT (get_last_io(io_is_todo.name)).infos_data::JSON))
            )
            , io_id AS (
                SELECT io_json.value::TEXT::INT id
                FROM io_each io
                    CROSS JOIN JSON_EACH(io.ios) io_json
            )
            SELECT io.* FROM io_id JOIN public.io_history io ON io_id.id = io.id
        );

        _io_more_recents := ARRAY[]::BOOLEAN[];
        _io_with_differences := ARRAY[]::BOOLEAN[];
        FOR _i IN 1 .. ARRAY_UPPER(_io_depends, 1) LOOP
            _io_more_recents := ARRAY_APPEND(_io_more_recents,
                (
                    _io_lasts[public.io_get_subscript_from_array_by_name(_io_lasts, _io_depends[_i])].dt_data_end >
                    _io_currents[public.io_get_subscript_from_array_by_name(_io_currents, _io_depends[_i])].dt_data_end
                )
            );

            _with_difference := FALSE;
            IF _io_more_recents[_i] THEN
                _with_difference := public.io_with_difference(name => _io_depends[_i]);
            END IF;
            _io_with_differences := ARRAY_APPEND(_io_with_differences, _with_difference);

            -- enough to do if one more recent
            IF NOT simulation AND _with_difference THEN
                EXIT;
            END IF;
        END LOOP;
    END IF;

    RETURN _todo;
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
