SELECT drop_all_functions_if_exists('fr', 'fix_72_restore_io_attributes');
CREATE OR REPLACE PROCEDURE fr.fix_72_restore_io_attributes(
    filter IN VARCHAR DEFAULT '.*',
    reset IN BOOLEAN DEFAULT FALSE,
    simulation IN BOOLEAN DEFAULT FALSE
)
AS
$proc$
DECLARE
    _list           RECORD;
    _id_last        INT;
    _id_depends     INT[];
    _attributes     VARCHAR;
BEGIN
    FOR _list IN (
        WITH
        io AS (
            SELECT name, id FROM io_list l WHERE EXISTS(
                SELECT 1 FROM io_relation r WHERE r.id = l.id
            )
        ),
        relation AS (
            SELECT
                io.name,
                l2.name depends
            FROM
                io
                    JOIN io_relation r ON io.id = r.id
                    JOIN io_list l2 ON r.id_child = l2.id
        )
        SELECT name, ARRAY_AGG(depends) depends FROM relation GROUP BY name ORDER BY 1
    )
    LOOP
        -- to do?
        IF REGEXP_MATCH(_list.name, filter) IS NULL THEN CONTINUE; END IF;

        RAISE NOTICE 'IO % avec dépendences %', _list.name, _list.depends;
        _id_last := (get_last_io(_list.name)).id;
        RAISE NOTICE ' id=%', _id_last;
        _attributes := NULL;
        FOR _i IN 1 .. CARDINALITY(_list.depends)
        LOOP
            _id_depends[_i] := (get_last_io(_list.depends[_i])).id;
            IF _id_depends[_i] IS NOT NULL THEN
                RAISE NOTICE ' id(%)=%', _list.depends[_i], _id_depends[_i];
                _attributes := CONCAT(
                    _attributes,
                    CASE WHEN LENGTH(_attributes) > 0 THEN ',' END,
                    CONCAT('"', _list.depends[_i], '":', _id_depends[_i])
                );
            END IF;
        END LOOP;

        -- w/ _attributes ?
        IF _attributes IS NULL THEN CONTINUE; END IF;

        -- apply on db ?
        IF NOT simulation THEN
            IF reset THEN
                -- reset all
                UPDATE io_history SET attributes = NULL WHERE name = _list.name;
            END IF;
            -- update last one
            UPDATE io_history SET
                attributes = CONCAT('{', _attributes, '}')
            WHERE
                id = _id_last
            ;
        END IF;
        RAISE NOTICE ' %: {%}', _list.name, _attributes;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.fix_72_restore_io_attributes(simulation => true);
CALL fr.fix_72_restore_io_attributes(simulation => true, filter => '^FR-TERRITORY');
 */
