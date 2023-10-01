/***
 * FR-IO definition
 */

/*
 * initialize FR IOs
 */
SELECT public.drop_all_functions_if_exists('fr', 'set_io');
CREATE OR REPLACE PROCEDURE fr.set_io(
    reset BOOLEAN DEFAULT FALSE
) AS
$proc$
DECLARE
    _io_list public.io_list[];
    _id_1 INT;
    _id_2 INT;
    _id_3 INT;
    _id INT;
BEGIN
    IF reset THEN
        DELETE FROM public.io_list WHERE name ~ '^FR-';
        DELETE FROM public.io_relation r WHERE NOT EXISTS(
            SELECT 1 FROM public.io_list l WHERE r.id = l.id
        );
    END IF;

    -- FR-TERRITORY -----------------------------------------------------------
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY');
    END IF;
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-LAPOSTE' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-LAPOSTE');
    END IF;
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-INSEE' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-INSEE');
    END IF;
    -- ADMIN EXPRESS
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-IGN' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-IGN');
    END IF;
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-BANATIC' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-BANATIC');
    END IF;
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-MUNICIPALITY-INSEE-EVENT' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-MUNICIPALITY-INSEE-EVENT');
    END IF;
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-MUNICIPALITY-WIKIPEDIA-EVENT' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-MUNICIPALITY-WIKIPEDIA-EVENT');
    END IF;
    -- SOURCE ORGA
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-LAPOSTE-ORGANIZATION' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-LAPOSTE-ORGANIZATION');
    END IF;

    -- FR-ADDRESS -----------------------------------------------------------
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-ADDRESS' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-ADDRESS');
    END IF;
    -- RAN
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-ADDRESS-LAPOSTE' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-ADDRESS-LAPOSTE');
    END IF;
    -- GEOPAD
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-ADDRESS-LAPOSTE-DELIVERY-POINT');
    END IF;
    -- RAO
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION');
    END IF;

    _io_list := ARRAY(SELECT io_list FROM public.io_list);
    _id_1 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY');
    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE');
    _id_3 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS');

    -- FR-TERRITORY -----------------------------------------------------------
    -- FR-TERRITORY / FR-TERRITORY-LAPOSTE
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id_2);

    -- FR-TERRITORY / FR-TERRITORY-INSEE
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-INSEE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-TERRITORY-IGN
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-TERRITORY-BANATIC
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-BANATIC');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-MUNICIPALITY-INSEE-EVENT
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-MUNICIPALITY-INSEE-EVENT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-ADDRESS-LAPOSTE-DELIVERY-POINT
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY-LAPOSTE ---------------------------------------------------
    -- FR-TERRITORY-LAPOSTE / FR-ADDRESS-LAPOSTE
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    -- FR-TERRITORY-LAPOSTE / FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    -- FR-TERRITORY-LAPOSTE / FR-TERRITORY-LAPOSTE-ORGANIZATION
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    -- FR-ADDRESS ---------------------------------------------------
    -- FR-ADDRESS / FR-ADDRESS-LAPOSTE
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_3, id2 => _id);
END
$proc$ LANGUAGE plpgsql;
