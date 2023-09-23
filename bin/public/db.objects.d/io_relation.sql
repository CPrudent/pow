/***
 * add IO relation
 */

CREATE TABLE IF NOT EXISTS public.io_relation (
    id INT NOT NULL
    , id_child INT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_io_relation_ids ON public.io_relation(id, id_child);

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

DO $INIT$
DECLARE
    _io_list public.io_list[] := ARRAY(SELECT io_list FROM public.io_list);
    _id_1 INT := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY');
    _id_2 INT := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE');
    _id_3 INT := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-ADDRESS');
    _id INT;
BEGIN
    -- FR-TERRITORY -----------------------------------------------------------
    -- FR-TERRITORY / FR-TERRITORY-LAPOSTE
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id_2);

    -- FR-TERRITORY / FR-TERRITORY-INSEE
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-INSEE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-TERRITORY-IGN
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-IGN');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-TERRITORY-BANATIC
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-BANATIC');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-MUNICIPALITY-INSEE-EVENT
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-MUNICIPALITY-INSEE-EVENT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY / FR-TERRITORY-LAPOSTE-DELIVERY-POINT
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-DELIVERY-POINT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);

    -- FR-TERRITORY-LAPOSTE ---------------------------------------------------
    -- FR-TERRITORY-LAPOSTE / FR-ADDRESS-LAPOSTE
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    -- FR-TERRITORY-LAPOSTE / FR-TERRITORY-LAPOSTE-DELIVERY-ORGANIZATION
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-DELIVERY-ORGANIZATION');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    -- FR-TERRITORY-LAPOSTE / FR-TERRITORY-LAPOSTE-ORGANIZATION
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    -- FR-ADDRESS ---------------------------------------------------
    -- FR-ADDRESS / FR-ADDRESS-LAPOSTE
    _id := public.io_get_id_by_name_from_array(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_3, id2 => _id);

END $INIT$;
