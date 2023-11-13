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

    -- IO DECLARATION ---------------------------------------------------------

    -- FR-TERRITORY

    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA-UPD');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA-EVENT');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-SUPRA');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE-MUNICIPALITY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE-SUPRA');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE-EVENT');
    -- ADMIN EXPRESS
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-MUNICIPALITY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-GEOMETRY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-EVENT');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-BANATIC');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-BANATIC-LIST');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-BANATIC-SET');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GEOMETRY');
    -- SOURCE ORGA
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION');

    CALL public.io_add_if_not_exists(name => 'FR-MUNICIPALITY-EVENT-INSEE');
    CALL public.io_add_if_not_exists(name => 'FR-MUNICIPALITY-EVENT-WIKIPEDIA');

    -- FR-ADDRESS

    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS');
    -- RAN
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE');
    -- GEOPAD
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY');
    -- RAO
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION');

    -- get all IOs in memory
    _io_list := ARRAY(SELECT io_list FROM public.io_list);

    -- IO DEPENDENCIES --------------------------------------------------------

    _id_1 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY');

    /*
       FR-TERRITORY
            |-> FR-TERRITORY-LAPOSTE
            |-> FR-TERRITORY-INSEE
            |-> FR-TERRITORY-IGN
            |-> FR-TERRITORY-BANATIC
            |-> FR-TERRITORY-GEOMETRY
     */

    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-INSEE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-BANATIC');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-GEOMETRY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);


    /*
       FR-TERRITORY-LAPOSTE
            |-> FR-TERRITORY-LAPOSTE-AREA
            |-> FR-TERRITORY-LAPOSTE-SUPRA
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-SUPRA');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-TERRITORY-LAPOSTE-AREA
            |-> FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL
            |-> FR-TERRITORY-LAPOSTE-AREA-UPD
            |-> FR-TERRITORY-LAPOSTE-AREA-EVENT
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA-UPD');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA-EVENT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-TERRITORY-LAPOSTE-SUPRA
            |-> FR-TERRITORY-LAPOSTE-AREA
            |-> FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION
            |-> FR-TERRITORY-LAPOSTE-ORGANIZATION
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-SUPRA');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-TERRITORY-INSEE
            |-> FR-TERRITORY-INSEE-MUNICIPALITY
            |-> FR-TERRITORY-INSEE-SUPRA
            |-> FR-TERRITORY-INSEE-EVENT
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-INSEE');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-INSEE-MUNICIPALITY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-INSEE-SUPRA');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-INSEE-EVENT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-TERRITORY-IGN
            |-> FR-TERRITORY-IGN-MUNICIPALITY
            |-> FR-TERRITORY-IGN-EVENT
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN-MUNICIPALITY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN-EVENT');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-TERRITORY-BANATIC
            |-> FR-TERRITORY-BANATIC-LIST
            |-> FR-TERRITORY-BANATIC-SET
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-BANATIC');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-BANATIC-LIST');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-BANATIC-SET');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-TERRITORY-GEOMETRY
            |-> FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL
            |-> FR-TERRITORY-IGN-GEOMETRY
            |-> FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY
     */

    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-GEOMETRY');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN-GEOMETRY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);

    /*
       FR-ADDRESS
            |-> FR-ADDRESS-LAPOSTE
     */

    _id_3 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE');
    CALL public.io_add_relation_if_not_exists(id1 => _id_3, id2 => _id);
END
$proc$ LANGUAGE plpgsql;
