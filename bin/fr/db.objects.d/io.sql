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
    _ids INT[];
    _ID_GROUP       INT := 1;
    _ID_ITEM_1      INT := 2;
    _ID_ITEM_2      INT := 3;
    _ID_ITEM_3      INT := 4;
    _ID_ITEM_4      INT := 5;
    _ID_ITEM_5      INT := 6;
BEGIN
    -- NOTE no reset of sequence! better is to drop table & restart db.objects.sh
    IF reset THEN
        DELETE FROM public.io_list WHERE name ~ '^FR-';
        DELETE FROM public.io_relation r WHERE NOT EXISTS(
            SELECT 1 FROM public.io_list l WHERE r.id = l.id
        );
    END IF;

    -- IO DECLARATION ---------------------------------------------------------

    -- FR-TERRITORY

    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY');
    -- LAPOSTE
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-AREA-UPD');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-SUPRA');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-EVENT');
    -- SOURCE ORGA
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION');
    -- INSEE
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE-MUNICIPALITY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE-SUPRA');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-INSEE-EVENT');
    -- IGN
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-MUNICIPALITY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-MUNICIPALITY-POPULATION');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-GEOMETRY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-IGN-EVENT');
    -- GOUV (BANATIC)
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GOUV');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GOUV-EPCI-LIST');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GOUV-EPCI-SET');
    --
    --CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GEOMETRY');

    -- municipality events
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

    -- FR-CONSTANT

    CALL public.io_add_if_not_exists(name => 'FR-CONSTANT');
    -- ADDRESS
    CALL public.io_add_if_not_exists(name => 'FR-CONSTANT-ADDRESS');

    -- get all IOs in memory
    _io_list := ARRAY(SELECT io_list FROM public.io_list);

    -- IO DEPENDENCES ---------------------------------------------------------

    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY'
    );

    /*
       FR-TERRITORY
            |-> FR-TERRITORY-LAPOSTE
            |-> FR-TERRITORY-INSEE
            |-> FR-TERRITORY-IGN
            |-> FR-TERRITORY-BANATIC
            --|-> FR-TERRITORY-GEOMETRY
     */

    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-INSEE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );
    _ids[_ID_ITEM_3] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-IGN'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_3]
    );
    _ids[_ID_ITEM_4] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-BANATIC'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_4]
    );

    --_id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-GEOMETRY');
    --CALL public.io_add_relation_if_not_exists(id1 => _id_1, id2 => _id);


    /*
       FR-TERRITORY-LAPOSTE
            |-> FR-TERRITORY-LAPOSTE-AREA
            |-> FR-TERRITORY-LAPOSTE-SUPRA
     */

    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-AREA'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-SUPRA'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );

    /*
       FR-TERRITORY-LAPOSTE-AREA
            |-> FR-ADDRESS-LAPOSTE
            |-> FR-TERRITORY-LAPOSTE-EVENT
            |-> FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL
            |-> FR-TERRITORY-LAPOSTE-AREA-UPD
     */

    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-AREA'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-EVENT'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );
    _ids[_ID_ITEM_3] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_3]
    );
    _ids[_ID_ITEM_4] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-AREA-UPD'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_4]
    );

    /*
       FR-TERRITORY-LAPOSTE-SUPRA
            |-> FR-ADDRESS-LAPOSTE
            |-> FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION
            |-> FR-TERRITORY-LAPOSTE-ORGANIZATION
     */

    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-SUPRA'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );
    _ids[_ID_ITEM_3] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_3]
    );

    /*
       FR-TERRITORY-INSEE
            |-> FR-TERRITORY-INSEE-MUNICIPALITY
            |-> FR-TERRITORY-INSEE-SUPRA
            |-> FR-TERRITORY-INSEE-EVENT
     */


    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-INSEE'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-INSEE-MUNICIPALITY'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-INSEE-SUPRA'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );
    _ids[_ID_ITEM_3] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-INSEE-EVENT'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_3]
    );

    /*
       FR-TERRITORY-IGN
            |-> FR-TERRITORY-IGN-MUNICIPALITY
            |-> FR-TERRITORY-IGN-MUNICIPALITY-POPULATION
            |-> FR-TERRITORY-IGN-EVENT
     */

    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-IGN'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-IGN-MUNICIPALITY'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-IGN-MUNICIPALITY-POPULATION'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );
    _ids[_ID_ITEM_3] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-IGN-EVENT'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_3]
    );

    /*
       FR-TERRITORY-GOUV
            |-> FR-TERRITORY-GOUV-EPCI-LIST
            |-> FR-TERRITORY-GOUV-EPCI-SET
     */


    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-GOUV'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-GOUV-EPCI-LIST'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
    _ids[_ID_ITEM_2] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-GOUV-EPCI-SET'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_2]
    );

    /*
       FR-TERRITORY-GEOMETRY
            |-> FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL
            |-> FR-TERRITORY-IGN-GEOMETRY
            |-> FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY


    _id_2 := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-GEOMETRY');
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-TERRITORY-IGN-GEOMETRY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
    _id := public.io_get_id_from_array_by_name(from_array => _io_list, name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY');
    CALL public.io_add_relation_if_not_exists(id1 => _id_2, id2 => _id);
     */

    /*
       FR-ADDRESS
            |-> FR-ADDRESS-LAPOSTE
     */


    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );

    /*
       FR-CONSTANT
            |-> FR-CONSTANT-ADDRESS
     */

    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-CONSTANT'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-CONSTANT-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );

    /*
       FR-CONSTANT-ADDRESS
            |-> FR-ADDRESS-LAPOSTE
     */
    _ids[_ID_GROUP] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-CONSTANT-ADDRESS'
    );
    _ids[_ID_ITEM_1] := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _ids[_ID_GROUP],
        id2 => _ids[_ID_ITEM_1]
    );
END
$proc$ LANGUAGE plpgsql;
