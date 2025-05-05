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
    _id INT;
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
    -- GOUV (EPCI)
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GOUV-EPCI');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GOUV-EPCI-LIST');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GOUV-EPCI-SET');

    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-GEOMETRY');
    CALL public.io_add_if_not_exists(name => 'FR-TERRITORY-NEXT');

    -- municipality events
    CALL public.io_add_if_not_exists(name => 'FR-MUNICIPALITY-EVENT-INSEE');
    CALL public.io_add_if_not_exists(name => 'FR-MUNICIPALITY-EVENT-WIKIPEDIA');

    CALL public.io_add_if_not_exists(name => 'FR-MUNICIPALITY-ALTITUDE');

    -- FR-ADDRESS

    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS');
    -- RAN
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-AREA');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-STREET');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-HOUSENUMBER');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-COMPLEMENT');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-SUSTAINABILITY');

    -- GEOPAD
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT');
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY');
    -- RAO
    CALL public.io_add_if_not_exists(name => 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION');

    -- FR-CONSTANT

    CALL public.io_add_if_not_exists(name => 'FR-CONSTANT');
    -- ADDRESS
    CALL public.io_add_if_not_exists(name => 'FR-CONSTANT-ADDRESS');
    CALL public.io_add_if_not_exists(name => 'FR-CONSTANT-ADDRESS-LAPOSTE');
    CALL public.io_add_if_not_exists(name => 'FR-CONSTANT-ADDRESS-LAPOSTE-CORRECTION');

    -- get all IOs in memory
    _io_list := ARRAY(SELECT io_list FROM public.io_list);

    -- IO DEPENDENCES ---------------------------------------------------------

    /*
       FR-TERRITORY
            |-> FR-TERRITORY-LAPOSTE
            |-> FR-TERRITORY-LAPOSTE-AREA
            |-> FR-TERRITORY-INSEE
            |-> FR-TERRITORY-IGN
            |-> FR-TERRITORY-GOUV-EPCI
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-AREA'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-INSEE'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-IGN'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-GOUV-EPCI'
                )
    );

    /*
       FR-TERRITORY-LAPOSTE
            |-> FR-TERRITORY-LAPOSTE-EVENT
            |-> FR-TERRITORY-LAPOSTE-SUPRA
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-EVENT'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-SUPRA'
                )
    );

    /*
       FR-TERRITORY-LAPOSTE-SUPRA
            |-> FR-TERRITORY-LAPOSTE-AREA
            |-> FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION
            |-> FR-TERRITORY-LAPOSTE-ORGANIZATION
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-SUPRA'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-AREA'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-ORGANIZATION'
                )
    );

    /*
       FR-TERRITORY-LAPOSTE-AREA
            |-> FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL
            |-> FR-TERRITORY-LAPOSTE-AREA-UPD
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-LAPOSTE-AREA'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-AREA-UPD'
                )
    );

    /*
       FR-TERRITORY-INSEE
            |-> FR-TERRITORY-INSEE-MUNICIPALITY
            |-> FR-TERRITORY-INSEE-SUPRA
            |-> FR-TERRITORY-INSEE-EVENT
     */


    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-INSEE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-INSEE-MUNICIPALITY'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-INSEE-SUPRA'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-INSEE-EVENT'
                )
    );

    /*
       FR-TERRITORY-IGN
            |-> FR-TERRITORY-IGN-MUNICIPALITY
            |-> FR-TERRITORY-IGN-MUNICIPALITY-POPULATION
            |-> FR-TERRITORY-IGN-EVENT
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-IGN'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-IGN-MUNICIPALITY'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-IGN-MUNICIPALITY-POPULATION'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-IGN-EVENT'
                )
    );

    /*
       FR-TERRITORY-GOUV-EPCI
            |-> FR-TERRITORY-GOUV-EPCI-LIST
            |-> FR-TERRITORY-GOUV-EPCI-SET
     */


    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-GOUV-EPCI'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-GOUV-EPCI-LIST'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-GOUV-EPCI-SET'
                )
    );

    /*
       FR-TERRITORY-GEOMETRY
            |-> FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL
            |-> FR-TERRITORY-IGN-GEOMETRY
            |-> FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-GEOMETRY'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-LAPOSTE-AREA-ADD-OR-DEL'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-IGN-GEOMETRY'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT-GEOMETRY'
                )
    );

    /*
       FR-TERRITORY-NEXT
            |-> FR-TERRITORY-GEOMETRY
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY-NEXT'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-GEOMETRY'
                )
    );

    /*
       FR-TERRITORY
            R
            |-> FR-TERRITORY-GEOMETRY
            |-> FR-TERRITORY-NEXT
            |-> FR-MUNICIPALITY-ALTITUDE
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-TERRITORY'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-GEOMETRY'
                ),
        type => 'R'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-TERRITORY-NEXT'
                ),
        type => 'R'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-MUNICIPALITY-ALTITUDE'
                ),
        type => 'R'
    );

    /*
       FR-ADDRESS
            |-> FR-ADDRESS-LAPOSTE
     */


    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-ADDRESS'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-ADDRESS-LAPOSTE'
                )
    );

    /*
       FR-CONSTANT
            |-> FR-CONSTANT-ADDRESS
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-CONSTANT'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-CONSTANT-ADDRESS'
                )
    );

    /*
       FR-CONSTANT-ADDRESS
            |-> FR-ADDRESS-LAPOSTE
            |-> FR-CONSTANT-ADDRESS-LAPOSTE
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-CONSTANT-ADDRESS'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-ADDRESS-LAPOSTE'
                )
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-CONSTANT-ADDRESS-LAPOSTE'
                )
    );

    /*
       FR-CONSTANT-ADDRESS-LAPOSTE
            |-> FR-CONSTANT-ADDRESS-LAPOSTE-CORRECTION
     */

    _id := public.io_get_id_from_array_by_name(
        from_array => _io_list,
        name => 'FR-CONSTANT-ADDRESS-LAPOSTE'
    );
    CALL public.io_add_relation_if_not_exists(
        id1 => _id,
        id2 => public.io_get_id_from_array_by_name(
                    from_array => _io_list,
                    name => 'FR-CONSTANT-ADDRESS-LAPOSTE-CORRECTION'
                )
    );

END
$proc$ LANGUAGE plpgsql;
