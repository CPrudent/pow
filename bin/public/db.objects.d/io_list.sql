/***
 * add IO list
 */

CREATE TABLE IF NOT EXISTS public.io_list (
    id SERIAL NOT NULL
    , name VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_io_list_id ON public.io_list(id);
CREATE UNIQUE INDEX IF NOT EXISTS uix_io_list_name ON public.io_list(name);

SELECT public.drop_all_functions_if_exists('public', 'io_get_id_by_name_from_array');
CREATE OR REPLACE FUNCTION public.io_get_id_by_name_from_array(
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
            _id := _i;
            EXIT;
        ELSE
            _i := _i +1;
        END IF;
    END LOOP;

    RETURN _id;
END
$func$ LANGUAGE plpgsql;

DO $INIT$
BEGIN
    -- FR-TERRITORY -----------------------------------------------------------
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY');
    END IF;
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-LAPOSTE' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-LAPOSTE');
    END IF;
    -- RAN
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-ADDRESS-LAPOSTE' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-ADDRESS-LAPOSTE');
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
    -- GEOPAD
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-LAPOSTE-DELIVERY-POINT' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-LAPOSTE-DELIVERY-POINT');
    END IF;
    -- RAO
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-LAPOSTE-DELIVERY-ORGANIZATION' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-LAPOSTE-DELIVERY-ORGANIZATION');
    END IF;
    -- SOURCE ORGA
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-TERRITORY-LAPOSTE-ORGANIZATION' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-TERRITORY-LAPOSTE-ORGANIZATION');
    END IF;

    -- FR-ADDRESS -----------------------------------------------------------
    IF NOT EXISTS(SELECT 1 FROM public.io_list WHERE name = 'FR-ADDRESS' LIMIT 1) THEN
        INSERT INTO public.io_list(name) VALUES ('FR-ADDRESS');
    END IF;

END $INIT$;
