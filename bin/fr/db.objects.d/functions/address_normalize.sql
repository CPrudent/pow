/***
 * add FR-ADDRESS facilities (normalized label, following AFNOR NF Z 10-011 (1/2013))
 */

SELECT drop_all_functions_if_exists('fr', 'normalize_address');
CREATE OR REPLACE FUNCTION fr.normalize_address(
    address IN RECORD                   -- address to normalize
    , columns_map IN HSTORE             -- mapping address(client)/address(reference)
)
RETURNS fr.address_normalized AS
$func$
DECLARE
    _address_normalized fr.address_normalized;
    _column_map VARCHAR[];
    _geom GEOMETRY;
    _geom_x DOUBLE PRECISION;
    _geom_y DOUBLE PRECISION;
    _geom_srid SMALLINT;
    _geom_srid_default SMALLINT := 2154;
    _cadastre_parcel_number VARCHAR;
    _cadastre_parcel_section VARCHAR;
    _cadastre_parcel_prefix CHAR(3);
    _street_type VARCHAR;
    _street_type_short VARCHAR;
BEGIN
    FOREACH _column_map SLICE 1 IN ARRAY %# columns_map LOOP
        _column_map[2] := CONCAT('$1.', _column_map[2]);
        BEGIN
            CASE _column_map[1]
                WHEN 'id' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.id
                        USING address;
                WHEN 'complement' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.complement
                        USING address;
                    _address_normalized.complement := NULLIF(TRIM(public.clean_address_label(_address_normalized.complement)), '');
                WHEN 'housenumber' THEN
                    EXECUTE CONCAT('SELECT NULLIF(TRIM(', _column_map[2], '::TEXT), '''')::INTEGER')
                        INTO _address_normalized.housenumber
                        USING address;
                    --SELECT '33' ~ '^[0-9]*$'
                    --A ETUDIER : ne permet plus de forcer la recherche d'un numéro si activé
                    --_address_normalized.housenumber := NULLIF(_address_normalized.housenumber, 0);
                    IF _address_normalized.housenumber::VARCHAR !~ '^[0-9]*$' THEN
                        RAISE NOTICE 'Numéro de voie ignoré car invalide : %', _address_normalized.housenumber;
                        _address_normalized.housenumber := NULL;
                    END IF;
                WHEN 'housenumber_extension' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.housenumber_extension
                        USING address;
                    _address_normalized.housenumber_extension := NULLIF(TRIM(public.clean_address_label(_address_normalized.housenumber_extension)), '');
                WHEN 'street' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.street
                        USING address;
                    _address_normalized.street := NULLIF(TRIM(public.clean_address_label(_address_normalized.street)), '');

                    -- 1st word = type of street (abbreviated ?)
                    _street_type_short := (REGEXP_MATCH(_address_normalized.street, '^\S+'))[1];

                    SELECT type
                    INTO _street_type
                    FROM fr.laposte_address_street_type
                    WHERE type_abbreviated = _street_type_short
                    ORDER BY occurs DESC;
                    -- not abbreviated?
                    IF _street_type IS NULL THEN
                        SELECT type
                        INTO _street_type
                        FROM fr.laposte_address_street_type
                        WHERE type = _street_type_short
                        ORDER BY occurs DESC;
                        _street_type_short := NULL;
                    END IF;
                    _address_normalized.street_type := _street_type;
                    -- abbreviated: extend name (w/ full type)
                    IF _street_type_short IS NOT NULL THEN
                        _address_normalized.street_type_short := _street_type_short;
                        _address_normalized.street := REGEXP_REPLACE(_address_normalized.street, '^\S+', _address_normalized.street_type);
                    END IF;
                WHEN 'municipality_code' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.municipality_code
                        USING address;
                    -- SELECT '33063' ~ '^([0-9]{5}|2[1-9AB][0-9]{3})$'
                    -- SELECT '2A001' ~ '^([0-9]{5}|2[1-9AB][0-9]{3})$'
                    IF _address_normalized.municipality_code !~ '^([0-9]{5}|2[1-9AB][0-9]{3})$' THEN
                        RAISE NOTICE 'Code INSEE commune ignoré car invalide : %', _address_normalized.municipality_code;
                        _address_normalized.municipality_code := NULL;
                    END IF;
                WHEN 'postcode' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.postcode
                        USING address;
                    --SELECT '33000' ~ '^[0-9]{5}$'
                    IF _address_normalized.postcode !~ '^[0-9]{5}$' THEN
                        RAISE NOTICE 'Code Postal commune ignoré car invalide : %', _address_normalized.postcode;
                        _address_normalized.postcode := NULL;
                    END IF;
                WHEN 'municipality_name' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.municipality_name
                        USING address;
                    -- transform SAINT|SAINTE in ST|STE
                    _address_normalized.municipality_name := REGEXP_REPLACE(NULLIF(TRIM(public.clean_address_label(_address_normalized.municipality_name)), ''), '\mSAINT([E]?)\M', 'ST\1', 'g');

                -- TODO à intégrer dans lb_ligneX
                -- mention CEDEX OU libellé Ancienne Commune OU les 2 accollées
                -- RE=^((BP|CS|CE|CP) *[0-9]+)? *([A-Z ]+)?$
                WHEN 'municipality_old_name' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _address_normalized.municipality_old_name
                        USING address;

                    --RAISE NOTICE '%: ligne5=% (avant normalisation)', _address_normalized.id, _address_normalized.municipality_old_name;
                    _address_normalized.municipality_old_name := REGEXP_REPLACE(NULLIF(TRIM(public.clean_address_label(_address_normalized.municipality_old_name)), ''), '\mSAINT([E]?)\M', 'ST\1', 'g');
                    --RAISE NOTICE '%: ligne5=% (après normalisation)', _address_normalized.id, _address_normalized.municipality_old_name;

                WHEN 'geo_xy' THEN
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',1)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',2)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                WHEN 'geo_latlon' THEN
                    -- latitude = Y, longitude = X
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',2)::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                    EXECUTE CONCAT('SELECT REPLACE(SPLIT_PART(', _column_map[2], ','','',1)::VARCHAR, '','' ,''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                    _geom_srid_default := 4326;
                WHEN 'geo_x' THEN
                    EXECUTE CONCAT('SELECT REPLACE(', _column_map[2], '::VARCHAR, '','', ''.'')::DOUBLE PRECISION')
                        INTO _geom_x
                        USING address;
                WHEN 'geo_y' THEN
                    EXECUTE CONCAT('SELECT REPLACE(', _column_map[2], '::VARCHAR, '','' ,''.'')::DOUBLE PRECISION')
                        INTO _geom_y
                        USING address;
                WHEN 'geo_srid' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2], '::SMALLINT')
                        INTO _geom_srid
                        USING address;
                WHEN 'geo_wkt' THEN
                    EXECUTE CONCAT('SELECT ST_PointFromText(', _column_map[2], ')')
                        INTO _geom
                        USING address;
                WHEN 'geo_json' THEN
                    EXECUTE CONCAT('SELECT ST_GeomFromGeoJSON(', _column_map[2], ')')
                        INTO _geom
                        USING address;
                WHEN 'geo' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _geom
                        USING address;

                WHEN 'cadastre_parcel_number' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2], '::INTEGER::VARCHAR')
                        INTO _cadastre_parcel_number
                        USING address;
                WHEN 'cadastre_parcel_section' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _cadastre_parcel_section
                        USING address;
                    --On enlève les éventuel 0 préfixant l'identifiant de section cadastrale
                    --Alternative : ne prendre que les lettre alphabéthiques ?
                    _cadastre_parcel_section := REPLACE(_cadastre_parcel_section, '0', '');
                WHEN 'cadastre_parcel_prefix' THEN
                    EXECUTE CONCAT('SELECT ', _column_map[2])
                        INTO _cadastre_parcel_prefix
                        USING address;
            ELSE
                RAISE NOTICE 'Attribut % ignoré car inconnu', _column_map[1];
            END CASE;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Attribut % ignoré car provoquant une erreur à l''évaluation de % : %', _column_map[1], _column_map[2], SQLERRM;
        END;
    END LOOP;

    IF _address_normalized.id IS NULL THEN
        RAISE 'Vous devez spécifier un code identifiant de l''adresse';
    END IF;

    IF _geom IS NULL
    AND _geom_x IS NOT NULL
    AND _geom_y IS NOT NULL THEN
        _geom := ST_MakePoint(_geom_x,_geom_y);
    END IF;

    IF _geom IS NOT NULL THEN
        IF ST_SRID(_geom) = 0 THEN
            _geom := ST_SetSRID(_geom, COALESCE(_geom_srid, _geom_srid_default));
        END IF;
        IF NOT public.is_valid_geometry_in_SRID_bounds(_geom) THEN
            RAISE NOTICE 'Coordonnées en dehors des limites du système de projection : %, SRID %', ST_AsText(_geom), ST_SRID(_geom);
        ELSE
            _address_normalized.geom := ST_Transform(_geom, 3857);
        END IF;
    END IF;

    _address_normalized.level :=
    CASE
        WHEN _address_normalized.complement IS NOT NULL THEN 'L3'
        WHEN _address_normalized.housenumber IS NOT NULL THEN 'NUMERO'
        WHEN _address_normalized.street IS NOT NULL THEN 'VOIE'
        WHEN _address_normalized.municipality_code IS NOT NULL THEN 'ZA'
    END;

    IF _address_normalized.postcode IS NOT NULL
    OR _address_normalized.municipality_code IS NOT NULL
    OR _address_normalized.municipality_name IS NOT NULL
    THEN
        _address_normalized._order_code_area := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name));
        IF _address_normalized.street IS NOT NULL THEN
            _address_normalized._order_code_street := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name, _address_normalized.street));
            IF _address_normalized.housenumber IS NOT NULL THEN
                _address_normalized._order_code_housenumber := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name, _address_normalized.street, _address_normalized.housenumber, _address_normalized.housenumber_extension));
            END IF;
            IF _address_normalized.complement IS NOT NULL THEN
                _address_normalized._order_code_complement := MD5(CONCAT(_address_normalized.postcode, _address_normalized.municipality_code, _address_normalized.municipality_name, _address_normalized.street, _address_normalized.housenumber, _address_normalized.housenumber_extension, _address_normalized.complement));
            END IF;
        END IF;
    END IF;

    /*
    -- calcul mot directeur, si absent
    IF _address_normalized.lb_voie_mot_directeur IS NULL AND _address_normalized.street IS NOT NULL THEN
        _address_normalized.lb_voie_mot_directeur := getVoieMotDirecteur(_address_normalized.street);
    END IF;
     */

    RETURN _address_normalized;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'normalize_municipality_name');
CREATE OR REPLACE FUNCTION fr.normalize_municipality_name(
    code VARCHAR
    , name VARCHAR
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _name VARCHAR;
    _name_normalized VARCHAR;
    _words TEXT[];
    _words_normalized VARCHAR[];
    _word_end VARCHAR;
BEGIN
    -- deal w/ exceptions
    SELECT value
    INTO _name_normalized
    FROM fr.constant
    WHERE
        usecase = 'LAPOSTE_MUNICIPALITY_EXCEPTION'
        AND
        key = code
        ;
    IF FOUND THEN
        return _name_normalized;
    END IF;

    -- only upper and not special characters
    _name := clean_address_label(name);

    -- replace (SAINT|SAINTE)
    IF _name LIKE '% SAINT' OR _name LIKE '% SAINTE' THEN
        -- exception if it's the name itself
        return _name;
    END IF;
    -- as starting word
    IF _name LIKE 'SAINT %' THEN
        _name := CONCAT('ST ', SUBSTR(_name, 7));
    ELSIF _name LIKE 'SAINTE %' THEN
        _name := CONCAT('STE ', SUBSTR(_name, 8));
    END IF;
    -- else anywhere (but at the end)
    return REPLACE(REPLACE(_name, ' SAINTE ', ' STE '), ' SAINT ', ' ST ');

    /* NOTE
     avoid REGEX because too expansive! in run-time
     */
    _words := REGEXP_SPLIT_TO_ARRAY(_name, '\s+');
    FOR _i IN 1..ARRAY_LENGTH(_words, 1) LOOP
        IF _words[_i] ~* '^SAINT[E]?$' THEN
            -- exception if it's the name itself
            IF _i = 2 THEN
                IF _words[_i -1] ~* '^(LE|LA)$' THEN
                    _words_normalized := ARRAY_APPEND(_words_normalized, _words[_i]);
                    CONTINUE;
                END IF;
            END IF;
            _word_end := (REGEXP_MATCH(_words[_i], 'SAINT([E]?)', 'i'))[1];
            _words_normalized := ARRAY_APPEND(_words_normalized, CONCAT('ST', UPPER(_word_end)));
            CONTINUE;
        END IF;
        _words_normalized := ARRAY_APPEND(_words_normalized, _words[_i]);
    END LOOP;

    return ARRAY_TO_STRING(_words_normalized, ' ');
END
$func$ LANGUAGE plpgsql;

/* TEST
-- municipality differences
SELECT *
FROM (
    SELECT
        za.co_insee_commune AS municipality_code
        , c.nom AS name
        , fr.normalize_municipality_name(c.insee_com, c.nom) AS name_normalized
        , za.lb_ach_nn AS name_normalized_laposte
    FROM
        fr.laposte_address_area za
            JOIN fr.ign_municipality c ON za.co_insee_commune = c.insee_com
    WHERE
        za.fl_active
        AND
        za.lb_l5_nn IS NULL
    ) t
WHERE
    name_normalized != name_normalized_laposte
ORDER BY
    1
    ;
 */
