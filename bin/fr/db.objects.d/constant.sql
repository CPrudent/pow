/***
 * FR-CONSTANT
 */

CREATE TABLE IF NOT EXISTS fr.constant (
    usecase CHARACTER VARYING NOT NULL,
    key VARCHAR NOT NULL,
    value VARCHAR
);

DO $$
BEGIN
    IF column_exists('fr', 'constant', 'list') THEN
        ALTER TABLE fr.constant RENAME COLUMN "list" TO usecase;
        DROP INDEX IF EXISTS ix_constant_list_key;
    END IF;
END $$;

SELECT drop_all_functions_if_exists('fr', 'set_constant_index');
CREATE OR REPLACE PROCEDURE fr.set_constant_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_constant_usecase_key ON fr.constant (usecase, key);
END
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_territory_overseas');
CREATE OR REPLACE PROCEDURE fr.set_territory_overseas()
AS
$proc$
BEGIN
    DELETE FROM fr.constant WHERE usecase = 'TERRITORY_OVERSEAS_NAME';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        ('TERRITORY_OVERSEAS_NAME', '97501', 'Miquelon-Langlade'),
        ('TERRITORY_OVERSEAS_NAME', '97502', 'Saint-Pierre'),
        ('TERRITORY_OVERSEAS_NAME', '97701', 'Saint-Barthélemy'),
        ('TERRITORY_OVERSEAS_NAME', '97801', 'Saint-Martin'),
        ('TERRITORY_OVERSEAS_NAME', '98714', 'Bora-Bora'),
        ('TERRITORY_OVERSEAS_NAME', '98718', 'Fatu-Hiva'),
        ('TERRITORY_OVERSEAS_NAME', '98723', 'Hiva-Oa'),
        ('TERRITORY_OVERSEAS_NAME', '98729', 'Moorea-Maiao'),
        ('TERRITORY_OVERSEAS_NAME', '98731', 'Nuku-Hiva'),
        ('TERRITORY_OVERSEAS_NAME', '98747', 'Taiarapu-Est'),
        ('TERRITORY_OVERSEAS_NAME', '98748', 'Taiarapu-Ouest'),
        ('TERRITORY_OVERSEAS_NAME', '98756', 'Ua-Huka'),
        ('TERRITORY_OVERSEAS_NAME', '98757', 'Ua-Pou'),
        ('TERRITORY_OVERSEAS_NAME', '98801', 'Bélep'),
        ('TERRITORY_OVERSEAS_NAME', '98805', 'Dumbéa'),
        ('TERRITORY_OVERSEAS_NAME', '98807', 'Hienghène'),
        ('TERRITORY_OVERSEAS_NAME', '98808', 'Houaïlou'),
        ('TERRITORY_OVERSEAS_NAME', '98809', 'Île des Pins'),
        ('TERRITORY_OVERSEAS_NAME', '98810', 'Kaala-Gomen'),
        ('TERRITORY_OVERSEAS_NAME', '98811', 'Koné'),
        ('TERRITORY_OVERSEAS_NAME', '98815', 'Maré'),
        ('TERRITORY_OVERSEAS_NAME', '98817', 'Mont-Dore'),
        ('TERRITORY_OVERSEAS_NAME', '98819', 'Ouégoa'),
        ('TERRITORY_OVERSEAS_NAME', '98820', 'Ouvéa'),
        ('TERRITORY_OVERSEAS_NAME', '98821', 'Païta'),
        ('TERRITORY_OVERSEAS_NAME', '98822', 'Poindimié'),
        ('TERRITORY_OVERSEAS_NAME', '98823', 'Ponérihouen'),
        ('TERRITORY_OVERSEAS_NAME', '98824', 'Pouébo'),
        ('TERRITORY_OVERSEAS_NAME', '98828', 'Sarraméa'),
        ('TERRITORY_OVERSEAS_NAME', '98832', 'Yaté'),

        ('TERRITORY_OVERSEAS_NAME', '9871', 'Îles Marquises'),
        ('TERRITORY_OVERSEAS_NAME', '9872', 'Îles Tuamotu-Gambier'),
        ('TERRITORY_OVERSEAS_NAME', '9873', 'Îles du Vent'),
        ('TERRITORY_OVERSEAS_NAME', '9874', 'Îles Sous-le-Vent'),
        ('TERRITORY_OVERSEAS_NAME', '9875', 'Îles Australes'),
        ('TERRITORY_OVERSEAS_NAME', '9881', 'Province Sud'),
        ('TERRITORY_OVERSEAS_NAME', '9882', 'Province Nord'),
        ('TERRITORY_OVERSEAS_NAME', '9883', 'Îles Loyauté'),

        ('TERRITORY_OVERSEAS_NAME', '975', 'Saint-Pierre-et-Miquelon'),
        ('TERRITORY_OVERSEAS_NAME', '977', 'Saint-Barthélemy'),
        ('TERRITORY_OVERSEAS_NAME', '978', 'Saint-Martin'),
        ('TERRITORY_OVERSEAS_NAME', '986', 'Wallis et Futuna'),
        ('TERRITORY_OVERSEAS_NAME', '987', 'Polynésie française'),
        ('TERRITORY_OVERSEAS_NAME', '988', 'Nouvelle Calédonie'),
        ('TERRITORY_OVERSEAS_NAME', '989', 'Île de Clipperton'),

        ('TERRITORY_OVERSEAS_NAME', '97', 'Îles en Atlantique'),
        ('TERRITORY_OVERSEAS_NAME', '98', 'Îles en Pacifique')
    ;

    DELETE FROM fr.constant WHERE usecase = 'TERRITORY_OVERSEAS_RELATION';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        --9871 Îles Marquises
        ('TERRITORY_OVERSEAS_RELATION', '9871', '98718'),
        ('TERRITORY_OVERSEAS_RELATION', '9871', '98723'),
        ('TERRITORY_OVERSEAS_RELATION', '9871', '98731'),
        ('TERRITORY_OVERSEAS_RELATION', '9871', '98746'),
        ('TERRITORY_OVERSEAS_RELATION', '9871', '98756'),
        ('TERRITORY_OVERSEAS_RELATION', '9871', '98757'),
        --9872 Îles Tuamotu-Gambier
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98711'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98713'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98716'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98717'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98719'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98720'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98721'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98726'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98727'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98730'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98732'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98737'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98740'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98742'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98749'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98751'),
        ('TERRITORY_OVERSEAS_RELATION', '9872', '98755'),
        --9873 Îles du Vent
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98729'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98712'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98715'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98722'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98725'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98733'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98734'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98735'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98736'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98738'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98747'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98748'),
        ('TERRITORY_OVERSEAS_RELATION', '9873', '98752'),
        --9874 Îles Sous-le-Vent
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98714'),
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98724'),
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98728'),
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98745'),
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98750'),
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98754'),
        ('TERRITORY_OVERSEAS_RELATION', '9874', '98758'),
        --9875 Îles Australes
        ('TERRITORY_OVERSEAS_RELATION', '9875', '98739'),
        ('TERRITORY_OVERSEAS_RELATION', '9875', '98741'),
        ('TERRITORY_OVERSEAS_RELATION', '9875', '98743'),
        ('TERRITORY_OVERSEAS_RELATION', '9875', '98744'),
        ('TERRITORY_OVERSEAS_RELATION', '9875', '98753'),

        --9881 Province Sud
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98829'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98832'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98809'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98817'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98818'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98805'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98821'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98802'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98813'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98828'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98806'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98816'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98803'),
        ('TERRITORY_OVERSEAS_RELATION', '9881', '98827'),  -- 'SUD'
        --9882 Province Nord
        --('TERRITORY_OVERSEAS_RELATION', '9882', '98827') -- 'NORD' !
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98825'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98811'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98831'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98810'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98812'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98826'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98801'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98819'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98824'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98807'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98830'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98822'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98823'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98808'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98833'),
        ('TERRITORY_OVERSEAS_RELATION', '9882', '98804'),
        --9883 Îles Loyauté
        ('TERRITORY_OVERSEAS_RELATION', '9883', '98820'),
        ('TERRITORY_OVERSEAS_RELATION', '9883', '98814'),
        ('TERRITORY_OVERSEAS_RELATION', '9883', '98815')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_fault_list');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_fault_list()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase ~ '^LAPOSTE_ADDRESS_FAULT_';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        ('LAPOSTE_ADDRESS_FAULT_LINK', 'COMPLEMENT_WITH_STREET_ERROR', '0'),

        ('LAPOSTE_ADDRESS_FAULT_AREA', 'BAD_SPACE', '100'),

        ('LAPOSTE_ADDRESS_FAULT_STREET', 'BAD_SPACE', '200'),
        ('LAPOSTE_ADDRESS_FAULT_STREET', 'DUPLICATE_WORD', '201'),
        ('LAPOSTE_ADDRESS_FAULT_STREET', 'WITH_ABBREVIATION', '202'),
        ('LAPOSTE_ADDRESS_FAULT_STREET', 'TYPO_ERROR', '203'),
        ('LAPOSTE_ADDRESS_FAULT_STREET', 'DESCRIPTORS', '204'),
        ('LAPOSTE_ADDRESS_FAULT_STREET', 'TYPE', '205'),

        ('LAPOSTE_ADDRESS_FAULT_HOUSENUMBER', 'BAD_NUMBER', '300'),
        ('LAPOSTE_ADDRESS_FAULT_HOUSENUMBER', 'BAD_EXTENSION', '301'),

        ('LAPOSTE_ADDRESS_FAULT_COMPLEMENT', 'BAD_SPACE', '400'),
        ('LAPOSTE_ADDRESS_FAULT_COMPLEMENT', 'DUPLICATE_WORD', '401'),
        ('LAPOSTE_ADDRESS_FAULT_COMPLEMENT', 'WITH_ABBREVIATION', '402'),
        ('LAPOSTE_ADDRESS_FAULT_COMPLEMENT', 'TYPO_ERROR', '403'),
        ('LAPOSTE_ADDRESS_FAULT_COMPLEMENT', 'DESCRIPTORS', '404')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_fault_exception');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_fault_exception()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant
        WHERE usecase = 'LAPOSTE_ADDRESS_FAULT_EXCEPTION';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        -- true double words!
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'AH'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'BADEN'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'BIN'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'BLIN'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'BORA'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'BOUTSI'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'CACHE'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'CAI'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'CASSE'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'COLLES'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'COTTE'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'CRI'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'CUIS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'FOU'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'FROUS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'GABA'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'HA'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'JEAN'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'HOURA'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'MOUCOU'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'MOUKOUS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'NOEL'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PAUL'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PEUT'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PHI'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PIC'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PILI'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PITE'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PIOU'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'POC'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'POUET'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'POUSSE'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'PRIS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'RENE'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'SOEURS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'QUIN'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TCHA'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TCHAT'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TECS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TRIN'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TUIS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TUIT'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'TUITS'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'VALA'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'YLANG'),
        ('LAPOSTE_ADDRESS_FAULT_EXCEPTION', 'DUPLICATE_WORD', 'YLANGS')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_address_constant');
CREATE OR REPLACE PROCEDURE fr.set_address_constant()
AS
$proc$
BEGIN
    DELETE FROM fr.constant WHERE usecase = 'FR_ADDRESS';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        ('FR_ADDRESS', 'EPCI_KIND', 'METRO|MET69|CA|CC|CU'),
        -- Lyon, Marseille, Paris
        ('FR_ADDRESS', 'MUNICIPALITY_DISTRICT', '69123|13055|75056')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_match_iris_usecase');
CREATE OR REPLACE PROCEDURE fr.set_match_iris_usecase()
AS
$proc$
BEGIN
    DELETE FROM fr.constant WHERE usecase = 'FR_MATCH_IRIS';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        ('FR_MATCH_IRIS', 'NO_POLYGON', '0'),
        ('FR_MATCH_IRIS', 'SINGLE_POLYGON', '1'),
        ('FR_MATCH_IRIS', 'TOO_LOW_POINT_PRECISION_AND_MULTIPLE_POLYGONS', '2'),
        ('FR_MATCH_IRIS', 'NEAR_POLYGON', '3'),
        ('FR_MATCH_IRIS', 'TOO_MANY_NEAR_POLYGONS', '4'),
        ('FR_MATCH_IRIS', 'NO_POLYGON_FOUND', '5')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_global_variables');
CREATE OR REPLACE PROCEDURE fr.set_global_variables()
AS
$proc$
BEGIN
    --ALTER DATABASE pow RESET ALL;
    --ALTER DATABASE pow RESET <conf_key>;

    -- reset obsolete values
    ALTER DATABASE pow RESET fr.similarity.area.threshold;
    ALTER DATABASE pow RESET fr.similarity.street.threshold;
    ALTER DATABASE pow RESET fr.similarity.complement.threshold;
    /*
    ALTER DATABASE pow SET fr.similarity.area.threshold = '0.5';
    ALTER DATABASE pow SET fr.similarity.street.threshold = '0.7';
    ALTER DATABASE pow SET fr.similarity.complement.threshold = '0.7';
     */

    -- matching status values
    ALTER DATABASE pow SET fr.status.match.strict = 'OK_1';
    ALTER DATABASE pow SET fr.status.match.near = 'OK_2';
    ALTER DATABASE pow SET fr.status.match.not_found = 'KO_10';
    ALTER DATABASE pow SET fr.status.match.not_near = 'KO_11';
    ALTER DATABASE pow SET fr.status.match.too_similar = 'KO_12';
    ALTER DATABASE pow SET fr.status.match.too_many = 'KO_13';

    -- weighted criteria values (better word)
    ALTER DATABASE pow SET fr.weight.match.similarity = '6';
    ALTER DATABASE pow SET fr.weight.match.rarity = '2';
    ALTER DATABASE pow SET fr.weight.match.descriptor = '3';

    -- threshold level/descriptor values
    ALTER DATABASE pow SET fr.threshold.match.area = '0.5';
    ALTER DATABASE pow SET fr.threshold.match.street = '0.7';
    ALTER DATABASE pow SET fr.threshold.match.complement = '0.5';
    ALTER DATABASE pow SET fr.threshold.match.a = '0';
    ALTER DATABASE pow SET fr.threshold.match.c = '0.7';
    ALTER DATABASE pow SET fr.threshold.match.e = '0.3';
    ALTER DATABASE pow SET fr.threshold.match.n = '0.7';
    ALTER DATABASE pow SET fr.threshold.match.p = '0.5';
    ALTER DATABASE pow SET fr.threshold.match.t = '0.6';
    ALTER DATABASE pow SET fr.threshold.match.v = '0.3';
    ALTER DATABASE pow SET fr.threshold.match.g = '0.3';
    ALTER DATABASE pow SET fr.threshold.match.h = '0.3';
    ALTER DATABASE pow SET fr.threshold.match.i = '0.3';

    -- ratio level values
    ALTER DATABASE pow SET fr.similarity.area.ratio = '0.1';
    ALTER DATABASE pow SET fr.similarity.street.ratio = '0.15';
    ALTER DATABASE pow SET fr.similarity.complement.ratio = '0.15';

    -- uncommon max values
    ALTER DATABASE pow SET fr.max.street.occurs = '10';
    ALTER DATABASE pow SET fr.max.housenumber.occurs = '1';
    ALTER DATABASE pow SET fr.max.complement.occurs = '10';
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_constant_address');
CREATE OR REPLACE PROCEDURE fr.set_constant_address()
AS
$proc$
BEGIN
    SELECT public.drop_table_indexes('fr', 'constant');
    CALL fr.set_laposte_address_fault_list();
    CALL fr.set_laposte_address_fault_exception();

    CALL fr.set_territory_overseas();

    CALL fr.set_address_constant();
    CALL fr.set_match_iris_usecase();
    CALL fr.set_global_variables();
    CALL fr.set_constant_index();
END;
$proc$ LANGUAGE plpgsql;
