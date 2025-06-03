/***
 * FR-DATAMART
 */

-- build LAPOSTE municipality : list of normalized label exceptions
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_municipality_normalized_label_exception');
CREATE OR REPLACE PROCEDURE fr.set_laposte_municipality_normalized_label_exception()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_area') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_MUNICIPALITY_EXCEPTION';
    INSERT INTO fr.constant (
        SELECT
            'LAPOSTE_MUNICIPALITY_EXCEPTION',
            t.*
        FROM (
            SELECT
                co_insee_commune,
                lb_ach_nn
            FROM fr.laposte_address_area
            WHERE
                fl_active
                AND
                -- difference normalized label w/ delivery one : exception!
                (
                    (lb_nn != lb_ach_nn)
                    OR
                    -- w/o ST|STE : delete article(s)
                    ((LENGTH(lb_in_ext_loc) > 32) AND (lb_in_ext_loc !~ '\mSAINT[E]?\M'))
                )
                AND
                lb_l5_nn IS NULL

            UNION

            SELECT
                co_insee_commune,
                lb_ach_nn
            FROM
                fr.laposte_address_area
                    JOIN fr.insee_municipality
                        ON co_insee_commune = codgeo
            WHERE
                -- some municipality w/ () in its name
                -- ex: 16052 Bors (Canton de Charente-Sud)
                POSITION('(' IN libgeo) > 0

            ORDER BY
                1
        ) t
        WHERE
            -- except municipalities w/ districts (Lyon, Marseille et Paris) and (Polynésie, Nouvelle Calédonie)
            co_insee_commune !~ '^(98|693|751|132)'
    );
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE municipality : nof infra (area)
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_municipality_infra');
CREATE OR REPLACE PROCEDURE fr.set_laposte_municipality_infra(
    municipality_subsection VARCHAR DEFAULT 'ZA',
    location_min INT DEFAULT 4
)
AS
$proc$
DECLARE
    _nrows INT[];
    _n1 INT;
    _n2 INT;
BEGIN
    DROP TABLE IF EXISTS fr.laposte_municipality_infra;
    CREATE TABLE fr.laposte_municipality_infra AS (
        SELECT
            co_insee_commune,
            COUNT(*) n_infra
        FROM
            fr.laposte_address_area
        WHERE
            fl_active
        GROUP BY
            co_insee_commune
    )
    ;
    _nrows[1] := COUNT(*) FROM fr.laposte_municipality_infra WHERE n_infra = 1;
    _nrows[2] := COUNT(*) FROM fr.laposte_municipality_infra WHERE n_infra > 1;

    DROP TABLE fr.laposte_municipality_infra_with_delivery;
    CREATE TABLE IF NOT EXISTS fr.laposte_municipality_infra_with_delivery AS (
        WITH
        municipality_subsection AS (
            SELECT DISTINCT
                a.co_insee_commune,
                a.co_cea subsection
            FROM
                fr.laposte_municipality_infra mi
                    JOIN fr.laposte_address_area a ON mi.co_insee_commune = a.co_insee_commune
            WHERE
                a.fl_active
                AND
                mi.n_infra > 1
        )
        SELECT *
        FROM municipality_subsection ms
        WHERE
            EXISTS(
                SELECT 1
                FROM
                    fr.delivery_point_view dp
                WHERE
                    -- exception: all PDI w/ fl_active FALSE !
                    (
                        (ms.co_insee_commune != '24364' AND fl_active)
                        OR
                        (ms.co_insee_commune = '24364')
                    )
                    AND fl_diffusable
                    AND pdi_etat = 1
                    AND pdi_visible
                    -- at least street-center (=4)
                    AND pdi_no_type_localisation_coord >= location_min
                    AND pdi_coord_native IS NOT NULL
                    AND dp.co_insee_commune = ms.co_insee_commune
                    AND ms.subsection = dp.co_adr_za
            )
    )
    ;

    WITH
    subsection AS (
        SELECT
            co_insee_commune,
            COUNT(*) n_infra
        FROM
            fr.laposte_municipality_infra_with_delivery
        GROUP BY
            co_insee_commune
    )
    SELECT
        SUM(CASE WHEN n_infra = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN n_infra > 1 THEN 1 ELSE 0 END)
    INTO
        _n1,
        _n2
    FROM
        subsection
    ;

    CALL public.log_info(CONCAT('Communes (1-INFRA): ', _nrows[1]));
    CALL public.log_info(CONCAT('Communes (n-INFRA): ', _nrows[2]));
    CALL public.log_info(CONCAT('Communes (n-INFRA  mono-distribuées) : ', _n1));
    CALL public.log_info(CONCAT('Communes (n-INFRA multi-distribuées) : ', _n2));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of types
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_type');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_type()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des types dans le nom des voies');

    CALL public.log_info(' Purge');
    DELETE FROM fr.laposte_address_keyword WHERE "group" = 'TYPE';

    CALL public.log_info(' Initialisation');
    /* NOTE
    lb_type_abrege must be AN32 abbreviation (not NN38) !
     */
    INSERT INTO fr.laposte_address_keyword("group", name, name_abbreviated)
        WITH
        type_with_abbr AS (
            SELECT
                lb_type,
                lb_type_abrege,
                COUNT(*) n
            FROM fr.laposte_address_street
            WHERE
                lb_type IS NOT NULL
                AND
                fl_active
            GROUP BY
                lb_type,
                lb_type_abrege
        ),
        type_with_larger_value AS (
            SELECT
                lb_type,
                FIRST(lb_type_abrege ORDER BY n DESC) lb_type_abrege
            FROM
                type_with_abbr
            GROUP BY
                lb_type
        )
        SELECT
            'TYPE',
            lb_type,
            CASE
                -- no abbreviation !
                WHEN lb_type = 'ABBAYE' THEN NULL
                ELSE lb_type_abrege
                END
        FROM type_with_larger_value
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Types: ', _nrows));

    /*              AN32            NN38
    AGGLOMERATION   AGL   0   1     AGGL
    AUTOROUTE       AUT   0   1     AUTO
    BOUCLE          BCLE  0   1     BOUC
    CARREFOUR       CAR   0   10    CARR
    CHAUSSEE        CHS   0   2     CHAU
    CHEMIN          CHE   0   246   CHEM
    DOMAINE         DOM   0   3     DOMA
    ESCALIER        ESC   0   1     ESCA
    ESPLANADE       ESP   0   10    ESPL
    GALERIE         GAL   0   2     GALE
    MONTEE          MTE   0   3     MONT
    MOULIN          MLN   0   1     MOUL
    PARVIS          PRV   0   2     PARV
    PASSERELLE      PLE   0   1     PASS
    QUARTIER        QUA   0   4     QUAR
    RACCOURCI       RAC   0   1     RACC
    REMPART         REM   0   2     REMP
    RUELLE          RLE   0   2     RUEL
    SENTE           SEN   0   2     SENT
    SENTIER         SEN   0   2     SENT
    TERRASSE        TSSE  0   1     TERR
    TRAVERSE        TRA   0   2     TRAV
    */
    WITH
    correction_abbr AS (
        SELECT *
        FROM (
            VALUES
                ('ANCIEN CHEMIN', 'ANCI CHEMIN'),
                ('ANCIENNE ROUTE', 'ANCI ROUTE'),
                ('CHEMIN VICINAL', 'CHEM VICINAL'),
                ('MAISON FORESTIERE', 'MAIS FORESTIERE'),
                ('PASSAGE A NIVEAU', 'PASS A NIVEAU'),
                ('PETIT CHEMIN', 'PETI CHEMIN'),
                ('PETITE ROUTE', 'PETI ROUTE'),
                ('VIEUX CHEMIN', 'VIEU CHEMIN'),
                ('VIELLE ROUTE', 'VIEL ROUTE'),
                ('AGGLOMERATION', 'AGGL'),
                ('AUTOROUTE', 'AUTO'),
                ('BOUCLE', 'BOUC'),
                ('CARREFOUR', 'CARR'),
                ('CHAUSSEE', 'CHAU'),
                -- ('CHAUSSEES', 'CHAU'),
                ('CHEMIN', 'CHEM'),
                -- ('CHEMINS', 'CHEM'),
                ('DOMAINE', 'DOMA'),
                -- ('DOMAINES', 'DOMA'),
                ('ESCALIER', 'ESCA'),
                -- ('ESCALIERS', 'ESCA'),
                ('ESPLANADE', 'ESPL'),
                ('GALERIE', 'GALE'),
                ('MONTEE', 'MONT'),
                -- ('MONTEES', 'MONT'),
                ('MOULIN', 'MOUL'),
                -- ('MOULINS', 'MOUL'),
                ('PARVIS', 'PARV'),
                ('PASSERELLE', 'PASS'),
                ('QUARTIER', 'QUAR'),
                ('RACCOURCI', 'RACC'),
                ('REMPART', 'REMP'),
                ('RUELLE', 'RUEL'),
                -- ('RUELLES', 'RUEL'),
                ('SENTE', 'SENT'),
                -- ('SENTES', 'SENT'),
                ('SENTIER', 'SENT'),
                -- ('SENTIERS', 'SENT'),
                ('TERRASSE', 'TERR'),
                -- ('TERRASSES', 'TERR'),
                ('TRAVERSE', 'TRAV')
        ) AS x(name, name_abbreviated)
    )
    UPDATE fr.laposte_address_keyword st SET
        name_abbreviated = ca.name_abbreviated
        FROM
            correction_abbr ca
        WHERE
            "group" = 'TYPE'
            AND
            st.name = ca.name
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (abréviation au singulier): ', _nrows));

    WITH
    type_singular_plural AS (
        SELECT
            k1.name type_singular,
            k1.name_abbreviated type_abbr_singular,
            k2.name type_plural,
            k2.name_abbreviated type_abbr_plural
        FROM
            fr.laposte_address_keyword k1
                JOIN fr.laposte_address_keyword k2
                ON k2.name = CONCAT(k1.name, 'S') AND k2.group = k1.group
        WHERE k1.group = 'TYPE'
    )
    UPDATE fr.laposte_address_keyword k SET
        name_abbreviated = sp.type_abbr_singular
        FROM type_singular_plural sp
        WHERE k.name = sp.type_plural
        AND k.name_abbreviated != sp.type_abbr_singular
        AND k.group = 'TYPE'
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (abréviation au pluriel): ', _nrows));

    WITH
    first_word_of_type AS (
        SELECT
            name,
            CASE
                WHEN POSITION(' ' IN name) = 0 THEN NULL
                ELSE SUBSTR(name, 1, POSITION(' ' IN name) -1)
                END first_word
        FROM fr.laposte_address_keyword
        WHERE "group" = 'TYPE'
    ),
    occurs_type AS (
        SELECT
            lb_type name,
            COUNT(*) occurs
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        GROUP BY lb_type
    )
    UPDATE fr.laposte_address_keyword st SET
          first_word = fw.first_word
        , occurs = ot.occurs
        FROM
            first_word_of_type fw,
            occurs_type ot
        WHERE
            "group" = 'TYPE'
            AND
            st.name = fw.name
            AND
            st.name = ot.name
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (premier mot, occurence): ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of firstnames
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_firstname');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_firstname()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des prénoms dans le nom des voies');

    CALL public.log_info(' Purge');
    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.constant (
        SELECT DISTINCT
            'LAPOSTE_STREET_FIRSTNAME',
            mots.mot
        FROM fr.laposte_address_street AS voie_ran
        INNER JOIN LATERAL UNNEST(REGEXP_SPLIT_TO_ARRAY(voie_ran.lb_voie, '\s+'))
            WITH ORDINALITY AS mots(mot, ordre)
            ON TRUE
        INNER JOIN LATERAL UNNEST(STRING_TO_ARRAY(voie_ran.lb_desc, NULL))
            WITH ORDINALITY AS descripteurs(descripteur, ordre)
            ON mots.ordre = descripteurs.ordre AND descripteurs.descripteur = 'P'
        WHERE
            LENGTH(mots.mot) > 1
            AND
            -- not article!
            NOT fr.is_normalized_article(mots.mot)
            AND
            -- fault!
            NOT mots.mot = ANY('{GAY,FLEUR,PARIS,HUTTES,PRIX}')
    );
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Prénoms: ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of names
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_name');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_name()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des noms (avec abbréviation) dans le nom des voies');

    CALL public.log_info(' Purge');
    DELETE FROM fr.laposte_address_keyword WHERE "group" = 'NAME';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_keyword("group", name, name_abbreviated)
        SELECT *
        FROM (
            VALUES
                ('NAME', 'ANCIENS', 'ANC'),
                ('NAME', 'COMBATTANTS', 'COMB')
        ) AS x("group", name, name_abbreviated)
        ;

    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Noms: ', _nrows));

    WITH
    name_occurs AS (
        SELECT
            k.name,
            COUNT(*) occurs
        FROM
            fr.laposte_address_street s,
            fr.laposte_address_keyword k
        WHERE
            s.fl_active
            AND
            k.group = 'NAME'
            AND (
                s.lb_voie ~ CONCAT('^', k.name, ' ')
                OR
                s.lb_voie ~ CONCAT(' ', k.name, ' ')
                OR
                s.lb_voie ~ CONCAT(' ', k.name, '$')
            )
        GROUP BY
            k.name
    )
    --SELECT * FROM name_occurs ORDER BY 1
    UPDATE fr.laposte_address_keyword k SET
        occurs = o.occurs
        FROM name_occurs o
        WHERE
            k.group = 'NAME'
            AND
            k.name = o.name
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (occurence): ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE extension (of housenumber), w/ abbreviated value
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_extension_of_housenumber');
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_ext');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_ext()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_housenumber') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des extensions dans le nom des numéros');

    CALL public.log_info(' Purge');
    DELETE FROM fr.laposte_address_keyword WHERE "group" = 'EXT';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_keyword("group", name, name_abbreviated, first_word)
        SELECT DISTINCT 'EXT', lb_ext, lb_abr_nn, NULL
        FROM fr.laposte_address_housenumber
        WHERE fl_active AND lb_ext IS NOT NULL
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Extensions: ', _nrows));

    WITH
    ext_occurs AS (
        SELECT lb_ext, COUNT(*) n FROM fr.laposte_address_housenumber
        WHERE fl_active AND lb_ext IS NOT NULL
        GROUP BY lb_ext
    )
    UPDATE fr.laposte_address_keyword k SET
        occurs = o.n
        FROM ext_occurs o
        WHERE
            k.group = 'EXT'
            AND
            k.name = o.lb_ext
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (occurence): ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE titles
-- Query returned successfully in 1 min 24 secs.
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_title');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_title()
AS
$proc$
DECLARE
    _set RECORD;
    _words TEXT[];
    _descriptors TEXT[];
    _words_normalized TEXT[];
    _descriptors_normalized TEXT[];
    _abbr_i INT;
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des titres dans le nom des voies');

    CALL public.log_info(' Purge');
    DELETE FROM fr.laposte_address_keyword WHERE "group" = 'TITLE';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_keyword("group", name, name_abbreviated)
        SELECT *
        FROM (
            VALUES
                ('TITLE', 'ABBAYE', 'ABBA'),
                --('TITLE', 'ABBE', NULL),
                ('TITLE', 'ADJUDANT', 'ADJ'),
                ('TITLE', 'AERODROME', 'AER'),
                ('TITLE', 'AEROGARE', NULL),
                ('TITLE', 'AERONAUTIQUE', NULL),
                ('TITLE', 'AEROPORT', NULL),
                ('TITLE', 'AGENCE', NULL),
                ('TITLE', 'AGRICOLE', 'AGRIC'),
                --('TITLE', 'AMIRAL', NULL),
                ('TITLE', 'ANCIEN', 'ANC'),
                ('TITLE', 'ARMEMENT', NULL),
                ('TITLE', 'ARRONDISSEMENT', 'ARR'),
                ('TITLE', 'ASPIRANT', 'ASP'),
                ('TITLE', 'ASSOCIATION', NULL),
                ('TITLE', 'ATELIER', NULL),
                --('TITLE', 'AUTOROUTE', 'AUTO'),
                ('TITLE', 'BAS', NULL),
                ('TITLE', 'BASSE', 'BAS'),
                ('TITLE', 'BASSES', 'BAS'),
                ('TITLE', 'BASTIDE', 'BAST'),
                ('TITLE', 'BATAILLON', 'BTN'),
                ('TITLE', 'BATAILLONS', 'BTN'),
                ('TITLE', 'BATIMENT', NULL),
                ('TITLE', 'BATIMENTS', NULL),
                ('TITLE', 'BOURG', 'BOUR'),
                ('TITLE', 'BUTTE', 'BUTT'),
                ('TITLE', 'CABINET', NULL),
                ('TITLE', 'CAMPAGNE', 'CAMP'),
                --('TITLE', 'CANAL', NULL),
                ('TITLE', 'CANTON', 'CANT'),
                --('TITLE', 'CAPITAINE', NULL),
                ('TITLE', 'CARDINAL', 'CDL'),
                ('TITLE', 'CARREAU', 'CARR'),
                --('TITLE', 'CARREFOUR', 'CARR'),
                ('TITLE', 'CARRIERE', 'CARR'),
                ('TITLE', 'CARRIERES', 'CARR'),
                ('TITLE', 'CASERNE', 'CASR'),
                ('TITLE', 'CAVEE', 'CAVE'),
                ('TITLE', 'CHAMBRE', NULL),
                --('TITLE', 'CHANOINE', NULL),
                ('TITLE', 'CHAPELLE', 'CHAP'),
                ('TITLE', 'CHATEAU', 'CHAT'),
                --('TITLE', 'CHEMIN', 'CHEM'),
                ('TITLE', 'CHEMINS', 'CHEM'),
                ('TITLE', 'CITADELLE', NULL),
                ('TITLE', 'COLLEGE', NULL),
                ('TITLE', 'COLLINE', 'COLL'),
                ('TITLE', 'COLLINES', 'COLL'),
                ('TITLE', 'COLONEL', 'CNL'),
                ('TITLE', 'COLONIE', 'COLO'),
                ('TITLE', 'COMITE', NULL),
                ('TITLE', 'COMMANDANT', 'CDT'),
                ('TITLE', 'COMMERCIAL', 'CIAL'),
                ('TITLE', 'COMMUNAL', 'COM'),
                ('TITLE', 'COMMUNAUX', 'COM'),
                ('TITLE', 'COMMUNE', 'COM'),
                ('TITLE', 'COMPAGNIE', 'CIE'),
                ('TITLE', 'COMPAGNON', NULL),
                ('TITLE', 'COMPAGNONS', 'COMP'),
                ('TITLE', 'COOPERATIVE', 'COOP'),
                ('TITLE', 'COULOIR', NULL),
                ('TITLE', 'COUR', 'COUR'),
                ('TITLE', 'COURS', 'COUR'),
                ('TITLE', 'CROIX', 'CRX'),
                ('TITLE', 'DEPARTEMENTAL', 'DEP'),
                ('TITLE', 'DIGUE', 'DIGU'),
                ('TITLE', 'DIRECTEUR', NULL),
                ('TITLE', 'DIRECTION', 'DIR'),
                ('TITLE', 'DIVISION', 'DIV'),
                ('TITLE', 'DOCTEUR', 'DR'),
                --('TITLE', 'DOMAINE', 'DOMA'),
                ('TITLE', 'ECLUSE', 'ECLU'),
                --('TITLE', 'ECOLE', NULL),
                ('TITLE', 'ECONOMIQUE', 'ECO'),
                ('TITLE', 'ECRIVAINS', 'ECRIV'),
                ('TITLE', 'EGLISE', 'EGLI'),
                ('TITLE', 'ENSEIGNEMENT', NULL),
                ('TITLE', 'ENSEMBLE', NULL),
                ('TITLE', 'ENTREE', 'ENT'),
                ('TITLE', 'ENTREES', NULL),
                ('TITLE', 'ENTREPRISE', NULL),
                ('TITLE', 'EPOUX', NULL),
                --('TITLE', 'ESPLANADE', 'ESPL'),
                ('TITLE', 'ESPLANADES', 'ESPL'),
                ('TITLE', 'ETABLISSEMENT', NULL),
                ('TITLE', 'ETABLISSEMENTS', NULL),
                ('TITLE', 'ETANG', 'ETAN'),
                ('TITLE', 'EVEQUE', NULL),
                ('TITLE', 'FACULTE', NULL),
                ('TITLE', 'FAUBOURG', 'FAUB'),
                ('TITLE', 'FERME', 'FERM'),
                ('TITLE', 'FONTAINE', 'FONT'),
                ('TITLE', 'FORESTIER', NULL),
                ('TITLE', 'FORET', 'FOR'),
                ('TITLE', 'FOSSE', 'FOSS'),
                ('TITLE', 'FOSSES', 'FOSS'),
                ('TITLE', 'FRANCAIS', 'FR'),
                ('TITLE', 'FRANCAISE', 'FR'),
                ('TITLE', 'FUSILIERS', NULL),
                ('TITLE', 'GARENNE', 'GARE'),
                ('TITLE', 'GENDARMERIE', NULL),
                ('TITLE', 'GENERAL', 'GAL'),
                ('TITLE', 'GOUVERNEUR', 'GOUV'),
                ('TITLE', 'GRAND', 'GD'),
                ('TITLE', 'GRANDE', 'GDE'),
                ('TITLE', 'GRANDES', 'GDES'),
                ('TITLE', 'GRANDS', 'GDS'),
                ('TITLE', 'GROUPE', 'GROU'),
                --('TITLE', 'HALAGE', NULL),
                ('TITLE', 'HALLE', 'HALL'),
                ('TITLE', 'HAMEAU', 'HAME'),
                ('TITLE', 'HAMEAUX', 'HAME'),
                ('TITLE', 'HAUT', 'HT'),
                ('TITLE', 'HAUTE', 'HTE'),
                ('TITLE', 'HAUTES', 'HTES'),
                ('TITLE', 'HAUTS', 'HTS'),
                ('TITLE', 'HIPPODROME', 'HIPP'),
                ('TITLE', 'HOPITAL', 'HOP'),
                ('TITLE', 'HOSPICE', NULL),
                ('TITLE', 'HOSPITALIER', NULL),
                ('TITLE', 'HOTEL', 'HOT'),
                --('TITLE', 'ILOT', NULL),
                ('TITLE', 'INFANTERIE', 'INFANT'),
                ('TITLE', 'INFERIEUR', NULL),
                ('TITLE', 'INFERIEURE', NULL),
                ('TITLE', 'INGENIEUR', 'ING'),
                ('TITLE', 'INSPECTEUR', NULL),
                ('TITLE', 'INSTITUT', NULL),
                ('TITLE', 'INTERNATIONAL', NULL),
                ('TITLE', 'INTERNATIONALE', 'INTERN'),
                ('TITLE', 'LIEUTENANT', 'LT'),
                ('TITLE', 'LIEUTENANT DE VAISSEAU', 'LTDV'),
                ('TITLE', 'MADAME', 'MME'),
                ('TITLE', 'MADEMOISELLE', 'MLLE'),
                ('TITLE', 'MAGASIN', NULL),
                --('TITLE', 'MAIRIE', NULL),
                ('TITLE', 'MAISON', 'MAIS'),
                ('TITLE', 'MAITRE', 'ME'),
                --('TITLE', 'MARAIS', NULL),
                ('TITLE', 'MARCHE', 'MARC'),
                ('TITLE', 'MARECHAL', 'MAL'),
                ('TITLE', 'MARITIME', NULL),
                ('TITLE', 'MARTYR', NULL),
                ('TITLE', 'MARTYRS', 'MYR'),
                ('TITLE', 'MEDECIN', 'MED'),
                ('TITLE', 'MEDICAL', 'MED'),
                ('TITLE', 'MESDEMOISELLES', NULL),
                ('TITLE', 'MESSIEURS', NULL),
                ('TITLE', 'MILITAIRE', 'MIL'),
                ('TITLE', 'MONSEIGNEUR', 'MGR'),
                ('TITLE', 'MONSIEUR', 'M'),
                --('TITLE', 'MONTEE', 'MONT'),
                --('TITLE', 'MOULIN', 'MOUL'),
                ('TITLE', 'MOULINS', 'MOUL'),
                ('TITLE', 'MUNICIPAL', 'MUN'),
                ('TITLE', 'MUSEE', 'MUSE'),
                ('TITLE', 'NATIONAL', 'NAL'),
                ('TITLE', 'NOTRE DAME', 'ND'),
                ('TITLE', 'NOUVEAU', 'NOUV'),
                ('TITLE', 'NOUVELLE', 'NOUV'),
                ('TITLE', 'OBSERVATOIRE', NULL),
                ('TITLE', 'PALAIS', 'PALA'),
                ('TITLE', 'PARCS', NULL),
                ('TITLE', 'PARKING', 'PARK'),
                --('TITLE', 'PARVIS', 'PARV'),
                --('TITLE', 'PASSERELLE', 'PASS'),
                ('TITLE', 'PASSERELLES', NULL),
                ('TITLE', 'PASSES', NULL),
                ('TITLE', 'PASTEUR', 'PAST'),
                ('TITLE', 'PAVILLONS', 'PAVI'),
                ('TITLE', 'PETIT', 'PT'),
                ('TITLE', 'PETITE', 'PTE'),
                ('TITLE', 'PETITES', 'PTE'),
                ('TITLE', 'PETITS', 'PT'),
                ('TITLE', 'PLAINE', 'PLAI'),
                ('TITLE', 'PLATEAU', 'PLAT'),
                ('TITLE', 'PLATEAUX', 'PLAT'),
                ('TITLE', 'POINTE', 'POIN'),
                ('TITLE', 'POLICE', 'POL'),
                ('TITLE', 'PORTE', 'PORT'),
                ('TITLE', 'PREFET', NULL),
                ('TITLE', 'PRESIDENT', 'PDT'),
                ('TITLE', 'PRESQU ILE', 'PRES ILE'),
                ('TITLE', 'PROFESSEUR', 'PR'),
                ('TITLE', 'PROLONGE', 'PROL'),
                ('TITLE', 'PROLONGEE', 'PROL'),
                ('TITLE', 'PROPRIETE', NULL),
                --('TITLE', 'QUARTIER', 'QUAR'),
                --('TITLE', 'RACCOURCI', 'RACC'),
                ('TITLE', 'RECTEUR', 'RECT'),
                ('TITLE', 'REGIMENT', 'RGT'),
                ('TITLE', 'REGIONAL', NULL),
                ('TITLE', 'REPUBLIQUE', 'REP'),
                ('TITLE', 'RESIDENCES', 'RESI'),
                ('TITLE', 'RESTAURANT', NULL),
                ('TITLE', 'ROTONDE', 'ROTO'),
                --('TITLE', 'RUELLE', 'RUEL'),
                ('TITLE', 'SAINT', 'ST'),
                ('TITLE', 'SAINTE', 'STE'),
                ('TITLE', 'SAINTES', NULL),
                ('TITLE', 'SAINTS', NULL),
                --('TITLE', 'SENTE', 'SENT'),
                --('TITLE', 'SENTIER', 'SENT'),
                ('TITLE', 'SERGENT', 'SGT'),
                ('TITLE', 'SERVICE', 'SCE'),
                ('TITLE', 'SOCIETE', NULL),
                ('TITLE', 'SOUS PREFET', NULL),
                ('TITLE', 'STADE', 'STAD'),
                ('TITLE', 'STATION', 'STAT'),
                ('TITLE', 'SUPERIEUR', NULL),
                ('TITLE', 'SUPERIEURE', NULL),
                ('TITLE', 'SYNDICAT', NULL),
                ('TITLE', 'TECHNIQUE', NULL),
                ('TITLE', 'TERRAIN', 'TERR'),
                --('TITLE', 'TERRASSES', 'TERR'),
                --('TITLE', 'TRAVERSE', 'TRAV'),
                ('TITLE', 'TUNNEL', 'TUN'),
                ('TITLE', 'UNIVERSITAIRE', 'UNVT'),
                ('TITLE', 'UNIVERSITE', 'UNIV'),
                ('TITLE', 'VALLEE', 'VALL'),
                ('TITLE', 'VALLON', 'VALL'),
                ('TITLE', 'VELODROME', NULL),
                ('TITLE', 'VEUVE', NULL),
                ('TITLE', 'VIEILLE', 'VIEL'),
                ('TITLE', 'VIEILLES', 'VIEL'),
                ('TITLE', 'VIEUX', 'VX'),
                ('TITLE', 'VILLAS', 'VILL')
        ) AS x("group", name, name_abbreviated)
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Titres: ', _nrows));

    -- update first word
    UPDATE fr.laposte_address_keyword kt SET
        first_word = (REGEXP_MATCH(kt.name, '^\S+'))[1]
        WHERE
            kt.group = 'TITLE'
            AND
            count_words(kt.name) > 1
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (premier mot): ', _nrows));

    -- update occurs
    /* NOTE
    this count occurs every where in the name (possibly also for other group, as type)
     */
    WITH
    title_occurs AS (
        SELECT
            k.name,
            COUNT(*) occurs
        FROM
            fr.laposte_address_street s,
            fr.laposte_address_keyword k
        WHERE
            s.fl_active
            AND
            k.group = 'TITLE'
            AND (
                s.lb_voie ~ CONCAT('^', k.name, ' ')
                OR
                s.lb_voie ~ CONCAT(' ', k.name, ' ')
                OR
                s.lb_voie ~ CONCAT(' ', k.name, '$')
            )
        GROUP BY
            k.name
    )
    --SELECT * FROM title_occurs ORDER BY 1
    UPDATE fr.laposte_address_keyword kt SET
        occurs = sto.occurs
        FROM title_occurs sto
        WHERE
            kt.group = 'TITLE'
            AND
            kt.name = sto.name
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (occurence): ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE complement : list of types
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_complement_type');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_type()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_complement') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Gestion des types dans le nom des compléments (L3)');

    CALL public.log_info(' Purge');
    DELETE FROM fr.laposte_address_keyword WHERE "group" ~ 'GROUP[1-3]';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_keyword("group", name, name_abbreviated, occurs)
        WITH
        complement_keyword AS (
            SELECT
                'GROUP3' "group",
                lb_type_groupe3_l3 name,
                lb_abrev_g3_nn abbr,
                COUNT(*) nb
            FROM
                fr.laposte_address_complement
            WHERE
                fl_active
                AND
                lb_type_groupe3_l3 IS NOT NULL
                AND
                /* NOTE
                exceptions due to referential-faults
                so occurs count is not complete for these 2 kw
                 */
                NOT lb_type_groupe3_l3 = ANY('{TOUR,VILLA}')
            GROUP BY
                lb_type_groupe3_l3,
                lb_abrev_g3_nn
            UNION
            SELECT
                'GROUP2' "group",
                lb_type_groupe2_l3,
                lb_abrev_g2_nn,
                COUNT(*)
            FROM
                fr.laposte_address_complement
            WHERE
                fl_active
                AND
                lb_type_groupe2_l3 IS NOT NULL
            GROUP BY
                lb_type_groupe2_l3,
                lb_abrev_g2_nn
            UNION
            SELECT
                'GROUP1' "group",
                lb_type_groupe1_l3,
                lb_abrev_g1_nn,
                COUNT(*)
            FROM
                fr.laposte_address_complement
            WHERE
                fl_active
                AND
                lb_type_groupe1_l3 IS NOT NULL
            GROUP BY
                lb_type_groupe1_l3,
                lb_abrev_g1_nn
        )
        SELECT "group", name, abbr, nb FROM complement_keyword
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Types: ', _nrows));

    WITH
    first_word_of_type AS (
        SELECT
            name,
            CASE
                WHEN POSITION(' ' IN name) = 0 THEN NULL
                ELSE SUBSTR(name, 1, POSITION(' ' IN name) -1)
                END first_word
        FROM fr.laposte_address_keyword
        WHERE "group" ~ 'GROUP[1-3]'
    )
    UPDATE fr.laposte_address_keyword k SET
        first_word = fw.first_word
        FROM
            first_word_of_type fw
        WHERE
            k.group ~ 'GROUP[1-3]'
            AND
            k.name = fw.name
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Mises à jour (premier mot): ', _nrows));
END;
$proc$ LANGUAGE plpgsql;

/* TEST
08:59:46.360 Gestion des types dans le nom des compléments (L3)
08:59:46.361  Purge
08:59:46.361  Initialisation
08:59:47.136  Types: 29
08:59:47.142  Mises à jour (premier mot): 29

Query returned successfully in 801 msec.
 */

-- prepare DATAMART data (from LAPOSTE addresses)
SELECT public.drop_all_functions_if_exists('fr', 'set_datamart_address');
CREATE OR REPLACE PROCEDURE fr.set_datamart_address()
AS
$proc$
DECLARE
    _listof INT[];
BEGIN
    /* NOTE
     evaluate infra by municipality (but not only address domain)
     set_datamart_distribution() ?

     perhaps good idea would be to correct DISTRIBUTION/DELIVERY faults
     as 24364 municipality w/ all points inactive!
     */
    CALL fr.set_laposte_municipality_infra();

    -- MUNICIPALITY -----------------------------------------------------------

    CALL fr.set_laposte_municipality_normalized_label_exception();

    -- STREET -----------------------------------------------------------------

    -- build street-dictionary
    CALL fr.set_laposte_address_street_uniq(
        set_case => 'DICTIONARY'
    );
    -- and links dictionary w/ referential
    CALL fr.set_laposte_address_street_reference();
    -- and membership of words
    CALL fr.set_laposte_address_street_membership(
        set_case => 'CREATION'
    );

    -- link-faults (address table w/ links-element)
    CALL fr.fix_laposte_address_fault_links();

    -- street-faults (part/1)
    CALL fr.set_laposte_address_fault(element => 'STREET');
    CALL fr.fix_laposte_address_fault_street(
        element => 'STREET',
        fault => 'BAD_SPACE,DUPLICATE_WORD,WITH_ABBREVIATION,TYPO_ERROR'
    );

    -- following street-faults fixes, have to fix membership too!
    _listof := ARRAY(
        SELECT DISTINCT
            f.name_id
        FROM
            fr.laposte_address_fault f,
            fr.constant c
        WHERE
            c.usecase = 'LAPOSTE_ADDRESS_FAULT_STREET'
            AND
            c.key = ANY('{DUPLICATE_WORD,TYPO_ERROR}')
            AND
            f.element = 'STREET'
            AND
            f.fault_id = c.value::INT
    );
    IF _listof IS NOT NULL THEN
        CALL fr.set_laposte_address_street_membership(
            set_case => 'CORRECTION',
            listof => _listof
        );
    END IF;

    -- classify all words, by (descriptor, level)
    CALL fr.set_laposte_address_street_word_descriptor();
    CALL fr.set_laposte_address_street_word_level();
    -- define keywords (type, title, ...) and exceptions
    CALL fr.set_laposte_address_street_type();
    CALL fr.set_laposte_address_street_ext();
    CALL fr.set_laposte_address_street_title();
    CALL fr.set_laposte_address_street_firstname();
    CALL fr.set_laposte_address_street_kw_exception();

    -- once all fixed street-faults:
    -- set attributs in dictionary
    CALL fr.set_laposte_address_street_uniq(
        set_case => 'ATTRIBUTS'
    );
    -- street-faults (part/2)
    CALL fr.fix_laposte_address_fault(
        element => 'STREET',
        fault => 'DESCRIPTORS,TYPE'
    );

    -- HOUSENUMBER ------------------------------------------------------------

    -- build housenumber-dictionary
    CALL fr.set_laposte_address_housenumber_uniq();
    -- and links dictionary w/ referential
    CALL fr.set_laposte_address_housenumber_reference();

    -- COMPLEMENT -------------------------------------------------------------

    -- build complement-dictionary
    CALL fr.set_laposte_address_complement_uniq(
        set_case => 'DICTIONARY'
    );
    -- and links dictionary w/ referential
    CALL fr.set_laposte_address_complement_reference();
    -- and membership of words
    CALL fr.set_laposte_address_complement_membership(
        set_case => 'CREATION'
    );

    -- complement-faults (part/1)
    CALL fr.set_laposte_address_fault(element => 'COMPLEMENT');
    CALL fr.fix_laposte_address_fault_street(
        element => 'COMPLEMENT',
        fault => 'BAD_SPACE,DUPLICATE_WORD,WITH_ABBREVIATION,TYPO_ERROR'
    );

    -- following complement-faults fixes, have to fix membership too!
    _listof := ARRAY(
        SELECT DISTINCT
            f.name_id
        FROM
            fr.laposte_address_fault f,
            fr.constant c
        WHERE
            c.usecase = 'LAPOSTE_ADDRESS_FAULT_COMPLEMENT'
            AND
            c.key = ANY('{DUPLICATE_WORD,TYPO_ERROR}')
            AND
            f.element = 'COMPLEMENT'
            AND
            f.fault_id = c.value::INT
    );
    IF _listof IS NOT NULL THEN
        CALL fr.set_laposte_address_complement_membership(
            set_case => 'CORRECTION',
            listof => _listof
        );
    END IF;

    -- classify all words, by (descriptor, level)
    CALL fr.set_laposte_address_complement_word_descriptor();
    CALL fr.set_laposte_address_complement_word_level();
    -- define keywords (type)
    CALL fr.set_laposte_address_complement_type();

    -- once all fixed complement-faults:
    -- set attributs in dictionary
    CALL fr.set_laposte_address_complement_uniq(
        set_case => 'ATTRIBUTS'
    );
    -- street-faults (part/2)
    CALL fr.fix_laposte_address_fault(
        element => 'COMPLEMENT',
        fault => 'DESCRIPTORS'
    );
END;
$proc$ LANGUAGE plpgsql;
