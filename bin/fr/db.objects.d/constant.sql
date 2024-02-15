/***
 * FR-CONSTANTS
 */

CREATE TABLE IF NOT EXISTS fr.constant (
    usecase CHARACTER VARYING NOT NULL
    , key VARCHAR NOT NULL
    , value VARCHAR
);

DO $$
BEGIN
    IF column_exists('fr', 'constant', 'list') THEN
        ALTER TABLE fr.constant RENAME COLUMN "list" TO usecase;
        DROP INDEX IF EXISTS ix_constant_list_key;
    END IF;

    IF table_exists('fr', 'laposte_street_type') AND NOT table_exists('fr', 'laposte_address_street_type') THEN
        ALTER TABLE fr.laposte_street_type RENAME TO laposte_address_street_type;
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
    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'TYPE';

    CALL public.log_info(' Initialisation');
    /* NOTE
    lb_type_abrege must be AN32 abbreviation (not NN38) !
     */
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated)
        WITH
        type_with_abbr AS (
            SELECT
                lb_type
                , lb_type_abrege
                , COUNT(*) n
            FROM fr.laposte_address_street
            WHERE
                lb_type IS NOT NULL
                AND
                fl_active
            GROUP BY
                lb_type
                , lb_type_abrege
        )
        , type_with_larger_value AS (
            SELECT
                lb_type
                , FIRST(lb_type_abrege ORDER BY n DESC) lb_type_abrege
            FROM
                type_with_abbr
            GROUP BY
                lb_type
        )
        SELECT
            'TYPE'
            , lb_type
            , CASE
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
                  ('ANCIEN CHEMIN', 'ANCI CHEMIN')
                , ('ANCIENNE ROUTE', 'ANCI ROUTE')
                , ('CHEMIN VICINAL', 'CHEM VICINAL')
                , ('MAISON FORESTIERE', 'MAIS FORESTIERE')
                , ('PASSAGE A NIVEAU', 'PASS A NIVEAU')
                , ('PETIT CHEMIN', 'PETI CHEMIN')
                , ('PETITE ROUTE', 'PETI ROUTE')
                , ('VIEUX CHEMIN', 'VIEU CHEMIN')
                , ('VIELLE ROUTE', 'VIEL ROUTE')
                , ('AGGLOMERATION', 'AGGL')
                , ('AUTOROUTE', 'AUTO')
                , ('BOUCLE', 'BOUC')
                , ('CARREFOUR', 'CARR')
                , ('CHAUSSEE', 'CHAU')
                --, ('CHAUSSEES', 'CHAU')
                , ('CHEMIN', 'CHEM')
                --, ('CHEMINS', 'CHEM')
                , ('DOMAINE', 'DOMA')
                --, ('DOMAINES', 'DOMA')
                , ('ESCALIER', 'ESCA')
                --, ('ESCALIERS', 'ESCA')
                , ('ESPLANADE', 'ESPL')
                , ('GALERIE', 'GALE')
                , ('MONTEE', 'MONT')
                --, ('MONTEES', 'MONT')
                , ('MOULIN', 'MOUL')
                --, ('MOULINS', 'MOUL')
                , ('PARVIS', 'PARV')
                , ('PASSERELLE', 'PASS')
                , ('QUARTIER', 'QUAR')
                , ('RACCOURCI', 'RACC')
                , ('REMPART', 'REMP')
                , ('RUELLE', 'RUEL')
                --, ('RUELLES', 'RUEL')
                , ('SENTE', 'SENT')
                --, ('SENTES', 'SENT')
                , ('SENTIER', 'SENT')
                --, ('SENTIERS', 'SENT')
                , ('TERRASSE', 'TERR')
                --, ('TERRASSES', 'TERR')
                , ('TRAVERSE', 'TRAV')
        ) AS x(name, name_abbreviated)
    )
    UPDATE fr.laposte_address_street_keyword st SET
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
            k1.name type_singular
            , k1.name_abbreviated type_abbr_singular
            , k2.name type_plural
            , k2.name_abbreviated type_abbr_plural
        FROM
            fr.laposte_address_street_keyword k1
                JOIN fr.laposte_address_street_keyword k2
                ON k2.name = CONCAT(k1.name, 'S') AND k2.group = k1.group
        WHERE k1.group = 'TYPE'
    )
    UPDATE fr.laposte_address_street_keyword k SET
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
            name
            , CASE
                WHEN POSITION(' ' IN name) = 0 THEN NULL
                ELSE SUBSTR(name, 1, POSITION(' ' IN name) -1)
                END first_word
        FROM fr.laposte_address_street_keyword
        WHERE "group" = 'TYPE'
    )
    , occurs_type AS (
        SELECT
            lb_type name
            , COUNT(*) occurs
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        GROUP BY lb_type
    )
    UPDATE fr.laposte_address_street_keyword st SET
        first_word = fw.first_word
        , occurs = ot.occurs
        FROM
            first_word_of_type fw
            , occurs_type ot
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
            'LAPOSTE_STREET_FIRSTNAME'
            , mots.mot
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
    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'NAME';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated)
        SELECT *
        FROM (
            VALUES
                  ('NAME', 'ANCIENS', 'ANC')
                , ('NAME', 'COMBATTANTS', 'COMB')
        ) AS x("group", name, name_abbreviated)
        ;

    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Noms: ', _nrows));

    WITH
    name_occurs AS (
        SELECT
            k.name
            , COUNT(*) occurs
        FROM
            fr.laposte_address_street s
            , fr.laposte_address_street_keyword k
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
    UPDATE fr.laposte_address_street_keyword k SET
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
    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'EXT';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated, first_word)
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
    UPDATE fr.laposte_address_street_keyword k SET
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
    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'TITLE';

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated)
        SELECT *
        FROM (
            VALUES
                  ('TITLE', 'ABBAYE', 'ABBA')
                --, ('TITLE', 'ABBE', NULL)
                , ('TITLE', 'ADJUDANT', 'ADJ')
                , ('TITLE', 'AERODROME', 'AER')
                , ('TITLE', 'AEROGARE', NULL)
                , ('TITLE', 'AERONAUTIQUE', NULL)
                , ('TITLE', 'AEROPORT', NULL)
                , ('TITLE', 'AGENCE', NULL)
                , ('TITLE', 'AGRICOLE', 'AGRIC')
                --, ('TITLE', 'AMIRAL', NULL)
                , ('TITLE', 'ANCIEN', 'ANC')
                , ('TITLE', 'ARMEMENT', NULL)
                , ('TITLE', 'ARRONDISSEMENT', 'ARR')
                , ('TITLE', 'ASPIRANT', 'ASP')
                , ('TITLE', 'ASSOCIATION', NULL)
                , ('TITLE', 'ATELIER', NULL)
                --, ('TITLE', 'AUTOROUTE', 'AUTO')
                , ('TITLE', 'BAS', NULL)
                , ('TITLE', 'BASSE', 'BAS')
                , ('TITLE', 'BASSES', 'BAS')
                , ('TITLE', 'BASTIDE', 'BAST')
                , ('TITLE', 'BATAILLON', 'BTN')
                , ('TITLE', 'BATAILLONS', 'BTN')
                , ('TITLE', 'BATIMENT', NULL)
                , ('TITLE', 'BATIMENTS', NULL)
                , ('TITLE', 'BOURG', 'BOUR')
                , ('TITLE', 'BUTTE', 'BUTT')
                , ('TITLE', 'CABINET', NULL)
                , ('TITLE', 'CAMPAGNE', 'CAMP')
                --, ('TITLE', 'CANAL', NULL)
                , ('TITLE', 'CANTON', 'CANT')
                --, ('TITLE', 'CAPITAINE', NULL)
                , ('TITLE', 'CARDINAL', 'CDL')
                , ('TITLE', 'CARREAU', 'CARR')
                --, ('TITLE', 'CARREFOUR', 'CARR')
                , ('TITLE', 'CARRIERE', 'CARR')
                , ('TITLE', 'CARRIERES', 'CARR')
                , ('TITLE', 'CASERNE', 'CASR')
                , ('TITLE', 'CAVEE', 'CAVE')
                , ('TITLE', 'CHAMBRE', NULL)
                --, ('TITLE', 'CHANOINE', NULL)
                , ('TITLE', 'CHAPELLE', 'CHAP')
                , ('TITLE', 'CHATEAU', 'CHAT')
                --, ('TITLE', 'CHEMIN', 'CHEM')
                , ('TITLE', 'CHEMINS', 'CHEM')
                , ('TITLE', 'CITADELLE', NULL)
                , ('TITLE', 'COLLEGE', NULL)
                , ('TITLE', 'COLLINE', 'COLL')
                , ('TITLE', 'COLLINES', 'COLL')
                , ('TITLE', 'COLONEL', 'CNL')
                , ('TITLE', 'COLONIE', 'COLO')
                , ('TITLE', 'COMITE', NULL)
                , ('TITLE', 'COMMANDANT', 'CDT')
                , ('TITLE', 'COMMERCIAL', 'CIAL')
                , ('TITLE', 'COMMUNAL', 'COM')
                , ('TITLE', 'COMMUNAUX', 'COM')
                , ('TITLE', 'COMMUNE', 'COM')
                , ('TITLE', 'COMPAGNIE', 'CIE')
                , ('TITLE', 'COMPAGNON', NULL)
                , ('TITLE', 'COMPAGNONS', 'COMP')
                , ('TITLE', 'COOPERATIVE', 'COOP')
                , ('TITLE', 'COULOIR', NULL)
                , ('TITLE', 'COUR', 'COUR')
                , ('TITLE', 'COURS', 'COUR')
                , ('TITLE', 'CROIX', 'CRX')
                , ('TITLE', 'DEPARTEMENTAL', 'DEP')
                , ('TITLE', 'DIGUE', 'DIGU')
                , ('TITLE', 'DIRECTEUR', NULL)
                , ('TITLE', 'DIRECTION', 'DIR')
                , ('TITLE', 'DIVISION', 'DIV')
                , ('TITLE', 'DOCTEUR', 'DR')
                --, ('TITLE', 'DOMAINE', 'DOMA')
                , ('TITLE', 'ECLUSE', 'ECLU')
                --, ('TITLE', 'ECOLE', NULL)
                , ('TITLE', 'ECONOMIQUE', 'ECO')
                , ('TITLE', 'ECRIVAINS', 'ECRIV')
                , ('TITLE', 'EGLISE', 'EGLI')
                , ('TITLE', 'ENSEIGNEMENT', NULL)
                , ('TITLE', 'ENSEMBLE', NULL)
                , ('TITLE', 'ENTREE', 'ENT')
                , ('TITLE', 'ENTREES', NULL)
                , ('TITLE', 'ENTREPRISE', NULL)
                , ('TITLE', 'EPOUX', NULL)
                --, ('TITLE', 'ESPLANADE', 'ESPL')
                , ('TITLE', 'ESPLANADES', 'ESPL')
                , ('TITLE', 'ETABLISSEMENT', NULL)
                , ('TITLE', 'ETABLISSEMENTS', NULL)
                , ('TITLE', 'ETANG', 'ETAN')
                , ('TITLE', 'EVEQUE', NULL)
                , ('TITLE', 'FACULTE', NULL)
                , ('TITLE', 'FAUBOURG', 'FAUB')
                , ('TITLE', 'FERME', 'FERM')
                , ('TITLE', 'FONTAINE', 'FONT')
                , ('TITLE', 'FORESTIER', NULL)
                , ('TITLE', 'FORET', 'FOR')
                , ('TITLE', 'FOSSE', 'FOSS')
                , ('TITLE', 'FOSSES', 'FOSS')
                , ('TITLE', 'FRANCAIS', 'FR')
                , ('TITLE', 'FRANCAISE', 'FR')
                , ('TITLE', 'FUSILIERS', NULL)
                , ('TITLE', 'GARENNE', 'GARE')
                , ('TITLE', 'GENDARMERIE', NULL)
                , ('TITLE', 'GENERAL', 'GAL')
                , ('TITLE', 'GOUVERNEUR', 'GOUV')
                , ('TITLE', 'GRAND', 'GD')
                , ('TITLE', 'GRANDE', 'GDE')
                , ('TITLE', 'GRANDES', 'GDES')
                , ('TITLE', 'GRANDS', 'GDS')
                , ('TITLE', 'GROUPE', 'GROU')
                --, ('TITLE', 'HALAGE', NULL)
                , ('TITLE', 'HALLE', 'HALL')
                , ('TITLE', 'HAMEAU', 'HAME')
                , ('TITLE', 'HAMEAUX', 'HAME')
                , ('TITLE', 'HAUT', 'HT')
                , ('TITLE', 'HAUTE', 'HTE')
                , ('TITLE', 'HAUTES', 'HTES')
                , ('TITLE', 'HAUTS', 'HTS')
                , ('TITLE', 'HIPPODROME', 'HIPP')
                , ('TITLE', 'HOPITAL', 'HOP')
                , ('TITLE', 'HOSPICE', NULL)
                , ('TITLE', 'HOSPITALIER', NULL)
                , ('TITLE', 'HOTEL', 'HOT')
                --, ('TITLE', 'ILOT', NULL)
                , ('TITLE', 'INFANTERIE', 'INFANT')
                , ('TITLE', 'INFERIEUR', NULL)
                , ('TITLE', 'INFERIEURE', NULL)
                , ('TITLE', 'INGENIEUR', 'ING')
                , ('TITLE', 'INSPECTEUR', NULL)
                , ('TITLE', 'INSTITUT', NULL)
                , ('TITLE', 'INTERNATIONAL', NULL)
                , ('TITLE', 'INTERNATIONALE', 'INTERN')
                , ('TITLE', 'LIEUTENANT', 'LT')
                , ('TITLE', 'LIEUTENANT DE VAISSEAU', 'LTDV')
                , ('TITLE', 'MADAME', 'MME')
                , ('TITLE', 'MADEMOISELLE', 'MLLE')
                , ('TITLE', 'MAGASIN', NULL)
                --, ('TITLE', 'MAIRIE', NULL)
                , ('TITLE', 'MAISON', 'MAIS')
                , ('TITLE', 'MAITRE', 'ME')
                --, ('TITLE', 'MARAIS', NULL)
                , ('TITLE', 'MARCHE', 'MARC')
                , ('TITLE', 'MARECHAL', 'MAL')
                , ('TITLE', 'MARITIME', NULL)
                , ('TITLE', 'MARTYR', NULL)
                , ('TITLE', 'MARTYRS', 'MYR')
                , ('TITLE', 'MEDECIN', 'MED')
                , ('TITLE', 'MEDICAL', 'MED')
                , ('TITLE', 'MESDEMOISELLES', NULL)
                , ('TITLE', 'MESSIEURS', NULL)
                , ('TITLE', 'MILITAIRE', 'MIL')
                , ('TITLE', 'MONSEIGNEUR', 'MGR')
                , ('TITLE', 'MONSIEUR', 'M')
                --, ('TITLE', 'MONTEE', 'MONT')
                --, ('TITLE', 'MOULIN', 'MOUL')
                , ('TITLE', 'MOULINS', 'MOUL')
                , ('TITLE', 'MUNICIPAL', 'MUN')
                , ('TITLE', 'MUSEE', 'MUSE')
                , ('TITLE', 'NATIONAL', 'NAL')
                , ('TITLE', 'NOTRE DAME', 'ND')
                , ('TITLE', 'NOUVEAU', 'NOUV')
                , ('TITLE', 'NOUVELLE', 'NOUV')
                , ('TITLE', 'OBSERVATOIRE', NULL)
                , ('TITLE', 'PALAIS', 'PALA')
                , ('TITLE', 'PARCS', NULL)
                , ('TITLE', 'PARKING', 'PARK')
                --, ('TITLE', 'PARVIS', 'PARV')
                --, ('TITLE', 'PASSERELLE', 'PASS')
                , ('TITLE', 'PASSERELLES', NULL)
                , ('TITLE', 'PASSES', NULL)
                , ('TITLE', 'PASTEUR', 'PAST')
                , ('TITLE', 'PAVILLONS', 'PAVI')
                , ('TITLE', 'PETIT', 'PT')
                , ('TITLE', 'PETITE', 'PTE')
                , ('TITLE', 'PETITES', 'PTE')
                , ('TITLE', 'PETITS', 'PT')
                , ('TITLE', 'PLAINE', 'PLAI')
                , ('TITLE', 'PLATEAU', 'PLAT')
                , ('TITLE', 'PLATEAUX', 'PLAT')
                , ('TITLE', 'POINTE', 'POIN')
                , ('TITLE', 'POLICE', 'POL')
                , ('TITLE', 'PORTE', 'PORT')
                , ('TITLE', 'PREFET', NULL)
                , ('TITLE', 'PRESIDENT', 'PDT')
                , ('TITLE', 'PRESQU ILE', 'PRES ILE')
                , ('TITLE', 'PROFESSEUR', 'PR')
                , ('TITLE', 'PROLONGE', 'PROL')
                , ('TITLE', 'PROLONGEE', 'PROL')
                , ('TITLE', 'PROPRIETE', NULL)
                --, ('TITLE', 'QUARTIER', 'QUAR')
                --, ('TITLE', 'RACCOURCI', 'RACC')
                , ('TITLE', 'RECTEUR', 'RECT')
                , ('TITLE', 'REGIMENT', 'RGT')
                , ('TITLE', 'REGIONAL', NULL)
                , ('TITLE', 'REPUBLIQUE', 'REP')
                , ('TITLE', 'RESIDENCES', 'RESI')
                , ('TITLE', 'RESTAURANT', NULL)
                , ('TITLE', 'ROTONDE', 'ROTO')
                --, ('TITLE', 'RUELLE', 'RUEL')
                , ('TITLE', 'SAINT', 'ST')
                , ('TITLE', 'SAINTE', 'STE')
                , ('TITLE', 'SAINTES', NULL)
                , ('TITLE', 'SAINTS', NULL)
                --, ('TITLE', 'SENTE', 'SENT')
                --, ('TITLE', 'SENTIER', 'SENT')
                , ('TITLE', 'SERGENT', 'SGT')
                , ('TITLE', 'SERVICE', 'SCE')
                , ('TITLE', 'SOCIETE', NULL)
                , ('TITLE', 'SOUS PREFET', NULL)
                , ('TITLE', 'STADE', 'STAD')
                , ('TITLE', 'STATION', 'STAT')
                , ('TITLE', 'SUPERIEUR', NULL)
                , ('TITLE', 'SUPERIEURE', NULL)
                , ('TITLE', 'SYNDICAT', NULL)
                , ('TITLE', 'TECHNIQUE', NULL)
                , ('TITLE', 'TERRAIN', 'TERR')
                --, ('TITLE', 'TERRASSES', 'TERR')
                --, ('TITLE', 'TRAVERSE', 'TRAV')
                , ('TITLE', 'TUNNEL', 'TUN')
                , ('TITLE', 'UNIVERSITAIRE', 'UNVT')
                , ('TITLE', 'UNIVERSITE', 'UNIV')
                , ('TITLE', 'VALLEE', 'VALL')
                , ('TITLE', 'VALLON', 'VALL')
                , ('TITLE', 'VELODROME', NULL)
                , ('TITLE', 'VEUVE', NULL)
                , ('TITLE', 'VIEILLE', 'VIEL')
                , ('TITLE', 'VIEILLES', 'VIEL')
                , ('TITLE', 'VIEUX', 'VX')
                , ('TITLE', 'VILLAS', 'VILL')
        ) AS x("group", name, name_abbreviated)
        ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Titres: ', _nrows));

    -- update first word
    UPDATE fr.laposte_address_street_keyword kt SET
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
            k.name
            , COUNT(*) occurs
        FROM
            fr.laposte_address_street s
            , fr.laposte_address_street_keyword k
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
    UPDATE fr.laposte_address_street_keyword kt SET
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
            'LAPOSTE_MUNICIPALITY_EXCEPTION'
            , t.*
        FROM (
            SELECT
                co_insee_commune
                , lb_ach_nn
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
                co_insee_commune
                , lb_ach_nn
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

SELECT public.drop_all_functions_if_exists('fr', 'set_territory_overseas');
CREATE OR REPLACE PROCEDURE fr.set_territory_overseas()
AS
$proc$
BEGIN
    DELETE FROM fr.constant WHERE usecase = 'TERRITORY_OVERSEAS_NAME';
    INSERT INTO fr.constant (usecase, key, value) VALUES
          ('TERRITORY_OVERSEAS_NAME', '97501', 'Miquelon-Langlade')
        , ('TERRITORY_OVERSEAS_NAME', '97502', 'Saint-Pierre')
        , ('TERRITORY_OVERSEAS_NAME', '97701', 'Saint-Barthélemy')
        , ('TERRITORY_OVERSEAS_NAME', '97801', 'Saint-Martin')
        , ('TERRITORY_OVERSEAS_NAME', '98714', 'Bora-Bora')
        , ('TERRITORY_OVERSEAS_NAME', '98718', 'Fatu-Hiva')
        , ('TERRITORY_OVERSEAS_NAME', '98723', 'Hiva-Oa')
        , ('TERRITORY_OVERSEAS_NAME', '98729', 'Moorea-Maiao')
        , ('TERRITORY_OVERSEAS_NAME', '98731', 'Nuku-Hiva')
        , ('TERRITORY_OVERSEAS_NAME', '98747', 'Taiarapu-Est')
        , ('TERRITORY_OVERSEAS_NAME', '98748', 'Taiarapu-Ouest')
        , ('TERRITORY_OVERSEAS_NAME', '98756', 'Ua-Huka')
        , ('TERRITORY_OVERSEAS_NAME', '98757', 'Ua-Pou')
        , ('TERRITORY_OVERSEAS_NAME', '98801', 'Bélep')
        , ('TERRITORY_OVERSEAS_NAME', '98805', 'Dumbéa')
        , ('TERRITORY_OVERSEAS_NAME', '98807', 'Hienghène')
        , ('TERRITORY_OVERSEAS_NAME', '98808', 'Houaïlou')
        , ('TERRITORY_OVERSEAS_NAME', '98809', 'Île des Pins')
        , ('TERRITORY_OVERSEAS_NAME', '98810', 'Kaala-Gomen')
        , ('TERRITORY_OVERSEAS_NAME', '98811', 'Koné')
        , ('TERRITORY_OVERSEAS_NAME', '98815', 'Maré')
        , ('TERRITORY_OVERSEAS_NAME', '98817', 'Mont-Dore')
        , ('TERRITORY_OVERSEAS_NAME', '98819', 'Ouégoa')
        , ('TERRITORY_OVERSEAS_NAME', '98820', 'Ouvéa')
        , ('TERRITORY_OVERSEAS_NAME', '98821', 'Païta')
        , ('TERRITORY_OVERSEAS_NAME', '98822', 'Poindimié')
        , ('TERRITORY_OVERSEAS_NAME', '98823', 'Ponérihouen')
        , ('TERRITORY_OVERSEAS_NAME', '98824', 'Pouébo')
        , ('TERRITORY_OVERSEAS_NAME', '98828', 'Sarraméa')
        , ('TERRITORY_OVERSEAS_NAME', '98832', 'Yaté')

        , ('TERRITORY_OVERSEAS_NAME', '9871', 'Îles Marquises')
        , ('TERRITORY_OVERSEAS_NAME', '9872', 'Îles Tuamotu-Gambier')
        , ('TERRITORY_OVERSEAS_NAME', '9873', 'Îles du Vent')
        , ('TERRITORY_OVERSEAS_NAME', '9874', 'Îles Sous-le-Vent')
        , ('TERRITORY_OVERSEAS_NAME', '9875', 'Îles Australes')
        , ('TERRITORY_OVERSEAS_NAME', '9881', 'Province Sud')
        , ('TERRITORY_OVERSEAS_NAME', '9882', 'Province Nord')
        , ('TERRITORY_OVERSEAS_NAME', '9883', 'Îles Loyauté')

        , ('TERRITORY_OVERSEAS_NAME', '975', 'Saint-Pierre-et-Miquelon')
        , ('TERRITORY_OVERSEAS_NAME', '977', 'Saint-Barthélemy')
        , ('TERRITORY_OVERSEAS_NAME', '978', 'Saint-Martin')
        , ('TERRITORY_OVERSEAS_NAME', '986', 'Wallis et Futuna')
        , ('TERRITORY_OVERSEAS_NAME', '987', 'Polynésie française')
        , ('TERRITORY_OVERSEAS_NAME', '988', 'Nouvelle Calédonie')
        , ('TERRITORY_OVERSEAS_NAME', '989', 'Île de Clipperton')

        , ('TERRITORY_OVERSEAS_NAME', '97', 'Îles en Atlantique')
        , ('TERRITORY_OVERSEAS_NAME', '98', 'Îles en Pacifique')
    ;

    DELETE FROM fr.constant WHERE usecase = 'TERRITORY_OVERSEAS_RELATION';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        --9871 Îles Marquises
          ('TERRITORY_OVERSEAS_RELATION', '9871', '98718')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98723')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98731')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98746')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98756')
        , ('TERRITORY_OVERSEAS_RELATION', '9871', '98757')
        --9872 Îles Tuamotu-Gambier
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98711')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98713')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98716')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98717')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98719')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98720')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98721')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98726')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98727')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98730')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98732')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98737')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98740')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98742')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98749')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98751')
        , ('TERRITORY_OVERSEAS_RELATION', '9872', '98755')
        --9873 Îles du Vent
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98729')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98712')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98715')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98722')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98725')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98733')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98734')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98735')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98736')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98738')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98747')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98748')
        , ('TERRITORY_OVERSEAS_RELATION', '9873', '98752')
        --9874 Îles Sous-le-Vent
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98714')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98724')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98728')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98745')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98750')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98754')
        , ('TERRITORY_OVERSEAS_RELATION', '9874', '98758')
        --9875 Îles Australes
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98739')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98741')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98743')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98744')
        , ('TERRITORY_OVERSEAS_RELATION', '9875', '98753')

        --9881 Province Sud
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98829')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98832')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98809')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98817')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98818')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98805')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98821')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98802')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98813')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98828')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98806')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98816')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98803')
        , ('TERRITORY_OVERSEAS_RELATION', '9881', '98827') -- 'SUD'
        --9882 Province Nord
        --, ('TERRITORY_OVERSEAS_RELATION', '9882', '98827') -- 'NORD' !
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98825')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98811')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98831')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98810')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98812')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98826')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98801')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98819')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98824')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98807')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98830')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98822')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98823')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98808')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98833')
        , ('TERRITORY_OVERSEAS_RELATION', '9882', '98804')
        --9883 Îles Loyauté
        , ('TERRITORY_OVERSEAS_RELATION', '9883', '98820')
        , ('TERRITORY_OVERSEAS_RELATION', '9883', '98814')
        , ('TERRITORY_OVERSEAS_RELATION', '9883', '98815')
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
        ('LAPOSTE_ADDRESS_FAULT_ADDRESS', 'COMPLEMENT_WITH_STREET_ERROR', '0')

        , ('LAPOSTE_ADDRESS_FAULT_AREA', 'BAD_SPACE', '100')

        , ('LAPOSTE_ADDRESS_FAULT_STREET', 'BAD_SPACE', '200')
        , ('LAPOSTE_ADDRESS_FAULT_STREET', 'DUPLICATE_WORD', '201')
        , ('LAPOSTE_ADDRESS_FAULT_STREET', 'WITH_ABBREVIATION', '202')
        , ('LAPOSTE_ADDRESS_FAULT_STREET', 'TYPO_ERROR', '203')
        , ('LAPOSTE_ADDRESS_FAULT_STREET', 'DESCRIPTORS', '204')
        , ('LAPOSTE_ADDRESS_FAULT_STREET', 'TYPE', '205')

        , ('LAPOSTE_ADDRESS_FAULT_HOUSENUMBER', 'BAD_NUMBER', '300')
        , ('LAPOSTE_ADDRESS_FAULT_HOUSENUMBER', 'BAD_EXTENSION', '301')

        , ('LAPOSTE_ADDRESS_FAULT_COMPLEMENT', 'BAD_SPACE', '400')
    ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_constant_address');
CREATE OR REPLACE PROCEDURE fr.set_constant_address()
AS
$proc$
DECLARE
    _listof INT[];
BEGIN
    SELECT public.drop_table_indexes('fr', 'constant');
    CALL fr.set_laposte_address_fault_list();

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

    -- street-faults (part/1)
    CALL fr.set_laposte_address_fault_street();
    CALL fr.fix_laposte_address_fault_street(
        fault => 'BAD_SPACE,DUPLICATE_WORD,WITH_ABBREVIATION,TYPO_ERROR'
    );

    -- following street-faults fixes, have to fix membership too!
    _listof := ARRAY(
        SELECT
            name_id
        FROM
            fr.laposte_address_fault_street fs
            , fr.constant c
        WHERE
            c.usecase = 'LAPOSTE_ADDRESS_FAULT_STREET'
            AND
            c.key = 'DUPLICATE_WORD'
            AND
            fs.fault_id = c.value::INT
    );
    IF _listof IS NOT NULL THEN
        CALL fr.set_laposte_address_street_membership(
            set_case => 'CORRECTION'
            , listof => _listof
        );
    END IF;

    -- once all fixed street-faults:
    -- set attributs in dictionary
    CALL fr.set_laposte_address_street_uniq(
        set_case => 'ATTRIBUTS'
    );
    -- street-faults (part/2)
    CALL fr.fix_laposte_address_fault_street(
        fault => 'DESCRIPTORS,TYPE'
    );

    -- classify all words
    CALL fr.set_laposte_address_street_word();
    -- define keywords (type, title, ...) and exceptions
    CALL fr.set_laposte_address_street_type();
    CALL fr.set_laposte_address_street_ext();
    CALL fr.set_laposte_address_street_title();
    CALL fr.set_laposte_address_street_firstname();
    CALL fr.set_laposte_address_street_kw_exception();

    CALL fr.set_laposte_municipality_normalized_label_exception();
    CALL fr.set_territory_overseas();

    CALL fr.set_constant_index();
END;
$proc$ LANGUAGE plpgsql;

/* TEST
16:02:28.937 Dictionnaire des voies
16:02:28.937  Purge
16:02:28.965  Initialisation
16:02:36.153  Création: 1120741
16:02:41.586  Indexation
16:02:41.614 Référence des voies (Dictionnaire/Référentiel)
16:02:41.614  Purge
DROP INDEX IF EXISTS fr.ix_laposte_address_street_reference_name_id
DROP INDEX IF EXISTS fr.ix_laposte_address_street_reference_address_id
16:02:42.034  Initialisation
16:02:56.092  Création: 2392148
16:03:02.807  Indexation
16:03:02.835 Gestion de l'appartenance des mots dans les noms de voies
16:03:02.835  Purge
DROP INDEX IF EXISTS fr.ix_laposte_address_street_membership_word
16:03:03.381  Initialisation
16:07:42.004  Appartenance (mots): 2726082
16:07:45.044  Indexation
 */
