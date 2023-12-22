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

-- build LAPOSTE street : list of types
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_type');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_type()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'TYPE';
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated)
        SELECT DISTINCT
            'TYPE'
            , lb_type
            , lb_type_abrege
        FROM fr.laposte_address_street
        WHERE lb_type IS NOT NULL
        ;
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
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE street : list of firstnames
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_street_firstname');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_firstname()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_STREET_FIRSTNAME';
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
            NOT mots.mot = ANY('{A,AU,AUX,D,DE,DES,DU,EN,ET,L,LA,LE,LES,SOUS,SUR,UN,UNE}')
    );
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE extension (of housenumber), w/ abbreviated value
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_extension_of_housenumber');
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_extension_of_housenumber');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_extension_of_housenumber()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_housenumber') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'EXT';
    INSERT INTO fr.laposte_address_street_keyword("group", name, name_abbreviated, first_word)
        SELECT DISTINCT 'EXT', lb_ext, lb_abr_nn, NULL
        FROM fr.laposte_address_housenumber
        WHERE fl_active AND lb_ext IS NOT NULL
        ;

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
END;
$proc$ LANGUAGE plpgsql;

-- build LAPOSTE titles
SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_titles');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_titles()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.laposte_address_street_keyword WHERE "group" = 'TITLE';
    INSERT INTO fr.laposte_address_street_keyword(
        "group", name, name_abbreviated, first_word, occurs
    ) VALUES
        ('TITLE', 'ABBAYE', 'ABBA', NULL, 1)
        , ('TITLE', 'ABBE', 'ABBE', NULL, 1)
        , ('TITLE', 'ACTIVITE', 'A', NULL, 1)
        , ('TITLE', 'ADJUDANT', 'ADJ', NULL, 1)
        , ('TITLE', 'AERODROME', 'AER', NULL, 1)
        , ('TITLE', 'AEROGARE', 'AEROGARE', NULL, 1)
        , ('TITLE', 'AERONAUTIQUE', 'AERONAUTIQUE', NULL, 1)
        , ('TITLE', 'AEROPORT', 'AERP', NULL, 1)
        , ('TITLE', 'AGENCE', 'AGENCE', NULL, 1)
        , ('TITLE', 'AGGLOMERATION', 'AGGL', NULL, 1)
        , ('TITLE', 'AGRICOLE', 'AGRIC', NULL, 1)
        , ('TITLE', 'AIRE', 'AIRE', NULL, 1)
        , ('TITLE', 'AIRES', 'AIRE', NULL, 1)
        , ('TITLE', 'ALLEE', 'ALL', NULL, 1)
        , ('TITLE', 'ALLEES', 'ALL', NULL, 1)
        , ('TITLE', 'AMENAGEMENT', 'A', NULL, 1)
        , ('TITLE', 'AMIRAL', 'AMIRAL', NULL, 1)
        , ('TITLE', 'ANCIEN', 'ANC', NULL, 1)
        , ('TITLE', 'ANCIENNE', 'ANCI', NULL, 1)
        , ('TITLE', 'ANSE', 'ANSE', NULL, 1)
        , ('TITLE', 'APPARTEMENT', 'APPT', NULL, 1)
        , ('TITLE', 'ARCADE', 'ARCA', NULL, 1)
        , ('TITLE', 'ARCADES', 'ARCA', NULL, 1)
        , ('TITLE', 'ARMEMENT', 'ARMEMENT', NULL, 1)
        , ('TITLE', 'ARRONDISSEMENT', 'ARR', NULL, 1)
        , ('TITLE', 'ARTISANALE', 'ARTISANALE', NULL, 1)
        , ('TITLE', 'ASPIRANT', 'ASP', NULL, 1)
        , ('TITLE', 'ASSOCIATION', 'ASSOCIATION', NULL, 1)
        , ('TITLE', 'ATELIER', 'ATELIER', NULL, 1)
        , ('TITLE', 'AUTOROUTE', 'AUTO', NULL, 1)
        , ('TITLE', 'BARRIERE', 'BARR', NULL, 1)
        , ('TITLE', 'BARRIERES', 'BARR', NULL, 1)
        , ('TITLE', 'BAS', 'BAS', NULL, 1)
        , ('TITLE', 'BASSE', 'BAS', NULL, 1)
        , ('TITLE', 'BASSES', 'BAS', NULL, 1)
        , ('TITLE', 'BASTIDE', 'BAST', NULL, 1)
        , ('TITLE', 'BASTION', 'BASTION', NULL, 1)
        , ('TITLE', 'BATAILLON', 'BTN', NULL, 1)
        , ('TITLE', 'BATAILLONS', 'BTN', NULL, 1)
        , ('TITLE', 'BATIMENT', 'BAT', NULL, 1)
        , ('TITLE', 'BATIMENTS', 'BAT', NULL, 1)
        , ('TITLE', 'BEGUINAGE', 'BEGUINAGE', NULL, 1)
        , ('TITLE', 'BERGE', 'BERG', NULL, 1)
        , ('TITLE', 'BERGES', 'BERG', NULL, 1)
        , ('TITLE', 'BOIS', 'BOIS', NULL, 1)
        , ('TITLE', 'BOUCLE', 'BOUC', NULL, 1)
        , ('TITLE', 'BOURG', 'BOUR', NULL, 1)
        , ('TITLE', 'BUTTE', 'BUTT', NULL, 1)
        , ('TITLE', 'CABINET', 'CABINET', NULL, 1)
        , ('TITLE', 'CALE', 'CALE', NULL, 1)
        , ('TITLE', 'CAMP', 'CAMP', NULL, 1)
        , ('TITLE', 'CAMPAGNE', 'CAMP', NULL, 1)
        , ('TITLE', 'CAMPING', 'CAMPING', NULL, 1)
        , ('TITLE', 'CANAL', 'CANAL', NULL, 1)
        , ('TITLE', 'CANTON', 'CANT', NULL, 1)
        , ('TITLE', 'CAPITAINE', 'C', NULL, 1)
        , ('TITLE', 'CARDINAL', 'CDL', NULL, 1)
        , ('TITLE', 'CARRE', 'CARR', NULL, 1)
        , ('TITLE', 'CARREAU', 'CARR', NULL, 1)
        , ('TITLE', 'CARREFOUR', 'CARR', NULL, 1)
        , ('TITLE', 'CARRIERE', 'CARR', NULL, 1)
        , ('TITLE', 'CARRIERES', 'CARR', NULL, 1)
        , ('TITLE', 'CASERNE', 'CASR', NULL, 1)
        , ('TITLE', 'CASTEL', 'CASTEL', NULL, 1)
        , ('TITLE', 'CAVEE', 'CAVE', NULL, 1)
        , ('TITLE', 'CENTRAL', 'CENTRAL', NULL, 1)
        , ('TITLE', 'CENTRE', 'CTRE', NULL, 1)
        , ('TITLE', 'CHALET', 'CHALET', NULL, 1)
        , ('TITLE', 'CHAMBRE', 'CHAMBRE', NULL, 1)
        , ('TITLE', 'CHANOINE', 'CHANOINE', NULL, 1)
        , ('TITLE', 'CHAPELLE', 'CHAP', NULL, 1)
        , ('TITLE', 'CHARMILLE', 'CHARMILLE', NULL, 1)
        , ('TITLE', 'CHATEAU', 'CHAT', NULL, 1)
        , ('TITLE', 'CHAUSSEE', 'CHAUSSEE', NULL, 1)
        , ('TITLE', 'CHAUSSEES', 'CHAUSSEES', NULL, 1)
        , ('TITLE', 'CHEMIN', 'CHEM', NULL, 1)
        , ('TITLE', 'CHEMINS', 'CHEM', NULL, 1)
        , ('TITLE', 'CHEZ', 'CHEZ', NULL, 1)
        , ('TITLE', 'CITADELLE', 'CITADELLE', NULL, 1)
        , ('TITLE', 'CITES', 'CITES', NULL, 1)
        , ('TITLE', 'CLOITRE', 'CLOITRE', NULL, 1)
        , ('TITLE', 'COL', 'COL', NULL, 1)
        , ('TITLE', 'COLLEGE', 'COLLEGE', NULL, 1)
        , ('TITLE', 'COLLINE', 'COLL', NULL, 1)
        , ('TITLE', 'COLLINES', 'COLL', NULL, 1)
        , ('TITLE', 'COLONEL', 'COL', NULL, 1)
        , ('TITLE', 'COLONIE', 'COLO', NULL, 1)
        , ('TITLE', 'COMITE', 'COMITE', NULL, 1)
        , ('TITLE', 'COMMANDANT', 'CDT', NULL, 1)
        , ('TITLE', 'COMMERCIAL', 'CIAL', NULL, 1)
        , ('TITLE', 'COMMUNAL', 'COM', NULL, 1)
        , ('TITLE', 'COMMUNALE', 'C', NULL, 1)
        , ('TITLE', 'COMMUNAUX', 'COM', NULL, 1)
        , ('TITLE', 'COMMUNE', 'COM', NULL, 1)
        , ('TITLE', 'COMPAGNIE', 'CIE', NULL, 1)
        , ('TITLE', 'COMPAGNON', 'COMP', NULL, 1)
        , ('TITLE', 'COMPAGNONS', 'COMP', NULL, 1)
        , ('TITLE', 'CONCERTE', 'CONCERTE', NULL, 1)
        , ('TITLE', 'CONTOUR', 'CONTOUR', NULL, 1)
        , ('TITLE', 'COOPERATIVE', 'COOP', NULL, 1)
        , ('TITLE', 'CORNICHE', 'CORNICHE', NULL, 1)
        , ('TITLE', 'CORNICHES', 'CORNICHES', NULL, 1)
        , ('TITLE', 'COTE', 'COTE', NULL, 1)
        , ('TITLE', 'COTEAU', 'COTE', NULL, 1)
        , ('TITLE', 'COTTAGE', 'COTT', NULL, 1)
        , ('TITLE', 'COTTAGES', 'COTT', NULL, 1)
        , ('TITLE', 'COULOIR', 'COULOIR', NULL, 1)
        , ('TITLE', 'COUR', 'COUR', NULL, 1)
        , ('TITLE', 'COURS', 'COUR', NULL, 1)
        , ('TITLE', 'CROIX', 'CRX', NULL, 1)
        , ('TITLE', 'DEGRE', 'DEGRE', NULL, 1)
        , ('TITLE', 'DEGRES', 'DEGRES', NULL, 1)
        , ('TITLE', 'DEPARTEMENTAL', 'DEP', NULL, 1)
        , ('TITLE', 'DESCENTE', 'DESCENTE', NULL, 1)
        , ('TITLE', 'DIGUE', 'DIGU', NULL, 1)
        , ('TITLE', 'DIRECTEUR', 'DIRECTEUR', NULL, 1)
        , ('TITLE', 'DIRECTION', 'DIR', NULL, 1)
        , ('TITLE', 'DIT', 'D', NULL, 1)
        , ('TITLE', 'DIVISION', 'DIV', NULL, 1)
        , ('TITLE', 'DOCTEUR', 'DR', NULL, 1)
        , ('TITLE', 'DOMAINE', 'DOMA', NULL, 1)
        , ('TITLE', 'DOMAINES', 'DOMA', NULL, 1)
        , ('TITLE', 'ECLUSE', 'ECLU', NULL, 1)
        , ('TITLE', 'ECLUSES', 'ECLU', NULL, 1)
        , ('TITLE', 'ECOLE', 'ECOLE', NULL, 1)
        , ('TITLE', 'ECONOMIQUE', 'ECO', NULL, 1)
        , ('TITLE', 'ECRIVAINS', 'ECRIV', NULL, 1)
        , ('TITLE', 'EGLISE', 'EGLI', NULL, 1)
        , ('TITLE', 'ENCEINTE', 'ENCEINTE', NULL, 1)
        , ('TITLE', 'ENCLAVE', 'ENCL', NULL, 1)
        , ('TITLE', 'ENCLOS', 'ENCL', NULL, 1)
        , ('TITLE', 'ENSEIGNEMENT', 'ENST', NULL, 1)
        , ('TITLE', 'ENSEMBLE', 'ENSEMBLE', NULL, 1)
        , ('TITLE', 'ENTREE', 'ENT', NULL, 1)
        , ('TITLE', 'ENTREES', 'ENT', NULL, 1)
        , ('TITLE', 'ENTREPRISE', 'ENTR', NULL, 1)
        , ('TITLE', 'EPOUX', 'EP', NULL, 1)
        , ('TITLE', 'ESCALIER', 'ESC', NULL, 1)
        , ('TITLE', 'ESCALIERS', 'ESC', NULL, 1)
        , ('TITLE', 'ESPACE', 'ESPA', NULL, 1)
        , ('TITLE', 'ESPLANADE', 'ESPL', NULL, 1)
        , ('TITLE', 'ESPLANADES', 'ESPL', NULL, 1)
        , ('TITLE', 'ETABLISSEMENT', 'ETABLISSEMENT', NULL, 1)
        , ('TITLE', 'ETABLISSEMENTS', 'ETABLISSEMENT', NULL, 1)
        , ('TITLE', 'ETANG', 'ETAN', NULL, 1)
        , ('TITLE', 'EVEQUE', 'EVEQUE', NULL, 1)
        , ('TITLE', 'FACULTE', 'FACULTE', NULL, 1)
        , ('TITLE', 'FAUBOURG', 'FAUB', NULL, 1)
        , ('TITLE', 'FERME', 'FERM', NULL, 1)
        , ('TITLE', 'FERMES', 'FERM', NULL, 1)
        , ('TITLE', 'FONTAINE', 'FONT', NULL, 1)
        , ('TITLE', 'FORESTIER', 'FORESTIER', NULL, 1)
        , ('TITLE', 'FORESTIERE', 'FORESTIERE', NULL, 1)
        , ('TITLE', 'FORET', 'FOR', NULL, 1)
        , ('TITLE', 'FORT', 'FORT', NULL, 1)
        , ('TITLE', 'FORUM', 'FORUM', NULL, 1)
        , ('TITLE', 'FOSSE', 'FOSS', NULL, 1)
        , ('TITLE', 'FOSSES', 'FOSS', NULL, 1)
        , ('TITLE', 'FOYER', 'FOYE', NULL, 1)
        , ('TITLE', 'FRANCAIS', 'FR', NULL, 1)
        , ('TITLE', 'FRANCAISE', 'FR', NULL, 1)
        , ('TITLE', 'FUSILIERS', 'FUSILIERS', NULL, 1)
        , ('TITLE', 'GALERIE', 'GALE', NULL, 1)
        , ('TITLE', 'GALERIES', 'GALE', NULL, 1)
        , ('TITLE', 'GARE', 'GARE', NULL, 1)
        , ('TITLE', 'GARENNE', 'GARE', NULL, 1)
        , ('TITLE', 'GENDARMERIE', 'GENDARMERIE', NULL, 1)
        , ('TITLE', 'GENERAL', 'GAL', NULL, 1)
        , ('TITLE', 'GOUVERNEUR', 'GOUV', NULL, 1)
        , ('TITLE', 'GRAND', 'GD', NULL, 1)
        , ('TITLE', 'GRANDE', 'GDE', NULL, 1)
        , ('TITLE', 'GRANDES', 'GDES', NULL, 1)
        , ('TITLE', 'GRANDS', 'GDS', NULL, 1)
        , ('TITLE', 'GRILLE', 'GRI', NULL, 1)
        , ('TITLE', 'GRIMPETTE', 'GRIMPETTE', NULL, 1)
        , ('TITLE', 'GROUPE', 'GROU', NULL, 1)
        , ('TITLE', 'GROUPEMENT', 'GROUPEMENT', NULL, 1)
        , ('TITLE', 'HALAGE', 'HALAGE', NULL, 1)
        , ('TITLE', 'HALLE', 'HALL', NULL, 1)
        , ('TITLE', 'HALLES', 'HALL', NULL, 1)
        , ('TITLE', 'HAMEAU', 'HAME', NULL, 1)
        , ('TITLE', 'HAMEAUX', 'HAME', NULL, 1)
        , ('TITLE', 'HAUT', 'HT', NULL, 1)
        , ('TITLE', 'HAUTE', 'HTE', NULL, 1)
        , ('TITLE', 'HAUTES', 'HTES', NULL, 1)
        , ('TITLE', 'HAUTS', 'HTS', NULL, 1)
        , ('TITLE', 'HIPPODROME', 'HIPP', NULL, 1)
        , ('TITLE', 'HLM', 'HLM', NULL, 1)
        , ('TITLE', 'HOPITAL', 'HOP', NULL, 1)
        , ('TITLE', 'HOSPICE', 'HOSPICE', NULL, 1)
        , ('TITLE', 'HOSPITALIER', 'HOSPITALIER', NULL, 1)
        , ('TITLE', 'HOTEL', 'HOT', NULL, 1)
        , ('TITLE', 'ILE', 'ILE', NULL, 1)
        , ('TITLE', 'ILOT', 'ILOT', NULL, 1)
        , ('TITLE', 'IMMEUBLE', 'IMM', NULL, 1)
        , ('TITLE', 'IMMEUBLES', 'IMM', NULL, 1)
        , ('TITLE', 'IMPASSE', 'IMP', NULL, 1)
        , ('TITLE', 'IMPASSES', 'IMP', NULL, 1)
        , ('TITLE', 'INFANTERIE', 'INFANT', NULL, 1)
        , ('TITLE', 'INFERIEUR', 'INF', NULL, 1)
        , ('TITLE', 'INFERIEURE', 'INF', NULL, 1)
        , ('TITLE', 'INGENIEUR', 'ING', NULL, 1)
        , ('TITLE', 'INSPECTEUR', 'INSPECTEUR', NULL, 1)
        , ('TITLE', 'INSTITUT', 'INST', NULL, 1)
        , ('TITLE', 'INTERNATIONAL', 'INTERN', NULL, 1)
        , ('TITLE', 'INTERNATIONALE', 'INTERN', NULL, 1)
        , ('TITLE', 'JARDIN', 'JARD', NULL, 1)
        , ('TITLE', 'JARDINS', 'JARD', NULL, 1)
        , ('TITLE', 'JETEE', 'JETEE', NULL, 1)
        , ('TITLE', 'LABORATOIRE', 'LABORATOIRE', NULL, 1)
        , ('TITLE', 'LEVEE', 'LEVEE', NULL, 1)
        , ('TITLE', 'LIEUTENANT', 'LT', NULL, 2)
        , ('TITLE', 'LIEUTENANT DE VAISSEAU', 'LTDV', 'LIEUTENANT', 1)
        , ('TITLE', 'MADAME', 'MME', NULL, 1)
        , ('TITLE', 'MADEMOISELLE', 'MLLE', NULL, 1)
        , ('TITLE', 'MAGASIN', 'MAG', NULL, 1)
        , ('TITLE', 'MAIL', 'MAIL', NULL, 1)
        , ('TITLE', 'MAIRIE', 'MAIRIE', NULL, 1)
        , ('TITLE', 'MAISON', 'MAIS', NULL, 1)
        , ('TITLE', 'MAITRE', 'MAITRE', NULL, 1)
        , ('TITLE', 'MANOIR', 'MANOIR', NULL, 1)
        , ('TITLE', 'MARAIS', 'MARAIS', NULL, 1)
        , ('TITLE', 'MARCHE', 'MARC', NULL, 1)
        , ('TITLE', 'MARCHES', 'MARC', NULL, 1)
        , ('TITLE', 'MARECHAL', 'MAL', NULL, 1)
        , ('TITLE', 'MARITIME', 'MARITIME', NULL, 1)
        , ('TITLE', 'MARTYR', 'MYR', NULL, 1)
        , ('TITLE', 'MARTYRS', 'MYR', NULL, 1)
        , ('TITLE', 'MAS', 'MAS', NULL, 1)
        , ('TITLE', 'MEDECIN', 'MED', NULL, 1)
        , ('TITLE', 'MEDICAL', 'MED', NULL, 1)
        , ('TITLE', 'MESDEMOISELLES', 'MESDEMOISELLES', NULL, 1)
        , ('TITLE', 'MESSIEURS', 'MESSIEURS', NULL, 1)
        , ('TITLE', 'METRO', 'METR', NULL, 1)
        , ('TITLE', 'MILITAIRE', 'MIL', NULL, 1)
        , ('TITLE', 'MONSEIGNEUR', 'MGR', NULL, 1)
        , ('TITLE', 'MONSIEUR', 'M', NULL, 1)
        , ('TITLE', 'MONTEE', 'MONT', NULL, 1)
        , ('TITLE', 'MONTEES', 'MONT', NULL, 1)
        , ('TITLE', 'MOULIN', 'MOUL', NULL, 1)
        , ('TITLE', 'MOULINS', 'MOUL', NULL, 1)
        , ('TITLE', 'MUNICIPAL', 'MUN', NULL, 1)
        , ('TITLE', 'MUSEE', 'MUSE', NULL, 1)
        , ('TITLE', 'NATIONAL', 'NAT', NULL, 1)
        , ('TITLE', 'NOTRE DAME', 'ND', 'NOTRE', 1)
        , ('TITLE', 'NOUVEAU', 'NOUV', NULL, 1)
        , ('TITLE', 'NOUVELLE', 'NOUV', NULL, 1)
        , ('TITLE', 'OBSERVATOIRE', 'OBSERVATOIRE', NULL, 1)
        , ('TITLE', 'PALAIS', 'PALA', NULL, 1)
        , ('TITLE', 'PARC', 'PARC', NULL, 1)
        , ('TITLE', 'PARCS', 'PARC', NULL, 1)
        , ('TITLE', 'PARKING', 'PARK', NULL, 1)
        , ('TITLE', 'PARVIS', 'PARV', NULL, 1)
        , ('TITLE', 'PASSAGE', 'PAS', NULL, 1)
        , ('TITLE', 'PASSE', 'PASS', NULL, 1)
        , ('TITLE', 'PASSERELLE', 'PASS', NULL, 1)
        , ('TITLE', 'PASSERELLES', 'PASS', NULL, 1)
        , ('TITLE', 'PASSES', 'PASSES', NULL, 1)
        , ('TITLE', 'PASTEUR', 'PAST', NULL, 1)
        , ('TITLE', 'PATIO', 'PATIO', NULL, 1)
        , ('TITLE', 'PAVILLON', 'PAVI', NULL, 1)
        , ('TITLE', 'PAVILLONS', 'PAVI', NULL, 1)
        , ('TITLE', 'PETIT', 'PT', NULL, 1)
        , ('TITLE', 'PETITE', 'PTE', NULL, 1)
        , ('TITLE', 'PETITES', 'PTE', NULL, 1)
        , ('TITLE', 'PETITS', 'PT', NULL, 1)
        , ('TITLE', 'PLACE', 'PL', NULL, 1)
        , ('TITLE', 'PLACIS', 'PLACIS', NULL, 1)
        , ('TITLE', 'PLAGE', 'PLAG', NULL, 1)
        , ('TITLE', 'PLAGES', 'PLAG', NULL, 1)
        , ('TITLE', 'PLAINE', 'PLAI', NULL, 1)
        , ('TITLE', 'PLAN', 'PLAN', NULL, 1)
        , ('TITLE', 'PLATEAU', 'PLAT', NULL, 1)
        , ('TITLE', 'PLATEAUX', 'PLAT', NULL, 1)
        , ('TITLE', 'POINTE', 'POIN', NULL, 1)
        , ('TITLE', 'POLICE', 'POL', NULL, 1)
        , ('TITLE', 'PONT', 'PONT', NULL, 1)
        , ('TITLE', 'PONTS', 'PONT', NULL, 1)
        , ('TITLE', 'PORCHE', 'PORCHE', NULL, 1)
        , ('TITLE', 'PORT', 'PORT', NULL, 1)
        , ('TITLE', 'PORTE', 'PORT', NULL, 1)
        , ('TITLE', 'PORTIQUES', 'PORTIQUES', NULL, 1)
        , ('TITLE', 'POTERNE', 'POTERNE', NULL, 1)
        , ('TITLE', 'POURTOUR', 'POURTOUR', NULL, 1)
        , ('TITLE', 'PRE', 'PRE', NULL, 1)
        , ('TITLE', 'PREFET', 'PREFET', NULL, 1)
        , ('TITLE', 'PRESIDENT', 'PDT', NULL, 1)
        , ('TITLE', 'PRESQU', 'PRESQU', NULL, 1)
        , ('TITLE', 'PROFESSEUR', 'PR', NULL, 1)
        , ('TITLE', 'PROFESSIONNEL', 'PROF', NULL, 1)
        , ('TITLE', 'PROLONGE', 'PROL', NULL, 1)
        , ('TITLE', 'PROLONGEE', 'PROL', NULL, 1)
        , ('TITLE', 'PROMENADE', 'PROM', NULL, 1)
        , ('TITLE', 'PROPRIETE', 'PROPRIETE', NULL, 1)
        , ('TITLE', 'QUAI', 'QUAI', NULL, 1)
        , ('TITLE', 'QUARTIER', 'QUAR', NULL, 1)
        , ('TITLE', 'QUINQUIES', 'QUINQUIES', NULL, 1)
        , ('TITLE', 'RACCOURCI', 'RACC', NULL, 1)
        , ('TITLE', 'RAIDILLON', 'RAIDILLON', NULL, 1)
        , ('TITLE', 'RAMPE', 'RAMPE', NULL, 1)
        , ('TITLE', 'RECTEUR', 'RECT', NULL, 1)
        , ('TITLE', 'REGIMENT', 'RGT', NULL, 1)
        , ('TITLE', 'REGIONAL', 'REGIONAL', NULL, 1)
        , ('TITLE', 'REMPART', 'REMPART', NULL, 1)
        , ('TITLE', 'REPUBLIQUE', 'REP', NULL, 1)
        , ('TITLE', 'RESIDENCE', 'RES', NULL, 1)
        , ('TITLE', 'RESIDENCES', 'RESI', NULL, 1)
        , ('TITLE', 'RESTAURANT', 'REST', NULL, 1)
        , ('TITLE', 'ROC', 'ROC', NULL, 1)
        , ('TITLE', 'ROCADE', 'ROCADE', NULL, 1)
        , ('TITLE', 'ROQUET', 'ROQUET', NULL, 1)
        , ('TITLE', 'ROTONDE', 'ROTO', NULL, 1)
        , ('TITLE', 'SAINT', 'ST', NULL, 1)
        , ('TITLE', 'SAINTE', 'STE', NULL, 1)
        , ('TITLE', 'SAINTS', 'ST', NULL, 1)
        , ('TITLE', 'SAINTES', 'STS', NULL, 1)
        , ('TITLE', 'SERGENT', 'SGT', NULL, 1)
        , ('TITLE', 'SERVICE', 'SCE', NULL, 1)
        , ('TITLE', 'SOCIETE', 'SOC', NULL, 1)
        , ('TITLE', 'STADE', 'STAD', NULL, 1)
        , ('TITLE', 'STATION', 'STAT', NULL, 1)
        , ('TITLE', 'SUPERIEUR', 'SUP', NULL, 1)
        , ('TITLE', 'SUPERIEURE', 'SUP', NULL, 1)
        , ('TITLE', 'SYNDICAT', 'SYNDICAT', NULL, 1)
        , ('TITLE', 'TECHNICIEN', 'TECHNICIEN', NULL, 1)
        , ('TITLE', 'TECHNIQUE', 'TECHNIQUE', NULL, 1)
        , ('TITLE', 'TERRAIN', 'TERR', NULL, 1)
        , ('TITLE', 'TERRASSE', 'TERR', NULL, 1)
        , ('TITLE', 'TERRASSES', 'TERR', NULL, 1)
        , ('TITLE', 'TERTRE', 'TERT', NULL, 1)
        , ('TITLE', 'TERTRES', 'TERT', NULL, 1)
        , ('TITLE', 'TOUR', 'TOUR', NULL, 1)
        , ('TITLE', 'TRAVERSE', 'TRAV', NULL, 1)
        , ('TITLE', 'TUNNEL', 'TUN', NULL, 1)
        , ('TITLE', 'UNIVERSITAIRE', 'UNVT', NULL, 1)
        , ('TITLE', 'UNIVERSITE', 'UNIV', NULL, 1)
        , ('TITLE', 'VAL', 'VAL', NULL, 1)
        , ('TITLE', 'VALLEE', 'VALL', NULL, 1)
        , ('TITLE', 'VALLON', 'VALL', NULL, 1)
        , ('TITLE', 'VELODROME', 'VELOD', NULL, 1)
        , ('TITLE', 'VENELLE', 'VENE', NULL, 1)
        , ('TITLE', 'VENELLES', 'VENE', NULL, 1)
        , ('TITLE', 'VIEILLE', 'VIEI', NULL, 1)
        , ('TITLE', 'VIEILLES', 'VIEL', NULL, 1)
        , ('TITLE', 'VIEUX', 'VX', NULL, 1)
        , ('TITLE', 'VILLA', 'VILL', NULL, 1)
        , ('TITLE', 'VILLAGE', 'VLGE', NULL, 1)
        , ('TITLE', 'VILLAGES', 'VILL', NULL, 1)
        , ('TITLE', 'VILLAS', 'VILL', NULL, 1)
        , ('TITLE', 'VILLE', 'V', NULL, 1)
        , ('TITLE', 'VILLES', 'V', NULL, 1)
        , ('TITLE', 'VOIE', 'V', NULL, 1)
        , ('TITLE', 'VOIES', 'V', NULL, 1)
        ;
END;
$proc$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'set_laposte_address_correction_list');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_correction_list()
AS
$proc$
BEGIN
    IF NOT table_exists('fr', 'laposte_address') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    DELETE FROM fr.constant WHERE usecase = 'LAPOSTE_ADDRESS_CORRECTION';
    INSERT INTO fr.constant (usecase, key, value) VALUES
        ('LAPOSTE_ADDRESS_CORRECTION', 'TOO_SPACE', '1')
        , ('LAPOSTE_ADDRESS_CORRECTION', 'COMPLEMENT_WITH_STREET_ERROR', '2')
    ;
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

CREATE INDEX IF NOT EXISTS ix_constant_usecase_key ON fr.constant (usecase, key);
