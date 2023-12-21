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
        SELECT name, (REGEXP_MATCHES(name, '([^ ]*)'))[1] first_word
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
        SELECT DISTINCT 'EXT', lb_ext, lb_abr_nn, lb_ext
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
        ('TITLE', 'ABBAYE', 'ABBA', 'ABBAYE', 1)
        , ('TITLE', 'ABBE', 'ABBE', 'ABBE', 1)
        , ('TITLE', 'ACTIVITE', 'A', 'ACTIVITE', 1)
        , ('TITLE', 'ADJUDANT', 'ADJ', 'ADJUDANT', 1)
        , ('TITLE', 'AERODROME', 'AER', 'AERODROME', 1)
        , ('TITLE', 'AEROGARE', 'AEROGARE', 'AEROGARE', 1)
        , ('TITLE', 'AERONAUTIQUE', 'AERONAUTIQUE', 'AERONAUTIQUE', 1)
        , ('TITLE', 'AEROPORT', 'AERP', 'AEROPORT', 1)
        , ('TITLE', 'AGENCE', 'AGENCE', 'AGENCE', 1)
        , ('TITLE', 'AGGLOMERATION', 'AGGL', 'AGGLOMERATION', 1)
        , ('TITLE', 'AGRICOLE', 'AGRIC', 'AGRICOLE', 1)
        , ('TITLE', 'AIRE', 'AIRE', 'AIRE', 1)
        , ('TITLE', 'AIRES', 'AIRE', 'AIRES', 1)
        , ('TITLE', 'ALLEE', 'ALL', 'ALLEE', 1)
        , ('TITLE', 'ALLEES', 'ALL', 'ALLEES', 1)
        , ('TITLE', 'AMENAGEMENT', 'A', 'AMENAGEMENT', 1)
        , ('TITLE', 'AMIRAL', 'AMIRAL', 'AMIRAL', 1)
        , ('TITLE', 'ANCIEN', 'ANC', 'ANCIEN', 1)
        , ('TITLE', 'ANCIENNE', 'ANCI', 'ANCIENNE', 1)
        , ('TITLE', 'ANSE', 'ANSE', 'ANSE', 1)
        , ('TITLE', 'APPARTEMENT', 'APPT', 'APPARTEMENT', 1)
        , ('TITLE', 'ARCADE', 'ARCA', 'ARCADE', 1)
        , ('TITLE', 'ARCADES', 'ARCA', 'ARCADES', 1)
        , ('TITLE', 'ARMEMENT', 'ARMEMENT', 'ARMEMENT', 1)
        , ('TITLE', 'ARRONDISSEMENT', 'ARR', 'ARRONDISSEMENT', 1)
        , ('TITLE', 'ARTISANALE', 'ARTISANALE', 'ARTISANALE', 1)
        , ('TITLE', 'ASPIRANT', 'ASP', 'ASPIRANT', 1)
        , ('TITLE', 'ASSOCIATION', 'ASSOCIATION', 'ASSOCIATION', 1)
        , ('TITLE', 'ATELIER', 'ATELIER', 'ATELIER', 1)
        , ('TITLE', 'AUTOROUTE', 'AUTO', 'AUTOROUTE', 1)
        , ('TITLE', 'BARRIERE', 'BARR', 'BARRIERE', 1)
        , ('TITLE', 'BARRIERES', 'BARR', 'BARRIERES', 1)
        , ('TITLE', 'BAS', 'BAS', 'BAS', 1)
        , ('TITLE', 'BASSE', 'BAS', 'BASSE', 1)
        , ('TITLE', 'BASSES', 'BAS', 'BASSES', 1)
        , ('TITLE', 'BASTIDE', 'BAST', 'BASTIDE', 1)
        , ('TITLE', 'BASTION', 'BASTION', 'BASTION', 1)
        , ('TITLE', 'BATAILLON', 'BTN', 'BATAILLON', 1)
        , ('TITLE', 'BATAILLONS', 'BTN', 'BATAILLONS', 1)
        , ('TITLE', 'BATIMENT', 'BAT', 'BATIMENT', 1)
        , ('TITLE', 'BATIMENTS', 'BAT', 'BATIMENTS', 1)
        , ('TITLE', 'BEGUINAGE', 'BEGUINAGE', 'BEGUINAGE', 1)
        , ('TITLE', 'BERGE', 'BERG', 'BERGE', 1)
        , ('TITLE', 'BERGES', 'BERG', 'BERGES', 1)
        , ('TITLE', 'BIS', 'B', 'BIS', 1)
        , ('TITLE', 'BOIS', 'BOIS', 'BOIS', 1)
        , ('TITLE', 'BOUCLE', 'BOUC', 'BOUCLE', 1)
        , ('TITLE', 'BOURG', 'BOUR', 'BOURG', 1)
        , ('TITLE', 'BUTTE', 'BUTT', 'BUTTE', 1)
        , ('TITLE', 'CABINET', 'CABINET', 'CABINET', 1)
        , ('TITLE', 'CALE', 'CALE', 'CALE', 1)
        , ('TITLE', 'CAMP', 'CAMP', 'CAMP', 1)
        , ('TITLE', 'CAMPAGNE', 'CAMP', 'CAMPAGNE', 1)
        , ('TITLE', 'CAMPING', 'CAMPING', 'CAMPING', 1)
        , ('TITLE', 'CANAL', 'CANAL', 'CANAL', 1)
        , ('TITLE', 'CANTON', 'CANT', 'CANTON', 1)
        , ('TITLE', 'CAPITAINE', 'C', 'CAPITAINE', 1)
        , ('TITLE', 'CARDINAL', 'CDL', 'CARDINAL', 1)
        , ('TITLE', 'CARRE', 'CARR', 'CARRE', 1)
        , ('TITLE', 'CARREAU', 'CARR', 'CARREAU', 1)
        , ('TITLE', 'CARREFOUR', 'CARR', 'CARREFOUR', 1)
        , ('TITLE', 'CARRIERE', 'CARR', 'CARRIERE', 1)
        , ('TITLE', 'CARRIERES', 'CARR', 'CARRIERES', 1)
        , ('TITLE', 'CASERNE', 'CASR', 'CASERNE', 1)
        , ('TITLE', 'CASTEL', 'CASTEL', 'CASTEL', 1)
        , ('TITLE', 'CAVEE', 'CAVE', 'CAVEE', 1)
        , ('TITLE', 'CENTRAL', 'CENTRAL', 'CENTRAL', 1)
        , ('TITLE', 'CENTRE', 'CTRE', 'CENTRE', 1)
        , ('TITLE', 'CHALET', 'CHALET', 'CHALET', 1)
        , ('TITLE', 'CHAMBRE', 'CHAMBRE', 'CHAMBRE', 1)
        , ('TITLE', 'CHANOINE', 'CHANOINE', 'CHANOINE', 1)
        , ('TITLE', 'CHAPELLE', 'CHAP', 'CHAPELLE', 1)
        , ('TITLE', 'CHARMILLE', 'CHARMILLE', 'CHARMILLE', 1)
        , ('TITLE', 'CHATEAU', 'CHAT', 'CHATEAU', 1)
        , ('TITLE', 'CHAUSSEE', 'CHAUSSEE', 'CHAUSSEE', 1)
        , ('TITLE', 'CHAUSSEES', 'CHAUSSEES', 'CHAUSSEES', 1)
        , ('TITLE', 'CHEMIN', 'CHEM', 'CHEMIN', 1)
        , ('TITLE', 'CHEMINS', 'CHEM', 'CHEMINS', 1)
        , ('TITLE', 'CHEZ', 'CHEZ', 'CHEZ', 1)
        , ('TITLE', 'CITADELLE', 'CITADELLE', 'CITADELLE', 1)
        , ('TITLE', 'CITE', 'CITE', 'CITE', 1)
        , ('TITLE', 'CITES', 'CITES', 'CITES', 1)
        , ('TITLE', 'CLOITRE', 'CLOITRE', 'CLOITRE', 1)
        , ('TITLE', 'CLOS', 'CLOS', 'CLOS', 1)
        , ('TITLE', 'COL', 'COL', 'COL', 1)
        , ('TITLE', 'COLLEGE', 'COLLEGE', 'COLLEGE', 1)
        , ('TITLE', 'COLLINE', 'COLL', 'COLLINE', 1)
        , ('TITLE', 'COLLINES', 'COLL', 'COLLINES', 1)
        , ('TITLE', 'COLONEL', 'COL', 'COLONEL', 1)
        , ('TITLE', 'COLONIE', 'COLO', 'COLONIE', 1)
        , ('TITLE', 'COMITE', 'COMITE', 'COMITE', 1)
        , ('TITLE', 'COMMANDANT', 'CDT', 'COMMANDANT', 1)
        , ('TITLE', 'COMMERCIAL', 'CIAL', 'COMMERCIAL', 1)
        , ('TITLE', 'COMMUNAL', 'COM', 'COMMUNAL', 1)
        , ('TITLE', 'COMMUNALE', 'C', 'COMMUNALE', 1)
        , ('TITLE', 'COMMUNAUX', 'COM', 'COMMUNAUX', 1)
        , ('TITLE', 'COMMUNE', 'COM', 'COMMUNE', 1)
        , ('TITLE', 'COMPAGNIE', 'CIE', 'COMPAGNIE', 1)
        , ('TITLE', 'COMPAGNON', 'COMP', 'COMPAGNON', 1)
        , ('TITLE', 'COMPAGNONS', 'COMP', 'COMPAGNONS', 1)
        , ('TITLE', 'CONCERTE', 'CONCERTE', 'CONCERTE', 1)
        , ('TITLE', 'CONTOUR', 'CONTOUR', 'CONTOUR', 1)
        , ('TITLE', 'COOPERATIVE', 'COOP', 'COOPERATIVE', 1)
        , ('TITLE', 'CORNICHE', 'CORNICHE', 'CORNICHE', 1)
        , ('TITLE', 'CORNICHES', 'CORNICHES', 'CORNICHES', 1)
        , ('TITLE', 'COTE', 'COTE', 'COTE', 1)
        , ('TITLE', 'COTEAU', 'COTE', 'COTEAU', 1)
        , ('TITLE', 'COTTAGE', 'COTT', 'COTTAGE', 1)
        , ('TITLE', 'COTTAGES', 'COTT', 'COTTAGES', 1)
        , ('TITLE', 'COULOIR', 'COULOIR', 'COULOIR', 1)
        , ('TITLE', 'COUR', 'COUR', 'COUR', 1)
        , ('TITLE', 'COURS', 'COUR', 'COURS', 1)
        , ('TITLE', 'CROIX', 'CRX', 'CROIX', 1)
        , ('TITLE', 'DEGRE', 'DEGRE', 'DEGRE', 1)
        , ('TITLE', 'DEGRES', 'DEGRES', 'DEGRES', 1)
        , ('TITLE', 'DEPARTEMENTAL', 'DEP', 'DEPARTEMENTAL', 1)
        , ('TITLE', 'DESCENTE', 'DESCENTE', 'DESCENTE', 1)
        , ('TITLE', 'DIGUE', 'DIGU', 'DIGUE', 1)
        , ('TITLE', 'DIRECTEUR', 'DIRECTEUR', 'DIRECTEUR', 1)
        , ('TITLE', 'DIRECTION', 'DIR', 'DIRECTION', 1)
        , ('TITLE', 'DIT', 'D', 'DIT', 1)
        , ('TITLE', 'DIVISION', 'DIV', 'DIVISION', 1)
        , ('TITLE', 'DOCTEUR', 'DR', 'DOCTEUR', 1)
        , ('TITLE', 'DOMAINE', 'DOMA', 'DOMAINE', 1)
        , ('TITLE', 'DOMAINES', 'DOMA', 'DOMAINES', 1)
        , ('TITLE', 'ECLUSE', 'ECLU', 'ECLUSE', 1)
        , ('TITLE', 'ECLUSES', 'ECLU', 'ECLUSES', 1)
        , ('TITLE', 'ECOLE', 'ECOLE', 'ECOLE', 1)
        , ('TITLE', 'ECONOMIQUE', 'ECO', 'ECONOMIQUE', 1)
        , ('TITLE', 'ECRIVAINS', 'ECRIV', 'ECRIVAINS', 1)
        , ('TITLE', 'EGLISE', 'EGLI', 'EGLISE', 1)
        , ('TITLE', 'ENCEINTE', 'ENCEINTE', 'ENCEINTE', 1)
        , ('TITLE', 'ENCLAVE', 'ENCL', 'ENCLAVE', 1)
        , ('TITLE', 'ENCLOS', 'ENCL', 'ENCLOS', 1)
        , ('TITLE', 'ENSEIGNEMENT', 'ENST', 'ENSEIGNEMENT', 1)
        , ('TITLE', 'ENSEMBLE', 'ENSEMBLE', 'ENSEMBLE', 1)
        , ('TITLE', 'ENTREE', 'ENT', 'ENTREE', 1)
        , ('TITLE', 'ENTREES', 'ENT', 'ENTREES', 1)
        , ('TITLE', 'ENTREPRISE', 'ENTR', 'ENTREPRISE', 1)
        , ('TITLE', 'EPOUX', 'EP', 'EPOUX', 1)
        , ('TITLE', 'ESCALIER', 'ESC', 'ESCALIER', 1)
        , ('TITLE', 'ESCALIERS', 'ESC', 'ESCALIERS', 1)
        , ('TITLE', 'ESPACE', 'ESPA', 'ESPACE', 1)
        , ('TITLE', 'ESPLANADE', 'ESPL', 'ESPLANADE', 1)
        , ('TITLE', 'ESPLANADES', 'ESPL', 'ESPLANADES', 1)
        , ('TITLE', 'ETABLISSEMENT', 'ETABLISSEMENT', 'ETABLISSEMENT', 1)
        , ('TITLE', 'ETABLISSEMENTS', 'ETABLISSEMENT', 'ETABLISSEMENTS', 1)
        , ('TITLE', 'ETANG', 'ETAN', 'ETANG', 1)
        , ('TITLE', 'EVEQUE', 'EVEQUE', 'EVEQUE', 1)
        , ('TITLE', 'FACULTE', 'FACULTE', 'FACULTE', 1)
        , ('TITLE', 'FAUBOURG', 'FAUB', 'FAUBOURG', 1)
        , ('TITLE', 'FERME', 'FERM', 'FERME', 1)
        , ('TITLE', 'FERMES', 'FERM', 'FERMES', 1)
        , ('TITLE', 'FONTAINE', 'FONT', 'FONTAINE', 1)
        , ('TITLE', 'FORESTIER', 'FORESTIER', 'FORESTIER', 1)
        , ('TITLE', 'FORESTIERE', 'FORESTIERE', 'FORESTIERE', 1)
        , ('TITLE', 'FORET', 'FOR', 'FORET', 1)
        , ('TITLE', 'FORT', 'FORT', 'FORT', 1)
        , ('TITLE', 'FORUM', 'FORUM', 'FORUM', 1)
        , ('TITLE', 'FOSSE', 'FOSS', 'FOSSE', 1)
        , ('TITLE', 'FOSSES', 'FOSS', 'FOSSES', 1)
        , ('TITLE', 'FOYER', 'FOYE', 'FOYER', 1)
        , ('TITLE', 'FRANCAIS', 'FR', 'FRANCAIS', 1)
        , ('TITLE', 'FRANCAISE', 'FR', 'FRANCAISE', 1)
        , ('TITLE', 'FUSILIERS', 'FUSILIERS', 'FUSILIERS', 1)
        , ('TITLE', 'GALERIE', 'GALE', 'GALERIE', 1)
        , ('TITLE', 'GALERIES', 'GALE', 'GALERIES', 1)
        , ('TITLE', 'GARE', 'GARE', 'GARE', 1)
        , ('TITLE', 'GARENNE', 'GARE', 'GARENNE', 1)
        , ('TITLE', 'GENDARMERIE', 'GENDARMERIE', 'GENDARMERIE', 1)
        , ('TITLE', 'GENERAL', 'GAL', 'GENERAL', 1)
        , ('TITLE', 'GOUVERNEUR', 'GOUV', 'GOUVERNEUR', 1)
        , ('TITLE', 'GRAND', 'GD', 'GRAND', 1)
        , ('TITLE', 'GRANDE', 'GDE', 'GRANDE', 1)
        , ('TITLE', 'GRANDES', 'GDES', 'GRANDES', 1)
        , ('TITLE', 'GRANDS', 'GDS', 'GRANDS', 1)
        , ('TITLE', 'GRILLE', 'GRI', 'GRILLE', 1)
        , ('TITLE', 'GRIMPETTE', 'GRIMPETTE', 'GRIMPETTE', 1)
        , ('TITLE', 'GROUPE', 'GROU', 'GROUPE', 1)
        , ('TITLE', 'GROUPEMENT', 'GROUPEMENT', 'GROUPEMENT', 1)
        , ('TITLE', 'HALAGE', 'HALAGE', 'HALAGE', 1)
        , ('TITLE', 'HALLE', 'HALL', 'HALLE', 1)
        , ('TITLE', 'HALLES', 'HALL', 'HALLES', 1)
        , ('TITLE', 'HAMEAU', 'HAME', 'HAMEAU', 1)
        , ('TITLE', 'HAMEAUX', 'HAME', 'HAMEAUX', 1)
        , ('TITLE', 'HAUT', 'HT', 'HAUT', 1)
        , ('TITLE', 'HAUTE', 'HTE', 'HAUTE', 1)
        , ('TITLE', 'HAUTES', 'HTES', 'HAUTES', 1)
        , ('TITLE', 'HAUTS', 'HTS', 'HAUTS', 1)
        , ('TITLE', 'HIPPODROME', 'HIPP', 'HIPPODROME', 1)
        , ('TITLE', 'HLM', 'HLM', 'HLM', 1)
        , ('TITLE', 'HOPITAL', 'HOP', 'HOPITAL', 1)
        , ('TITLE', 'HOSPICE', 'HOSPICE', 'HOSPICE', 1)
        , ('TITLE', 'HOSPITALIER', 'HOSPITALIER', 'HOSPITALIER', 1)
        , ('TITLE', 'HOTEL', 'HOT', 'HOTEL', 1)
        , ('TITLE', 'ILE', 'ILE', 'ILE', 1)
        , ('TITLE', 'ILOT', 'ILOT', 'ILOT', 1)
        , ('TITLE', 'IMMEUBLE', 'IMM', 'IMMEUBLE', 1)
        , ('TITLE', 'IMMEUBLES', 'IMM', 'IMMEUBLES', 1)
        , ('TITLE', 'IMPASSE', 'IMP', 'IMPASSE', 1)
        , ('TITLE', 'IMPASSES', 'IMP', 'IMPASSES', 1)
        , ('TITLE', 'INFANTERIE', 'INFANT', 'INFANTERIE', 1)
        , ('TITLE', 'INFERIEUR', 'INF', 'INFERIEUR', 1)
        , ('TITLE', 'INFERIEURE', 'INF', 'INFERIEURE', 1)
        , ('TITLE', 'INGENIEUR', 'ING', 'INGENIEUR', 1)
        , ('TITLE', 'INSPECTEUR', 'INSPECTEUR', 'INSPECTEUR', 1)
        , ('TITLE', 'INSTITUT', 'INST', 'INSTITUT', 1)
        , ('TITLE', 'INTERNATIONAL', 'INTERN', 'INTERNATIONAL', 1)
        , ('TITLE', 'INTERNATIONALE', 'INTERN', 'INTERNATIONALE', 1)
        , ('TITLE', 'JARDIN', 'JARD', 'JARDIN', 1)
        , ('TITLE', 'JARDINS', 'JARD', 'JARDINS', 1)
        , ('TITLE', 'JETEE', 'JETEE', 'JETEE', 1)
        , ('TITLE', 'LABORATOIRE', 'LABORATOIRE', 'LABORATOIRE', 1)
        , ('TITLE', 'LEVEE', 'LEVEE', 'LEVEE', 1)
        , ('TITLE', 'LIEUTENANT', 'LT', 'LIEUTENANT', 2)
        , ('TITLE', 'LIEUTENANT DE VAISSEAU', 'LTDV', 'LIEUTENANT', 1)
        , ('TITLE', 'MADAME', 'MME', 'MADAME', 1)
        , ('TITLE', 'MADEMOISELLE', 'MLLE', 'MADEMOISELLE', 1)
        , ('TITLE', 'MAGASIN', 'MAG', 'MAGASIN', 1)
        , ('TITLE', 'MAIL', 'MAIL', 'MAIL', 1)
        , ('TITLE', 'MAIRIE', 'MAIRIE', 'MAIRIE', 1)
        , ('TITLE', 'MAISON', 'MAIS', 'MAISON', 1)
        , ('TITLE', 'MAITRE', 'MAITRE', 'MAITRE', 1)
        , ('TITLE', 'MANOIR', 'MANOIR', 'MANOIR', 1)
        , ('TITLE', 'MARAIS', 'MARAIS', 'MARAIS', 1)
        , ('TITLE', 'MARCHE', 'MARC', 'MARCHE', 1)
        , ('TITLE', 'MARCHES', 'MARC', 'MARCHES', 1)
        , ('TITLE', 'MARECHAL', 'MAL', 'MARECHAL', 1)
        , ('TITLE', 'MARITIME', 'MARITIME', 'MARITIME', 1)
        , ('TITLE', 'MARTYR', 'MYR', 'MARTYR', 1)
        , ('TITLE', 'MARTYRS', 'MYR', 'MARTYRS', 1)
        , ('TITLE', 'MAS', 'MAS', 'MAS', 1)
        , ('TITLE', 'MEDECIN', 'MED', 'MEDECIN', 1)
        , ('TITLE', 'MEDICAL', 'MED', 'MEDICAL', 1)
        , ('TITLE', 'MESDEMOISELLES', 'MESDEMOISELLES', 'MESDEMOISELLES', 1)
        , ('TITLE', 'MESSIEURS', 'MESSIEURS', 'MESSIEURS', 1)
        , ('TITLE', 'METRO', 'METR', 'METRO', 1)
        , ('TITLE', 'MILITAIRE', 'MIL', 'MILITAIRE', 1)
        , ('TITLE', 'MONSEIGNEUR', 'MGR', 'MONSEIGNEUR', 1)
        , ('TITLE', 'MONSIEUR', 'M', 'MONSIEUR', 1)
        , ('TITLE', 'MONTEE', 'MONT', 'MONTEE', 1)
        , ('TITLE', 'MONTEES', 'MONT', 'MONTEES', 1)
        , ('TITLE', 'MOULIN', 'MOUL', 'MOULIN', 1)
        , ('TITLE', 'MOULINS', 'MOUL', 'MOULINS', 1)
        , ('TITLE', 'MUNICIPAL', 'MUN', 'MUNICIPAL', 1)
        , ('TITLE', 'MUSEE', 'MUSE', 'MUSEE', 1)
        , ('TITLE', 'NATIONAL', 'NAT', 'NATIONAL', 1)
        , ('TITLE', 'NOTRE DAME', 'ND', 'NOTRE', 1)
        , ('TITLE', 'NOUVEAU', 'NOUV', 'NOUVEAU', 1)
        , ('TITLE', 'NOUVELLE', 'NOUV', 'NOUVELLE', 1)
        , ('TITLE', 'OBSERVATOIRE', 'OBSERVATOIRE', 'OBSERVATOIRE', 1)
        , ('TITLE', 'PALAIS', 'PALA', 'PALAIS', 1)
        , ('TITLE', 'PARC', 'PARC', 'PARC', 1)
        , ('TITLE', 'PARCS', 'PARC', 'PARCS', 1)
        , ('TITLE', 'PARKING', 'PARK', 'PARKING', 1)
        , ('TITLE', 'PARVIS', 'PARV', 'PARVIS', 1)
        , ('TITLE', 'PASSAGE', 'PAS', 'PASSAGE', 1)
        , ('TITLE', 'PASSE', 'PASS', 'PASSE', 1)
        , ('TITLE', 'PASSERELLE', 'PASS', 'PASSERELLE', 1)
        , ('TITLE', 'PASSERELLES', 'PASS', 'PASSERELLES', 1)
        , ('TITLE', 'PASSES', 'PASSES', 'PASSES', 1)
        , ('TITLE', 'PASTEUR', 'PAST', 'PASTEUR', 1)
        , ('TITLE', 'PATIO', 'PATIO', 'PATIO', 1)
        , ('TITLE', 'PAVILLON', 'PAVI', 'PAVILLON', 1)
        , ('TITLE', 'PAVILLONS', 'PAVI', 'PAVILLONS', 1)
        , ('TITLE', 'PETIT', 'PT', 'PETIT', 1)
        , ('TITLE', 'PETITE', 'PTE', 'PETITE', 1)
        , ('TITLE', 'PETITES', 'PTE', 'PETITES', 1)
        , ('TITLE', 'PETITS', 'PT', 'PETITS', 1)
        , ('TITLE', 'PLACE', 'PL', 'PLACE', 1)
        , ('TITLE', 'PLACIS', 'PLACIS', 'PLACIS', 1)
        , ('TITLE', 'PLAGE', 'PLAG', 'PLAGE', 1)
        , ('TITLE', 'PLAGES', 'PLAG', 'PLAGES', 1)
        , ('TITLE', 'PLAINE', 'PLAI', 'PLAINE', 1)
        , ('TITLE', 'PLAN', 'PLAN', 'PLAN', 1)
        , ('TITLE', 'PLATEAU', 'PLAT', 'PLATEAU', 1)
        , ('TITLE', 'PLATEAUX', 'PLAT', 'PLATEAUX', 1)
        , ('TITLE', 'POINTE', 'POIN', 'POINTE', 1)
        , ('TITLE', 'POLICE', 'POL', 'POLICE', 1)
        , ('TITLE', 'PONT', 'PONT', 'PONT', 1)
        , ('TITLE', 'PONTS', 'PONT', 'PONTS', 1)
        , ('TITLE', 'PORCHE', 'PORCHE', 'PORCHE', 1)
        , ('TITLE', 'PORT', 'PORT', 'PORT', 1)
        , ('TITLE', 'PORTE', 'PORT', 'PORTE', 1)
        , ('TITLE', 'PORTIQUES', 'PORTIQUES', 'PORTIQUES', 1)
        , ('TITLE', 'POTERNE', 'POTERNE', 'POTERNE', 1)
        , ('TITLE', 'POURTOUR', 'POURTOUR', 'POURTOUR', 1)
        , ('TITLE', 'PRE', 'PRE', 'PRE', 1)
        , ('TITLE', 'PREFET', 'PREFET', 'PREFET', 1)
        , ('TITLE', 'PRESIDENT', 'PDT', 'PRESIDENT', 1)
        , ('TITLE', 'PRESQU', 'PRESQU', 'PRESQU', 1)
        , ('TITLE', 'PROFESSEUR', 'PR', 'PROFESSEUR', 1)
        , ('TITLE', 'PROFESSIONNEL', 'PROF', 'PROFESSIONNEL', 1)
        , ('TITLE', 'PROLONGE', 'PROL', 'PROLONGE', 1)
        , ('TITLE', 'PROLONGEE', 'PROL', 'PROLONGEE', 1)
        , ('TITLE', 'PROMENADE', 'PROM', 'PROMENADE', 1)
        , ('TITLE', 'PROPRIETE', 'PROPRIETE', 'PROPRIETE', 1)
        , ('TITLE', 'QUAI', 'QUAI', 'QUAI', 1)
        , ('TITLE', 'QUARTIER', 'QUAR', 'QUARTIER', 1)
        , ('TITLE', 'QUINQUIES', 'QUINQUIES', 'QUINQUIES', 1)
        , ('TITLE', 'RACCOURCI', 'RACC', 'RACCOURCI', 1)
        , ('TITLE', 'RAIDILLON', 'RAIDILLON', 'RAIDILLON', 1)
        , ('TITLE', 'RAMPE', 'RAMPE', 'RAMPE', 1)
        , ('TITLE', 'RECTEUR', 'RECT', 'RECTEUR', 1)
        , ('TITLE', 'REGIMENT', 'RGT', 'REGIMENT', 1)
        , ('TITLE', 'REGIONAL', 'REGIONAL', 'REGIONAL', 1)
        , ('TITLE', 'REMPART', 'REMPART', 'REMPART', 1)
        , ('TITLE', 'REPUBLIQUE', 'REP', 'REPUBLIQUE', 1)
        , ('TITLE', 'RESIDENCE', 'RES', 'RESIDENCE', 1)
        , ('TITLE', 'RESIDENCES', 'RESI', 'RESIDENCES', 1)
        , ('TITLE', 'RESTAURANT', 'REST', 'RESTAURANT', 1)
        , ('TITLE', 'ROC', 'ROC', 'ROC', 1)
        , ('TITLE', 'ROCADE', 'ROCADE', 'ROCADE', 1)
        , ('TITLE', 'ROQUET', 'ROQUET', 'ROQUET', 1)
        , ('TITLE', 'ROTONDE', 'ROTO', 'ROTONDE', 1)
        , ('TITLE', 'SAINT', 'ST', 'SAINT', 1)
        , ('TITLE', 'SAINTE', 'STE', 'SAINTE', 1)
        , ('TITLE', 'SAINTS', 'ST', 'SAINTS', 1)
        , ('TITLE', 'SAINTES', 'STS', 'SAINTES', 1)
        , ('TITLE', 'SERGENT', 'SGT', 'SERGENT', 1)
        , ('TITLE', 'SERVICE', 'SCE', 'SERVICE', 1)
        , ('TITLE', 'SOCIETE', 'SOC', 'SOCIETE', 1)
        , ('TITLE', 'STADE', 'STAD', 'STADE', 1)
        , ('TITLE', 'STATION', 'STAT', 'STATION', 1)
        , ('TITLE', 'SUPERIEUR', 'SUP', 'SUPERIEUR', 1)
        , ('TITLE', 'SUPERIEURE', 'SUP', 'SUPERIEURE', 1)
        , ('TITLE', 'SYNDICAT', 'SYNDICAT', 'SYNDICAT', 1)
        , ('TITLE', 'TECHNICIEN', 'TECHNICIEN', 'TECHNICIEN', 1)
        , ('TITLE', 'TECHNIQUE', 'TECHNIQUE', 'TECHNIQUE', 1)
        , ('TITLE', 'TERRAIN', 'TERR', 'TERRAIN', 1)
        , ('TITLE', 'TERRASSE', 'TERR', 'TERRASSE', 1)
        , ('TITLE', 'TERRASSES', 'TERR', 'TERRASSES', 1)
        , ('TITLE', 'TERTRE', 'TERT', 'TERTRE', 1)
        , ('TITLE', 'TERTRES', 'TERT', 'TERTRES', 1)
        , ('TITLE', 'TOUR', 'TOUR', 'TOUR', 1)
        , ('TITLE', 'TRAVERSE', 'TRAV', 'TRAVERSE', 1)
        , ('TITLE', 'TUNNEL', 'TUN', 'TUNNEL', 1)
        , ('TITLE', 'UNIVERSITAIRE', 'UNVT', 'UNIVERSITAIRE', 1)
        , ('TITLE', 'UNIVERSITE', 'UNIV', 'UNIVERSITE', 1)
        , ('TITLE', 'VAL', 'VAL', 'VAL', 1)
        , ('TITLE', 'VALLEE', 'VALL', 'VALLEE', 1)
        , ('TITLE', 'VALLON', 'VALL', 'VALLON', 1)
        , ('TITLE', 'VELODROME', 'VELOD', 'VELODROME', 1)
        , ('TITLE', 'VENELLE', 'VENE', 'VENELLE', 1)
        , ('TITLE', 'VENELLES', 'VENE', 'VENELLES', 1)
        , ('TITLE', 'VIEILLE', 'VIEI', 'VIEILLE', 1)
        , ('TITLE', 'VIEILLES', 'VIEL', 'VIEILLES', 1)
        , ('TITLE', 'VIEUX', 'VX', 'VIEUX', 1)
        , ('TITLE', 'VILLA', 'VILL', 'VILLA', 1)
        , ('TITLE', 'VILLAGE', 'VLGE', 'VILLAGE', 1)
        , ('TITLE', 'VILLAGES', 'VILL', 'VILLAGES', 1)
        , ('TITLE', 'VILLAS', 'VILL', 'VILLAS', 1)
        , ('TITLE', 'VILLE', 'V', 'VILLE', 1)
        , ('TITLE', 'VILLES', 'V', 'VILLES', 1)
        , ('TITLE', 'VOIE', 'V', 'VOIE', 1)
        , ('TITLE', 'VOIES', 'V', 'VOIES', 1)
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
