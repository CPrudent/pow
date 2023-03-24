/***
 * add FR-ADDRESS facilities
 */

-- deduce department code from municipality code
SELECT public.drop_all_functions_if_exists('fr', 'get_department_code_from_municipality_code');
CREATE OR REPLACE FUNCTION fr.get_department_code_from_municipality_code(
    municipality_code CHARACTER(5)
)
RETURNS CHARACTER VARYING(3)
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE
        -- DOM + (98) = POLYNESIE
        WHEN LEFT(municipality_code, 2) IN ('97', '98') THEN LEFT(municipality_code, 3)
        -- FRANCE métropolitaine + (99) = MONACO
        ELSE LEFT(municipality_code, 2)
        END;
END
$func$ LANGUAGE plpgsql;

-- get project code from department code
SELECT public.drop_all_functions_if_exists('FR','get_project_code_from_department_code');
CREATE OR REPLACE FUNCTION FR.get_project_code_from_department_code(
    department_code CHARACTER(5)
)
RETURNS CHARACTER(1)
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE
        /* Guadeloupe Martinique (971XX et 972XX)
            * + Saint-Barthélemy (977XX), île francophone des Caraïbes
            * + Saint-Martin (978XX). Fait partie des îles Leeward dans la mer des Caraïbes. Elle est divisée entre 2 pays distincts : sa partie nord, appelée Saint-Martin, est française, et sa partie sud, Sint Maarten, est néerlandaise.
         */
        WHEN department_code IN ('971','972','977','978') THEN '2'

        /* Obsolète (LAMBERT II ETENDU)
        WHEN XXXX THEN '3'
         */

        --Guyane française (973XX), région d'outre-mer située sur la côte nord-est de l'Amérique du Sud
        WHEN department_code = '973' THEN '4'

        --Ile de la Réunion (974XX)
        WHEN department_code = '974' THEN '5'

        --Mayotte (976XX et 985XX sur co_adr), archipel de l'océan Indien situé entre Madagascar et la côte du Mozambique
        WHEN department_code IN ('976','985'/*Ancien code ? les codes adresses RAN commencent par 985*/) THEN '6'

        /* Saint-Pierre-et-Miquelon (975XX), archipel français au sud de l'île canadienne de Terre-Neuve
         * Pas de code projection RAN défini, ni d'adresse RAN existante
         */
        WHEN department_code = '975' THEN NULL --Saint-Pierre-et-Miquelon

        -- France Métropolitaine, Monaco
        ELSE '1'

        END;
END
$func$ LANGUAGE plpgsql;

-- get SRID from project code
SELECT public.drop_all_functions_if_exists('fr','get_srid_from_project_code');
CREATE OR REPLACE FUNCTION fr.get_srid_from_project_code(
    project_code CHARACTER
)
RETURNS SMALLINT
IMMUTABLE
AS
$func$
BEGIN
    RETURN CASE project_code
        -- France Métropolitaine, Monaco
        WHEN '1' THEN 2154

        /* Guadeloupe Martinique (971XX et 972XX)
            * + Saint-Barthélemy (977XX), île francophone des Caraïbes
            * + Saint-Martin (978XX). Fait partie des îles Leeward dans la mer des Caraïbes. Elle est divisée entre 2 pays distincts : sa partie nord, appelée Saint-Martin, est française, et sa partie sud, Sint Maarten, est néerlandaise.
         */
        WHEN '2' THEN 4559

        /* Obsolète (LAMBERT II ETENDU)
        WHEN '3' THEN XXXX
         */

        --Guyane française (973XX), région d'outre-mer située sur la côte nord-est de l'Amérique du Sud
        WHEN '4' THEN 2972

        --Ile de la Réunion (974XX)
        WHEN '5' THEN 2975

        --Mayotte (976XX), archipel de l'océan Indien situé entre Madagascar et la côte du Mozambique
        WHEN '6' THEN 4471

        /* Saint-Pierre-et-Miquelon (975XX), archipel français au sud de l'île canadienne de Terre-Neuve
            * Pas de code projection RAN défini, ni d'adresse RAN existante
        'X' THEN 4467
         */
        END;
END
$func$ LANGUAGE plpgsql;

-- get SRID from department code
SELECT public.drop_all_functions_if_exists('fr', 'get_srid_from_department_code');
CREATE OR REPLACE FUNCTION fr.get_srid_from_department_code(
    department_code CHARACTER(5)
)
RETURNS SMALLINT
IMMUTABLE
AS
$func$
BEGIN
	RETURN fr.get_srid_from_project_code(fr.get_project_code_from_department_code(department_code));
END
$func$ LANGUAGE plpgsql;

