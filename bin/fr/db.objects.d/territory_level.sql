/***
 * FR-TERRITORY level definition
 */

/*
 * initialize FR hierarchies (administrative, postal)
 */
SELECT public.drop_all_functions_if_exists('fr', 'set_territory_level');
CREATE OR REPLACE PROCEDURE fr.set_territory_level(
    municipality_subsection VARCHAR DEFAULT 'ZA'
) AS
$proc$
BEGIN
    CALL fr.check_municipality_subsection(
        municipality_subsection => municipality_subsection
        , check_level => FALSE
        , check_territory => FALSE
    );

    DELETE FROM public.territory_level WHERE country = 'FR';

    INSERT INTO public.territory_level(
        country
        , level
    )
    (
        SELECT 'FR', UNNEST(ARRAY[
            municipality_subsection
                , 'CP'
                    , 'PDC_PPDC'
                        , 'PPDC_PDC'
                                , 'DEX'
        , 'IRIS'
                , 'COM'
                    , 'COM_GLOBALE_ARM'
                        , 'EPCI'
                        , 'CV'
                        , 'ARR'
                            , 'DEP'
                                , 'REG'
                                    , 'METROPOLE_DOM_TOM'
                                        , 'PAYS'
        ])
    );

    UPDATE public.territory_level
    SET name =
        CASE level
            WHEN municipality_subsection    THEN
                                                CASE
                                                WHEN municipality_subsection = 'ZA'     THEN 'Zone d''Adresse'
                                                WHEN municipality_subsection = 'COM_CP' THEN 'Croisement Commune & Code Postal'
                                                END
                WHEN 'CP'                   THEN 'Code Postal'
                    WHEN 'PDC_PPDC'			THEN 'Zone de distribution du Courrier'
                        WHEN 'PPDC_PDC'     THEN 'Zone de préparation/distribution du Courrier'
                            WHEN 'DEX'      THEN 'Direction exécutive du Courrier'
        WHEN 'IRIS'                         THEN 'IRIS'
                WHEN 'COM'                  THEN 'Commune'
                    WHEN 'COM_GLOBALE_ARM'  THEN 'Commune globale composée d''arrondissements municipaux'
                        WHEN 'EPCI'         THEN 'EPCI (Etablissement Public de Coopération Intercommunale)'
                        WHEN 'CV'           THEN 'Canton ville'
                        WHEN 'ARR'          THEN 'Arrondissement départemental'
                            WHEN 'DEP'      THEN 'Département'
                                WHEN 'REG'  THEN 'Région'
                                    WHEN 'METROPOLE_DOM_TOM' THEN 'Métropole ou territoire d''outre-mer'
                                        WHEN 'PAYS'          THEN 'Pays'
            ELSE level
        END

        , name_short =
        CASE level
            WHEN municipality_subsection    THEN
                                                CASE
                                                WHEN municipality_subsection = 'ZA'     THEN 'Zone Adresse'
                                                WHEN municipality_subsection = 'COM_CP' THEN 'Commune/CP'
                                                END
                WHEN 'CP'                   THEN 'Code Postal'
                    WHEN 'PDC_PPDC'			THEN 'Zone distri. courrier'
                        WHEN 'PPDC_PDC'     THEN 'Zone prépa. courrier'
                            WHEN 'DEX'      THEN 'DEX'
        WHEN 'IRIS'                         THEN 'IRIS'
                WHEN 'COM'                  THEN 'Commune'
                    WHEN 'COM_GLOBALE_ARM'  THEN 'Commune globale'
                        WHEN 'EPCI'         THEN 'EPCI'
                        WHEN 'CV'           THEN 'Canton ville'
                        WHEN 'ARR'          THEN 'Arrondissement dép.'
                            WHEN 'DEP'      THEN 'Département'
                                WHEN 'REG'  THEN 'Région'
                                    WHEN 'METROPOLE_DOM_TOM' THEN 'Métropole ou DOM/TOM'
                                        WHEN 'PAYS'          THEN 'Pays'
            ELSE level
        END
        , name_plural =
        CASE level
            WHEN municipality_subsection    THEN
                                                CASE
                                                WHEN municipality_subsection = 'ZA'     THEN 'Zones d''Adresses'
                                                WHEN municipality_subsection = 'COM_CP' THEN 'Croisements Communes & Code Postaux'
                                                END
                WHEN 'CP'                   THEN 'Code Postaux'
                    WHEN 'PDC_PPDC'         THEN 'Zones de distribution du Courrier'
                        WHEN 'PPDC_PDC'     THEN 'Zones de préparation du Courrier'
                            WHEN 'DEX'      THEN 'Directions exécutives du Courrier'
        WHEN 'IRIS'                         THEN 'IRIS'
                WHEN 'COM'                  THEN 'Communes'
                    WHEN 'COM_GLOBALE_ARM'  THEN 'Communes globales composées d''arrondissements municipaux'
                        WHEN 'EPCI'         THEN 'EPCI (Etablissement Public de Coopération Intercommunale)'
                        WHEN 'CV'           THEN 'Cantons ville'
                        WHEN 'ARR'          THEN 'Arrondissements départementaux'
                            WHEN 'DEP'      THEN 'Départements'
                                WHEN 'REG'  THEN 'Régions'
                                    WHEN 'METROPOLE_DOM_TOM' THEN 'Métropoles ou territoires d''outre-mer'
                                        WHEN 'PAYS'          THEN 'Pays'
            ELSE level
        END
        , article =
        CASE level
            WHEN municipality_subsection    THEN
                                                CASE
                                                WHEN municipality_subsection = 'ZA'     THEN 'la'
                                                WHEN municipality_subsection = 'COM_CP' THEN 'le'
                                                END
                WHEN 'CP'                   THEN 'le'
                    WHEN 'PDC_PPDC'         THEN 'la'
                        WHEN 'PPDC_PDC'     THEN 'la'
                            WHEN 'DEX'      THEN 'la'
        WHEN 'IRIS'                         THEN 'l'''
                WHEN 'COM'                  THEN 'la'
                    WHEN 'COM_GLOBALE_ARM'  THEN 'la'
                        WHEN 'EPCI'         THEN 'l'''
                        WHEN 'CV'           THEN 'le'
                        WHEN 'ARR'          THEN 'l'''
                            WHEN 'DEP'      THEN 'le'
                                WHEN 'REG'  THEN 'la'
                                    WHEN 'METROPOLE_DOM_TOM' THEN 'la'
                                        WHEN 'PAYS'          THEN 'le'
            ELSE level
        END
        , hierarchy =
        CASE level
            WHEN municipality_subsection    THEN 110
                WHEN 'CP'                   THEN 210
                    WHEN 'PDC_PPDC'			THEN 310
                        WHEN 'PPDC_PDC'     THEN 410
                            WHEN 'DEX'      THEN 510
        WHEN 'IRIS'                         THEN 000
                WHEN 'COM'                  THEN 200
                    WHEN 'COM_GLOBALE_ARM'  THEN 300
                        WHEN 'EPCI'         THEN 400
                        WHEN 'CV'           THEN 401
                        WHEN 'ARR'          THEN 402
                            WHEN 'DEP'      THEN 500
                                WHEN 'REG'  THEN 600
                                    WHEN 'METROPOLE_DOM_TOM' THEN 700
                                        WHEN 'PAYS'          THEN 800
            ELSE NULL
        END
        , sublevels =
        CASE level
            WHEN municipality_subsection    THEN NULL
                WHEN 'CP'                   THEN ARRAY[municipality_subsection]
                    WHEN 'PDC_PPDC'         THEN ARRAY['CP']
                        WHEN 'PPDC_PDC'     THEN ARRAY['PDC_PPDC']
                            WHEN 'DEX'      THEN ARRAY['PPDC_PDC']
        WHEN 'IRIS'                         THEN NULL
                WHEN 'COM'                  THEN ARRAY[municipality_subsection, 'IRIS']
                    WHEN 'COM_GLOBALE_ARM'  THEN ARRAY['COM']
                        WHEN 'EPCI'         THEN ARRAY['COM']
                        WHEN 'CV'           THEN ARRAY['COM']
                        WHEN 'ARR'          THEN ARRAY['COM']
                            WHEN 'DEP'      THEN ARRAY['ARR', 'CV']
                                WHEN 'REG'  THEN ARRAY['DEP']
                                    WHEN 'METROPOLE_DOM_TOM' THEN ARRAY['REG']
                                        WHEN 'PAYS'          THEN ARRAY['METROPOLE_DOM_TOM']
            ELSE NULL
        END
    WHERE
        country = 'FR'
    ;
END
$proc$ LANGUAGE plpgsql;

-- get common level between 2 levels
SELECT public.drop_all_functions_if_exists('fr', 'get_common_level');
CREATE OR REPLACE FUNCTION fr.get_common_level(
    level_a VARCHAR
    , level_b VARCHAR
)
RETURNS VARCHAR AS
$func$
DECLARE
BEGIN
    IF level_a = level_b THEN
        RETURN level_a;
    ELSE
        --TODO : voir s'il serait plus performant de trouver un niveau commun plus grand quand cela est possible
        RETURN CASE
            WHEN public.is_level_below('fr', level_a, level_b) THEN level_a
            WHEN public.is_level_below('fr', level_b, level_a) THEN level_b
            WHEN public.is_level_below('fr', 'COM', level_a) AND public.is_level_below('fr', 'COM', level_b) THEN 'COM'
            WHEN public.is_level_below('fr', 'CP', level_a) AND public.is_level_below('fr', 'CP', level_b) THEN 'CP'
            ELSE
                -- base level (ZA|COM_CP)
                public.get_bigger_sublevel('fr', 'CP')
        END;
    END IF;
END
$func$ LANGUAGE plpgsql;
