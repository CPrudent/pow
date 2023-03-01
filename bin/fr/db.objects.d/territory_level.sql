/***
 * FR-TERRITORY level definition
 */

SELECT drop_all_functions_if_exists('public', 'territory_level');
DELETE FROM public.territory_level WHERE country = 'FR';
INSERT INTO public.territory_level(
    country
    , level
)
(
    'FR'
    , SELECT UNNEST(ARRAY[
        'ZA'
            ,'CP'
                ,'PPDC_PDC'
                        ,'DEX'
    ,'IRIS'
            ,'COM'
                ,'COM_GLOBALE_ARM'
                    ,'EPCI'
                    ,'CV'
                    ,'ARR'
                        ,'DEP'
                            ,'REG'
                                ,'METROPOLE_DOM_TOM'
                                    ,'PAYS'
    ])
);

UPDATE public.territory_level
SET name =
    CASE "level"
        WHEN 'ZA'                       THEN 'Croisement Commune & Code Postal'
            WHEN 'CP'                   THEN 'Code Postal'
                WHEN 'PPDC_PDC'         THEN 'Zone de préparation du Courrier'
                    WHEN 'DEX'          THEN 'Direction exécutive Courrier'
    WHEN 'IRIS' THEN 'IRIS'
            WHEN 'COM'                  THEN 'Commune'
                WHEN 'COM_GLOBALE_ARM'  THEN 'Commune globale composée d''arrondissements municipaux'
                    WHEN 'EPCI'         THEN 'EPCI (Etablissement Public de Coopération Intercommunale)'
                    WHEN 'CV'           THEN 'Canton ville'
                    WHEN 'ARR'          THEN 'Arrondissement départemental'
                        WHEN 'DEP'      THEN 'Département'
                            WHEN 'REG'  THEN 'Région'
                                WHEN 'METROPOLE_DOM_TOM' THEN 'Métropole ou territoire d''outre-mer'
                                    WHEN 'PAYS'          THEN 'Pays'
        ELSE "level"
    END

    ,name_short =
    CASE "level"
        WHEN 'ZA'                       THEN 'Commune/CP'
            WHEN 'CP'                   THEN 'Code Postal'
                WHEN 'PPDC_PDC'         THEN 'Zone prépa. courrier'
                    WHEN 'DEX'          THEN 'DEX'
    WHEN 'IRIS' THEN 'IRIS'
            WHEN 'COM'                  THEN 'Commune'
                WHEN 'COM_GLOBALE_ARM'  THEN 'Commune globale'
                    WHEN 'EPCI'         THEN 'EPCI'
                    WHEN 'CV'           THEN 'Canton ville'
                    WHEN 'ARR'          THEN 'Arrondissement dép.'
                        WHEN 'DEP'      THEN 'Département'
                            WHEN 'REG'  THEN 'Région'
                                WHEN 'METROPOLE_DOM_TOM' THEN 'Métropole ou DOM/TOM'
                                    WHEN 'PAYS'          THEN 'Pays'
        ELSE "level"
    END
    ,name_article =
    CASE "level"
        WHEN 'ZA'                       THEN 'le'
            WHEN 'CP'                   THEN 'le'
                WHEN 'PPDC_PDC'         THEN 'la'
                    WHEN 'DEX'          THEN 'la'
    WHEN 'IRIS' THEN 'IRIS'
            WHEN 'COM'                  THEN 'la'
                WHEN 'COM_GLOBALE_ARM'  THEN 'la'
                    WHEN 'EPCI'         THEN 'l'''
                    WHEN 'CV'           THEN 'le'
                    WHEN 'ARR'          THEN 'l'''
                        WHEN 'DEP'      THEN 'le'
                            WHEN 'REG'  THEN 'la'
                                WHEN 'METROPOLE_DOM_TOM' THEN 'la'
                                    WHEN 'PAYS'          THEN 'le'
        ELSE "level"
    END
    ,name_plural =
    CASE "level"
        WHEN 'ZA'                       THEN 'Croisements Communes & Code Postaux'
            WHEN 'CP'                   THEN 'Code Postaux'
                WHEN 'PPDC_PDC'         THEN 'Zones de préparation du courrier'
                    WHEN 'DEX'          THEN 'Directions exécutives courrier'
    WHEN 'IRIS' THEN 'IRIS'
            WHEN 'COM'                  THEN 'Communes'
                WHEN 'COM_GLOBALE_ARM'  THEN 'Communes globales composées d''arrondissements municipaux'
                    WHEN 'EPCI'         THEN 'EPCI (Etablissement Public de Coopération Intercommunale)'
                    WHEN 'CV'           THEN 'Cantons ville'
                    WHEN 'ARR'          THEN 'Arrondissements départementaux'
                        WHEN 'DEP'      THEN 'Départements'
                            WHEN 'REG'  THEN 'Régions'
                                WHEN 'METROPOLE_DOM_TOM' THEN 'Métropoles ou territoires d''outre-mer'
                                    WHEN 'PAYS'          THEN 'Pays'
        ELSE "level"
    END
    ,"order" =
    CASE "level"
        WHEN 'ZA'                       THEN 0110
            WHEN 'CP'                   THEN 0210
                WHEN 'PPDC_PDC'         THEN 0410
                    WHEN 'DEX'          THEN 0510
    WHEN 'IRIS'                         THEN 0000
            WHEN 'COM'                  THEN 0200
                WHEN 'COM_GLOBALE_ARM'  THEN 0300
                    WHEN 'EPCI'         THEN 0400
                    WHEN 'CV'           THEN 0401
                    WHEN 'ARR'          THEN 0402
                        WHEN 'DEP'      THEN 0500
                            WHEN 'REG'  THEN 0600
                                WHEN 'METROPOLE_DOM_TOM' THEN 0700
                                    WHEN 'PAYS'          THEN 0800
        ELSE NULL
    END
    ,sublevels =
    CASE "level"
        WHEN 'ZA'                       THEN NULL
            WHEN 'CP'                   THEN ARRAY['ZA']
                WHEN 'PPDC_PDC'         THEN ARRAY['CP']
                    WHEN 'DEX'          THEN ARRAY['PPDC_PDC']
    WHEN 'IRIS'                         THEN NULL
            WHEN 'COM'                  THEN ARRAY['ZA','IRIS']
                WHEN 'COM_GLOBALE_ARM'  THEN ARRAY['COM']
                    WHEN 'EPCI'         THEN ARRAY['COM']
                    WHEN 'CV'           THEN ARRAY['COM']
                    WHEN 'ARR'          THEN ARRAY['COM']
                        WHEN 'DEP'      THEN ARRAY['ARR','CV']
                            WHEN 'REG'  THEN ARRAY['DEP']
                                WHEN 'METROPOLE_DOM_TOM' THEN ARRAY['REG','DR','DEX_SCC']
                                    WHEN 'PAYS'          THEN ARRAY['METROPOLE_DOM_TOM']
        ELSE NULL
    END
;

CREATE UNIQUE INDEX iux_territory_level_fr_level ON public.territory_level (country, level) WHERE country = 'FR';
