/***
 * FR-TERRITORY level definition
 */

/*
 * initialize FR hierarchies (administrative, postal)
 */
SELECT public.drop_all_functions_if_exists('fr', 'set_territory_level');
CREATE OR REPLACE PROCEDURE fr.set_territory_level() AS
$proc$
BEGIN
    DELETE FROM public.territory_level WHERE country = 'FR';

    INSERT INTO public.territory_level(
        country
        , level
    )
    (
        SELECT 'FR', UNNEST(ARRAY[
            'ZA'
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
            WHEN 'ZA'                       THEN 'Croisement Commune & Code Postal'
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
            WHEN 'ZA'                       THEN 'Commune/CP'
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
            WHEN 'ZA'                       THEN 'Croisements Communes & Code Postaux'
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
            WHEN 'ZA'                       THEN 'le'
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
            WHEN 'ZA'                       THEN 110
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
            WHEN 'ZA'                       THEN NULL
                WHEN 'CP'                   THEN ARRAY['ZA']
                    WHEN 'PDC_PPDC'         THEN ARRAY['CP']
                        WHEN 'PPDC_PDC'     THEN ARRAY['PDC_PPDC']
                            WHEN 'DEX'      THEN ARRAY['PPDC_PDC']
        WHEN 'IRIS'                         THEN NULL
                WHEN 'COM'                  THEN ARRAY['ZA', 'IRIS']
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

-- is level A a sublevel of level B
SELECT public.drop_all_functions_if_exists('fr', 'is_level_below');
CREATE OR REPLACE FUNCTION fr.is_level_below(
    level_a IN VARCHAR
    , level_b IN VARCHAR
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _immediate_sublevels_b VARCHAR[];
    _immediate_sublevel_b VARCHAR;
BEGIN
    IF (level_a = level_b) THEN RETURN FALSE; END IF;
    SELECT sublevels INTO _immediate_sublevels_b FROM public.territory_level
        WHERE level = level_b AND country = 'FR';
    IF (_immediate_sublevels_b IS NOT NULL) THEN
        -- level A is an immediate sublevel from leval B
        IF (level_a = ANY(_immediate_sublevels_b)) THEN RETURN TRUE; END IF;
        FOREACH _immediate_sublevel_b IN ARRAY _immediate_sublevels_b
        LOOP
            -- level A is a deeper sublevel from leval B (no immediate) : it's a sublevel of a sublevel from level B
            IF (fr.is_level_below(
                level_a
                , _immediate_sublevel_b
            )) THEN RETURN TRUE; END IF;
        END LOOP;
    END IF;
    RETURN FALSE;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.is_level_below('COM', 'EPCI'); --> TRUE
SELECT fr.is_level_below('EPCI', 'COM'); --> FALSE
SELECT fr.is_level_below('IRIS', 'COM'); --> TRUE
SELECT fr.is_level_below('CP', 'COM'); --> FALSE
 */

-- get common level between 2 levels
SELECT public.drop_all_functions_if_exists('fr', 'get_common_level');
CREATE OR REPLACE FUNCTION fr.get_common_level(
    level_a IN VARCHAR
    , level_b IN VARCHAR
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
            WHEN fr.is_level_below(level_a, level_b) THEN level_a
            WHEN fr.is_level_below(level_b, level_a) THEN level_b
            WHEN fr.is_level_below('COM', level_a) AND fr.is_level_below('COM', level_b) THEN 'COM'
            WHEN fr.is_level_below('CP', level_a) AND fr.is_level_below('CP', level_b) THEN 'CP'
            ELSE 'ZA'
        END;
    END IF;
END
$func$ LANGUAGE plpgsql;
-- get common level between a set of levels
CREATE OR REPLACE FUNCTION fr.get_common_level(
    levels IN VARCHAR[]
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
    _common_level VARCHAR;
BEGIN
    FOREACH _level IN ARRAY levels
    LOOP
        IF _common_level IS NULL THEN
            _common_level := _level;
        ELSE
            _common_level := fr.get_common_level(_level, _common_level);
        END IF;
    END LOOP;
    RETURN _common_level;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.get_common_level('COM', 'COM') --> COM
SELECT fr.get_common_level('EPCI', 'COM') --> COM
SELECT fr.get_common_level('COM', 'EPCI') --> COM
SELECT fr.get_common_level('DEP', 'REG') --> DEP
SELECT fr.get_common_level('DEP', 'ARR') --> ARR
SELECT fr.get_common_level('DEP', 'EPCI') --> COM
SELECT fr.get_common_level('ARR', 'EPCI') --> COM
SELECT fr.get_common_level('DEP', 'CP') --> ZA
SELECT fr.get_common_level('DEP', 'DSCC') --> ZA
SELECT fr.get_common_level(ARRAY['DEP', 'DSCC', 'CP', 'COM']) --> ZA
SELECT fr.get_common_level(ARRAY['DEP', 'EPCI', 'REG']) --> COM
SELECT fr.get_common_level(ARRAY['DEP', 'REG', 'PAYS']) --> DEP
 */

-- get list of levels
SELECT public.drop_all_functions_if_exists('fr', 'get_levels');
CREATE OR REPLACE FUNCTION fr.get_levels(
    order_in IN VARCHAR DEFAULT 'ASC'
    , subfilter IN VARCHAR DEFAULT NULL         -- result list from this level (smaller one)
    , among_levels IN VARCHAR[] DEFAULT NULL    -- list of levels to order
)
RETURNS VARCHAR[] AS
$func$
DECLARE
    _levels VARCHAR[];
    _level VARCHAR;
BEGIN
    IF order_in = 'DESC' THEN
        SELECT ARRAY_AGG(level ORDER BY hierarchy DESC)
        INTO _levels
        FROM public.territory_level
        WHERE country = 'FR';
    ELSE
        SELECT ARRAY_AGG(level ORDER BY hierarchy ASC)
        INTO _levels
        FROM public.territory_level
        WHERE country = 'FR';
    END IF;

    IF subfilter IS NOT NULL THEN
        FOREACH _level IN ARRAY _levels
        LOOP
            IF _level = subfilter
            OR fr.is_level_below(subfilter, _level) = FALSE
            THEN
                _levels := ARRAY_REMOVE(_levels, _level);
            END IF;
        END LOOP;
    END IF;

    IF among_levels IS NOT NULL THEN
        FOREACH _level IN ARRAY _levels
        LOOP
            IF NOT(_level = ANY(among_levels))
            THEN
                _levels := ARRAY_REMOVE(_levels, _level);
            END IF;
        END LOOP;
    END IF;

    RETURN _levels;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('fr', 'get_bigger_sublevel');
CREATE OR REPLACE FUNCTION fr.get_bigger_sublevel(
    level_in IN VARCHAR
    , among_levels IN VARCHAR[] DEFAULT NULL
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    --FOREACH _level IN ARRAY fr.get_levels(order_in => 'DESC')
    FOR _level IN (
        --On récupère les niveaux intermédiaires (pouvant servir à construire un autre niveau)
        --En les ordonnant par ordre du plus grand niveau parent et par ordre d'apparition dans la liste des sous niveaux directs sur niveau parent
        SELECT level FROM (
            SELECT level, ROW_NUMBER() OVER() AS hierarchy FROM (
                SELECT UNNEST(sublevels) AS level FROM public.territory_level
                WHERE country = 'FR'
                ORDER BY hierarchy DESC
            ) AS sq
        ) AS sq2
        GROUP BY level
        ORDER BY MAX(hierarchy)
    )
    LOOP
        IF among_levels IS NOT NULL AND NOT(_level = ANY(among_levels)) THEN CONTINUE; END IF;
        IF fr.is_level_below(_level, level_in) THEN
            RETURN _level;
        END IF;
    END LOOP;
    RETURN NULL;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.get_bigger_sublevel('COM', ARRAY['DEP', 'EPCI', 'REG']);
 */

SELECT public.drop_all_functions_if_exists('fr', 'get_smaller_sublevel');
CREATE OR REPLACE FUNCTION fr.get_smaller_sublevel(
    level_in IN VARCHAR
    , among_levels IN VARCHAR[] DEFAULT NULL
)
RETURNS VARCHAR AS
$func$
DECLARE
    _level VARCHAR;
BEGIN
    --FOREACH _level IN ARRAY fr.get_levels(order_in => 'DESC')
    FOR _level IN (
        --On récupère les niveaux intermédiaires (pouvant servir à construire un autre niveau)
        --En les ordonnant par ordre du plus grand niveau parent et par ordre d'apparition dans la liste des sous niveaux directs sur niveau parent
        SELECT level FROM (
            SELECT level, ROW_NUMBER() OVER() AS hierarchy FROM (
                SELECT UNNEST(sublevels) AS level FROM public.territory_level
                WHERE country = 'FR'
                ORDER BY hierarchy DESC
            ) AS sq
        ) AS sq2
        GROUP BY level
        ORDER BY MAX(hierarchy) DESC
    )
    LOOP
        IF among_levels IS NOT NULL AND NOT(_level = ANY(among_levels)) THEN CONTINUE; END IF;
        IF fr.is_level_below(_level, level_in) THEN
            RETURN _level;
        END IF;
    END LOOP;
    RETURN NULL;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.get_smaller_sublevel('DEP')
 */
