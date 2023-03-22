/***
 * FR-TERRITORY : maintains municipality up to date
 */

/*
 * get min|max date of FR INSEE municipality event
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_date_insee_municipality_event');
CREATE OR REPLACE FUNCTION fr.get_date_insee_municipality_event(
    minmax IN BOOLEAN DEFAULT FALSE
)
RETURNS DATE AS
$func$
DECLARE
    _date_min DATE;
    _date_max DATE;
    _fr_municipality_event VARCHAR := 'fr.insee_municipality_event';
BEGIN
    IF NOT table_exists('fr', 'insee_municipality_event') THEN
        _date_min := NOW();
        _date_max := '1970-01-01'::DATE;
        IF minmax THEN RETURN _date_min; ELSE RETURN _date_max; END IF;
    END IF;

    IF minmax THEN
        IF NULLIF(current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_min')), '') IS NULL THEN
            RAISE 'RELOAD';
        END IF;
        RETURN TO_DATE(
            NULLIF(
                current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_min'))
                , 'NULL'
            )
            , 'DD/MM/YYYY'
        );
    ELSE
        IF NULLIF(current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_max')), '') IS NULL THEN
            RAISE 'RELOAD';
        END IF;
        RETURN TO_DATE(
            NULLIF(
                current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_max'))
                , 'NULL'
            )
            , 'DD/MM/YYYY'
        );
    END IF;
EXCEPTION WHEN OTHERS THEN
    SELECT MIN(date_eff) - 1, MAX(date_eff)
    INTO _date_min, _date_max
    FROM fr.insee_municipality_event;
    EXECUTE CONCAT(
        'SELECT set_config('''
        , CONCAT_WS('.', _fr_municipality_event, 'date_min')
        , ''', '''
        , COALESCE(TO_CHAR(_date_min, 'DD/MM/YYYY'), 'NULL')
        , ''', TRUE)'
    );
    EXECUTE CONCAT(
        'SELECT set_config('''
        , CONCAT_WS('.', _fr_municipality_event, 'date_max')
        , ''', '''
        , COALESCE(TO_CHAR(_date_max, 'DD/MM/YYYY'), 'NULL')
        , ''', TRUE)'
    );
    IF minmax THEN RETURN _date_min; ELSE RETURN _date_max; END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.get_date_insee_municipality_event()
SELECT fr.get_date_insee_municipality_event(minmax => TRUE)
 */

/*
 * get min|max date of WIKIPEDIA municipality event
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_date_wikipedia_municipality_event');
CREATE OR REPLACE FUNCTION fr.get_date_wikipedia_municipality_event(
    minmax IN BOOLEAN DEFAULT FALSE
)
RETURNS DATE AS
$func$
DECLARE
    _date_min DATE;
    _date_max DATE;
    _fr_municipality_event VARCHAR := 'fr.wikipedia_municipality_event';
BEGIN
    IF NOT table_exists('fr', 'wikipedia_municipality_event') THEN
        _date_min := NOW();
        _date_max := '1970-01-01'::DATE;
        IF minmax THEN RETURN _date_min; ELSE RETURN _date_max; END IF;
    END IF;

    IF minmax THEN
        IF NULLIF(current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_min')), '') IS NULL THEN
            RAISE 'RELOAD';
        END IF;
        RETURN TO_DATE(
            NULLIF(
                current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_min'))
                , 'NULL'
            )
            , 'DD/MM/YYYY'
        );
    ELSE
        IF NULLIF(current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_max')), '') IS NULL THEN
            RAISE 'RELOAD';
        END IF;
        RETURN TO_DATE(
            NULLIF(
                current_setting(CONCAT_WS('.', _fr_municipality_event, 'date_max'))
                , 'NULL'
            )
            , 'DD/MM/YYYY'
        );
    END IF;
EXCEPTION WHEN OTHERS THEN
    SELECT MIN(dt_effet) - 1, MAX(dt_effet)
    INTO _date_min, _date_max
    FROM fr.wikipedia_municipality_event;
    EXECUTE CONCAT(
        'SELECT set_config('''
        , CONCAT_WS('.', _fr_municipality_event, 'date_min')
        , ''', '''
        , COALESCE(TO_CHAR(_date_min, 'DD/MM/YYYY'), 'NULL')
        , ''', TRUE)'
    );
    EXECUTE CONCAT(
        'SELECT set_config('''
        , CONCAT_WS('.', _fr_municipality_event, 'date_max')
        , ''', '''
        , COALESCE(TO_CHAR(_date_max, 'DD/MM/YYYY'), 'NULL')
        , ''', TRUE)'
    );
    IF minmax THEN RETURN _date_min; ELSE RETURN _date_max; END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.get_date_wikipedia_municipality_event()
 */

/*
 * get most recent date of municipality event (INSEE or WIKIPEDIA)
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_most_recent_municipality_to_date');
CREATE OR REPLACE FUNCTION fr.get_most_recent_municipality_to_date(
    date_geography_from IN DATE DEFAULT NULL    -- date from which apply updates
    , date_geography_to IN DATE DEFAULT NOW()   -- date up to which apply updates
)
RETURNS DATE AS
$func$
BEGIN
    RETURN GREATEST(
        date_geography_from
        , LEAST(
            date_geography_to
            , GREATEST(
                fr.get_date_insee_municipality_event()
                , fr.get_date_wikipedia_municipality_event()
            )
        )
    );
END
$func$ LANGUAGE plpgsql;

/*
 * from INSEE: apply event(s) into [from..to] date interval on a given municipality
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_municipality_to_date_from_insee');
CREATE OR REPLACE FUNCTION fr.get_municipality_to_date_from_insee(
    code IN VARCHAR
    , date_geography_from IN DATE
    , name IN VARCHAR DEFAULT NULL
    , distribution IN NUMERIC DEFAULT 1
    , information IN TEXT DEFAULT NULL
    , codes_event IN INTEGER[] DEFAULT NULL     -- only if all events not treated (to date)
    , date_geography_to IN DATE DEFAULT NOW()
    , is_new IN BOOLEAN DEFAULT FALSE
    , with_deleted IN BOOLEAN DEFAULT FALSE
    , code_previous IN VARCHAR DEFAULT NULL
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
    _date_effect DATE;
    _code_new VARCHAR;
    _name_new VARCHAR;
    _distribution_new NUMERIC;
    _code_previous_new VARCHAR;
    _code_event INTEGER;
    _codes_new VARCHAR[];
    _names_new VARCHAR[];
    _i INTEGER;

    --_back_to_code VARCHAR;
BEGIN
    /*
     * code event ('mod' column)
     *
        Changement de nom                                       10
        Création                                                20
        Rétablissement                                          21
        Suppression                                             30
        Fusion simple                                           31
        Création de commune nouvelle                            32
        Fusion association                                      33
        Transformation de fusion association en fusion simple   34
        Changement de code dû à un changement de département    41
        Changement de code dû à un transfert de chef-lieu       50

        Suppression de commune déléguée                         35
            --> quel intérêt si le libellé et le code ne changent pas ?
            SELECT ROUND(SUM(CASE WHEN (libelle_av != libelle_ap OR com_av != com_ap) THEN 1 ELSE 0 END)::NUMERIC * 100 / COUNT(*), 2), COUNT(*) AS pct_avec_changement
                FROM fr.insee_municipality_event
                where mod = 34 and typecom_av = 'COM' and typecom_ap = 'COM'
            SELECT *
                FROM fr.insee_municipality_event
                where mod = 34 and typecom_av = 'COM' and typecom_ap = 'COM'
                AND (libelle_av != libelle_ap OR com_av != com_ap)

        Transformation de commune associé en commune déléguée   70
            SELECT ROUND(SUM(CASE WHEN (libelle_av != libelle_ap OR com_av != com_ap) THEN 1 ELSE 0 END)::NUMERIC * 100 / COUNT(*), 2) AS pct_avec_changement
            FROM fr.insee_municipality_event
            where mod = 70 and typecom_av = 'COM' and typecom_ap = 'COM'
     */

    IF date_geography_to IS NULL OR date_geography_to > date_geography_from THEN
        SELECT date_eff, mod, ARRAY_AGG(com_ap), ARRAY_AGG(libelle_ap)
        INTO _date_effect, _code_event, _codes_new, _names_new
        FROM fr.insee_municipality_event
        WHERE (
            --n'importe quel evènement futur
            date_eff > date_geography_from
            --ou bien un évement du jour, qui n'a pas déjà été traité, alors que d'autre l'on été (= ce n'est pas le jour initial de départ)
            OR (codes_event IS NOT NULL AND ARRAY_LENGTH(codes_event, 1) > 0 AND date_eff = date_geography_from AND NOT(mod = ANY(codes_event)))
        )
        AND com_av = code
        AND typecom_av = 'COM'
        AND typecom_ap = 'COM'
        AND (date_geography_to IS NULL OR date_geography_to >= date_eff)
        --/* géré dans boucle
        --Si codgeo précédent renseigné, et rétablissement (suite fusion / création commune nouvelle)
        --Alors on revient sur la commune avant sa fusion
        AND (
            code_previous IS NULL OR mod != 21
            OR code_previous = com_ap
        )
        --*/
        --Ce type de modification est intéressant que si elle change le code ou bien le libellé
        AND (mod != 34 OR (libelle_av != libelle_ap OR com_av != com_ap))
        GROUP BY date_eff, mod
        ORDER BY date_eff ASC, mod ASC
        LIMIT 1;
    ELSE
        -- NOTE : pour le retour vers le passé, on considère tous les évènements ayant effet à J-1
        SELECT date_eff - 1, mod, ARRAY_AGG(com_av), ARRAY_AGG(libelle_av)
        INTO _date_effect, _code_event, _codes_new, _names_new
        FROM fr.insee_municipality_event
        WHERE (
            --n'importe quel evènement passé
            (date_eff - 1) < date_geography_from
            --ou bien un évement du jour, qui n'a pas déjà été traité, alors que d'autres l'on été (= ce n'est pas le jour initial de départ)
            OR (codes_event IS NOT NULL AND ARRAY_LENGTH(codes_event, 1) > 0 AND (date_eff - 1) = date_geography_from AND NOT(mod = ANY(codes_event)))
        )
        AND com_ap = code
        AND typecom_av = 'COM'
        AND typecom_ap = 'COM'
        AND date_geography_to <= (date_eff - 1)
        --/* géré dans boucle
        --Si codgeo précédent renseigné, et retour dans le passé sur une fusion / création commune nouvelle
        --Alors on revient sur la commune avant sa fusion
        AND (
            code_previous IS NULL OR mod NOT IN (31, 32, 33)
            OR code_previous = com_av
            OR EXISTS (
                SELECT *
                FROM fr.insee_municipality_event AS evenement_commune_precedent
                WHERE typecom_av = 'COM'
                AND typecom_ap = 'COM'
                AND mod != 21
                --AND evenement_commune_precedent.date_eff < '2020-01-01'::date
                --AND evenement_commune_precedent.com_ap = '89420'
                AND evenement_commune_precedent.date_eff < evenement_commune.date_eff
                AND evenement_commune_precedent.date_eff >= '2010-01-01'::date
                AND evenement_commune_precedent.com_ap = evenement_commune.com_av
                AND evenement_commune_precedent.com_av = code_previous
            )
        )
        --*/
        --Ce type de modification est intéressante que si elle change le code ou bien le libellé
        AND (mod != 34 OR (libelle_av != libelle_ap OR com_av != com_ap))
        GROUP BY (date_eff - 1), mod
        ORDER BY (date_eff - 1) DESC, mod ASC
        LIMIT 1;
    END IF;

    -- no event
    IF _date_effect IS NULL THEN
        /*
        IF date_geography_to < date_geography_from AND code_previous IS NOT NULL THEN
            IF code_previous = code THEN
                distribution := 1;
            ELSE
                RETURN;
            END IF;
        END IF;
        */

        RETURN NEXT ROW (
            code
            , code_previous
            , name
            , CASE
                WHEN date_geography_to IS NULL OR date_geography_to > date_geography_from THEN
                    GREATEST(
                        date_geography_from
                        , LEAST(
                            date_geography_to
                            , fr.get_date_insee_municipality_event()
                        )
                    )
                ELSE
                    LEAST(
                        date_geography_from
                        , GREATEST(
                            date_geography_to
                            , fr.get_date_insee_municipality_event(minmax => TRUE)
                        )
                    )
            END
            , distribution
            , information
            , is_new
        );
    ELSE
        _i := 0;
        _distribution_new := (distribution / ARRAY_LENGTH(_codes_new, 1));
        _code_previous_new := code_previous;
        IF date_geography_to IS NULL OR date_geography_to > date_geography_from THEN
            --Suppression
            IF _code_event = 30 /* TODO : gérer la suppression de commune déléguée ainsi ? : OR _code_event = 35 */ THEN
                IF with_deleted = FALSE THEN
                    RETURN;
                END IF;
                _distribution_new := 0;
            ELSE
                IF _code_event = 21 THEN
                    _code_previous_new := NULL;
                /* géré dans requête pour ne pas prendre un rétablissement sur une portion de la commune nouvelle non concerné (rétablissement partiel, ex : 14712)
                --Si codgeo précédent renseigné, et rétablissement (suite fusion / création commune nouvelle / changement de code ?)
                --Alors on revient sur la commune précédente
                IF
                    code_previous IS NOT NULL
                    AND _code_event = 21
                    AND code_previous = ANY(_codes_new)
                THEN
                    _back_to_code := code_previous;
                    _code_previous_new := NULL;
                    _distribution_new := distribution;
                */
                ELSIF
                    code_previous IS NULL
                    --fusion / commune nouvelle, ou changement de code
                    AND (_code_event IN (31, 32, 33) OR (ARRAY_LENGTH(_codes_new, 1) = 1 AND code != _codes_new[1]))
                THEN
                    _code_previous_new := code;
                END IF;
            END IF;
        --retour vers le passé
        ELSE
            --Annulation création
            IF _code_event = 20 THEN
                IF with_deleted = FALSE THEN
                    RETURN;
                END IF;
                _distribution_new := 0;
            --Annulation de suppression : quelque chose de spécial à faire ?
            --ELSIF _code_event = 30 THEN
            ELSE
                IF _code_event IN (31, 32, 33) THEN
                    --FIXME : Pose un problème en cas d'annulation de fusion successives, exemple : 24362
                    --_code_previous_new := NULL;
                /* géré dans requête pour ne pas prendre un rétablissement sur un portion de la commune nouvelle non concerné (rétablissement partiel, ex : 14712)
                IF code_previous IS NOT NULL
                --annulation de fusion / commune nouvelle, ou de changement de code
                AND (_code_event IN (31, 32, 33) OR (ARRAY_LENGTH(_codes_new, 1) = 1 AND code != _codes_new[1]))
                AND code_previous = ANY(_codes_new)
                THEN
                    _back_to_code := code_previous;
                    _code_previous_new := NULL;
                    _distribution_new := distribution;
                */
                ELSIF
                    code_previous IS NULL
                    --annulation de rétablissement = retour sur la commune nouvelle
                    AND _code_event = 21
                THEN
                    _code_previous_new := code;
                END IF;
            END IF;
        END IF;
        FOREACH _code_new IN ARRAY _codes_new LOOP
            _i := _i + 1;
            _name_new := _names_new[_i];
            --Si on change de date
            IF codes_event IS NOT NULL AND date_geography_from != _date_effect
            --Ou de code géographique
            --à voir OR code != _code_new
            THEN
                --On réinitialise la liste de évènements déjà traités
                codes_event := NULL::INTEGER[];
            END IF;

            /*
            IF _back_to_code IS NOT NULL AND _back_to_code != _code_new THEN
                CONTINUE;
            END IF;
             */

            RETURN QUERY
                SELECT *
                FROM fr.get_municipality_to_date_from_insee(
                    code => _code_new
                    , name => _name_new
                    , date_geography_from => _date_effect
                    , distribution => _distribution_new
                    , information => CONCAT_WS(' -> '
                        , COALESCE(information, CONCAT(CONCAT_WS(' ', CONCAT_WS('/', code, code_previous), name), ' le ', date_geography_from))
                        , CONCAT(CONCAT_WS('/', _code_new, _code_previous_new), ' ', _name_new, ' le ', _date_effect, ' (mod=', _code_event, ')')
                    )
                    , codes_event => ARRAY_APPEND(codes_event, _code_event)
                    , date_geography_to => date_geography_to
                    , is_new => TRUE
                    , with_deleted => with_deleted
                    --En cas de succession de changement de code géo, on mémorise le premier seulement
                    , code_previous => _code_previous_new
                );
        END LOOP;
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST

--Cas de division
SELECT * FROM get_municipality_to_date_from_insee('76676', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> OK, on a bien :
    76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76601 Saint-Lucien le 2017-01-01 (mod=21)
    76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76676 Sigy-en-Bray le 2017-01-01 (mod=21)

--Cas de suppression
SELECT * FROM get_municipality_to_date_from_insee('51440', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> OK, on a rien
SELECT * FROM get_municipality_to_date_from_insee(code=>'51440', date_geography_from=>TO_DATE('01/12/1900', 'DD/MM/YYYY'), with_deleted=>true)
--> OK, retourné avec répartition à 0

--Cas simple de fusion
SELECT * FROM get_municipality_to_date_from_insee('01341', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
-->OK, on a bien :
    01341 le 1900-12-01 -> 01227 Magnieu le 2019-01-01 (mod=32)

--Cas plus complexe de fusion
SELECT * FROM get_municipality_to_date_from_insee('49144', TO_DATE('2014', 'YYYY'))
-->OK, on a bien :
    49144 le 2014-01-01 -> 44225 Freigné le 2018-01-01 (mod=41) -> 44180 Vallons-de-l'Erdre le 2018-01-01 (mod=32)

SELECT *
FROM fr.insee_municipality_event
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
AND date_eff = '2019-01-01'::DATE AND com_ap = '01227'

--Doublons ?
SELECT date_eff, com_av, com_ap, COUNT(*)
FROM fr.insee_municipality_event
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
GROUP BY date_eff, com_av, com_ap
HAVING COUNT(*) > 1

WITH millesime_a_to_now AS (
    --SELECT commune_to_now.codgeo, commune_to_now.dt_reference, ROUND(commune_to_now.distribution, 1)
    --FROM fr.insee_administrative_cutting_municipality_and_district AS dcca
    --INNER JOIN get_municipality_to_date_from_insee(dcca.codgeo, TO_DATE(dcca.millesime::VARCHAR, 'YYYY')) AS commune_to_now ON TRUE
    --WHERE dcca.millesime = (SELECT MAX(millesime) FROM fr.insee_administrative_cutting_municipality_and_district)
    SELECT codgeo, TO_DATE(millesime::VARCHAR, 'YYYY') AS dt_reference, NULL::TEXT AS information, 1::DECIMAL AS distribution
    FROM fr.insee_administrative_cutting_municipality_and_district AS dcca
    WHERE dcca.millesime = '2019'
)
, millesime_b_to_now AS (
    SELECT commune_to_now.codgeo, commune_to_now.dt_reference, commune_to_now.information, ROUND(commune_to_now.distribution, 1)
    FROM fr.insee_administrative_cutting_municipality_and_district AS dcca
    INNER JOIN get_municipality_to_date_from_insee(
        code => dcca.codgeo
        , date_geography_from => TO_DATE(dcca.millesime::VARCHAR, 'YYYY')
        , date_geography_to => TO_DATE('01/01/2019', 'DD/MM/YYYY')
    ) AS commune_to_now ON TRUE
    WHERE dcca.millesime = '2015'
)
SELECT
    millesime_a_to_now.codgeo AS codgeo_a_to_now
    , millesime_a_to_now.information AS information_a_to_now
    , millesime_b_to_now.codgeo AS codgeo_b_to_now
    , millesime_b_to_now.information AS information_b_to_now
FROM millesime_b_to_now
FULL OUTER JOIN millesime_a_to_now ON millesime_a_to_now.codgeo = millesime_b_to_now.codgeo
WHERE millesime_a_to_now.codgeo IS NULL
OR millesime_b_to_now.codgeo IS NULL

--> 2014 : problème avec Loisey-Culey (55298) qui aurait dû être rétabli en Cyley 55298 + Loisey 55138 le 01/01/2014, mais toujours présent dans le referentiel insee du 1er janvier 2014
    SELECT * FROM getCommuneToNowFromEvenementInsee('55298', TO_DATE('31/12/2013', 'DD/MM/YYYY'), 1, TRUE)
    SELECT * FROM fr.insee_administrative_cutting_municipality_and_district WHERE millesime = '2014' AND codgeo = '55298'
    SELECT * FROM fr.insee_municipality_event WHERE date_eff = '2014-01-01' AND com_av = '55298' AND typecom_av = 'COM' AND typecom_ap = 'COM'
    https://fr.wikipedia.org/wiki/Loisey-Culey : Au 1er janvier 2014, les communes devaient retrouver leur indépendance, mais la procédure est reportée au 1er janvier 2015, ne pouvant avoir lieu dans l'année précédant une échéance électorale. Cependant, lors des élections municipales de 2014, un maire est élu dans chaque commune, et finalement, par décision du tribunal le 1er juillet 2014, les deux communes sont indépendantes.
    --> on retarde l'evenement au 1er juillet 2014
--> 2015 : problème avec Oudon (14697) qui aurait dû changer de code le 07/01/2014 (14472) et à nouveau en 2017 (14654), mais toujours présent dans le referentiel insee du 1er janvier 2015
    SELECT * FROM getCommuneToNowFromEvenementInsee('14697', TO_DATE('06/01/2014', 'DD/MM/YYYY'), 1, TRUE)
    SELECT * FROM fr.insee_administrative_cutting_municipality_and_district WHERE millesime = '2015' AND codgeo = '14697'
    SELECT * FROM fr.insee_municipality_event WHERE date_eff = '2014-01-07' AND typecom_av = 'COM' AND typecom_ap = 'COM' AND com_av = '14697'
    https://fr.wikipedia.org/wiki/L%27Oudon : Un nouvel arrêté préfectoral, le 7 janvier 2014, fait de la commune de Notre-Dame-de-Fresnay le nouveau chef-lieu. Afin de prendre en compte ce transfert de chef lieu, lors de la publication du COG 2016 l'INSEE décide de modifier le code commune de L'Oudon pour reprendre l'ancien code de Notre-Dame-de-Fresnay (14472).
    --> on retarde l'evenement au 1er janvier 2016

WITH test as (
SELECT CONCAT(com_av, '-', com_ap) as code, date_eff
FROM fr.insee_municipality_event
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
and com_ap != com_av
and mod != 21
UNION ALL
SELECT CONCAT(com_ap, '-', com_av), date_eff
FROM fr.insee_municipality_event
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
and com_ap != com_av
and mod = 21
)
SELECT code, array_agg(date_eff order by date_eff)
FROM test
group by code
having COUNT(*) > 1
 */

/*
 * from WIKIPEDIA: apply event(s) into [from..to] date interval on a given municipality
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_municipality_to_date_from_wikipedia');
CREATE OR REPLACE FUNCTION fr.get_municipality_to_date_from_wikipedia(
    code IN VARCHAR
    , date_geography_from IN DATE
    , name IN VARCHAR DEFAULT NULL
    , distribution IN NUMERIC DEFAULT 1
    , information IN TEXT DEFAULT NULL
    , date_geography_to IN DATE DEFAULT NOW()
    , is_new IN BOOLEAN DEFAULT FALSE
    , code_previous IN VARCHAR DEFAULT NULL
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
    _date_effect DATE;
    _code_new VARCHAR;
    _name_new VARCHAR;
    _distribution_new NUMERIC;
    _code_previous_new VARCHAR;
    _codes_new VARCHAR[];
    _names_new VARCHAR[];
    _i INTEGER;
    _back_to_code VARCHAR;
BEGIN
    IF date_geography_to IS NULL OR date_geography_to > date_geography_from THEN
        SELECT dt_effet, ARRAY[cn_code_insee], ARRAY[cn_nom]
        INTO _date_effect, _codes_new, _names_new
        FROM fr.wikipedia_municipality_event
        WHERE dt_effet > date_geography_from
        AND code::CHAR(5) = ANY(ac_codes_insee)
        AND (date_geography_to IS NULL OR date_geography_to >= dt_effet)
        ORDER BY dt_effet ASC
        LIMIT 1;
    ELSE
        SELECT dt_effet - 1, ac_codes_insee, ac_noms
        INTO _date_effect, _codes_new, _names_new
        FROM fr.wikipedia_municipality_event
        WHERE (dt_effet - 1) < date_geography_from
        AND code::CHAR(5) = cn_code_insee
        AND date_geography_to < (dt_effet - 1)
        ORDER BY dt_effet DESC
        LIMIT 1;
    END IF;

    -- no event
    IF _date_effect IS NULL THEN
        RETURN NEXT ROW (
            code
            , code_previous
            , name
            , CASE
                WHEN date_geography_to IS NULL OR date_geography_to > date_geography_from THEN
                    GREATEST(
                        date_geography_from
                        , LEAST(
                            date_geography_to
                            , fr.get_date_wikipedia_municipality_event()
                        )
                    )
                ELSE
                    LEAST(
                        date_geography_from
                        , GREATEST(
                            date_geography_to
                            , fr.get_date_wikipedia_municipality_event(minmax => TRUE)
                        )
                    )
            END
            , distribution
            , information
            , is_new
        );
    ELSE
        _i := 0;
        _distribution_new := (distribution / ARRAY_LENGTH(_codes_new, 1));
        _code_previous_new := code_previous;
        IF date_geography_to IS NULL OR date_geography_to > date_geography_from THEN
            _code_previous_new := COALESCE(code_previous, code);
        ELSE
            --Si codgeo précédent renseigné, et retour dans le passé sur une fusion / création commune nouvelle
            --Alors on revient sur la commune avant sa fusion
            IF code_previous IS NOT NULL
            AND date_geography_to < date_geography_from
            AND code_previous = ANY(_codes_new)
            THEN
                _back_to_code := code_previous;
                _code_previous_new := NULL;
                _distribution_new := distribution;
            END IF;
        END IF;

        FOREACH _code_new IN ARRAY _codes_new LOOP
            _i := _i + 1;
            _name_new := _names_new[_i];

            IF _back_to_code IS NOT NULL AND _back_to_code != _code_new THEN
                CONTINUE;
            END IF;

            RETURN QUERY
                SELECT *
                FROM fr.get_municipality_to_date_from_wikipedia(
                    code => _code_new
                    , name => _name_new
                    , date_geography_from => _date_effect
                    , distribution => _distribution_new
                    , information => CONCAT_WS(' -> '
                        , COALESCE(information, CONCAT(CONCAT_WS(' ', CONCAT_WS('/', code, code_previous), name), ' le ', date_geography_from))
                        , CONCAT(CONCAT_WS('/', _code_new, _code_previous_new), ' ', _name_new, ' le ', _date_effect)
                    )
                    , date_geography_to => date_geography_to
                    , is_new => TRUE
                    --En cas de succession de changement de code géo, on mémorise le premier seulement
                    , code_previous => _code_previous_new
                );
        END LOOP;
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST

--Cas de division
SELECT * FROM get_municipality_to_date_from_wikipedia('76676', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> KO, retourné tel quel, cas non géré, on devrait avoir :
    76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76601 Saint-Lucien le 2017-01-01 (mod=21)
    76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76676 Sigy-en-Bray le 2017-01-01 (mod=21)

--Cas de suppression
SELECT * FROM get_municipality_to_date_from_wikipedia('51440', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> KO, retourné tel quel, cas non géré, on ne devrait rien avoir en retour

--Cas simple de fusion
SELECT * FROM get_municipality_to_date_from_wikipedia('01341', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
-->OK, on a bien :
    01341 le 1900-12-01 -> 01227 Magnieu le 2019-01-01

--Cas plus complexe de fusion
SELECT * FROM get_municipality_to_date_from_wikipedia('49144', TO_DATE('2014', 'YYYY'))
-->OK, on a bien :
    49144 le 2014-01-01 -> 44180 Vallons-de-l'Erdre le 2018-01-01

WITH millesime_20xx_to_now AS (
    SELECT commune_to_now.codgeo, commune_to_now.dt_reference, ROUND(commune_to_now.distribution, 1), commune_to_now.information
    FROM fr.insee_administrative_cutting_municipality_and_district AS dcca
    INNER JOIN get_municipality_to_date_from_wikipedia(dcca.codgeo, TO_DATE(dcca.millesime::VARCHAR, 'YYYY')) AS commune_to_now ON TRUE
    WHERE dcca.millesime = '2015'
)
, millesime_last_to_now AS (
    SELECT commune_to_now.codgeo, commune_to_now.dt_reference, ROUND(commune_to_now.distribution, 1), commune_to_now.information
    FROM fr.insee_administrative_cutting_municipality_and_district AS dcca
    INNER JOIN get_municipality_to_date_from_wikipedia(dcca.codgeo, TO_DATE(dcca.millesime::VARCHAR, 'YYYY')) AS commune_to_now ON TRUE
    WHERE dcca.millesime = (SELECT MAX(millesime) FROM fr.insee_administrative_cutting_municipality_and_district)
)
SELECT
    millesime_last_to_now.codgeo AS codgeo_last_to_now
    , millesime_last_to_now.information AS information_last_to_now
    , millesime_20xx_to_now.codgeo AS codgeo_20xx_to_now
    , millesime_20xx_to_now.information AS information_20xx_to_now
FROM millesime_last_to_now
FULL OUTER JOIN millesime_20xx_to_now ON millesime_20xx_to_now.codgeo = millesime_last_to_now.codgeo
WHERE millesime_20xx_to_now.codgeo IS NULL
OR millesime_last_to_now.codgeo IS NULL

--> 2018/2019 : OK
--> 2017/2019 : OK, sauf cas particulier de Pont-Farcy (14513) qui est transféré (50649) et fusionné (50592) le meme jour
--> 2016/2019 : idem et cas de division (76601)
--> 2015/2019 : idem et cas de division (76601), et cas 14697 / 44060 / 68031 à étudier
 */

/*
 * apply event(s) into [from..to] date interval on a given municipality
 *
 * from INSEE is default choice
 * TODO : utiliser d'abord le référentiel le plus reculé pour une avancée dans le temps
 * TODO : utiliser d'abord le référentiel le plus avancé pour un retour dans le passé
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_municipality_to_date');
CREATE OR REPLACE FUNCTION fr.get_municipality_to_date(
    code IN VARCHAR
    , date_geography_from IN DATE
    , name IN VARCHAR DEFAULT NULL
    , distribution IN NUMERIC DEFAULT 1
    , information IN TEXT DEFAULT NULL
    , check_exists IN BOOLEAN DEFAULT TRUE
    , months_back_if_not_exists IN INTEGER DEFAULT 12
    , date_geography_to IN DATE DEFAULT NOW()   -- date up to which apply updates
                                                -- NOW() till today, NULL most as possible
    , is_new IN BOOLEAN DEFAULT FALSE
    , with_deleted IN BOOLEAN DEFAULT FALSE
    , date_geography_from_first IN DATE DEFAULT NULL
    , code_previous IN VARCHAR DEFAULT NULL
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
    --_territory_to_date_t public.territory_to_date_t%ROWTYPE;
    _territory_to_date_t public.territory_to_date_t;
    _exists BOOLEAN;
BEGIN
    /*
    RAISE NOTICE 'get_municipality_to_date(%, %, %, %, %, %, %, %, %, %, %)'
        , code
        , date_geography_from
        , name
        , distribution
        , information
        , check_exists
        , months_back_if_not_exists
        , date_geography_to
        , is_new
        , with_deleted
        , date_geography_from_first;
     */
    IF date_geography_from_first IS NULL THEN date_geography_from_first := date_geography_from; END IF;
    IF code = '97123'
    AND date_geography_from < TO_DATE('15/07/2007', 'DD/MM/YYYY')
    AND (date_geography_to IS NULL OR date_geography_to >= TO_DATE('15/07/2007', 'DD/MM/YYYY')) THEN
        RETURN NEXT ROW(
            '97701'::VARCHAR
            , code
            , 'Saint-Barthélemy'::VARCHAR
            , TO_DATE('15/07/2007', 'DD/MM/YYYY')
            , 1::NUMERIC
            , 'Les anciennes communes de Saint-Barthélemy (ancien code INSEE 97123) et Saint-Martin (ancien code INSEE 97127) ne font plus partie du département et la région d''outre-mer de Guadeloupe mais forment des collectivités d''outre-mer séparées depuis le 15 juillet 2007'::TEXT
            , TRUE
        );
        RETURN;
    ELSIF code = '97127'
    AND date_geography_from < TO_DATE('15/07/2007', 'DD/MM/YYYY')
    AND (date_geography_to IS NULL OR date_geography_to >= TO_DATE('15/07/2007', 'DD/MM/YYYY')) THEN
        RETURN NEXT ROW(
            '97801'::VARCHAR
            , code
            , 'Saint-Martin'::VARCHAR
            , TO_DATE('15/07/2007', 'DD/MM/YYYY')
            , 1::NUMERIC
            , 'Les anciennes communes de Saint-Barthélemy (ancien code INSEE 97123) et Saint-Martin (ancien code INSEE 97127) ne font plus partie du département et la région d''outre-mer de Guadeloupe mais forment des collectivités d''outre-mer séparées depuis le 15 juillet 2007'::TEXT
            , TRUE
        );
        RETURN;
    ELSIF code IN ('75056', '13055', '69123') THEN
        RAISE NOTICE 'Erreur : la commune % est une commune globale composée d''arrondissements', code;
        IF with_deleted THEN
            is_new := TRUE;
            distribution := 0;
            RETURN NEXT ROW (
                code
                , code_previous
                , name
                , date_geography_from
                , distribution
                , information
                , is_new
            );
        END IF;
        RETURN;
    ELSIF code = '99999' THEN
        RAISE NOTICE 'Erreur : la commune 99999 n''existe pas';
        IF with_deleted THEN
            is_new := TRUE;
            distribution := 0;
            RETURN NEXT ROW (
                code
                , code_previous
                , name
                , date_geography_from
                , distribution
                , information
                , is_new
            );
        END IF;
        RETURN;
    END IF;

    -- available w/ INSEE
    IF fr.get_date_insee_municipality_event() IS NOT NULL
        AND (
            (
                (date_geography_to IS NULL OR date_geography_from < date_geography_to)
                AND date_geography_from < LEAST(
                    date_geography_to
                    , fr.get_date_insee_municipality_event()
                )
                AND (
                    --Pas de MAJ possible par WIKIPEDIA
                    fr.get_date_wikipedia_municipality_event() IS NULL
                    --INSEE est plus reculé que WIKIPEDIA
                    OR fr.get_date_insee_municipality_event(minmax => TRUE) < fr.get_date_wikipedia_municipality_event(minmax => TRUE)
                    --OU BIEN on a épuisé la période de MAJ possible par WIKIPEDIA
                    OR date_geography_from >= fr.get_date_wikipedia_municipality_event()
                )
            )
            OR (
                --retour vers le passé
                (date_geography_from > date_geography_to)
                AND date_geography_from > GREATEST(
                    date_geography_to
                    , fr.get_date_insee_municipality_event(minmax => TRUE)
                )
                AND (
                    --Pas de MAJ possible par WIKIPEDIA
                    fr.get_date_wikipedia_municipality_event() IS NULL
                    --INSEE est plus avancé que WIKIPEDIA
                    OR fr.get_date_insee_municipality_event() > fr.get_date_wikipedia_municipality_event()
                    --OU BIEN on a épuisé la période de MAJ possible par WIKIPEDIA
                    OR date_geography_from <= fr.get_date_wikipedia_municipality_event(minmax => TRUE)
                )
            )
        )
    THEN
        FOR _territory_to_date_t IN (
            SELECT *
            FROM fr.get_municipality_to_date_from_insee(
                code => code
                , name => name
                , date_geography_from => date_geography_from
                , distribution => distribution
                , information => information
                , date_geography_to => date_geography_to
                , is_new => is_new
                , with_deleted => with_deleted
                , code_previous => code_previous
            )
        )
        LOOP
            RETURN QUERY
                SELECT * FROM fr.get_municipality_to_date(
                    code => _territory_to_date_t.code
                    , name => _territory_to_date_t.name
                    , date_geography_from => _territory_to_date_t.date_geography
                    , distribution => _territory_to_date_t.distribution
                    , information => _territory_to_date_t.information
                    , check_exists => check_exists
                    , months_back_if_not_exists => months_back_if_not_exists
                    , date_geography_to => date_geography_to
                    , is_new => _territory_to_date_t.is_new
                    , with_deleted => with_deleted
                    , date_geography_from_first => date_geography_from_first
                    , code_previous => _territory_to_date_t.code_previous
                );
        END LOOP;
        RETURN;

        -- available w/ WIKIPEDIA
    ELSIF fr.get_date_wikipedia_municipality_event() IS NOT NULL
        AND (
            (
                (date_geography_to IS NULL OR date_geography_from < date_geography_to)
                AND date_geography_from < LEAST(
                    date_geography_to
                    , fr.get_date_wikipedia_municipality_event()
                )
                AND (
                    --Pas de MAJ possible par INSEE
                    fr.get_date_insee_municipality_event() IS NULL
                    --WIKIPEDIA est plus reculé que INSEE
                    OR fr.get_date_wikipedia_municipality_event(minmax => TRUE) < fr.get_date_insee_municipality_event(minmax => TRUE)
                    --OU BIEN on a épuisé la période de MAJ possible par INSEE
                    OR date_geography_from >= fr.get_date_insee_municipality_event()
                )
            )
            OR (
                --retour vers le passé
                (date_geography_from > date_geography_to)
                AND date_geography_from > GREATEST(
                    date_geography_to
                    , fr.get_date_wikipedia_municipality_event(minmax => TRUE))
                AND (
                    --Pas de MAJ possible par INSEE
                    fr.get_date_insee_municipality_event() IS NULL
                    --WIKIPEDIA est plus avancé que INSEE
                    OR fr.get_date_wikipedia_municipality_event() > fr.get_date_insee_municipality_event()
                    --OU BIEN on a épuisé la période de MAJ possible par INSEE
                    OR date_geography_from <= fr.get_date_insee_municipality_event(minmax => TRUE)
                )
            )
        )
    THEN
        FOR _territory_to_date_t IN (
            SELECT * FROM fr.get_municipality_to_date_from_wikipedia(
                code => code
                , name => name
                , date_geography_from => date_geography_from
                , distribution => distribution
                , information => information
                , date_geography_to => date_geography_to
                , is_new => is_new
                , code_previous => code_previous
            )
        )
        LOOP
            RETURN QUERY
                SELECT * FROM fr.get_municipality_to_date(
                    code => _territory_to_date_t.code
                    , name => _territory_to_date_t.name
                    , date_geography_from => _territory_to_date_t.date_geography
                    , distribution => _territory_to_date_t.distribution
                    , information => _territory_to_date_t.information
                    , check_exists => check_exists
                    , months_back_if_not_exists => months_back_if_not_exists
                    , date_geography_to => date_geography_to
                    , is_new => _territory_to_date_t.is_new
                    , with_deleted => with_deleted
                    , date_geography_from_first => date_geography_from_first
                    , code_previous => _territory_to_date_t.code_previous
                );
        END LOOP;
        RETURN;
    ELSE
        IF check_exists AND information IS NULL THEN
            BEGIN
                SELECT TRUE
                INTO STRICT _exists
                FROM public.territory
                WHERE country = 'FR' AND level = 'COM' AND code = code;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                IF months_back_if_not_exists > 0 THEN
                    --ATTENTION CETTE METHODE N'EST PAS PARFAITE, ON NE GERE PAS LE CAS D'UNE DIVISION OU UNE PARTIE DES NOUVELLES COMMUNES OBTENUES N'EXISTERAIT PAS (cas ne se produisant à priori jamais)
                    --Alternative à enlever les mois un par un : on les enlève tous en une fois
                    --EXECUTE CONCAT('SELECT $1 - INTERVAL ''', months_back_if_not_exists, ' months''') INTO date_geography_from USING date_geography_from_first;
                    date_geography_from := (date_geography_from_first - INTERVAL '1 month')::DATE;
                    RAISE NOTICE 'Avertissement : la commune % n''existe pas dans public.territory : recherche d''un évènement un mois avant la date de référence initiale (soit à partir du %)', code, date_geography_from;
                    RETURN QUERY
                        SELECT * FROM fr.get_municipality_to_date(
                            code => code
                            , name => name
                            , date_geography_from => date_geography_from
                            , distribution => distribution
                            , information => information
                            , check_exists => check_exists
                            , months_back_if_not_exists => months_back_if_not_exists - 1
                            , date_geography_to => date_geography_to
                            , is_new => is_new
                            , with_deleted => with_deleted
                            , date_geography_from_first => date_geography_from
                            , code_previous => code_previous
                        );
                    RETURN;
                ELSE
                    RAISE NOTICE 'Erreur : la commune % n''existe pas dans public.territory', code;
                    IF with_deleted THEN
                        is_new := TRUE;
                        distribution := 0;
                    ELSE
                        RETURN;
                    END IF;
                END IF;
            END;
        END IF;
        RETURN NEXT ROW (
            code
            , code_previous
            , name
            , date_geography_from
            , distribution
            , information
            , is_new
        );
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM get_municipality_to_date('05043', TO_DATE('01/01/1900', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('33110', TO_DATE('01/01/1900', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('33063', TO_DATE('01/01/1900', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('76601', TO_DATE('01/01/1900', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('76676', TO_DATE('01/01/1900', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('76676', TO_DATE('01/01/2006', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('02344', TO_DATE('01/01/1800', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('49382', TO_DATE('01/01/1800', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('44060', TO_DATE('01/01/1800', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('97123', TO_DATE('01/01/1800', 'DD/MM/YYYY'));
SELECT * FROM get_municipality_to_date('31300', TO_DATE('01/01/1999', 'DD/MM/YYYY'));

-- municipality merge on 01/01/2018
SELECT fr.get_municipality_to_date(
    code => '16296'
    , date_geography_from => TO_DATE('2018-01-06', 'YYYY-MM-DD')
);
 */

SELECT public.drop_all_functions_if_exists('fr', 'get_municipality_to_date_from_laposte');
CREATE OR REPLACE FUNCTION fr.get_municipality_to_date_from_laposte(
    code IN VARCHAR
    , date_geography_from IN DATE
    , distribution IN NUMERIC DEFAULT 1
    , raise_notice IN BOOLEAN DEFAULT FALSE
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
    --_territory_to_date_t territory_to_date_t%ROWTYPE;
    _territory_to_date_t territory_to_date_t;
    _date_address DATE := (public.get_last_io(type_in => 'RAN_ADRESSE')).dt_data_end;
    _municipalities RECORD;
    _municipality VARCHAR;
    _return BOOLEAN := TRUE;
BEGIN
    FOR _municipalities IN (
        SELECT
            ARRAY_AGG(DISTINCT co_insee_commune) AS municipalities_now
            , 1::NUMERIC/COUNT(DISTINCT co_insee_commune) AS distribution
        FROM fr.laposte_zone_address
        WHERE co_insee_commune_precedente = code
        --WHERE co_insee_commune_precedente = '05088'
        --WHERE co_insee_commune_precedente = '05043'
        AND fl_active
        GROUP BY co_insee_commune_precedente
        --COALESCE(co_insee_commune_precedente, LEFT(co_adr, 5)) ? --permet de résoudre le pb sur 76676 / 76601 mais risqué car à ne pas refaire après une certaine date
    )
    LOOP
        _return := FALSE;
        FOREACH _municipality IN ARRAY _municipalities.municipalities_now LOOP
            RETURN NEXT ROW (
                _municipality
                , NULL
                , _date_address
                , _municipalities.distribution
            );
        END LOOP;
    END LOOP;

    IF _return THEN
        RETURN NEXT ROW (
            code
            , NULL
            , _date_address
            , distribution
        );
    END IF;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_zone_address_to_now');
CREATE OR REPLACE FUNCTION fr.get_zone_address_to_now(
    zone_address fr.laposte_zone_address
)
RETURNS fr.laposte_zone_address AS
$func$
DECLARE
    _municipality_to_now RECORD;
BEGIN
    --Cas de réactivation non géré pour le moment : est-ce un cas possible ?
    IF NOT zone_address.fl_active THEN RETURN zone_address; END IF;

    SELECT *
    INTO _municipality_to_now
    FROM fr.get_municipality_to_date(
        code => zone_address.co_insee_commune
        --On force l'algo à considérer en cas de fusion que cette ZA correspond à la portion avant fusion, même pour la commune déléguée chef lieu
        , code_previous => COALESCE(
            zone_address.co_insee_commune_precedente
            , zone_address.co_insee_commune
        )
        , date_geography_from => zone_address.dt_reference_commune
        , with_deleted => TRUE --Cas de suppression/désactivation non géré pour le moment : est-ce un cas possible ?
        , check_exists => FALSE --Ce test n'aurait pas de sens, puisque la liste des communes de la table territory est issue de RAN
    ) AS commune_to_now
    WHERE commune_to_now.is_new --Seulement ce qui est nouveau
    ;
    --Même en cas de fusion, on ne stocke pas dans RAN le code INSEE précédent s'il ne change pas (cas de la commune déléguée chef lieu)
    IF _municipality_to_now.code = _municipality_to_now.code_previous THEN
        _municipality_to_now.code_previous := NULL;
    END IF;

    IF _municipality_to_now.distribution = 1 THEN
        RAISE NOTICE 'Cas de (fusion de commune / création commune nouvelle) géré pour maj GEO de RAN ZA : %, % / %, % -> %, %'
            , zone_address.co_cea
            , zone_address.co_insee_commune
            , zone_address.co_insee_commune_precedente
            , zone_address.lb_nn
            , _municipality_to_now.code
            , _municipality_to_now.name;

        /* maj des libellés
        -- new normalized label, as L6-label
        zone_address.lb_ach_nn := address_label_normalize_municipality(_municipality_to_now.code, _municipality_to_now.name);
        -- TODO what about lb_nn ???
        -- remains merged municipality as L5-label
        IF zone_address.co_insee_commune != _municipality_to_now.code THEN
            zone_address.lb_l5_nn := zone_address.lb_ach_nn;
        END IF;
         */

        -- keep eventualy previuous code (if already merged)
        IF zone_address.co_insee_commune_precedente IS NULL THEN
            zone_address.co_insee_commune_precedente := zone_address.co_insee_commune;
        END IF;
        zone_address.co_insee_commune := _municipality_to_now.code;
        zone_address.co_insee_departement :=            fr.get_department_code_from_municipality_code(zone_address.co_insee_commune);
        zone_address.dt_reference_commune := _municipality_to_now.date_geography;

        /* NOTE : pour une MAJ des libellés
         *
         * 1) Y a t il vraiment un intérêt ?
         * 2) Demander les règles de maj :
         *  lb_nn -> libellé de la commune nouvelle ?
         *  lb_ach_nn -> libellé de la commune nouvelle ?
         *  lb_l5_nn -> libellé de la commune déléguée = lb_ach_nn si lb_l5_nn pas déjà renseigné ?
         * 3) Il faut gérer correctement le diff RAN pour mettre à jour, en sauvegardant les valeurs d'origines RAN dans une colonne dédiée, tel que fait avec co_insee_commune_ran / co_insee_commune_precedente_ran
         * 4) Il faut ignorer les différence due à la normalisation du libellé, le mieux étant d'appliquer les règles officielles de normalisation (quelles sont elles ?) :
         * public.removeMotsOutils(REPLACE(public.upperNoSpecialsCharsOnlyAlfaNum(_municipality_to_now.libgeo), 'SAINT', 'ST')) != public.removeMotsOutils(REPLACE(zone_address.lb_nn, 'SAINT', 'ST'))
         */

    ELSIF _municipality_to_now.distribution < 1 AND _municipality_to_now.distribution > 0 THEN
        RAISE NOTICE 'Cas de rétablissement de commune géré pour maj GEO de RAN ZA : %, % / %, % -> %, %'
            , zone_address.co_cea
            , zone_address.co_insee_commune
            , zone_address.co_insee_commune_precedente
            , zone_address.lb_nn
            , _municipality_to_now.code
            , _municipality_to_now.name;
        zone_address.co_insee_commune := zone_address.co_insee_commune_precedente;
        zone_address.co_insee_departement := fr.get_department_code_from_municipality_code(zone_address.co_insee_commune);
        zone_address.co_insee_commune_precedente := NULL;
        zone_address.dt_reference_commune := _municipality_to_now.date_geography;

        /* NOTE : Cas rare de division non géré pour le moment, pourrait être :
        zone_address.lb_nn := lb_l5_nn;
        zone_address.lb_ach_nn := lb_l5_nn;
        zone_address.co_insee_commune_precedente := NULL;
        zone_address.dt_reference_commune = _municipality_to_now.date_geography;
         */
    ELSIF _municipality_to_now.distribution = 0 THEN
        RAISE NOTICE 'Cas de suppression de commune non géré pour maj GEO de RAN ZA : %, % / %, %'
            , zone_address.co_cea
            , zone_address.co_insee_commune
            , zone_address.co_insee_commune_precedente
            , zone_address.lb_nn;
        /* NOTE : Cas rare (inexistant ?) de suppression non géré pour le moment, pourrait être
        zone_address.fl_active := FALSE;
        zone_address.dt_reference_commune := _municipality_to_now.date_geography;
         */
    END IF;

    RETURN zone_address;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_zone_address_to_now');
CREATE OR REPLACE FUNCTION fr.set_zone_address_to_now()
RETURNS BOOLEAN AS
$func$
DECLARE
    _zone_address_to_now RECORD;
    _date_address DATE := (public.get_last_io(type_in => 'RAN_ADRESSE')).dt_data_end;
    _laposte_updated BOOLEAN DEFAULT FALSE;
BEGIN
    FOR _zone_address_to_now IN (
        SELECT
            za_to_now.co_cea
            , za_to_now.co_insee_commune
            , za_to_now.co_insee_commune_precedente
            , za_to_now.dt_reference_commune
            , za_to_now.co_insee_departement
            --Si modification effective hormis la date de référence
            , CASE
                WHEN za_to_now.co_insee_commune != za.co_insee_commune
                    OR za_to_now.co_insee_commune_precedente IS DISTINCT FROM za.co_insee_commune_precedente
                --On considère l'adresse mise à jour à date de RAN + 1, de telle façon que les traitements DELTA traitent cette adresse lors de leur prochain lancement
                THEN _date_address + 1
                ELSE za.dt_reference
            END AS dt_reference
            , CASE
                WHEN za_to_now.co_insee_commune != za.co_insee_commune
                    OR za_to_now.co_insee_commune_precedente IS DISTINCT FROM za.co_insee_commune_precedente
                THEN TRUE
                ELSE FALSE
            END AS modification
        FROM fr.laposte_zone_address AS za
        CROSS JOIN fr.get_zone_address_to_now(za) AS za_to_now
        WHERE za_to_now.dt_reference_commune != za.dt_reference_commune
    )
    LOOP
        UPDATE fr.laposte_zone_address
        SET co_insee_commune = _zone_address_to_now.co_insee_commune
            , co_insee_commune_precedente = _zone_address_to_now.co_insee_commune_precedente
            , dt_reference_commune = _zone_address_to_now.dt_reference_commune
            , dt_reference = _zone_address_to_now.dt_reference
            , co_insee_departement = _zone_address_to_now.co_insee_departement
        WHERE za.co_cea = _zone_address_to_now.co_cea;

        --Si modification effective hormis la date de référence
        IF _zone_address_to_now.modification = TRUE THEN
            UPDATE fr.laposte_address
            SET dt_reference_za = _zone_address_to_now.dt_reference
                , dt_reference = GREATEST(dt_reference, _zone_address_to_now.dt_reference)
            WHERE co_cea_za = _zone_address_to_now.co_cea;

            --MAJ du code INSEE commune dénormalisé sur les voies de la ZA
            UPDATE fr.laposte_street street
            SET co_insee_commune = _zone_address_to_now.co_insee_commune
            FROM fr.laposte_address address
            WHERE adress.co_cea_determinant = street.co_cea
            AND adress.co_cea_za = _zone_address_to_now.co_cea --Voies de la ZA
            AND street.co_insee_commune != _zone_address_to_now.co_insee_commune; --Qui ont un code INSEE commune différent (à priori forcément vrai);
        END IF;

        _laposte_updated := TRUE;
    END LOOP;

    RETURN _laposte_updated;
END
$func$ LANGUAGE plpgsql;

/* TEST

INSERT INTO fr.wikipedia_municipality_event (
    millesime
    , cn_nom
    , cn_code_insee
    , ac_noms
    , ac_codes_insee
    , dt_effet
)
VALUES (
    '2020'
    , 'Bordignac'
    , '33063'
    , ARRAY['Bordeaux', 'Mérignac']
    , ARRAY['33063', '33281']
    , '14/02/2020'::DATE
);

SELECT * FROM fr.wikipedia_municipality_event;
SELECT public.get_municipality_to_date(
    code => '33281'
    , date_geography_from => '01/01/2019'::DATE
);

SELECT dt_fin_donnees FROM public.historique_import
WHERE co_etat = 'SUCCES' AND co_type = 'WIKIPEDIA_COMMUNE_NOUVELLE';

INSERT INTO historique_import (co_type, co_etat, dt_debut_donnees, dt_fin_donnees, nb_enregistrements_a_traiter)
VALUES ('WIKIPEDIA_COMMUNE_NOUVELLE', 'SUCCES', '01/01/2020'::DATE, '01/01/2020'::DATE, 0);

SELECT * FROM public.set_zone_address_to_now();
SELECT * FROM fr.laposte_zone_address WHERE co_insee_commune != co_insee_commune_ran;
 */

/*
 * update all data w/ geography
 */
SELECT drop_all_functions_if_exists('fr', 'set_data_with_geography_to_now');
CREATE OR REPLACE PROCEDURE fr.set_data_with_geography_to_now(
    execution_time IN INTERVAL DEFAULT NULL
)
AS
$func$
DECLARE
    _end_execution TIMESTAMP WITHOUT TIME ZONE := clock_timestamp() + execution_time;
    _raise_overtime VARCHAR := 'set_data_with_geography_to_now: Temps de traitement maximum dépassé';
BEGIN
    /* TODO
    --déjà fait toutes les semaines lors de l'intégration de RAN, suite import RAN, suite import et intégration GEOPAD / INSEE / IGN / ... (cf /public/adresse_ran.sh -> /public/territory.sh)
    PERFORM public.setTerritoireIgnGeoToNow();
    PERFORM public.setTerritoireInseeGeoToNow();
    --déjà fait toutes les semaines lors de l'import de RAN, (cf /ran/structure/za.sql)
    --donc théoriquement inutile, sauf retard ou MAJ des sources d'évènement avant application dans RAN (evenements commune insee, commune nouvelle wikipedia)
    PERFORM fr.set_zone_address_to_now();
    --à faire régulièrement ?
    PERFORM fr.set_territory_to_date(
        table_name => 'territoire_has_insee'
        , set_supra => TRUE
        -- pas nécessaire, on fait confiance à l'INSEE ? et pour garder l'indépendance avec la table territory ?
        , check_exists => FALSE
    );

    PERFORM public.setTerritoireGeoToNow();
    COMMIT;
     */

    IF clock_timestamp() > _end_execution THEN RAISE NOTICE '%', _raise_overtime; RETURN; END IF;

END
$func$ LANGUAGE plpgsql;

/*
 * test if exists 'level' value into a table
 */
SELECT drop_all_functions_if_exists('fr', 'exists_level');
CREATE OR REPLACE FUNCTION fr.exists_level(
    table_name VARCHAR
    , levels IN VARCHAR[]
    , schema_name VARCHAR DEFAULT 'public'
    , where_in IN VARCHAR DEFAULT NULL
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query TEXT;
    _level VARCHAR;
    _exists BOOLEAN DEFAULT FALSE;
BEGIN
    FOREACH _level IN ARRAY levels
    LOOP
        _query := CONCAT(
            'SELECT TRUE FROM ', schema_name, '.', table_name, ' AS source
            WHERE source.nivgeo = $1
            ', CASE WHEN NULLIF(where_in, '') IS NOT NULL THEN CONCAT(' AND ', where_in) END, '
            LIMIT 1'
        );
        EXECUTE _query INTO _exists USING _level;
        _exists := COALESCE(_exists, FALSE);
        IF NOT _exists THEN
            RAISE NOTICE 'data NIVGEO % inexistante', _level;
            RETURN FALSE;
        END IF;
    END LOOP;
    RETURN _exists;
END
$func$ LANGUAGE plpgsql;

/*
 * keep up to date a table w/ (nivgeo, codgeo, ...) mandatory columns
 * update aggregate columns according to municipality events
 */
SELECT drop_all_functions_if_exists('fr', 'set_territory_to_date');
CREATE OR REPLACE FUNCTION fr.set_territory_to_date(
    table_name IN VARCHAR
    , columns_agg IN TEXT[] DEFAULT NULL                -- NULL for all else list of column(s)
    , columns_groupby IN TEXT[] DEFAULT NULL            -- idem
    , where_in IN TEXT DEFAULT NULL
    , set_supra IN BOOLEAN DEFAULT TRUE
    , schema_name IN VARCHAR DEFAULT 'public'
    , check_exists IN BOOLEAN DEFAULT TRUE
    , date_geography_to IN DATE DEFAULT NOW()
    , date_geography_default_from IN DATE DEFAULT NULL  -- for first time update
    , upsert_mode IN BOOLEAN DEFAULT FALSE
    , simulation IN BOOLEAN DEFAULT FALSE
    , base_level IN VARCHAR DEFAULT 'COM'
    , date_geography_metadata IN VARCHAR DEFAULT 'dtrgeo'
)
RETURNS BOOLEAN AS                                      -- FALSE if nothing to do
$func$
DECLARE
    _date_geography_from DATE;
    _is_init BOOLEAN DEFAULT FALSE;

    _query TEXT;
    _query_where TEXT;
    _query_join TEXT;

    _tmp_table_name VARCHAR;
    _full_table_name VARCHAR := CONCAT(schema_name, '.', table_name);
    _columns_select TEXT := 'ARRAY_AGG(source.codgeo) AS anciens_codgeo, SUM(commune_to_now.repartition) AS sum_repartition';
    _columns_groupby TEXT := 'commune_to_now.codgeo';
    _columns_insert TEXT;
    _columns_onconflict TEXT;
    _columns_onconflict_set TEXT;
    _column_information information_schema.columns%ROWTYPE;
    _column_type VARCHAR;
    _column_name TEXT;
    _column_geometry_information RECORD;

    _nrows_deleted INTEGER := 0;
    _nrows_inserted INTEGER := 0;

    _levels VARCHAR[];
    _level VARCHAR;

    _self_use BOOLEAN := FALSE;
    _exists_supra BOOLEAN;
    _exists_nivgeo BOOLEAN := FALSE;
    _exists_libgeo BOOLEAN := FALSE;
    _start_time TIMESTAMP WITHOUT TIME ZONE;
BEGIN
    IF where_in IS NOT NULL AND date_geography_metadata = 'dtrgeo' THEN
        RAISE 'Veuillez préciser un nom de métadonnées dtrgeo spécifique à la condition where';
    END IF;

    _query_where := NULLIF(CONCAT_WS(' AND ', _query_where, where_in), '');
    FOREACH _column_name IN ARRAY get_table_columns(schema_name, table_name) LOOP
        IF _column_name LIKE 'codgeo_%_parent' THEN
            IF NOT _self_use THEN
                _self_use := TRUE;
                _levels := NULL::VARCHAR[];
            END IF;
            _level := UPPER(REPLACE(REPLACE(_column_name, 'codgeo_', ''), '_parent', ''));
            _levels := ARRAY_APPEND(_levels, _level);
        END IF;

        IF _column_name IN ('id_histo', 'nb_histo_use') THEN CONTINUE;
        ELSIF _column_name IN ('codgeo') THEN
            _columns_select := CONCAT_WS(', ', _columns_select, 'commune_to_now.codgeo');
            _columns_onconflict := CONCAT_WS(', ', _columns_onconflict, _column_name);
            _query_join := CONCAT_WS(' AND ', _query_join, CONCAT('source.', _column_name, ' = destination.', _column_name));
        ELSIF _column_name IN ('nivgeo') THEN
            _exists_nivgeo := TRUE;
            _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('''', base_level, '''::VARCHAR AS nivgeo'));
            _query_where := CONCAT_WS(' AND ', _query_where, CONCAT('source.nivgeo = ''', base_level, ''''));
            _columns_onconflict := CONCAT_WS(', ', _columns_onconflict, _column_name);
            _query_join := CONCAT_WS(' AND ', _query_join, CONCAT('source.', _column_name, ' = destination.', _column_name));
        ELSIF _column_name IN ('libgeo') THEN
            _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('FIRST(commune_to_now.', _column_name, ') AS libgeo'));
            _columns_onconflict_set := CONCAT_WS(', ', _columns_onconflict_set, CONCAT(_column_name, '=EXCLUDED.', _column_name));
        ELSIF _column_name IN ('dt_reference', 'dt_reference_data') OR (_column_name = ANY(columns_groupby)) THEN
            _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('source.', _column_name));
            _columns_groupby := CONCAT_WS(', ', _columns_groupby, CONCAT('source.', _column_name));
            _columns_onconflict := CONCAT_WS(', ', _columns_onconflict, _column_name);
            _query_join := CONCAT_WS(' AND ', _query_join, CONCAT('source.', _column_name, ' = destination.', _column_name));
        ELSE
            _column_information := public.get_column_information(schema_name, table_name, _column_name);
            _column_type := LOWER(COALESCE(NULLIF(_column_information.data_type, 'USER-DEFINED'), _column_information.udt_name));
            IF _column_type IN ('numeric', 'integer', 'real', 'smallint', 'bigint', 'double precision') THEN
                _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('SUM(source.', _column_name, ' * commune_to_now.repartition) AS ', _column_name));
                _columns_onconflict_set := CONCAT_WS(', ', _columns_onconflict_set, CONCAT(_column_name, '=(destination.', _column_name, '+EXCLUDED.', _column_name, ')'));
            ELSIF _column_type IN ('geometry') THEN
                SELECT srid, type
                INTO _column_geometry_information
                FROM ext_postgis.geometry_columns
                WHERE f_table_catalog = 'pow'
                AND f_table_schema = schema_name
                AND f_table_name = table_name
                AND f_geometry_column = _column_name;
                IF _column_geometry_information.type LIKE 'MULTI%' THEN
                    _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('ST_Multi(ST_Union(source.', _column_name, ')) AS ', _column_name));
                    _columns_onconflict_set := CONCAT_WS(', ', _columns_onconflict_set, CONCAT(_column_name, '=ST_Multi(ST_Union(destination.', _column_name, ', EXCLUDED.', _column_name, '))'));
                ELSE
                    _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('ST_Union(source.', _column_name, ') AS ', _column_name));
                    _columns_onconflict_set := CONCAT_WS(', ', _columns_onconflict_set, CONCAT(_column_name, '=ST_Union(destination.', _column_name, ', EXCLUDED.', _column_name, ')'));
                END IF;
            ELSIF _column_type IN ('array') THEN
                RAISE NOTICE 'Type % non géré, les valeurs NULL de la colonne % de la table %.% sont à recalculer', _column_type, _column_name, schema_name, table_name;
                _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('NULL AS ', _column_name));
                _columns_onconflict_set := CONCAT_WS(', ', _columns_onconflict_set, CONCAT(_column_name, '=NULL'));
            ELSE
                /* NOTE de GVOYAU : on utilise FIRST et non pas UNIQUE_AGG
                car quand plusieurs communes fusionnent, même si les valeurs textuelles ne sont pas les mêmes pour toutes les communes
                on garde la valeur de la commune qui absorbe
                exemple : une commune absorbée a un département différent de celui de la commune qui absorbe
                 */
                _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('FIRST(source.', _column_name, ' ORDER BY source.codgeo=commune_to_now.codgeo DESC) AS ', _column_name));
                /* NOTE de GVOYAU : pas de ONCONFLICT géré pour cette colonne, car pas nécessaire, car alors on garde la valeur actuelle
                 */
            END IF;
        END IF;
        _columns_insert := CONCAT_WS(', ', _columns_insert, _column_name);
    END LOOP;

    _levels = fr.get_levels(
        order_in => 'ASC'
        , among_levels => _levels --en cas de self use, on ordonne les niveaux
        , subfilter => base_level
    );

    IF NOT fr.exists_level(
        schema_name => schema_name
        , table_name => table_name
        , levels => ARRAY[base_level]
        , where_in => _query_where
    )
    THEN
        RAISE NOTICE 'Traitement GEO TO NOW % de %.% inutile (aucune donnée GEO)', base_level, schema_name, table_name;
        RETURN FALSE;
    END IF;

    _date_geography_from := TO_DATE(NULLIF(public.get_table_metadata(schema_name, table_name)->>date_geography_metadata, ''), 'DD/MM/YYYY');
    IF _date_geography_from IS NULL THEN
        _is_init := TRUE;
        IF date_geography_metadata IS NOT NULL THEN
            /* NOTE
            on considère que les données ne peuvent pas être plus à jour que le système de mise à jour ?
             */
            _date_geography_from := LEAST(date_geography_metadata, fr.get_most_recent_municipality_to_date(date_geography_to => date_geography_to));
            /* NOTE
            On préfère enregistrer l'information, pour s'en souvenir plus tard, au cas où la date de géo par défaut serait changeante dans le temps (exemple : addTerritoireHasDataHisto)
             */
            IF NOT simulation THEN
                --RAISE NOTICE 'set_table_metadata % % % = %', schema_name, table_name, date_geography_metadata, _date_geography_from;
                PERFORM public.set_table_metadata(schema_name, table_name, CONCAT('{"', date_geography_metadata, '":"', TO_CHAR(_date_geography_from, 'DD/MM/YYYY'), '"}'));
            END IF;
        ELSE
            RAISE 'Veuillez préciser la date de référence de la géographie initiale de la table %.%', schema_name, table_name;
        END IF;
    END IF;

    date_geography_to := fr.get_most_recent_municipality_to_date(date_geography_from => _date_geography_from, date_geography_to => date_geography_to);
    /* NOTE
    si c'est la première fois qu'on met à jour ces données, et qu'un contrôle d'existance est demandé, on fait quand même l'appel à getCommuneToNow
     */
    IF date_geography_to <= _date_geography_from AND (NOT _is_init OR NOT check_exists ) THEN
        RAISE NOTICE 'Traitement GEO TO NOW % de %.% inutile (GEO déjà à jour)', base_level, schema_name, table_name;
        IF NOT set_supra THEN RETURN FALSE; END IF;
    ELSE
        RAISE NOTICE '% : début traitement GEO TO NOW de %.% du % au %', TO_CHAR(clock_timestamp(), 'HH24:MI:SS'), schema_name, table_name, _date_geography_from, date_geography_to;

        _tmp_table_name := CONCAT('tmp_gtn_', MD5(CONCAT(table_name, _columns_insert)));
        _query := CONCAT(
            'CREATE TEMPORARY TABLE IF NOT EXISTS ', _tmp_table_name, ' AS (
                SELECT
                    NULL::VARCHAR[] AS anciens_codgeo
                    , NULL::NUMERIC AS sum_repartition
                    , ', _columns_insert, ' FROM ', _full_table_name, ' LIMIT 0
            ) WITH NO DATA;
            TRUNCATE TABLE ', _tmp_table_name, ';
            --DROP INDEX IF EXISTS idx_', _tmp_table_name, '_pk;
            INSERT INTO ', _tmp_table_name, '(
                anciens_codgeo, sum_repartition, ', _columns_insert, '
            )
            (
            ', CASE WHEN base_level = 'COM' THEN CONCAT(
                'WITH distinct_commune AS (
                    SELECT source.codgeo AS old_codgeo_com', CASE WHEN _exists_libgeo THEN ', source.libgeo' END, '
                    FROM ', _full_table_name, ' AS source
                    ', CASE WHEN _query_where IS NOT NULL THEN CONCAT('WHERE ', _query_where) END, '
                    GROUP BY source.codgeo
                )
                , commune_to_now AS (
                    SELECT
                        commune_to_now.codgeo
                        , commune_to_now.libgeo
                        , commune_to_now.repartition
                        , distinct_commune.old_codgeo_com AS old_codgeo
                    FROM distinct_commune
                    INNER JOIN fr.get_municipality_to_date(
                        code => distinct_commune.old_codgeo_com
                        , date_geography_from => $3
                        , name => ', CASE WHEN _exists_libgeo THEN 'distinct_commune.libgeo' ELSE 'NULL' END, '
                        , check_exists => $1
                        , with_deleted => $4 --Besoin des suppressions pour le mode UPSERT
                        , date_geography_to => $2
                    ) AS commune_to_now
                    ON (NOT $4 OR commune_to_now.is_new) --Uniquement ce qui est nouveau si on est en mode UPSERT
                )')
            WHEN base_level IN ('ZA', 'IRIS') THEN CONCAT(
                'WITH distinct_commune AS (
                    SELECT LEFT(source.codgeo, 5) AS old_codgeo_com, ARRAY_AGG(DISTINCT source.codgeo) AS old_codgeos_subcom
                    FROM ', _full_table_name, ' AS source
                    ', CASE WHEN _query_where IS NOT NULL THEN CONCAT('WHERE ', _query_where) END, '
                    GROUP BY LEFT(source.codgeo, 5)
                )
                , commune_to_now AS (
                    SELECT
                        CONCAT(commune_to_now.codgeo, ''-'', RIGHT(UNNEST(distinct_commune.old_codgeos_subcom), 5))::VARCHAR AS codgeo
                        , commune_to_now.libgeo --TODO : REPLACE [0-9]{5}
                        , commune_to_now.repartition
                        , UNNEST(distinct_commune.old_codgeos_subcom) AS old_codgeo
                    FROM distinct_commune
                    INNER JOIN fr.get_municipality_to_date(
                        code => distinct_commune.old_codgeo_com
                        , date_geography_from => $3
                        --TODO, name => ', CASE WHEN _exists_libgeo THEN 'distinct_commune.libgeo' ELSE 'NULL' END, '
                        , check_exists => $1
                        , with_deleted => $4 --Besoin des suppressions pour le mode UPSERT
                        , date_geography_to => $2
                    ) AS commune_to_now
                    ON (NOT $4 OR commune_to_now.is_new) --Uniquement ce qui est nouveau si on est en mode UPSERT
                )')
            END,
                'SELECT ', _columns_select, '
                FROM ', _full_table_name, ' AS source
                INNER JOIN commune_to_now ON commune_to_now.old_codgeo = source.codgeo
                ', CASE WHEN _query_where IS NOT NULL THEN CONCAT('WHERE ', _query_where) END, '
                GROUP BY ', _columns_groupby, '
            );
            UPDATE ', _tmp_table_name, ' AS destination
            SET anciens_codgeo=ARRAY_APPEND(destination.anciens_codgeo, source.codgeo), ', REPLACE(_columns_onconflict_set, 'EXCLUDED', 'source'), '
            FROM (
                --Cas des communes déléguées absentes à cause d''une date de référence erronée
                WITH destination AS (SELECT * FROM ', _tmp_table_name, ' WHERE NOT (codgeo = ANY(anciens_codgeo)))
                SELECT source.*
                FROM ', _full_table_name, ' AS source
                INNER JOIN destination ON ', _query_join, '
            ) AS source
            WHERE ', _query_join);
        IF NOT simulation THEN
            _start_time := clock_timestamp();
            EXECUTE _query USING check_exists, date_geography_to, _date_geography_from, upsert_mode;
            RAISE NOTICE 'Traitement GEO TO NOW "%" : %', LEFT(_query, 30), (clock_timestamp() - _start_time);
        ELSE
            RAISE NOTICE '% - % - % - % - %', _query, check_exists, date_geography_to, _date_geography_from, upsert_mode;
        END IF;

        IF NOT upsert_mode THEN
            _query := CONCAT(
                'DELETE FROM ', _full_table_name, ' AS source
                WHERE ', _query_where
            );
        ELSE
            IF _exists_nivgeo THEN
                _query := CONCAT(
                    'UPDATE ', _full_table_name, ' AS source
                    SET nivgeo = CONCAT(source.nivgeo, ''_A_'', TO_CHAR($1, ''YYYYMMDD''))
                    FROM ', _tmp_table_name, ' AS destination
                    WHERE ', REPLACE(_query_join, 'destination.codgeo', 'ANY(destination.anciens_codgeo)'), '
                    ', CASE WHEN _query_where IS NOT NULL THEN CONCAT(' AND ', _query_where) END, ' --a priori pas indispensable, la jointure sur des données déjà filtrée étant suffisante'
                );
            ELSE
                _query := CONCAT(
                    'DELETE FROM ', _full_table_name, ' AS source
                    USING ', _tmp_table_name, ' AS destination
                    WHERE ', REPLACE(_query_join, 'destination.codgeo', 'ANY(destination.anciens_codgeo)'), '
                    ', CASE WHEN _query_where IS NOT NULL THEN CONCAT(' AND ', _query_where) END, ' --a priori pas indispensable, la jointure sur des données déjà filtrée étant suffisante'
                );
            END IF;
        END IF;
        IF NOT simulation THEN
            _start_time := clock_timestamp();
            EXECUTE _query USING _date_geography_from;
            GET DIAGNOSTICS _nrows_deleted = ROW_COUNT;
            RAISE NOTICE 'Traitement GEO TO NOW "%" : %', LEFT(_query, 30), (clock_timestamp() - _start_time);
        ELSE
            RAISE NOTICE '%', _query;
        END IF;

        IF NOT upsert_mode THEN
            _query := CONCAT(
                'INSERT INTO ', _full_table_name, ' AS destination (', _columns_insert, ') (SELECT ', _columns_insert, ' FROM ', _tmp_table_name, ')'
            );
        ELSE
            /* NOTE
            en théorie tous les territoires anciens ont un évènement vers le nouveau territoire
            Mais dans le cas de d'une création d'une commune nouvelle et de date de géographie erronée, on ne peut détecter l'erreur pour la commune dont le code est gardé
            Il faut donc gérer un conflit lors de l'insert, mais la solution ON CONFLICT n'est pas compatible avec les vues + instead of
            Ce problème est résolu par un UPDATE de la table tmp_gtn juste après sa création
            */
            _query := CONCAT(
                'INSERT INTO ', _full_table_name, ' AS destination (', _columns_insert, ') (SELECT ', _columns_insert, ' FROM ', _tmp_table_name, ' AS source WHERE sum_repartition > 0 AND NOT EXISTS (SELECT 1 FROM ', _full_table_name, ' AS destination WHERE ', _query_join, '))'
                --ON CONFLICT (', _columns_onconflict, ') DO UPDATE SET ', _columns_onconflict_set
            );
        END IF;
        IF NOT simulation THEN
            _start_time := clock_timestamp();
            EXECUTE _query;
            GET DIAGNOSTICS _nrows_inserted = ROW_COUNT;
            RAISE NOTICE 'Traitement GEO TO NOW "%" : %', LEFT(_query, 30), (clock_timestamp() - _start_time);
        ELSE
            RAISE NOTICE '%', _query;
        END IF;

        RAISE NOTICE '% : Fin traitement GEO TO NOW % de %.% du % au % : % (%-%)', TO_CHAR(clock_timestamp(), 'HH24:MI:SS'), base_level, schema_name, table_name, _date_geography_from, date_geography_to, CONCAT(CASE WHEN (_nrows_inserted-_nrows_deleted) >=0 THEN '+' ELSE '-' END, ABS(_nrows_inserted-_nrows_deleted)), _nrows_inserted, _nrows_deleted;

        IF NOT simulation THEN
            --RAISE NOTICE 'set_table_metadata % % % = %', schema_name, table_name, date_geography_metadata, date_geography_to;
            PERFORM public.set_table_metadata(schema_name, table_name, CONCAT('{"', date_geography_metadata, '":"', TO_CHAR(date_geography_to, 'DD/MM/YYYY'), '"}'));
        END IF;
    END IF;

    IF set_supra THEN
        --Y a t-il des territoires d'un niveau autre que le niveau de base, qui est un sous découpage du niveau de base ?
        _exists_supra := FALSE;
        IF _nrows_inserted = 0
        AND _nrows_deleted = 0
        AND _is_init = FALSE --Si c'est la première fois qu'on met à jour ces données, on fait quand même la mise à jour SUPRA quoi qu'il en soit
        THEN
            _exists_supra := TRUE;
            --Vérification des niveaux parents à générer
            --On prend parmi les niveaux possibles
            FOREACH _level IN ARRAY _levels
            LOOP
                IF  --Ceux qui sont différents du niveau de base
                    _level != base_level
                THEN
                    IF NOT fr.exists_level(
                            schema_name => schema_name
                            , table_name => table_name
                            , levels => ARRAY[_level]
                            , where_in => where_in
                    )
                    THEN
                        RAISE NOTICE 'GEO SUPRA % inexistant', _level;
                        _exists_supra := FALSE;
                        EXIT;
                    END IF;
                END IF;
            END LOOP;
        END IF;

        IF _nrows_inserted = 0
        AND _nrows_deleted = 0
        AND _exists_supra = TRUE
        THEN
            RAISE NOTICE 'Traitement GEO SUPRA % de % inutile (GEO à jour, SUPRA déjà présent)', base_level, table_name;
            RETURN FALSE;
        ELSE
            PERFORM fr.set_territory_supra(
                table_name => table_name
                , columns_agg => columns_agg
                , columns_groupby => columns_groupby
                , where_in => where_in
                , schema_name => schema_name
                , base_level => base_level
                , simulation => simulation
            );
        END IF;
    END IF;

    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;
