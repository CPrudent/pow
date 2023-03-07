/***
 * FR-TERRITORY : maintains municipality up to date
 */

/*
 * get min|max date of FR INSEE municipality event
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_date_insee_municipality_event');
CREATE OR REPLACE FUNCTION fr.get_date_insee_municipality_event(
    _min IN BOOLEAN DEFAULT FALSE
)
RETURNS DATE AS
$func$
DECLARE
    _date_min DATE;
    _date_max DATE;
    _fr_municipality_event VARCHAR := 'fr.insee_municipality_event';
BEGIN
    IF table_exists('fr', 'insee_municipality_event') THEN
        IF _min THEN
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
            'set_config('''
            , CONCAT_WS('.', _fr_municipality_event, 'date_min')
            , ''', '''
            , COALESCE(TO_CHAR(_date_min, 'DD/MM/YYYY'), 'NULL')
            , ''', TRUE)'
        );
        EXECUTE CONCAT(
            'set_config('''
            , CONCAT_WS('.', _fr_municipality_event, 'date_max')
            , ''', '''
            , COALESCE(TO_CHAR(_date_max, 'DD/MM/YYYY'), 'NULL')
            , ''', TRUE)'
        );
    ELSE
        _date_min := NOW();
        _date_max := '1970-01-01'::DATE;
    END IF;

    IF _min THEN RETURN _date_min; ELSE RETURN _date_max; END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT fr.get_date_insee_municipality_event()
SELECT fr.get_date_insee_municipality_event(_min => TRUE)
 */

/*
 * get min|max date of FR WIKIPEDIA municipality event
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_date_wikipedia_municipality_event');
CREATE OR REPLACE FUNCTION fr.get_date_wikipedia_municipality_event(
    _min IN BOOLEAN DEFAULT FALSE
)
RETURNS DATE AS
$func$
DECLARE
    _date_min DATE;
    _date_max DATE;
    _fr_municipality_event VARCHAR := 'fr.wikipedia_municipality_event';
BEGIN
    IF table_exists('fr', 'wikipedia_municipality_event') THEN
        IF _min THEN
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
            'set_config('''
            , CONCAT_WS('.', _fr_municipality_event, 'date_min')
            , ''', '''
            , COALESCE(TO_CHAR(_date_min, 'DD/MM/YYYY'), 'NULL')
            , ''', TRUE)'
        );
        EXECUTE CONCAT(
            'set_config('''
            , CONCAT_WS('.', _fr_municipality_event, 'date_max')
            , ''', '''
            , COALESCE(TO_CHAR(_date_max, 'DD/MM/YYYY'), 'NULL')
            , ''', TRUE)'
        );
    ELSE
        _date_min := NOW();
        _date_max := '1970-01-01'::DATE;
    END IF;

    IF _min THEN RETURN _date_min; ELSE RETURN _date_max; END IF;
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
    _date_geography_from IN DATE DEFAULT NULL   -- date from which apply updates
    , _date_geography_to IN DATE DEFAULT NOW()   -- date up to which apply updates
)
RETURNS DATE AS
$func$
BEGIN
    RETURN GREATEST(
        _date_geography_from
        , LEAST(
            _date_geography_to
            , GREATEST(
                fr.get_date_insee_municipality_event()
                , fr.get_date_wikipedia_municipality_event()
            )
        )
    );
END
$func$ LANGUAGE plpgsql;

/*
 * apply event(s) into [FROM..to] date interval on a given municipality
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_municipality_to_date');
CREATE OR REPLACE FUNCTION fr.get_municipality_to_date(
    _code IN VARCHAR
    , _date_geography_from IN DATE
    , _name IN VARCHAR DEFAULT NULL
    , _distribution IN NUMERIC DEFAULT 1
    , _information IN TEXT DEFAULT NULL
    , _check_exists IN BOOLEAN DEFAULT TRUE
    , _months_back_if_not_exists IN INTEGER DEFAULT 12
    , _date_geography_to IN DATE DEFAULT NOW()  -- date up to which apply updates
                                                -- NOW() till today, NULL most as possible
    , _is_new IN BOOLEAN DEFAULT FALSE
    , _with_deleted IN BOOLEAN DEFAULT FALSE
    , _date_geography_from_first IN DATE DEFAULT NULL
    , _code_previous IN VARCHAR DEFAULT NULL
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
    _territory_to_date_t public.territory_to_date_t%ROWTYPE;
    _exists BOOLEAN;
BEGIN
    /*
    RAISE NOTICE 'get_municipality_to_date(%, %, %, %, %, %, %, %, %, %, %)'
        , _code
        , _date_geography_from
        , _name
        , _distribution
        , _information
        , _check_exists
        , _months_back_if_not_exists
        , _date_geography_to
        , _is_new
        , _with_deleted
        , _date_geography_from_first;
     */
    IF _date_geography_from_first IS NULL THEN _date_geography_from_first := _date_geography_from; END IF;
    IF _code = '97123'
    AND _date_geography_from < TO_DATE('15/07/2007', 'DD/MM/YYYY')
    AND (_date_geography_to IS NULL OR _date_geography_to >= TO_DATE('15/07/2007', 'DD/MM/YYYY')) THEN
        RETURN NEXT ROW(
            '97701'::VARCHAR
            , 'Saint-Barthélemy'::VARCHAR
            , TO_DATE('15/07/2007', 'DD/MM/YYYY')
            , 1::NUMERIC
            , 'Les anciennes communes de Saint-Barthélemy (ancien code INSEE 97123) et Saint-Martin (ancien code INSEE 97127) ne font plus partie du département et la région d''outre-mer de Guadeloupe mais forment des collectivités d''outre-mer séparées depuis le 15 juillet 2007'::TEXT
            , TRUE
            , _code
        );
        RETURN;
    ELSIF _code = '97127'
    AND _date_geography_from < TO_DATE('15/07/2007', 'DD/MM/YYYY')
    AND (_date_geography_to IS NULL OR _date_geography_to >= TO_DATE('15/07/2007', 'DD/MM/YYYY')) THEN
        RETURN NEXT ROW(
            '97801'::VARCHAR
            , 'Saint-Martin'::VARCHAR
            , TO_DATE('15/07/2007', 'DD/MM/YYYY')
            , 1::NUMERIC
            , 'Les anciennes communes de Saint-Barthélemy (ancien code INSEE 97123) et Saint-Martin (ancien code INSEE 97127) ne font plus partie du département et la région d''outre-mer de Guadeloupe mais forment des collectivités d''outre-mer séparées depuis le 15 juillet 2007'::TEXT
            , TRUE
            , _code
        );
        RETURN;
    ELSIF _code IN ('75056', '13055', '69123') THEN
        RAISE NOTICE 'Erreur : la commune % est une commune globale composée d''arrondissements', _code;
        IF _with_deleted THEN
            _is_new := TRUE;
            _distribution := 0;
            RETURN NEXT ROW (
                _code
                , _name
                , _date_geography_from
                , _distribution
                , _information
                , _is_new
                , _code_previous
            );
        END IF;
        RETURN;
    ELSIF _code = '99999' THEN
        RAISE NOTICE 'Erreur : la commune 99999 n''existe pas';
        IF _with_deleted THEN
            _is_new := TRUE;
            _distribution := 0;
            RETURN NEXT ROW (
                _code
                , _name
                , _date_geography_from
                , _distribution
                , _information
                , _is_new
                , _code_previous
            );
        END IF;
        RETURN;
    END IF;

    --FIXME : utiliser d'abord le référentiel le plus reculé pour une avancée dans le temps
    --FIXME : utiliser d'abord le référentiel le plus avancé pour un retour dans le passé

    -- available w/ INSEE
    IF fr.get_date_insee_municipality_event() IS NOT NULL
        AND (
            (
                (_date_geography_to IS NULL OR _date_geography_from < _date_geography_to)
                AND _date_geography_from < LEAST(
                    _date_geography_to
                    , fr.get_date_insee_municipality_event()
                )
                AND (
                    --Pas de MAJ possible par WIKIPEDIA
                    fr.get_date_wikipedia_municipality_event() IS NULL
                    --INSEE est plus reculé que WIKIPEDIA
                    OR fr.get_date_insee_municipality_event(in_min => TRUE) < fr.get_date_wikipedia_municipality_event(in_min => TRUE)
                    --OU BIEN on a épuisé la période de MAJ possible par WIKIPEDIA
                    OR _date_geography_from >= fr.get_date_wikipedia_municipality_event()
                )
            )
            OR (
                --retour vers le passé
                (_date_geography_from > _date_geography_to)
                AND _date_geography_from > GREATEST(
                    _date_geography_to
                    , fr.get_date_insee_municipality_event(_min => TRUE)
                )
                AND (
                    --Pas de MAJ possible par WIKIPEDIA
                    fr.get_date_wikipedia_municipality_event() IS NULL
                    --INSEE est plus avancé que WIKIPEDIA
                    OR fr.get_date_insee_municipality_event() > fr.get_date_wikipedia_municipality_event()
                    --OU BIEN on a épuisé la période de MAJ possible par WIKIPEDIA
                    OR _date_geography_from <= fr.get_date_wikipedia_municipality_event(_min => TRUE)
                )
            )
        )
    THEN
        FOR _territory_to_date_t IN (
            SELECT *
            FROM fr.getCommuneToNowFromEvenementCommuneInsee(
                _code => _code
                , _name => _name
                , _date_geography_from => _date_geography_from
                , _distribution => _distribution
                , _information => _information
                , _date_geography_to => _date_geography_to
                , _is_new => _is_new
                , _with_deleted => _with_deleted
                , _code_previous => _code_previous
            )
        )
        LOOP
            RETURN QUERY
                SELECT * FROM fr.get_municipality_to_date(
                    _code => _territory_to_date_t.code
                    , _name => _territory_to_date_t.name
                    , _date_geography_from => _territory_to_date_t.date_geography
                    , _distribution => _territory_to_date_t.distribution
                    , _information => _territory_to_date_t.information
                    , _check_exists => _check_exists
                    , _months_back_if_not_exists => _months_back_if_not_exists
                    , _date_geography_to => _date_geography_to
                    , _is_new => _territory_to_date_t.is_new
                    , _with_deleted => _with_deleted
                    , _date_geography_from_first => _date_geography_from_first
                    , _code_previous => _territory_to_date_t.code_previous
                );
        END LOOP;
        RETURN;

        -- available w/ WIKIPEDIA
    ELSIF fr.get_date_wikipedia_municipality_event() IS NOT NULL
        AND (
            (
                (_date_geography_to IS NULL OR _date_geography_from < _date_geography_to)
                AND _date_geography_from < LEAST(
                    _date_geography_to
                    , fr.get_date_wikipedia_municipality_event()
                )
                AND (
                    --Pas de MAJ possible par INSEE
                    fr.get_date_insee_municipality_event() IS NULL
                    --WIKIPEDIA est plus reculé que INSEE
                    OR fr.get_date_wikipedia_municipality_event(_min => TRUE) < fr.get_date_insee_municipality_event(_min => TRUE)
                    --OU BIEN on a épuisé la période de MAJ possible par INSEE
                    OR _date_geography_from >= fr.get_date_insee_municipality_event()
                )
            )
            OR (
                --retour vers le passé
                (_date_geography_from > _date_geography_to)
                AND _date_geography_from > GREATEST(
                    _date_geography_to
                    , fr.get_date_wikipedia_municipality_event(_min => TRUE))
                AND (
                    --Pas de MAJ possible par INSEE
                    fr.get_date_insee_municipality_event() IS NULL
                    --WIKIPEDIA est plus avancé que INSEE
                    OR fr.get_date_wikipedia_municipality_event() > fr.get_date_insee_municipality_event()
                    --OU BIEN on a épuisé la période de MAJ possible par INSEE
                    OR _date_geography_from <= fr.get_date_insee_municipality_event(_min => TRUE)
                )
            )
        )
    THEN
        FOR _territory_to_date_t IN (
            SELECT * FROM fr.getCommuneToNowFromCommuneNouvelleWikipedia(
                _code => _code
                , _name => _name
                , _date_geography_from => _date_geography_from
                , _distribution => _distribution
                , _information => _information
                , _date_geography_to => _date_geography_to
                , _is_new => _is_new
                , _code_previous => _code_previous
            )
        )
        LOOP
            RETURN QUERY
                SELECT * FROM fr.get_municipality_to_date(
                    _code => _territory_to_date_t.code
                    , _name => _territory_to_date_t.name
                    , _date_geography_from => _territory_to_date_t.date_geography
                    , _distribution => _territory_to_date_t.distribution
                    , _information => _territory_to_date_t.information
                    , _check_exists => _check_exists
                    , _months_back_if_not_exists => _months_back_if_not_exists
                    , _date_geography_to => _date_geography_to
                    , _is_new => _territory_to_date_t.is_new
                    , _with_deleted => _with_deleted
                    , _date_geography_from_first => _date_geography_from_first
                    , _code_previous => _territory_to_date_t.code_previous
                );
        END LOOP;
        RETURN;
    ELSE
        IF _check_exists AND _information IS NULL THEN
            BEGIN
                SELECT TRUE
                INTO STRICT _exists
                FROM public.territory
                WHERE country = 'FR' AND level = 'COM' AND code = _code;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                IF _months_back_if_not_exists > 0 THEN
                    --ATTENTION CETTE METHODE N'EST PAS PARFAITE, ON NE GERE PAS LE CAS D'UNE DIVISION OU UNE PARTIE DES NOUVELLES COMMUNES OBTENUES N'EXISTERAIT PAS (cas ne se produisant à priori jamais)
                    --Alternative à enlever les mois un par un : on les enlève tous en une fois
                    --EXECUTE CONCAT('SELECT $1 - INTERVAL ''', _months_back_if_not_exists, ' months''') INTO _date_geography_from USING _date_geography_from_first;
                    _date_geography_from := (_date_geography_from_first - INTERVAL '1 month')::DATE;
                    RAISE NOTICE 'Avertissement : la commune % n''existe pas dans public.territory : recherche d''un évènement un mois avant la date de référence initiale (soit à partir du %)', _code, _date_geography_from;
                    RETURN QUERY
                        SELECT * FROM fr.get_municipality_to_date(
                            _code => _code
                            , _name => _name
                            , _date_geography_from => _date_geography_from
                            , _distribution => _distribution
                            , _information => _information
                            , _check_exists => _check_exists
                            , _months_back_if_not_exists => _months_back_if_not_exists - 1
                            , _date_geography_to => _date_geography_to
                            , _is_new => _is_new
                            , _with_deleted => _with_deleted
                            , _date_geography_from_first => _date_geography_from
                            , _code_previous => _code_previous
                        );
                    RETURN;
                ELSE
                    RAISE NOTICE 'Erreur : la commune % n''existe pas dans public.territory', _code;
                    IF _with_deleted THEN
                        _is_new := TRUE;
                        _distribution := 0;
                    ELSE
                        RETURN;
                    END IF;
                END IF;
            END;
        END IF;
        RETURN NEXT ROW (
            _code
            , _name
            , _date_geography_from
            , _distribution
            , _information
            , _is_new
            , _code_previous
        );
    END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT * FROM get_municipality_to_date('05043', TO_DATE('01/01/1900', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('33110', TO_DATE('01/01/1900', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('33063', TO_DATE('01/01/1900', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('76601', TO_DATE('01/01/1900', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('76676', TO_DATE('01/01/1900', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('76676', TO_DATE('01/01/2006', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('02344', TO_DATE('01/01/1800', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('49382', TO_DATE('01/01/1800', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('44060', TO_DATE('01/01/1800', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('97123', TO_DATE('01/01/1800', 'DD/MM/YYYY'))
SELECT * FROM get_municipality_to_date('31300', TO_DATE('01/01/1999', 'DD/MM/YYYY'))

-- municipality merge on 01/01/2018
SELECT fr.get_municipality_to_date(
    _code => '16296'
    , _date_geography_from => TO_DATE('2018-01-06', 'YYYY-MM-DD')
)
 */

SELECT public.drop_all_functions_if_exists('public', 'getCommuneToNowFromCommuneNouvelleWikipedia');
CREATE OR REPLACE FUNCTION public.getCommuneToNowFromCommuneNouvelleWikipedia(
	in_codgeo IN VARCHAR
	, in_dt_reference IN DATE
	, in_libgeo IN VARCHAR DEFAULT NULL
	, in_repartition IN NUMERIC DEFAULT 1
	, in_information IN TEXT DEFAULT NULL
	, in_to_dtrgeo IN DATE DEFAULT NOW() --Date jusqu'à laquelle on souhaite mettre à jour, laisser indéfini pour mettre à jour au plus récent possible
	, in_is_new IN BOOLEAN DEFAULT FALSE
	, in_codgeo_precedent IN VARCHAR DEFAULT NULL
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
	v_dt_effet DATE;
	v_new_codgeo VARCHAR;
	v_new_libgeo VARCHAR;
	v_new_repartition NUMERIC;
	v_new_codgeo_precedent VARCHAR;

	v_liste_new_codgeo VARCHAR[];
	v_liste_new_libgeo VARCHAR[];
	v_index INTEGER;
	v_back_to_codgeo VARCHAR;
BEGIN
	IF in_to_dtrgeo IS NULL OR in_to_dtrgeo > in_dt_reference THEN
		SELECT dt_effet, ARRAY[cn_code_insee], ARRAY[cn_nom]
		INTO v_dt_effet, v_liste_new_codgeo, v_liste_new_libgeo
		FROM divers.wikipedia_commune_nouvelle
		WHERE dt_effet > in_dt_reference
		AND in_codgeo::CHAR(5) = ANY(ac_codes_insee)
		AND (in_to_dtrgeo IS NULL OR in_to_dtrgeo >= dt_effet)
		ORDER BY dt_effet ASC
		LIMIT 1;
	ELSE
		SELECT dt_effet - 1, ac_codes_insee, ac_noms
		INTO v_dt_effet, v_liste_new_codgeo, v_liste_new_libgeo
		FROM divers.wikipedia_commune_nouvelle
		WHERE (dt_effet - 1) < in_dt_reference
		AND in_codgeo::CHAR(5) = cn_code_insee
		AND in_to_dtrgeo < (dt_effet - 1)
		ORDER BY dt_effet DESC
		LIMIT 1;
	END IF;

	IF v_dt_effet IS NULL THEN
		--Pas d'évènement trouvé
		RETURN NEXT ROW (
			in_codgeo
			, in_libgeo
			, CASE
				WHEN in_to_dtrgeo IS NULL OR in_to_dtrgeo > in_dt_reference THEN
					GREATEST(in_dt_reference, LEAST(in_to_dtrgeo, public.get_date_wikipedia_municipality_event()))
				ELSE
					LEAST(in_dt_reference, GREATEST(in_to_dtrgeo, public.get_date_wikipedia_municipality_event(in_min => TRUE)))
			END
			--, in_dt_reference
			, in_repartition
			, in_information
			, in_is_new
			, in_codgeo_precedent
		);
	ELSE
		v_index := 0;
		v_new_repartition := (in_repartition / ARRAY_LENGTH(v_liste_new_codgeo, 1));
		v_new_codgeo_precedent := in_codgeo_precedent;
		IF in_to_dtrgeo IS NULL OR in_to_dtrgeo > in_dt_reference THEN
			v_new_codgeo_precedent := COALESCE(in_codgeo_precedent, in_codgeo);
		ELSE
			--Si codgeo précédent renseigné, et retour dans le passé sur une fusion / création commune nouvelle
			--Alors on revient sur la commune avant sa fusion
			IF in_codgeo_precedent IS NOT NULL
			AND in_to_dtrgeo < in_dt_reference
			AND in_codgeo_precedent = ANY(v_liste_new_codgeo)
			THEN
				v_back_to_codgeo := in_codgeo_precedent;
				v_new_codgeo_precedent := NULL;
				v_new_repartition := in_repartition;
			END IF;
		END IF;

		FOREACH v_new_codgeo IN ARRAY v_liste_new_codgeo LOOP
			v_index := v_index + 1;
			v_new_libgeo := v_liste_new_libgeo[v_index];

			IF v_back_to_codgeo IS NOT NULL AND v_back_to_codgeo != v_new_codgeo THEN
				CONTINUE;
			END IF;

			RETURN QUERY
				SELECT *
				FROM public.getCommuneToNowFromCommuneNouvelleWikipedia(
					in_codgeo => v_new_codgeo
					, in_libgeo => v_new_libgeo
					, in_dt_reference => v_dt_effet
					, in_repartition => v_new_repartition
					, in_information => CONCAT_WS(' -> ', COALESCE(in_information, CONCAT(CONCAT_WS(' ', CONCAT_WS('/', in_codgeo, in_codgeo_precedent), in_libgeo), ' le ', in_dt_reference)), CONCAT(CONCAT_WS('/', v_new_codgeo, v_new_codgeo_precedent), ' ', v_new_libgeo, ' le ', v_dt_effet))
					, in_to_dtrgeo => in_to_dtrgeo
					, in_is_new => TRUE
					--En cas de succession de changement de code géo, on mémorise le premier seulement
					, in_codgeo_precedent => v_new_codgeo_precedent
				);
		END LOOP;
	END IF;
END
$func$ LANGUAGE plpgsql;

/* TEST

--Cas de division
SELECT * FROM getCommuneToNowFromCommuneNouvelleWikipedia('76676', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> KO, retourné tel quel, cas non géré, on devrait avoir :
	76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76601 Saint-Lucien le 2017-01-01 (mod=21)
	76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76676 Sigy-en-Bray le 2017-01-01 (mod=21)

--Cas de suppression
SELECT * FROM getCommuneToNowFromCommuneNouvelleWikipedia('51440', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> KO, retourné tel quel, cas non géré, on ne devrait rien avoir en retour

--Cas simple de fusion
SELECT * FROM getCommuneToNowFromCommuneNouvelleWikipedia('01341', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
-->OK, on a bien :
	01341 le 1900-12-01 -> 01227 Magnieu le 2019-01-01

--Cas plus complexe de fusion
SELECT * FROM getCommuneToNowFromCommuneNouvelleWikipedia('49144', TO_DATE('2014', 'YYYY'))
-->OK, on a bien :
	49144 le 2014-01-01 -> 44180 Vallons-de-l'Erdre le 2018-01-01

WITH millesime_20xx_to_now AS (
	SELECT commune_to_now.codgeo, commune_to_now.dt_reference, ROUND(commune_to_now.repartition, 1), commune_to_now.information
	FROM insee.decoupage_communal_com_arm AS dcca
	INNER JOIN getCommuneToNowFromCommuneNouvelleWikipedia(dcca.codgeo, TO_DATE(dcca.millesime::VARCHAR, 'YYYY')) AS commune_to_now ON TRUE
	WHERE dcca.millesime = '2015'
)
, millesime_last_to_now AS (
	SELECT commune_to_now.codgeo, commune_to_now.dt_reference, ROUND(commune_to_now.repartition, 1), commune_to_now.information
	FROM insee.decoupage_communal_com_arm AS dcca
	INNER JOIN getCommuneToNowFromCommuneNouvelleWikipedia(dcca.codgeo, TO_DATE(dcca.millesime::VARCHAR, 'YYYY')) AS commune_to_now ON TRUE
	WHERE dcca.millesime = (SELECT MAX(millesime) FROM insee.decoupage_communal_com_arm)
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

SELECT public.drop_all_functions_if_exists('public', 'getCommuneToNowFromEvenementInsee');
SELECT public.drop_all_functions_if_exists('public', 'getCommuneToNowFromEvenementCommuneInsee');
CREATE OR REPLACE FUNCTION public.getCommuneToNowFromEvenementCommuneInsee(
	in_codgeo IN VARCHAR
	, in_dt_reference IN DATE
	, in_libgeo IN VARCHAR DEFAULT NULL
	, in_repartition IN NUMERIC DEFAULT 1
	, in_information IN TEXT DEFAULT NULL
	, in_last_mods IN INTEGER[] DEFAULT NULL --A préciser si tous les evenements n'ont pas été traités à date de référence
	, in_to_dtrgeo IN DATE DEFAULT NOW() --Date jusqu'à laquelle on souhaite mettre à jour, laisser indéfini pour mettre à jour au plus récent possible
	, in_is_new IN BOOLEAN DEFAULT FALSE
	, in_return_deleted IN BOOLEAN DEFAULT FALSE
	, in_codgeo_precedent IN VARCHAR DEFAULT NULL
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
	v_dt_effet DATE;
	v_new_codgeo VARCHAR;
	v_new_libgeo VARCHAR;
	v_new_repartition NUMERIC;
	v_new_codgeo_precedent VARCHAR;

	v_mod INTEGER;
	v_liste_new_codgeo VARCHAR[];
	v_liste_new_libgeo VARCHAR[];
	v_index INTEGER;

	v_evenement_exists BOOLEAN;

	v_back_to_codgeo VARCHAR;
BEGIN
	/* ESSAI D'OPTIMISATION NON CONCLUANT
	CREATE TEMPORARY TABLE IF NOT EXISTS tmp_com_has_evenement_insee AS (
		SELECT
			com_av AS codgeo
			, MIN(date_eff) AS dt_reference_min
			, MAX(date_eff) AS dt_reference_max
		FROM insee.evenement_commune
		WHERE typecom_av = 'COM'
		AND typecom_ap = 'COM'
		GROUP BY com_av
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_tmp_com_has_evenement_insee_pk ON tmp_com_has_evenement_insee(codgeo);
	SELECT TRUE
	INTO v_evenement_exists
	FROM tmp_com_has_evenement_insee
	WHERE codgeo = in_codgeo
	AND in_dt_reference BETWEEN dt_reference_min AND dt_reference_max;
	v_evenement_exists := COALESCE(v_evenement_exists, FALSE);
	*/
	/*
	Changement de nom	10
	Création	20
	Rétablissement	21
	Suppression	30
	Fusion simple	31
	Création de commune nouvelle	32
	Fusion association	33
	Transformation de fusion association en fusion simple	34
	Suppression de commune déléguée	35 -->TODO : à traiter ?
		--> quel intérêt si le libellé et le code ne changent pas ?
		SELECT ROUND(SUM(CASE WHEN (libelle_av != libelle_ap OR com_av != com_ap) THEN 1 ELSE 0 END)::NUMERIC * 100 / COUNT(*), 2), COUNT(*) AS pct_avec_changement
			FROM insee.evenement_commune
			where mod = 34 and typecom_av = 'COM' and typecom_ap = 'COM'
		SELECT *
			FROM insee.evenement_commune
			where mod = 34 and typecom_av = 'COM' and typecom_ap = 'COM'
			AND (libelle_av != libelle_ap OR com_av != com_ap)
	Changement de code dû à un changement de département	41
	Changement de code dû à un transfert de chef-lieu	50
	Transformation de commune associé en commune déléguée	70
		SELECT ROUND(SUM(CASE WHEN (libelle_av != libelle_ap OR com_av != com_ap) THEN 1 ELSE 0 END)::NUMERIC * 100 / COUNT(*), 2) AS pct_avec_changement
		FROM insee.evenement_commune
		where mod = 70 and typecom_av = 'COM' and typecom_ap = 'COM'
	*/

	--IF v_evenement_exists THEN
	IF in_to_dtrgeo IS NULL OR in_to_dtrgeo > in_dt_reference THEN
		SELECT date_eff, mod, ARRAY_AGG(com_ap), ARRAY_AGG(libelle_ap)
		INTO v_dt_effet, v_mod, v_liste_new_codgeo, v_liste_new_libgeo
		FROM insee.evenement_commune
		WHERE (
			--n'importe quel evènement futur
			date_eff > in_dt_reference
			--ou bien un évement du jour, qui n'a pas déjà été traité, alors que d'autre l'on été (= ce n'est pas le jour initial de départ)
			OR (in_last_mods IS NOT NULL AND ARRAY_LENGTH(in_last_mods, 1) > 0 AND date_eff = in_dt_reference AND NOT(mod = ANY(in_last_mods)))
		)
		AND com_av = in_codgeo
		AND typecom_av = 'COM'
		AND typecom_ap = 'COM'
		AND (in_to_dtrgeo IS NULL OR in_to_dtrgeo >= date_eff)
		--/* géré dans boucle
		--Si codgeo précédent renseigné, et rétablissement (suite fusion / création commune nouvelle)
		--Alors on revient sur la commune avant sa fusion
		AND (
			in_codgeo_precedent IS NULL OR mod != 21
			OR in_codgeo_precedent = com_ap
		)
		--*/
		--Ce type de modification est intéressante que si elle change le code ou bien le libellé
		AND (mod != 34 OR (libelle_av != libelle_ap OR com_av != com_ap))
		GROUP BY date_eff, mod
		ORDER BY date_eff ASC, mod ASC
		LIMIT 1;
	ELSE
		--Note : pour le retour vers le passé, on considère tous les évènements ayant effet à J-1
		SELECT date_eff - 1, mod, ARRAY_AGG(com_av), ARRAY_AGG(libelle_av)
		INTO v_dt_effet, v_mod, v_liste_new_codgeo, v_liste_new_libgeo
		FROM insee.evenement_commune
		WHERE (
			--n'importe quel evènement passé
			(date_eff - 1) < in_dt_reference
			--ou bien un évement du jour, qui n'a pas déjà été traité, alors que d'autres l'on été (= ce n'est pas le jour initial de départ)
			OR (in_last_mods IS NOT NULL AND ARRAY_LENGTH(in_last_mods, 1) > 0 AND (date_eff - 1) = in_dt_reference AND NOT(mod = ANY(in_last_mods)))
		)
		AND com_ap = in_codgeo
		AND typecom_av = 'COM'
		AND typecom_ap = 'COM'
		AND in_to_dtrgeo <= (date_eff - 1)
		--/* géré dans boucle
		--Si codgeo précédent renseigné, et retour dans le passé sur une fusion / création commune nouvelle
		--Alors on revient sur la commune avant sa fusion
		AND (
			in_codgeo_precedent IS NULL OR mod NOT IN (31, 32, 33)
			OR in_codgeo_precedent = com_av
			OR EXISTS (
				SELECT *
				FROM insee.evenement_commune AS evenement_commune_precedent
				WHERE typecom_av = 'COM'
				AND typecom_ap = 'COM'
				AND mod != 21
				--AND evenement_commune_precedent.date_eff < '2020-01-01'::date
				--AND evenement_commune_precedent.com_ap = '89420'
				AND evenement_commune_precedent.date_eff < evenement_commune.date_eff
				AND evenement_commune_precedent.date_eff >= '2010-01-01'::date
				AND evenement_commune_precedent.com_ap = evenement_commune.com_av
				AND evenement_commune_precedent.com_av = in_codgeo_precedent

			)
		)
		--*/
		--Ce type de modification est intéressante que si elle change le code ou bien le libellé
		AND (mod != 34 OR (libelle_av != libelle_ap OR com_av != com_ap))
		GROUP BY (date_eff - 1), mod
		ORDER BY (date_eff - 1) DESC, mod ASC
		LIMIT 1;
	END IF;
	--END IF;
	--create index test_evenement_commune on insee.evenement_commune (com_av) where typecom_av = 'COM' AND typecom_ap = 'COM'
	--drop index public.test_evenement_commune;

	IF v_dt_effet IS NULL THEN
		/*
		--Retour vers le passé, il n'y a plus d'évènements à traiter, et on en a traité au moins un
		IF in_to_dtrgeo < in_dt_reference AND in_last_mods IS NOT NULL AND ARRAY_LENGTH(in_last_mods, 1) > 0 THEN
			in_dt_reference := in_dt_reference - 1;
		END IF;
		*/

		/*
		IF in_to_dtrgeo < in_dt_reference AND in_codgeo_precedent IS NOT NULL THEN
			IF in_codgeo_precedent = in_codgeo THEN
				in_repartition := 1;
			ELSE
				RETURN;
			END IF;
		END IF;
		*/

		RETURN NEXT ROW (
			in_codgeo
			, in_libgeo
			, CASE
				WHEN in_to_dtrgeo IS NULL OR in_to_dtrgeo > in_dt_reference THEN
					GREATEST(in_dt_reference, LEAST(in_to_dtrgeo, public.get_date_insee_municipality_event()))
				ELSE
					LEAST(in_dt_reference, GREATEST(in_to_dtrgeo, public.get_date_insee_municipality_event(in_min => TRUE)))
			END
			--, in_dt_reference
			, in_repartition
			, in_information
			, in_is_new
			, in_codgeo_precedent
		);
	ELSE
		v_index := 0;
		v_new_repartition := (in_repartition / ARRAY_LENGTH(v_liste_new_codgeo, 1));
		v_new_codgeo_precedent := in_codgeo_precedent;
		IF in_to_dtrgeo IS NULL OR in_to_dtrgeo > in_dt_reference THEN
			--Suppression
			IF v_mod = 30 /* TODO : gérer la suppression de commune déléguée ainsi ? : OR v_mod = 35 */ THEN
				IF in_return_deleted = FALSE THEN
					RETURN;
				END IF;
				v_new_repartition := 0;
			ELSE
				IF v_mod = 21 THEN
					v_new_codgeo_precedent := NULL;
				/* géré dans requête pour ne pas prendre un rétablissement sur un portion de la commune nouvelle non concerné (rétablissement partiel, ex : 14712)
				--Si codgeo précédent renseigné, et rétablissement (suite fusion / création commune nouvelle / changement de code ?)
				--Alors on revient sur la commune précédente
				IF
					in_codgeo_precedent IS NOT NULL
					AND v_mod = 21
					AND in_codgeo_precedent = ANY(v_liste_new_codgeo)
				THEN
					v_back_to_codgeo := in_codgeo_precedent;
					v_new_codgeo_precedent := NULL;
					v_new_repartition := in_repartition;
				*/
				ELSIF
					in_codgeo_precedent IS NULL
					--fusion / commune nouvelle, ou changement de code
					AND (v_mod IN (31, 32, 33) OR (ARRAY_LENGTH(v_liste_new_codgeo, 1) = 1 AND in_codgeo != v_liste_new_codgeo[1]))
				THEN
					v_new_codgeo_precedent := in_codgeo;
				END IF;
			END IF;
		--retour vers le passé
		ELSE
			--Annulation création
			IF v_mod = 20 THEN
				IF in_return_deleted = FALSE THEN
					RETURN;
				END IF;
				v_new_repartition := 0;
			--Annulation de suppression : quelque chose de spécial à faire ?
			--ELSIF v_mod = 30 THEN
			ELSE
				IF v_mod IN (31, 32, 33) THEN
					--FIXME : Pose un problème en cas d'annulation de fusion successives, exemple : 24362
					--v_new_codgeo_precedent := NULL;
				/* géré dans requête pour ne pas prendre un rétablissement sur un portion de la commune nouvelle non concerné (rétablissement partiel, ex : 14712)
				IF in_codgeo_precedent IS NOT NULL
				--annulation de fusion / commune nouvelle, ou de changement de code
				AND (v_mod IN (31, 32, 33) OR (ARRAY_LENGTH(v_liste_new_codgeo, 1) = 1 AND in_codgeo != v_liste_new_codgeo[1]))
				AND in_codgeo_precedent = ANY(v_liste_new_codgeo)
				THEN
					v_back_to_codgeo := in_codgeo_precedent;
					v_new_codgeo_precedent := NULL;
					v_new_repartition := in_repartition;
				*/
				ELSIF
					in_codgeo_precedent IS NULL
					--annulation de rétablissement = retour sur la commune nouvelle
					AND v_mod = 21
				THEN
					v_new_codgeo_precedent := in_codgeo;
				END IF;
			END IF;
		END IF;
		FOREACH v_new_codgeo IN ARRAY v_liste_new_codgeo LOOP
			v_index := v_index + 1;
			v_new_libgeo := v_liste_new_libgeo[v_index];
			--Si on change de date
			IF in_last_mods IS NOT NULL AND in_dt_reference != v_dt_effet
			--Ou de code géographique
			--à voir OR in_codgeo != v_new_codgeo
			THEN
				--On réinitialise la liste de évènements déjà traités
				in_last_mods := NULL::INTEGER[];
			END IF;

			IF v_back_to_codgeo IS NOT NULL AND v_back_to_codgeo != v_new_codgeo THEN
				CONTINUE;
			END IF;

			RETURN QUERY
				SELECT *
				FROM public.getCommuneToNowFromEvenementCommuneInsee(
					in_codgeo => v_new_codgeo
					, in_libgeo => v_new_libgeo
					, in_dt_reference => v_dt_effet
					, in_repartition => v_new_repartition
					, in_information => CONCAT_WS(' -> ', COALESCE(in_information, CONCAT(CONCAT_WS(' ', CONCAT_WS('/', in_codgeo, in_codgeo_precedent), in_libgeo), ' le ', in_dt_reference)), CONCAT(CONCAT_WS('/', v_new_codgeo, v_new_codgeo_precedent), ' ', v_new_libgeo, ' le ', v_dt_effet, ' (mod=', v_mod, ')'))
					, in_last_mods => array_append(in_last_mods, v_mod)
					, in_to_dtrgeo => in_to_dtrgeo
					, in_is_new => TRUE
					, in_return_deleted => in_return_deleted
					--En cas de succession de changement de code géo, on mémorise le premier seulement
					, in_codgeo_precedent => v_new_codgeo_precedent
				);
		END LOOP;
	END IF;
END
$func$ LANGUAGE plpgsql;

/* TESTS

--Cas de division
SELECT * FROM getCommuneToNowFromEvenementCommuneInsee('76676', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> OK, on a bien :
	76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76601 Saint-Lucien le 2017-01-01 (mod=21)
	76676 le 1900-12-01 -> 76676 Sigy-en-Bray le 1962-04-09 (mod=10) -> 76676 Sigy-en-Bray le 1973-06-01 (mod=33) -> 76676 Sigy-en-Bray le 2017-01-01 (mod=21)

--Cas de suppression
SELECT * FROM getCommuneToNowFromEvenementCommuneInsee('51440', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
--> OK, on a rien
SELECT * FROM getCommuneToNowFromEvenementCommuneInsee(in_codgeo=>'51440', in_dt_reference=>TO_DATE('01/12/1900', 'DD/MM/YYYY'), in_return_deleted=>true)
--> OK, retourné avec répartition à 0

--Cas simple de fusion
SELECT * FROM getCommuneToNowFromEvenementCommuneInsee('01341', TO_DATE('01/12/1900', 'DD/MM/YYYY'))
-->OK, on a bien :
	01341 le 1900-12-01 -> 01227 Magnieu le 2019-01-01 (mod=32)

--Cas plus complexe de fusion
SELECT * FROM getCommuneToNowFromEvenementCommuneInsee('49144', TO_DATE('2014', 'YYYY'))
-->OK, on a bien :
	49144 le 2014-01-01 -> 44225 Freigné le 2018-01-01 (mod=41) -> 44180 Vallons-de-l'Erdre le 2018-01-01 (mod=32)

SELECT *
FROM insee.evenement_commune
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
AND date_eff = '2019-01-01'::DATE AND com_ap = '01227'

--Doublons ?
SELECT date_eff, com_av, com_ap, count(*)
FROM insee.evenement_commune
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
GROUP BY date_eff, com_av, com_ap
HAVING COUNT(*) > 1

WITH millesime_a_to_now AS (
	--SELECT commune_to_now.codgeo, commune_to_now.dt_reference, ROUND(commune_to_now.repartition, 1)
	--FROM insee.decoupage_communal_com_arm AS dcca
	--INNER JOIN getCommuneToNowFromEvenementCommuneInsee(dcca.codgeo, TO_DATE(dcca.millesime::VARCHAR, 'YYYY')) AS commune_to_now ON TRUE
	--WHERE dcca.millesime = (SELECT MAX(millesime) FROM insee.decoupage_communal_com_arm)
	SELECT codgeo, TO_DATE(millesime::VARCHAR, 'YYYY') AS dt_reference, NULL::TEXT AS information, 1::DECIMAL AS repartition
	FROM insee.decoupage_communal_com_arm AS dcca
	WHERE dcca.millesime = '2019'
)
, millesime_b_to_now AS (
	SELECT commune_to_now.codgeo, commune_to_now.dt_reference, commune_to_now.information, ROUND(commune_to_now.repartition, 1)
	FROM insee.decoupage_communal_com_arm AS dcca
	INNER JOIN getCommuneToNowFromEvenementCommuneInsee(
		in_codgeo => dcca.codgeo
		, in_dt_reference => TO_DATE(dcca.millesime::VARCHAR, 'YYYY')
		, in_to_dtrgeo => TO_DATE('01/01/2019', 'DD/MM/YYYY')
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
	SELECT * FROM insee.decoupage_communal_com_arm WHERE millesime = '2014' AND codgeo = '55298'
	SELECT * FROM insee.evenement_commune WHERE date_eff = '2014-01-01' AND com_av = '55298' AND typecom_av = 'COM' AND typecom_ap = 'COM'
	https://fr.wikipedia.org/wiki/Loisey-Culey : Au 1er janvier 2014, les communes devaient retrouver leur indépendance, mais la procédure est reportée au 1er janvier 2015, ne pouvant avoir lieu dans l'année précédant une échéance électorale. Cependant, lors des élections municipales de 2014, un maire est élu dans chaque commune, et finalement, par décision du tribunal le 1er juillet 2014, les deux communes sont indépendantes.
	--> on retarde l'evenement au 1er juillet 2014
--> 2015 : problème avec Oudon (14697) qui aurait dû changer de code le 07/01/2014 (14472) et à nouveau en 2017 (14654), mais toujours présent dans le referentiel insee du 1er janvier 2015
	SELECT * FROM getCommuneToNowFromEvenementInsee('14697', TO_DATE('06/01/2014', 'DD/MM/YYYY'), 1, TRUE)
	SELECT * FROM insee.decoupage_communal_com_arm WHERE millesime = '2015' AND codgeo = '14697'
	SELECT * FROM insee.evenement_commune WHERE date_eff = '2014-01-07' AND typecom_av = 'COM' AND typecom_ap = 'COM' AND com_av = '14697'
	https://fr.wikipedia.org/wiki/L%27Oudon : Un nouvel arrêté préfectoral, le 7 janvier 2014, fait de la commune de Notre-Dame-de-Fresnay le nouveau chef-lieu. Afin de prendre en compte ce transfert de chef lieu, lors de la publication du COG 2016 l'INSEE décide de modifier le code commune de L'Oudon pour reprendre l'ancien code de Notre-Dame-de-Fresnay (14472).
	--> on retarde l'evenement au 1er janvier 2016


WITH test as (
SELECT CONCAT(com_av, '-', com_ap) as code, date_eff
FROM insee.evenement_commune
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
and com_ap != com_av
and mod != 21
UNION ALL
SELECT CONCAT(com_ap, '-', com_av), date_eff
FROM insee.evenement_commune
WHERE typecom_av = 'COM' AND typecom_ap = 'COM'
and com_ap != com_av
and mod = 21
)
SELECT code, array_agg(date_eff order by date_eff)
FROM test
group by code
having count(*) > 1
 */

SELECT public.drop_all_functions_if_exists('public', 'getDateMajRan');
CREATE OR REPLACE FUNCTION public.getDateMajRan()
RETURNS DATE AS
$func$
DECLARE
	v_date_maj DATE;
BEGIN
	SELECT MAX(dt_fin_donnees)::DATE
	INTO STRICT v_date_maj
	FROM public.historique_import
	WHERE co_etat = 'SUCCES' AND co_type = 'RAN_ADRESSE';
	RETURN v_date_maj;
END
$func$ LANGUAGE plpgsql;

SELECT public.drop_all_functions_if_exists('public', 'getCommuneToNowFromRan');
CREATE OR REPLACE FUNCTION public.getCommuneToNowFromRan(in_codgeo IN VARCHAR, in_dt_reference IN DATE, in_repartition IN NUMERIC DEFAULT 1, in_raise_notice IN BOOLEAN DEFAULT FALSE)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
	_territory_to_date_t territory_to_date_t%ROWTYPE;
	v_date_maj_ran DATE := public.getDateMajRan(); --RAN est à jour jusqu'à cette date
	v_commune_ran RECORD;
	v_commune_now VARCHAR;
	v_return BOOLEAN := TRUE;
BEGIN
	FOR v_commune_ran IN
	(
		SELECT 	ARRAY_AGG(DISTINCT co_insee_commune) AS communes_now
			, 1::NUMERIC/COUNT(DISTINCT co_insee_commune) AS repartition
		FROM public.za_ran_ad_view
		WHERE co_insee_commune_precedente = in_codgeo
		--WHERE co_insee_commune_precedente = '05088'
		--WHERE co_insee_commune_precedente = '05043'
		GROUP BY co_insee_commune_precedente
		--COALESCE(co_insee_commune_precedente, LEFT(co_adr, 5)) ? --permet de résoudre le pb sur 76676 / 76601 mais risqué car à ne pas refaire après une certaine date
	)
	LOOP
		v_return := FALSE;
		FOREACH v_commune_now IN ARRAY v_commune_ran.communes_now LOOP
			RETURN NEXT ROW (
				v_commune_now
				, v_date_maj_ran
				, v_commune_ran.repartition
			);
		END LOOP;
	END LOOP;

	IF v_return = TRUE THEN
		RETURN NEXT ROW (
			in_codgeo
			, v_date_maj_ran
			, in_repartition
		);
	END IF;
 END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'getZaRanGeoToNow');
CREATE OR REPLACE FUNCTION public.getZaRanGeoToNow(in_za ran.za)
  RETURNS ran.za AS
$func$
DECLARE
	v_commune_to_now RECORD;
BEGIN
	--Cas de réactivation non géré pour le moment : est-ce un cas possible ?
	IF in_za.fl_active = FALSE THEN RETURN in_za; END IF;

	SELECT *
	INTO v_commune_to_now
	FROM public.get_municipality_to_date(
		in_codgeo => in_za.co_insee_commune
		--On force l'algo à considérer en cas de fusion que cette ZA correspond à la portion avant fusion, même pour la commune déléguée chef lieu
		, in_codgeo_precedent => COALESCE(in_za.co_insee_commune_precedente, in_za.co_insee_commune)
		, in_dt_reference => in_za.dt_reference_commune
		, in_return_deleted => TRUE --Cas de suppression/désactivation non géré pour le moment : est-ce un cas possible ?
		, in_check_exists => FALSE --Ce test n'aurait pas de sens, puisque la liste des communes de la table territory est issue de RAN
	) AS commune_to_now
	WHERE commune_to_now.is_new = TRUE --Seulement ce qui est nouveau
	;
	--Même en cas de fusion, on ne stocke pas dans RAN le code INSEE précédent s'il ne change pas (cas de la commune déléguée chef lieu)
	IF v_commune_to_now.codgeo = v_commune_to_now.codgeo_precedent THEN
		v_commune_to_now.codgeo_precedent := NULL;
	END IF;

	IF v_commune_to_now.repartition = 1 THEN
		RAISE NOTICE 'Cas de fusion de commune / création commune nouvelle géré pour maj GEO de RAN ZA : %, % / %, % -> %, %', in_za.co_cea, in_za.co_insee_commune, in_za.co_insee_commune_precedente, in_za.lb_nn, v_commune_to_now.codgeo, v_commune_to_now.libgeo;
		--A déjà fusionné : on garde le code INSEE précédent
		IF in_za.co_insee_commune_precedente IS NULL THEN
			in_za.co_insee_commune_precedente := in_za.co_insee_commune;
		END IF;
		in_za.co_insee_commune := v_commune_to_now.codgeo;
		in_za.co_insee_departement := public.getCodeInseeDepartementFromCodeInseeCommune(in_za.co_insee_commune);
		in_za.dt_reference_commune := v_commune_to_now.dt_reference;

		/* Note : pour une MAJ des libellés
		 * 1) Y a t il vraiment un intérêt ?
		 * 2) Demander les règles de maj :
		 *		lb_nn -> libellé de la commune nouvelle ?
		 *		lb_ach_nn -> libellé de la commune nouvelle ?
		 *		lb_l5_nn -> libellé de la commune déléguée = lb_ach_nn si lb_l5_nn pas déjà renseigné ?
		 * 3) Il faut gérer correctement le diff RAN pour mettre à jour, en sauvegardant les valeurs d'origines RAN dans une colonne dédiée, tel que fait avec co_insee_commune_ran / co_insee_commune_precedente_ran
		 * 4) Il faut ignorer les différence due à la normalisation du libellé, le mieux étant d'appliquer les règles officielles de normalisation (quelles sont elles ?) :
		 * public.removeMotsOutils(REPLACE(public.upperNoSpecialsCharsOnlyAlfaNum(v_commune_to_now.libgeo), 'SAINT', 'ST')) != public.removeMotsOutils(REPLACE(in_za.lb_nn, 'SAINT', 'ST'))
		 */
	ELSIF v_commune_to_now.repartition < 1 AND v_commune_to_now.repartition > 0 THEN
		RAISE NOTICE 'Cas de rétablissement de commune géré pour maj GEO de RAN ZA : %, % / %, % -> %, %', in_za.co_cea, in_za.co_insee_commune, in_za.co_insee_commune_precedente, in_za.lb_nn, v_commune_to_now.codgeo, v_commune_to_now.libgeo;
		in_za.co_insee_commune := in_za.co_insee_commune_precedente;
		in_za.co_insee_departement := public.getCodeInseeDepartementFromCodeInseeCommune(in_za.co_insee_commune);
		in_za.co_insee_commune_precedente := NULL;
		in_za.dt_reference_commune := v_commune_to_now.dt_reference;
		/* Cas rare de division non géré pour le moment, pourrait être :
		in_za.lb_nn := lb_l5_nn;
		in_za.lb_ach_nn := lb_l5_nn;
		in_za.co_insee_commune_precedente := NULL;
		in_za.dt_reference_commune = v_commune_to_now.dt_reference;
		*/
	ELSIF v_commune_to_now.repartition = 0 THEN
		RAISE NOTICE 'Cas de suppression de commune non géré pour maj GEO de RAN ZA : %, % / %, %', in_za.co_cea, in_za.co_insee_commune, in_za.co_insee_commune_precedente, in_za.lb_nn;
		/*
		Cas rare (inexistant ?) de suppression non géré pour le moment, pourrait être
		in_za.fl_active := FALSE;
		in_za.dt_reference_commune := v_commune_to_now.dt_reference;
		*/
	END IF;

	RETURN in_za;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'setRanGeoToNow');
CREATE OR REPLACE FUNCTION public.setRanGeoToNow()
  RETURNS BOOLEAN AS
$func$
DECLARE
	v_za_to_now RECORD;
	v_nb_row_affected INTEGER;
	v_dt_ran DATE;
	v_ran_updated BOOLEAN DEFAULT FALSE;
BEGIN
	SELECT MAX(dt_fin_donnees)::DATE
	INTO v_dt_ran
	FROM public.historique_import
	WHERE co_type = 'RAN_ADRESSE' AND co_etat = 'SUCCES';

	FOR v_za_to_now IN (
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
				THEN v_dt_ran + 1
				ELSE za.dt_reference
			END AS dt_reference
			, CASE
				WHEN za_to_now.co_insee_commune != za.co_insee_commune
					OR za_to_now.co_insee_commune_precedente IS DISTINCT FROM za.co_insee_commune_precedente
				THEN TRUE
				ELSE FALSE
			END AS modification
		FROM ran.za
		CROSS JOIN public.getZaRanGeoToNow(za) AS za_to_now
		WHERE za_to_now.dt_reference_commune != za.dt_reference_commune
	)
	LOOP
		UPDATE ran.za
		SET co_insee_commune = v_za_to_now.co_insee_commune
			, co_insee_commune_precedente = v_za_to_now.co_insee_commune_precedente
			, dt_reference_commune = v_za_to_now.dt_reference_commune
			, dt_reference = v_za_to_now.dt_reference
			, co_insee_departement = v_za_to_now.co_insee_departement
		WHERE za.co_cea = v_za_to_now.co_cea;

		--Si modification effective hormis la date de référence
		IF v_za_to_now.modification = TRUE THEN
			UPDATE ran.adresse
			SET dt_reference_za = v_za_to_now.dt_reference
				, dt_reference = GREATEST(dt_reference, v_za_to_now.dt_reference)
			WHERE co_cea_za = v_za_to_now.co_cea;

			--MAJ du code INSEE commune dénormalisé sur les voies de la ZA
			UPDATE ran.voie
			SET co_insee_commune = v_za_to_now.co_insee_commune
			FROM ran.adresse
			WHERE adresse.co_cea_determinant = voie.co_cea
			AND adresse.co_cea_za = v_za_to_now.co_cea --Voies de la ZA
			AND voie.co_insee_commune != v_za_to_now.co_insee_commune; --Qui ont un code INSEE commune différent (à priori forcément vrai);
		END IF;

		v_ran_updated := TRUE;
	END LOOP;

	RETURN v_ran_updated;
END
$func$ LANGUAGE plpgsql;

/* TEST

INSERT INTO divers.wikipedia_commune_nouvelle (
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

SELECT * FROM divers.wikipedia_commune_nouvelle

SELECT public.get_municipality_to_date(
	in_codgeo => '33281'
	, in_dt_reference => '01/01/2019'::DATE
)

SELECT dt_fin_donnees FROM public.historique_import
	WHERE co_etat = 'SUCCES' AND co_type = 'WIKIPEDIA_COMMUNE_NOUVELLE'

INSERT INTO historique_import (co_type, co_etat, dt_debut_donnees, dt_fin_donnees, nb_enregistrements_a_traiter)
	VALUES ('WIKIPEDIA_COMMUNE_NOUVELLE', 'SUCCES', '01/01/2020'::DATE, '01/01/2020'::DATE, 0);

SELECT * FROM public.setRanGeoToNow()

SELECT * FROM ran.za WHERE co_insee_commune != co_insee_commune_ran

*/

--Mise à jour globale des géographies
SELECT drop_all_functions_if_exists('public', 'setDataGeoToNow');
CREATE OR REPLACE PROCEDURE public.setDataGeoToNow(
	in_max_execution_time IN INTERVAL DEFAULT NULL --Temps d'execution maximum exprimé en interval de temps
)
AS
$func$
DECLARE
	v_max_end_time TIMESTAMP WITHOUT TIME ZONE := clock_timestamp() + in_max_execution_time;
BEGIN
	--déjà fait toutes les semaines lors de l'intégration de RAN, suite import RAN, suite import et intégration GEOPAD / INSEE / IGN / ... (cf /public/adresse_ran.sh -> /public/territory.sh)
	PERFORM public.setTerritoireIgnGeoToNow();
	PERFORM public.setTerritoireInseeGeoToNow();
	--déjà fait toutes les semaines lors de l'import de RAN, (cf /ran/structure/za.sql)
	--donc théoriquement inutile, sauf retard ou MAJ des sources d'évènement avant application dans RAN (evenements commune insee, commune nouvelle wikipedia)
	PERFORM public.setRanGeoToNow();
	--à faire régulièrement ?
	PERFORM public.setTerritoireHasDataGeoToNow(
		in_table => 'territoire_has_insee'
		, in_set_geo_supra => TRUE
		-- pas nécessaire, on fait confiance à l'INSEE ? et pour garder l'indépendance avec la table territory ?
		, in_check_exists => FALSE
	);
	PERFORM public.setTerritoireGeoToNow();
	COMMIT;
	--> si maj effective il faudrait relancer territory.sh, ou faire un geotonow sur territory
	IF clock_timestamp() > v_max_end_time THEN RAISE NOTICE 'Temps de traitement maximum dépassé sur setDataGeoToNow'; RETURN; END IF;


	--DEJA fait toutes les semaines avant ajout histo, suite intégration RAN, GEOPAD, etc ...
	--DANS LA LIMITE DE 10 MAX
	CALL public.setTerritoireAggAdrPdiHistoGeoToNow(
		in_commit => TRUE --Validation des transactions après chaque traitement d'historique, AINSI le traitement peut être stoppé en cours tout en gardant ce qui est validé
		--, in_limit_nb_traites => 40 --Environ 4 heures de traitement MAX = 48 historiques MAX, à raison de 5 minutes par historique ?
		, in_max_execution_time => (v_max_end_time - clock_timestamp()) / 2 --Partage du temps restant en 2 pour traiter aussi un peu de setTerritoireCritereAggAdrPdiHistoGeoToNow
	);

	IF clock_timestamp() > v_max_end_time THEN RAISE NOTICE 'Temps de traitement maximum dépassé sur setDataGeoToNow'; RETURN; END IF;

	CALL public.setTerritoireCritereAggAdrPdiHistoGeoToNow(
		in_commit => TRUE --Validation des transactions après chaque traitement d'historique, AINSI le traitement peut être stoppé en cours tout en gardant ce qui est validé
		--, in_limit_nb_traites => 40 --Environ 4 heures de traitement MAX = 48 historiques MAX, à raison de 5 minutes par historique ?
		, in_max_execution_time => v_max_end_time - clock_timestamp()
	);

	IF clock_timestamp() > v_max_end_time THEN RAISE NOTICE 'Temps de traitement maximum dépassé sur setDataGeoToNow'; RETURN; END IF;
	--IDEM
	PERFORM public.setTerritoireAggAdresseMajHistoGeoToNow();
	COMMIT;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.setTerritoireCritereAggAdrPdiHistoGeoToNow(in_max_execution_time => INTERVAL '1 min')
*/
