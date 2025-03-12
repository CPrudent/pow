/***
 * FR-TERRITORY postal definition
 */

CREATE TABLE IF NOT EXISTS fr.territory_laposte (
    nivgeo VARCHAR,
    codgeo VARCHAR,
    libgeo VARCHAR,
    codgeo_pdc_ppdc_parent CHARACTER(6),
    codgeo_ppdc_pdc_parent CHARACTER(6),
    codgeo_dex_parent CHARACTER(6)
);

SELECT drop_all_functions_if_exists('fr', 'set_territory_laposte');
CREATE OR REPLACE FUNCTION fr.set_territory_laposte()
RETURNS BOOLEAN AS $$
BEGIN
    TRUNCATE TABLE fr.territory_laposte;
    PERFORM public.drop_table_indexes('fr', 'territory_laposte');

    INSERT INTO fr.territory_laposte (
        nivgeo,
        codgeo,
        codgeo_pdc_ppdc_parent,
        codgeo_ppdc_pdc_parent,
        codgeo_dex_parent
    )
    (
        WITH cp_has_site AS (
            SELECT
                ran.co_postal AS codgeo_postal,
                rao.co_roc_site AS codgeo_pdc_ppdc,
                COUNT(*) AS nb_adr_rao
            FROM fr.address_view AS ran
            INNER JOIN fr.laposte_delivery_address rao on rao.co_adr = ran.co_adr
            GROUP BY ran.co_postal, rao.co_roc_site
        ),
        cp_has_best_site AS (
            SELECT
                codgeo_postal,
                FIRST(codgeo_pdc_ppdc ORDER BY nb_adr_rao DESC) AS codgeo_pdc_ppdc_parent
            FROM cp_has_site
            GROUP BY codgeo_postal
        ),
        cp AS (
            SELECT
                cp_has_best_site.codgeo_postal,
                cp_has_best_site.codgeo_pdc_ppdc_parent,
                site_source_orga.code_regate AS codgeo_regate_pdc_ppdc_parent,
                site_source_orga.code_rattachement_ppdc_pdc AS codgeo_ppdc_pdc_parent,
                site_source_orga.code_rattachement_dexc AS codgeo_dex_parent
            FROM cp_has_best_site
            LEFT OUTER JOIN fr.laposte_organization AS site_source_orga
                ON site_source_orga.code = cp_has_best_site.codgeo_pdc_ppdc_parent
        )
        SELECT
            'CP' AS nivgeo,
            cp.codgeo_postal AS codgeo,
            cp.codgeo_pdc_ppdc_parent,
            cp.codgeo_ppdc_pdc_parent,
            cp.codgeo_dex_parent
        FROM cp
    );

    CREATE UNIQUE INDEX iux_territory_laposte_nivgeo_codgeo ON fr.territory_laposte (nivgeo, codgeo);

    PERFORM fr.set_territory_laposte_to_now();
    IF fr.set_territory_supra(
        table_name => 'territory_laposte',
        schema_name => 'fr',
        base_level => 'CP'
    )
    THEN
        --Codes Postaux : libellé = code
        UPDATE fr.territory_laposte
        SET libgeo = codgeo
        WHERE nivgeo = 'CP'
        ;

        --Zones Postales : libellés SOURCE-ORGA (avec métiers COURRIER, ELP) sinon sites manquants du RLP (réseau, enseigne)
        UPDATE fr.territory_laposte
        SET libgeo =
            --On retire le mot "PARIS" qui est en préfixe du libellé de chaque DEX, sauf pour celle qui vraiment de Paris
            -- de même avec le mot "GENTILLY" en préfixe de la DEX OM (du métier ELP)
            CASE
                WHEN territory_laposte.nivgeo = 'DEX' AND source_orga.libelle LIKE 'PARIS DEX%'
                    THEN source_orga.libelle
                ELSE
                    REGEXP_REPLACE(source_orga.libelle, '^(PARIS|GENTILLY) ', '')
            END
        FROM fr.laposte_organization source_orga WHERE source_orga.code = territory_laposte.codgeo
        AND territory_laposte.nivgeo IN ('PDC_PPDC', 'PPDC_PDC', 'DEX')
        ;
    END IF;

    RETURN TRUE;
END $$ LANGUAGE plpgsql;

-- oldies
SELECT drop_all_functions_if_exists('fr', 'set_territory_laposte_to_now');
SELECT drop_all_functions_if_exists('fr', 'update_territory_laposte_supra');

-- not used
SELECT public.drop_all_functions_if_exists('fr', 'get_municipality_to_date_from_laposte');
CREATE OR REPLACE FUNCTION fr.get_municipality_to_date_from_laposte(
    code VARCHAR,
    date_geography_from DATE,
    distribution NUMERIC DEFAULT 1,
    raise_notice BOOLEAN DEFAULT FALSE
)
RETURNS SETOF public.territory_to_date_t AS
$func$
DECLARE
    _territory_to_date_t territory_to_date_t;
    _date_address DATE := (public.get_last_io(name => 'FR-ADDRESS-LAPOSTE')).date_data_end;
    _municipalities RECORD;
    _municipality VARCHAR;
    _return BOOLEAN := TRUE;
BEGIN
    FOR _municipalities IN (
        SELECT
            ARRAY_AGG(DISTINCT co_insee_commune) AS municipalities_now,
            1::NUMERIC/COUNT(DISTINCT co_insee_commune) AS distribution
        FROM fr.laposte_address_area
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
                _municipality,
                NULL,
                _date_address,
                _municipalities.distribution
            );
        END LOOP;
    END LOOP;

    IF _return THEN
        RETURN NEXT ROW (
            code,
            NULL,
            _date_address,
            distribution
        );
    END IF;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'get_address_area_to_now');
SELECT drop_all_functions_if_exists('fr', 'get_laposte_area_to_now');
CREATE OR REPLACE FUNCTION fr.get_laposte_area_to_now(
    address_area fr.laposte_address_area
)
RETURNS fr.laposte_address_area AS
$func$
DECLARE
    _municipality_to_now RECORD;
BEGIN
    --Cas de réactivation non géré pour le moment : est-ce un cas possible ?
    IF NOT address_area.fl_active THEN RETURN address_area; END IF;

    SELECT *
    INTO _municipality_to_now
    FROM fr.get_municipality_to_date(
        code => address_area.co_insee_commune,
        --On force l'algo à considérer en cas de fusion que cette ZA correspond à la portion avant fusion, même pour la commune déléguée chef lieu
        code_previous => COALESCE(
            address_area.co_insee_commune_precedente,
            address_area.co_insee_commune
        ),
        date_geography_from => address_area.dt_reference_commune,
        with_deleted => TRUE, --Cas de suppression/désactivation non géré pour le moment : est-ce un cas possible ?
        check_exists => FALSE --Ce test n'aurait pas de sens, puisque la liste des communes de la table territory est issue de RAN
    ) AS commune_to_now
    WHERE commune_to_now.is_new --Seulement ce qui est nouveau
    ;

    /* NOTE
    Même en cas de fusion, on ne stocke pas dans RAN le code INSEE précédent s'il ne change pas (cas de la commune déléguée chef lieu)
    IF _municipality_to_now.code = _municipality_to_now.code_previous THEN
        _municipality_to_now.code_previous := NULL;
    END IF;
     */

    IF _municipality_to_now.distribution = 1 THEN
        RAISE NOTICE 'Cas de (fusion de commune / création commune nouvelle) géré pour maj GEO de RAN ZA : %, % / %, % -> %, %',
            address_area.co_cea,
            address_area.co_insee_commune,
            address_area.co_insee_commune_precedente,
            address_area.lb_nn,
            _municipality_to_now.code,
            _municipality_to_now.name;

        -- rename municipality
        IF _municipality_to_now.code_previous IS NOT NULL THEN
            -- merged municipality (save name into L5 as old municipality)
            IF _municipality_to_now.code != _municipality_to_now.code_previous THEN
                -- keep eventualy previuous code (if not already merged)
                IF address_area.co_insee_commune_precedente IS NULL THEN
                    address_area.co_insee_commune_precedente := address_area.co_insee_commune;
                END IF;
                address_area.lb_l5_nn := address_area.lb_ach_nn;
            END IF;
        END IF;

        address_area.co_insee_commune := _municipality_to_now.code;
        address_area.lb_ach_nn := fr.normalize_municipality_name(_municipality_to_now.code, _municipality_to_now.name);
        address_area.co_insee_departement :=            fr.get_department_code_from_municipality_code(address_area.co_insee_commune);
        address_area.dt_reference_commune := _municipality_to_now.date_geography;

    ELSIF _municipality_to_now.distribution < 1 AND _municipality_to_now.distribution > 0 THEN
        RAISE NOTICE 'Cas de rétablissement de commune géré pour maj GEO de RAN ZA : %, % / %, % -> %, %',
            address_area.co_cea,
            address_area.co_insee_commune,
            address_area.co_insee_commune_precedente,
            address_area.lb_nn,
            _municipality_to_now.code,
            _municipality_to_now.name;

        address_area.co_insee_commune := address_area.co_insee_commune_precedente;
        address_area.co_insee_departement := fr.get_department_code_from_municipality_code(address_area.co_insee_commune);
        address_area.co_insee_commune_precedente := NULL;
        address_area.dt_reference_commune := _municipality_to_now.date_geography;

        /* NOTE : Cas rare de division non géré pour le moment, pourrait être :
        address_area.lb_nn := lb_l5_nn;
        address_area.lb_ach_nn := lb_l5_nn;
        address_area.co_insee_commune_precedente := NULL;
        address_area.dt_reference_commune = _municipality_to_now.date_geography;
         */
    ELSIF _municipality_to_now.distribution = 0 THEN
        RAISE NOTICE 'Cas de suppression de commune non géré pour maj GEO de RAN ZA : %, % / %, %',
            address_area.co_cea,
            address_area.co_insee_commune,
            address_area.co_insee_commune_precedente,
            address_area.lb_nn;
        /* NOTE : Cas rare (inexistant ?) de suppression non géré pour le moment, pourrait être
        address_area.fl_active := FALSE;
        address_area.dt_reference_commune := _municipality_to_now.date_geography;
         */
    END IF;

    RETURN address_area;
END
$func$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('fr', 'set_address_area_to_now');
SELECT drop_all_functions_if_exists('fr', 'set_laposte_area_to_now');
CREATE OR REPLACE FUNCTION fr.set_laposte_area_to_now()
RETURNS BOOLEAN AS
$func$
DECLARE
    _zone_address_to_now RECORD;
    _date_address DATE := (public.get_last_io(name => 'FR-ADDRESS-LAPOSTE')).date_data_end;
    _laposte_updated BOOLEAN DEFAULT FALSE;
    _query TEXT;
BEGIN
    FOR _zone_address_to_now IN (
        SELECT
            za_to_now.co_cea,
            za_to_now.co_insee_commune,
            za_to_now.co_insee_commune_precedente,
            za_to_now.dt_reference_commune,
            za_to_now.co_insee_departement,
            za_to_now.lb_ach_nn,
            za_to_now.lb_l5_nn,
            --Si modification effective hormis la date de référence
            CASE
                WHEN za_to_now.co_insee_commune != za.co_insee_commune
                    OR za_to_now.co_insee_commune_precedente IS DISTINCT FROM za.co_insee_commune_precedente
                --On considère l'adresse mise à jour à date de RAN + 1, de telle façon que les traitements DELTA traitent cette adresse lors de leur prochain lancement
                THEN _date_address + 1
                ELSE za.dt_reference
            END AS dt_reference,
            CASE
                WHEN za_to_now.co_insee_commune != za.co_insee_commune
                    OR za_to_now.co_insee_commune_precedente IS DISTINCT FROM za.co_insee_commune_precedente
                THEN TRUE
                ELSE FALSE
            END AS modification
        FROM fr.laposte_address_area AS za
        CROSS JOIN fr.get_laposte_area_to_now(za) AS za_to_now
        WHERE za_to_now.dt_reference_commune != za.dt_reference_commune
    )
    LOOP
        _query := CONCAT('
            INSERT INTO fr.laposte_address_history (
                code_address,
                date_change,
                change,
                kind,
                values
            )
            SELECT
                co_cea,
                TIMEOFDAY()::DATE,
                ', quote_literal('MUNICIPALITY_EVENT'),
                ', ', quote_literal('AREA'), ',
                ROW_TO_JSON(a.*)::JSONB
            FROM
                fr.laposte_address_area a
            WHERE
                co_cea = ''', _zone_address_to_now.co_cea, '''
            '
        );
        EXECUTE _query;

        UPDATE fr.laposte_address_area za
        SET co_insee_commune = _zone_address_to_now.co_insee_commune,
            co_insee_commune_precedente = _zone_address_to_now.co_insee_commune_precedente,
            dt_reference_commune = _zone_address_to_now.dt_reference_commune,
            dt_reference = _zone_address_to_now.dt_reference,
            co_insee_departement = _zone_address_to_now.co_insee_departement,
            lb_l5_nn = _zone_address_to_now.lb_l5_nn,
            lb_ach_nn = _zone_address_to_now.lb_ach_nn
        WHERE za.co_cea = _zone_address_to_now.co_cea;

        --Si modification effective hormis la date de référence
        IF _zone_address_to_now.modification THEN
            UPDATE fr.laposte_address
            SET dt_reference_za = _zone_address_to_now.dt_reference,
                dt_reference = GREATEST(dt_reference, _zone_address_to_now.dt_reference)
            WHERE co_cea_za = _zone_address_to_now.co_cea;

            --MAJ du code INSEE commune dénormalisé sur les voies de la ZA
            UPDATE fr.laposte_address_street street
            SET co_insee_commune = _zone_address_to_now.co_insee_commune
            FROM fr.laposte_address address
            WHERE address.co_cea_determinant = street.co_cea
            AND address.co_cea_za = _zone_address_to_now.co_cea --Voies de la ZA
            AND street.co_insee_commune != _zone_address_to_now.co_insee_commune; --Qui ont un code INSEE commune différent (à priori forcément vrai);
        END IF;

        _laposte_updated := TRUE;
    END LOOP;

    RETURN _laposte_updated;
END
$func$ LANGUAGE plpgsql;
