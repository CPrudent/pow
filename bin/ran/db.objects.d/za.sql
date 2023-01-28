DO $$
DECLARE
BEGIN
    IF NOT table_exists('ran', 'za') THEN
        DROP TABLE IF EXISTS ran.ra18 CASCADE;
        DROP TABLE IF EXISTS ran.za_ra18_histo CASCADE;
    END IF;
END $$;

-- data from RAN-RA18 file
CREATE TABLE IF NOT EXISTS ran.ra18(
    id CHARACTER VARYING(12) NOT NULL
    , co_cea CHAR(10) NOT NULL
    , co_insee CHAR(5) NOT NULL
    , lb_in_ext_loc CHARACTER VARYING(72) NOT NULL
    , lb_an CHARACTER VARYING(32) NOT NULL
    , lb_nn CHARACTER VARYING(38) NOT NULL
    , id_typ_loc INTEGER NOT NULL
    , lb_l5_an CHARACTER VARYING(32)
    , lb_l5_nn CHARACTER VARYING(38)
    , co_postal CHARACTER VARYING(5) NOT NULL
    , lb_ach_an CHARACTER VARYING(32) NOT NULL
    , lb_ach_nn CHARACTER VARYING(38) NOT NULL
    , co_insee_r CHAR(5)
    , fl_etat INTEGER NOT NULL
);

-- address-ZA with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.za
(
    co_cea CHAR(10) NOT NULL
    , dt_reference DATE NOT NULL
    , co_mouvement CHAR(1) NOT NULL
    , fl_active BOOLEAN NOT NULL
    , co_postal CHARACTER VARYING(5) NOT NULL
    , co_insee_commune CHAR(5) NOT NULL
    , co_insee_commune_precedente CHAR(5)
    , lb_in_ext_loc CHARACTER VARYING(72) NOT NULL
    , lb_nn CHARACTER VARYING(38) NOT NULL
    , lb_l5_nn CHARACTER VARYING(38) NULL
    , lb_ach_nn CHARACTER VARYING(38) NOT NULL
    , dt_reference_commune DATE NOT NULL
    , co_insee_commune_ran CHAR(5) NOT NULL
    , co_insee_commune_precedente_ran CHAR(5)
    , co_insee_departement VARCHAR(3) NOT NULL
);

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.ra18 SET (
    AUTOVACUUM_ENABLED = FALSE
);

CREATE TABLE IF NOT EXISTS ran.za_histo
(
	co_cea CHAR(10) NOT NULL,
	dt_reference DATE NOT NULL,
	co_mouvement CHAR(1) NOT NULL,
	fl_active BOOLEAN NOT NULL,
	co_postal CHARACTER VARYING(5) NOT NULL,
	co_insee_commune CHAR(5) NOT NULL,
	co_insee_commune_precedente CHAR(5),
	lb_in_ext_loc CHARACTER VARYING(72) NOT NULL,
	lb_nn CHARACTER VARYING(38) NOT NULL,
	lb_l5_nn CHARACTER VARYING(38),
	lb_ach_nn CHARACTER VARYING(38) NOT NULL,
	dt_reference_commune DATE NOT NULL, -- update date
	co_insee_commune_ran CHAR(5) NOT NULL, -- mode DELTA
	co_insee_commune_precedente_ran CHAR(5), -- mode DELTA
	co_insee_departement VARCHAR(3) NOT NULL
)
;

ALTER TABLE ran.za_histo SET (
    AUTOVACUUM_ENABLED = FALSE
);

COMMENT ON TABLE ran.ra18 IS 'Zones d''adresses (INSEE*, CP*, L5, L6*)';

SELECT drop_all_functions_if_exists('ran', 'setIndexZa');
CREATE OR REPLACE PROCEDURE ran.setIndexZa()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_za_co_cea') AND NOT index_exists('ran', 'iux_za_co_cea') THEN
        ALTER INDEX idx_za_co_cea RENAME TO iux_za_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_za_co_cea ON ran.za (co_cea);
    END IF;

    -- INSEE
    IF index_exists('ran', 'idx_za_co_insee_com_arr') AND NOT index_exists('ran', 'ix_za_co_insee_commune') THEN
        ALTER INDEX idx_za_co_insee_com_arr RENAME TO ix_za_co_insee_commune;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_co_insee_commune ON ran.za (co_insee_commune);
    END IF;

    -- old INSEE (used by IRISation)
    --	TEST : EXPLAIN SELECT * FROM ran.za AS za WHERE za.co_insee_commune = 'XXXXX' AND za.co_insee_commune_precedente = 'XXXXX'
    --	necessary COALESCE(commune_precedente, '') for use w/ NULL values
    IF index_exists('ran', 'idx_za_co_insee_com_arr_anc') AND NOT index_exists('ran', 'ix_za_co_insee_commune_anc') THEN
        ALTER INDEX idx_za_co_insee_com_arr_anc RENAME TO ix_za_co_insee_commune_anc;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_co_insee_commune_anc ON ran.za (co_insee_commune, COALESCE(co_insee_commune_precedente, ''));
    END IF;

    -- department (not useful)
    DROP INDEX IF EXISTS ran.idx_za_co_insee_dep;
    --CREATE INDEX IF NOT EXISTS idx_za_co_insee_departement ON ran.za (co_insee_departement);

    -- zip code
    IF index_exists('ran', 'idx_za_co_postal') AND NOT index_exists('ran', 'ix_za_co_postal') THEN
        ALTER INDEX idx_za_co_postal RENAME TO ix_za_co_postal;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_co_postal ON ran.za (co_postal);
    END IF;

    -- similar labels
    -- lb_l5_nn
    IF index_exists('ran', 'idx_za_lb_l5_nn') AND NOT index_exists('ran', 'ix_za_lb_l5_nn') THEN
        ALTER INDEX idx_za_lb_l5_nn RENAME TO ix_za_lb_l5_nn;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_l5_nn ON ran.za USING GIN(lb_l5_nn GIN_TRGM_OPS);
    END IF;
    -- lb_in_ext_loc
    IF index_exists('ran', 'idx_za_lb_in_ext_loc') AND NOT index_exists('ran', 'ix_za_lb_in_ext_loc') THEN
        ALTER INDEX idx_za_lb_in_ext_loc RENAME TO ix_za_lb_in_ext_loc;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_in_ext_loc ON ran.za USING GIN(lb_in_ext_loc GIN_TRGM_OPS);
    END IF;
    -- lb_nn
    IF index_exists('ran', 'idx_za_lb_nn') AND NOT index_exists('ran', 'ix_za_lb_nn') THEN
        ALTER INDEX idx_za_lb_nn RENAME TO ix_za_lb_nn;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_nn ON ran.za USING GIN(lb_nn GIN_TRGM_OPS);
    END IF;
    -- lb_ach_nn
    IF index_exists('ran', 'idx_za_lb_ach_nn') AND NOT index_exists('ran', 'ix_za_lb_ach_nn') THEN
        ALTER INDEX idx_za_lb_ach_nn RENAME TO ix_za_lb_ach_nn;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS ix_za_lb_ach_nn ON ran.za USING GIN(lb_ach_nn GIN_TRGM_OPS);
    END IF;

    -- date history
    DROP INDEX IF EXISTS ran.idx_za_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_za_histo_key ON ran.za_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO
$$
BEGIN
    PERFORM ran.setIndexZa();
END
$$ ;

SELECT drop_all_functions_if_exists('ran', 'getZaFromZaRa18');
CREATE OR REPLACE FUNCTION ran.getZaFromZaRa18(
    ra18 IN ran.ra18
    , dt_reference IN DATE
    , za_before IN ran.za DEFAULT NULL
    )
RETURNS ran.za AS
$func$
DECLARE
    _za ran.za%ROWTYPE;
    _za_before_exists BOOLEAN;
BEGIN
    --NOTE : (za_before IS NULL) only test OK !
    -- given NOT NULL record, tests IS NULL and IS NOT NULL return FALSE !!!
    IF za_before IS NULL THEN
        _za_before_exists := FALSE;
        -- to avoid evaluation error on missing attributs
        za_before := _za;
    ELSE
        _za_before_exists := za_before.co_cea IS NOT NULL;
    END IF;

    _za.co_cea := ra18.co_cea;
    _za.fl_active := ra18.fl_etat::INTEGER::BOOLEAN;
    _za.co_postal := ra18.co_postal;
    _za.co_insee_commune_ran :=
        CASE
        WHEN ra18.id_typ_loc = 2 /*ARM*/ THEN ra18.co_insee
        ELSE COALESCE(ra18.co_insee_r, ra18.co_insee)
        END;
    _za.co_insee_commune_precedente_ran :=
        CASE
        WHEN ra18.id_typ_loc = 3 THEN ra18.co_insee
        END;
    _za.lb_in_ext_loc := ra18.lb_in_ext_loc;
    _za.lb_nn := ra18.lb_nn;
    _za.lb_l5_nn := ra18.lb_l5_nn;
    _za.lb_ach_nn := ra18.lb_ach_nn;

    IF _za_before_exists
    AND za_before.fl_active IS NOT DISTINCT FROM _za.fl_active
    AND za_before.co_postal IS NOT DISTINCT FROM _za.co_postal
    AND za_before.co_insee_commune_ran IS NOT DISTINCT FROM _za.co_insee_commune_ran
    AND za_before.co_insee_commune_precedente_ran IS NOT DISTINCT FROM _za.co_insee_commune_precedente_ran --NULLABLE
    AND za_before.lb_in_ext_loc IS NOT DISTINCT FROM _za.lb_in_ext_loc
    AND za_before.lb_nn IS NOT DISTINCT FROM _za.lb_nn
    AND za_before.lb_l5_nn IS NOT DISTINCT FROM _za.lb_l5_nn --NULLABLE
    AND za_before.lb_ach_nn IS NOT DISTINCT FROM _za.lb_ach_nn
    THEN
        -- ZA already exists
        _za.dt_reference := za_before.dt_reference;
        -- reuse the same (will be again verified)
        _za.co_insee_commune := za_before.co_insee_commune;
        _za.co_insee_commune_precedente := za_before.co_insee_commune_precedente;
        _za.dt_reference_commune := za_before.dt_reference_commune;
        _za := public.getZaRanGeoToNow(_za);
        -- take RAN's date if dt_reference(INSEE) has changed
        IF _za.dt_reference_commune != za_before.dt_reference_commune THEN
            _za.dt_reference := dt_reference;
        END IF;
    ELSE
        -- ZA not exists or change
        _za.dt_reference := dt_reference;
        _za.co_insee_commune := _za.co_insee_commune_ran;
        _za.co_insee_commune_precedente := _za.co_insee_commune_precedente_ran;
        _za.dt_reference_commune := dt_reference;
        _za := public.getZaRanGeoToNow(_za);
    END IF;

    _za.co_insee_departement := public.get_department_code_from_district_code(_za.co_insee_commune);

    RETURN _za;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT za.*
FROM ran.ra18
CROSS JOIN ran.getZaFromZaRa18(ra18) AS za
 */

SELECT drop_all_functions_if_exists('ran', 'getZaDeltaFromRa18');
CREATE OR REPLACE FUNCTION ran.getZaDeltaFromRa18(
    dt_reference DATE
    )
RETURNS SETOF ran.za AS
$func$
BEGIN
    RETURN QUERY
        SELECT
            za.co_cea
            , za.dt_reference
            , CASE
                WHEN za.fl_active = FALSE THEN 'S'
                WHEN za_before.co_cea IS NULL OR za_before.co_mouvement = 'S' THEN 'C' /*recréation*/
                ELSE 'M'
            END::CHAR(1) AS co_mouvement
            , za.fl_active
            , za.co_postal
            , za.co_insee_commune
            , za.co_insee_commune_precedente
            , za.lb_in_ext_loc
            , za.lb_nn
            , za.lb_l5_nn
            , za.lb_ach_nn
            , za.dt_reference_commune
            , za.co_insee_commune_ran
            , za.co_insee_commune_precedente_ran
            , za.co_insee_departement
        FROM ran.ra18
        LEFT OUTER JOIN ran.za AS za_before ON za_before.co_cea = ra18.co_cea
        /* Alternative with history only :
        LEFT OUTER JOIN ran.za_histo AS za_before
        ON za_before.co_cea = za.co_cea
        AND za_before.dt_reference = (
            SELECT za_a_date.dt_reference
            FROM ran.za_histo AS za_a_date
            WHERE za_a_date.co_cea = za_before.co_cea
            AND za_a_date.dt_reference < dt_reference
            ORDER BY za_a_date.dt_reference DESC
            LIMIT 1
        )
        */
        CROSS JOIN ran.getZaFromZaRa18(
            ra18 => ran.ra18
            , dt_reference => getZaDeltaFromRa18.dt_reference
            , za_before => za_before
            ) AS za
        WHERE
        (
            -- not yet existing
            za_before.co_cea IS NULL
            -- or change
            OR za_before.dt_reference != za.dt_reference -- RAN
            OR za_before.dt_reference_commune != za.dt_reference_commune -- BCAA
        )
        ;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT *
FROM ran.getZaDeltaFromRa18(NOW()::DATE)
 */

SELECT drop_all_functions_if_exists('ran', 'setZaFromRa18');
CREATE OR REPLACE FUNCTION ran.setZaFromRa18(
    dt_reference IN DATE
    , init_mode BOOLEAN DEFAULT TRUE
    , with_history BOOLEAN DEFAULT TRUE
    )
RETURNS BOOLEAN AS
$func$
BEGIN
    IF init_mode THEN
        TRUNCATE TABLE ran.za;
        PERFORM public.drop_table_indexes('ran', 'za');
        INSERT INTO ran.za(
            co_cea
            , dt_reference
            , co_mouvement
            , fl_active
            , co_postal
            , co_insee_commune
            , co_insee_commune_precedente
            , lb_in_ext_loc
            , lb_nn
            , lb_l5_nn
            , lb_ach_nn
            , dt_reference_commune
            , co_insee_commune_ran
            , co_insee_commune_precedente_ran
            , co_insee_departement
        )
        (
            SELECT
                za.co_cea
                , za.dt_reference
                , CASE
                    WHEN za.fl_active = FALSE THEN 'S'
                    ELSE 'I'  --INIT
                END AS co_mouvement
                , za.fl_active
                , za.co_postal
                , za.co_insee_commune
                , za.co_insee_commune_precedente
                , za.lb_in_ext_loc
                , za.lb_nn
                , za.lb_l5_nn
                , za.lb_ach_nn
                , za.dt_reference_commune
                , za.co_insee_commune_ran
                , za.co_insee_commune_precedente_ran
                , za.co_insee_departement
            FROM ran.ra18
            CROSS JOIN ran.getZaFromZaRa18(ra18 => ra.ra18, dt_reference => setZaFromRa18.dt_reference) AS za
        );

        IF with_history THEN
            TRUNCATE TABLE ran.za_histo;
            PERFORM public.drop_table_indexes('ran', 'za_histo');
            INSERT INTO ran.za_histo(
                co_cea
                , dt_reference
                , co_mouvement
                , fl_active
                , co_postal
                , co_insee_commune
                , co_insee_commune_precedente
                , lb_in_ext_loc
                , lb_nn
                , lb_l5_nn
                , lb_ach_nn
                , dt_reference_commune
                , co_insee_commune_ran
                , co_insee_commune_precedente_ran
                , co_insee_departement
            )
            (
                SELECT
                    co_cea
                    , dt_reference
                    , co_mouvement
                    , fl_active
                    , co_postal
                    , co_insee_commune
                    , co_insee_commune_precedente
                    , lb_in_ext_loc
                    , lb_nn
                    , lb_l5_nn
                    , lb_ach_nn
                    , dt_reference_commune
                    , co_insee_commune_ran
                    , co_insee_commune_precedente_ran
                    , co_insee_departement
                FROM ran.za
            )
            ;
        END IF;
    ELSE
        IF with_history THEN
            -- use temporary table to update za and za_histo
            DROP TABLE IF EXISTS tmp_ran_za_ra18_delta;
            CREATE TEMPORARY TABLE tmp_ran_za_ra18_delta AS TABLE ran.za WITH NO DATA;
            INSERT INTO tmp_ran_za_ra18_delta
            (
                co_cea
                , dt_reference
                , co_mouvement
                , fl_active
                , co_postal
                , co_insee_commune
                , co_insee_commune_precedente
                , lb_in_ext_loc
                , lb_nn
                , lb_l5_nn
                , lb_ach_nn
                , dt_reference_commune
                , co_insee_commune_ran
                , co_insee_commune_precedente_ran
                , co_insee_departement
            )
            (
                SELECT
                    co_cea
                    , dt_reference
                    , co_mouvement
                    , fl_active
                    , co_postal
                    , co_insee_commune
                    , co_insee_commune_precedente
                    , lb_in_ext_loc
                    , lb_nn
                    , lb_l5_nn
                    , lb_ach_nn
                    , dt_reference_commune
                    , co_insee_commune_ran
                    , co_insee_commune_precedente_ran
                    , co_insee_departement
                FROM ran.getZaDeltaFromRa18(dt_reference)
            );

            -- update za
            INSERT INTO ran.za (SELECT * FROM tmp_ran_za_ra18_delta)
            ON CONFLICT(co_cea)
            DO UPDATE
                SET	dt_reference = EXCLUDED.dt_reference
                    , co_mouvement = EXCLUDED.co_mouvement
                    , fl_active = EXCLUDED.fl_active
                    , co_postal = EXCLUDED.co_postal
                    , co_insee_commune = EXCLUDED.co_insee_commune
                    , co_insee_commune_precedente = EXCLUDED.co_insee_commune_precedente
                    , lb_in_ext_loc = EXCLUDED.lb_in_ext_loc
                    , lb_nn = EXCLUDED.lb_nn
                    , lb_l5_nn = EXCLUDED.lb_l5_nn
                    , lb_ach_nn = EXCLUDED.lb_ach_nn
                    , dt_reference_commune = EXCLUDED.dt_reference_commune
                    , co_insee_commune_ran = EXCLUDED.co_insee_commune_ran
                    , co_insee_commune_precedente_ran = EXCLUDED.co_insee_commune_precedente_ran
                    , co_insee_departement = EXCLUDED.co_insee_departement
            ;

            -- update za_histo
            INSERT INTO ran.za_histo(
                co_cea
                , dt_reference
                , co_mouvement
                , fl_active
                , co_postal
                , co_insee_commune
                , co_insee_commune_precedente
                , lb_in_ext_loc
                , lb_nn
                , lb_l5_nn
                , lb_ach_nn
                , dt_reference_commune
                , co_insee_commune_ran
                , co_insee_commune_precedente_ran
                , co_insee_departement
            )
            (
                SELECT
                    co_cea
                    , dt_reference
                    , co_mouvement
                    , fl_active
                    , co_postal
                    , co_insee_commune
                    , co_insee_commune_precedente
                    , lb_in_ext_loc
                    , lb_nn
                    , lb_l5_nn
                    , lb_ach_nn
                    , dt_reference_commune
                    , co_insee_commune_ran
                    , co_insee_commune_precedente_ran
                    , co_insee_departement
                FROM tmp_ran_za_ra18_delta
            )
            ;
        ELSE
            INSERT INTO ran.za(
                co_cea
                , dt_reference
                , co_mouvement
                , fl_active
                , co_postal
                , co_insee_commune
                , co_insee_commune_precedente
                , lb_in_ext_loc
                , lb_nn
                , lb_l5_nn
                , lb_ach_nn
                , dt_reference_commune
                , co_insee_commune_ran
                , co_insee_commune_precedente_ran
                , co_insee_departement
            )
            (
                SELECT
                    co_cea
                    , dt_reference
                    , co_mouvement
                    , fl_active
                    , co_postal
                    , co_insee_commune
                    , co_insee_commune_precedente
                    , lb_in_ext_loc
                    , lb_nn
                    , lb_l5_nn
                    , lb_ach_nn
                    , dt_reference_commune
                    , co_insee_commune_ran
                    , co_insee_commune_precedente_ran
                    , co_insee_departement
                FROM ran.getZaDeltaFromRa18(setZaFromRa18.dt_reference)
            )
            ON CONFLICT(co_cea)
            DO UPDATE
                SET	dt_reference = EXCLUDED.dt_reference
                    , co_mouvement = EXCLUDED.co_mouvement
                    , fl_active = EXCLUDED.fl_active
                    , co_postal = EXCLUDED.co_postal
                    , co_insee_commune = EXCLUDED.co_insee_commune
                    , co_insee_commune_precedente = EXCLUDED.co_insee_commune_precedente
                    , lb_in_ext_loc = EXCLUDED.lb_in_ext_loc
                    , lb_nn = EXCLUDED.lb_nn
                    , lb_l5_nn = EXCLUDED.lb_l5_nn
                    , lb_ach_nn = EXCLUDED.lb_ach_nn
                    , dt_reference_commune = EXCLUDED.dt_reference_commune
                    , co_insee_commune_ran = EXCLUDED.co_insee_commune_ran
                    , co_insee_commune_precedente_ran = EXCLUDED.co_insee_commune_precedente_ran
                    , co_insee_departement = EXCLUDED.co_insee_departement
            ;
        END IF;
    END IF;

    -- uniq CEA
    CREATE UNIQUE INDEX IF NOT EXISTS idx_za_co_cea ON ran.za (co_cea);
    -- INSEE
    CREATE INDEX IF NOT EXISTS idx_za_co_insee_com_arr ON ran.za (co_insee_commune);
    -- old INSEE
    --CREATE INDEX IF NOT EXISTS idx_za_co_insee_com_arr_anc ON ran.za (co_insee_commune_precedente);
    -- INSEE + old INSEE (used by IRISation)
    --	TEST : EXPLAIN SELECT * FROM ran.za AS za WHERE za.co_insee_commune = 'XXXXX' AND za.co_insee_commune_precedente = 'XXXXX'
    --	necessary COALESCE(commune_precedente, '') for use w/ NULL values
    CREATE INDEX IF NOT EXISTS idx_za_co_insee_com_arr_com_arr_anc ON ran.za (co_insee_commune, COALESCE(co_insee_commune_precedente, ''));
    -- department
    --DROP INDEX IF EXISTS ran.idx_za_co_insee_dep;
    --CREATE INDEX IF NOT EXISTS idx_za_co_insee_departement ON ran.za (co_insee_departement);
    -- zip code
    CREATE INDEX IF NOT EXISTS idx_za_co_postal ON ran.za (co_postal);

    -- similar labels
    CREATE INDEX IF NOT EXISTS idx_za_lb_l5_nn ON ran.za USING GIN(lb_l5_nn gin_trgm_ops);
    -- Optimisation de filtre un libellé in extenso de la localité ressemblant à
    CREATE INDEX IF NOT EXISTS idx_za_lb_in_ext_loc ON ran.za USING GIN(lb_in_ext_loc gin_trgm_ops);
    -- Optimisation de filtre un libellé commune nouvelle norme ressemblant à
    CREATE INDEX IF NOT EXISTS idx_za_lb_nn ON ran.za USING GIN(lb_nn gin_trgm_ops);
    -- Optimisation de filtre un libellé acheminement nouvelle norme ressemblant à
    CREATE INDEX IF NOT EXISTS idx_za_lb_ach_nn ON ran.za USING GIN(lb_ach_nn gin_trgm_ops);

    -- date history
    CREATE UNIQUE INDEX IF NOT EXISTS idx_za_histo_key ON ran.za_histo (co_cea, dt_reference);

    TRUNCATE TABLE ran.ra18;

    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT ran.setZaFromRa18('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, TRUE, TRUE);
SELECT ran.setZaFromRa18('/data/bcaa/common_env/import/ran/test/', NOW()::DATE, FALSE, FALSE);
 */
