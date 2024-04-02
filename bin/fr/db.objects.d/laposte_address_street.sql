/***
 * FR: add LAPOSTE/RAN street
 */

DO $STREET$
BEGIN
    ALTER TABLE IF EXISTS fr.laposte_street RENAME TO laposte_address_street;
    ALTER INDEX IF EXISTS fr.iux_laposte_street_co_cea RENAME TO iux_laposte_address_street_co_cea;
    ALTER INDEX IF EXISTS fr.ix_laposte_street_lb_voie RENAME TO ix_laposte_address_street_lb_voie;
END $STREET$;

-- address-street with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_address_street (
    co_cea CHAR(10),
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    co_voie NUMERIC(8, 0) NOT NULL,
    lb_voie CHARACTER VARYING(60) NOT NULL,
    lb_voie_normalise CHARACTER VARYING(32) NOT NULL,
    lb_type CHARACTER VARYING(38) NULL,
    lb_type_abrege CHARACTER VARYING(4) NULL,
    lb_md CHARACTER VARYING(20) NULL,
    lb_desc CHARACTER VARYING(10) NOT NULL,
    co_insee_commune CHAR(5) NOT NULL, --FIXME : previously necessary for index on strong word (by municipality)
    co_cea_za CHAR(10) --NOTE : useful for index ix_laposte_address_street_co_cea_za_lb_voie (trigrams)
)
;

-- manual VACUUM
ALTER TABLE fr.laposte_address_street SET (
    AUTOVACUUM_ENABLED = FALSE
);

-- rename indexes after restore (original LAPOSTE data) or create them
SELECT drop_all_functions_if_exists('fr', 'set_laposte_street_index');
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_index()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_voie_co_cea') AND NOT index_exists('fr', 'iux_laposte_address_street_co_cea') THEN
        ALTER INDEX idx_voie_co_cea RENAME TO iux_laposte_address_street_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_street_co_cea ON fr.laposte_address_street (co_cea);
    END IF;

    --CREATE INDEX IF NOT EXISTS idx_voie_co_insee_commune_lb_md ON fr.laposte_address_street USING GIST(co_insee_commune, lb_md GIST_TRGM_OPS);
    --CREATE INDEX IF NOT EXISTS idx_voie_co_postal_lb_md ON fr.laposte_address_street USING GIST(co_postal, lb_md GIST_TRGM_OPS);
    --CREATE INDEX IF NOT EXISTS idx_voie_co_cea_za_lb_voie ON fr.laposte_address_street USING GIST(co_cea_za, lb_voie GIST_TRGM_OPS);
    DROP INDEX IF EXISTS fr.idx_voie_co_insee_commune_lb_md;
    DROP INDEX IF EXISTS fr.idx_voie_co_postal_lb_md;
    DROP INDEX IF EXISTS fr.idx_voie_co_cea_za_lb_voie;

    --CREATE INDEX IF NOT EXISTS idx_voie_co_insee_departement ON fr.laposte_address_street (public.get_department_code_from_district_code(co_insee_commune));
    DROP INDEX IF EXISTS fr.idx_voie_co_insee_departement;

    -- similar labels
    -- lb_voie
    IF index_exists('fr', 'idx_voie_lb_voie') AND NOT index_exists('fr', 'ix_laposte_address_street_lb_voie') THEN
        ALTER INDEX idx_voie_lb_voie RENAME TO ix_laposte_address_street_lb_voie;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_address_street_lb_voie ON fr.laposte_address_street USING GIN(lb_voie GIN_TRGM_OPS);
    END IF;

    -- lb_md
    --CREATE INDEX ix_voie_lb_md ON fr.laposte_address_street USING GIN(lb_md GIN_TRGM_OPS);

    DROP INDEX IF EXISTS fr.idx_voie_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_voie_histo_key ON fr.laposte_address_street_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
DECLARE
    _query TEXT;
BEGIN
    -- manage indexes
    CALL fr.set_laposte_address_street_index();

    /* NOTE
    add columns into laposte_address_street_reference to avoid laposte_address_street
    - co_voie
    - dt_reference
    - fl_active (fl_active & fl_diffusable)
     */
    -- create views
    _query := '
        SELECT
            -- STREET
              street.co_cea AS co_adr
            , street.dt_reference AS dt_reference_adr
            , street.co_voie
            , dict.name lb_voie
            , dict.name_normalized lb_voie_normalise
            , dict.descriptors lb_voie_desc
            , street.fl_active

            -- ADDRESS
            , area.co_cea AS co_adr_za
            , area.co_postal
            , area.lb_l5_nn AS lb_ligne5
            , area.lb_ach_nn AS lb_acheminement
            , area.co_insee_commune
            , area.co_insee_commune_precedente
            , area.co_insee_departement
            , area.fl_active AS fl_active_za

            -- XY
            , xy.dt_reference AS dt_reference_coord
            , xy.gm_coord
            , xy.no_type_localisation AS no_type_localisation_coord
            , xy.va_x AS x_natif_coord
            , xy.va_y AS y_natif_coord
            , fr.get_srid_from_department_code(fr.get_department_code_from_municipality_code(xy.co_insee)) AS srid_natif_coord
            , ST_SetSRID(
                ST_MakePoint(xy.va_x, xy.va_y)
                , fr.get_srid_from_department_code(fr.get_department_code_from_municipality_code(xy.co_insee))
            ) AS gm_coord_native_ran

            -- DELIVERY
            , delivery.co_type AS rao_co_type
            , delivery.lb_libelle AS rao_lb_libelle
            , delivery.co_roc_site
            , org.code_regate AS rao_co_regate
            , org.libelle AS rao_libelle_site
            , NULLIF(CONCAT(delivery.co_type, delivery.lb_libelle), '''') AS rao_co_tournee
        FROM
            fr.laposte_address_street street
                JOIN fr.laposte_address address ON address.co_cea_determinant = street.co_cea
                JOIN fr.laposte_address_area area ON area.co_cea = address.co_cea_za
                JOIN fr.laposte_address_street_reference ref ON street.co_cea = ref.address_id
                JOIN fr.laposte_address_street_uniq dict ON ref.name_id = dict.id
                LEFT OUTER JOIN fr.laposte_address_xy xy ON xy.co_cea = street.co_cea
                LEFT OUTER JOIN fr.laposte_delivery_address delivery ON delivery.co_adr = street.co_cea
                LEFT OUTER JOIN fr.laposte_organization org ON org.code = delivery.co_roc_site::VARCHAR
    ';

    DROP VIEW IF EXISTS fr.street_all_view CASCADE;
    EXECUTE CONCAT_WS(
        ' '
        , 'CREATE VIEW fr.street_all_view AS'
        , _query
    );
    DROP VIEW IF EXISTS fr.street_view CASCADE;
    EXECUTE CONCAT_WS(
        ' '
        , 'CREATE VIEW fr.street_view AS'
        , _query
        , 'WHERE street.fl_active AND street.fl_diffusable'
    );
END
$$;
