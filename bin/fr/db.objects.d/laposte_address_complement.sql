/***
 * FR: add LAPOSTE/RAN complement
 */

DO $COMPLEMENT$
BEGIN
    ALTER TABLE IF EXISTS fr.laposte_complement RENAME TO laposte_address_complement;
    ALTER INDEX IF EXISTS fr.iux_laposte_complement_co_cea RENAME TO iux_laposte_address_complement_co_cea;
    ALTER INDEX IF EXISTS fr.ix_laposte_complement_lb_standard_nn RENAME TO ix_laposte_address_complement_lb_standard_nn;
END $COMPLEMENT$;

-- address-complement with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_address_complement (
    co_cea CHAR(10) NOT NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    fl_active BOOLEAN NOT NULL,
    fl_diffusable BOOLEAN NOT NULL,
    lb_standard_nn CHARACTER VARYING(38) NOT NULL,
    id_type_groupe1_l3 INTEGER,
    lb_type_groupe1_l3 CHARACTER VARYING(38),
    lb_abrev_g1_an CHARACTER VARYING(10),
    lb_abrev_g1_nn CHARACTER VARYING(10),
    lb_groupe1 CHARACTER VARYING(38),
    id_type_groupe2_l3 INTEGER,
    lb_type_groupe2_l3 CHARACTER VARYING(38),
    lb_abrev_g2_an CHARACTER VARYING(10),
    lb_abrev_g2_nn CHARACTER VARYING(10),
    lb_groupe2 CHARACTER VARYING(38),
    id_type_groupe3_l3 INTEGER,
    lb_type_groupe3_l3 CHARACTER VARYING(38),
    lb_abrev_g3_an CHARACTER VARYING(10),
    lb_abrev_g3_nn CHARACTER VARYING(10),
    lb_groupe3 CHARACTER VARYING(38),
    lb_descr_an_groupe1 CHARACTER VARYING(10),
    lb_descr_nn_groupe1 CHARACTER VARYING(10),
    lb_mot_dir_groupe1 CHARACTER VARYING(38),
    lb_descr_an_groupe2 CHARACTER VARYING(10),
    lb_descr_nn_groupe2 CHARACTER VARYING(10),
    lb_mot_dir_groupe2 CHARACTER VARYING(38),
    lb_descr_an_groupe3 CHARACTER VARYING(10),
    lb_descr_nn_groupe3 CHARACTER VARYING(10),
    lb_mot_dir_groupe3 CHARACTER VARYING(38)
)
;

-- manual VACUUM
ALTER TABLE fr.laposte_address_complement SET (
    AUTOVACUUM_ENABLED = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_index()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_l3_co_cea') AND NOT index_exists('fr', 'iux_laposte_address_complement_co_cea') THEN
        ALTER INDEX idx_l3_co_cea RENAME TO iux_laposte_address_complement_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_address_complement_co_cea ON fr.laposte_address_complement (co_cea);
    END IF;

    -- similar labels
    -- lb_standard_nn
    IF index_exists('fr', 'idx_l3_lb_standard_nn') AND NOT index_exists('fr', 'ix_laposte_address_complement_lb_standard_nn') THEN
        ALTER INDEX idx_l3_lb_standard_nn RENAME TO ix_laposte_address_complement_lb_standard_nn;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_address_complement_lb_standard_nn ON fr.laposte_address_complement USING GIN(lb_standard_nn GIN_TRGM_OPS);
    END IF;

    DROP INDEX IF EXISTS fr.idx_l3_histo_key;
    --CREATE UNIQUE INDEX IF NOT EXISTS idx_l3_histo_key ON fr.laposte_address_complement_histo (co_cea, dt_reference);
END
$proc$ LANGUAGE plpgsql;

DO $$
DECLARE
    _query TEXT;
BEGIN
    -- manage indexes
    CALL fr.set_laposte_address_complement_index();

    /* NOTE
    add columns into laposte_address_complement_reference to avoid laposte_address_complement
    - dt_reference
    - fl_active (fl_active & fl_diffusable)
     */

    -- create views
    _query := '
        SELECT
            -- COMPLEMENT
            complement.co_cea AS co_adr,
            complement.dt_reference AS dt_reference_adr,
            dict.name lb_ligne3,
            dict.name_normalized lb_l3_normalise,
            dict.descriptors lb_l3_desc,
            complement.fl_active,

            -- ADDRESS
            address.co_cea_za AS co_adr_za,
            address.co_cea_voie AS co_adr_voie,
            address.co_cea_numero AS co_adr_numero,

            -- AREA
            area.co_postal,
            area.lb_l5_nn AS lb_ligne5,
            area.lb_ach_nn AS lb_acheminement,
            area.co_insee_commune
        FROM
            fr.laposte_address_complement complement
                JOIN fr.laposte_address address ON address.co_cea_determinant = complement.co_cea
                JOIN fr.laposte_address_area area ON area.co_cea = address.co_cea_za
                JOIN fr.laposte_address_complement_reference ref ON complement.co_cea = ref.address_id
                JOIN fr.laposte_address_complement_uniq dict ON ref.name_id = dict.id
    ';

    DROP VIEW IF EXISTS fr.complement_all_view CASCADE;
    EXECUTE CONCAT_WS(
        ' ',
        'CREATE VIEW fr.complement_all_view AS',
        _query
    );
    DROP VIEW IF EXISTS fr.complement_view CASCADE;
    EXECUTE CONCAT_WS(
        ' ',
        'CREATE VIEW fr.complement_view AS',
        _query,
        'WHERE complement.fl_active AND complement.fl_diffusable'
    );

    _query := '
        SELECT
            -- COMPLEMENT
            dict.id,
            dict.name,
            dict.descriptors,
            dict.as_words,
            dict.as_groups,
            dict.name_normalized,
            dict.descriptors_normalized,
            dict.as_words_normalized,
            dict.occurs,
            dict.words,
            dict.nwords,

            -- ADDRESS
            address.co_cea_determinant AS co_adr,
            address.co_cea_za AS co_adr_za,
            address.co_cea_voie AS co_adr_voie,
            address.co_cea_numero AS co_adr_numero
        FROM
            fr.laposte_address_complement_uniq dict
                JOIN fr.laposte_address_complement_reference ref ON dict.id = ref.name_id
                JOIN fr.laposte_address address ON address.co_cea_determinant = ref.address_id
    ';
    DROP VIEW IF EXISTS fr.complement_dict_view CASCADE;
    EXECUTE CONCAT_WS(
        ' ',
        'CREATE VIEW fr.complement_dict_view AS',
        _query
    );
END
$$;
