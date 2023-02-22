/***
 * FR: add LAPOSTE/RAN coordinates (XY)
 */

-- address-XY with history (date & type of last change)
CREATE TABLE IF NOT EXISTS fr.laposte_xy
(
    co_insee CHAR(5) NOT NULL,
    co_cea CHAR(10) NULL,
    dt_reference DATE NOT NULL,
    co_mouvement CHAR(1) NOT NULL,
    va_x DOUBLE PRECISION /*NOT*/ NULL,
    va_y DOUBLE PRECISION /*NOT*/ NULL,
    no_type_localisation INTEGER /*NOT*/ NULL,
    co_type_projection CHAR(1) /*NOT*/ NULL,
    gm_coord GEOMETRY(POINT,3857) /*NOT*/ NULL
)
;

-- manual VACUUM (fr/import.sh)
ALTER TABLE fr.laposte_xy SET (
    AUTOVACUUM_ENABLED = FALSE
);

SELECT drop_all_functions_if_exists('fr', 'setLaPosteIndexCoordinates');
CREATE OR REPLACE PROCEDURE fr.setLaPosteIndexCoordinates()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('fr', 'idx_laposte_xy_co_cea') AND NOT index_exists('fr', 'iux_laposte_xy_co_cea') THEN
        ALTER INDEX idx_laposte_xy_co_cea RENAME TO iux_laposte_xy_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_laposte_xy_co_cea ON fr.laposte_xy (co_cea);
    END IF;

    -- INSEE
    IF index_exists('fr', 'idx_laposte_xy_co_insee') AND NOT index_exists('fr', 'ix_laposte_xy_co_insee') THEN
        ALTER INDEX idx_laposte_xy_co_insee RENAME TO ix_laposte_xy_co_insee;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_xy_co_insee ON fr.laposte_xy (co_insee);
    END IF;

    -- parent
    IF index_exists('fr', 'idx_laposte_xy_gm_coord') AND NOT index_exists('fr', 'ix_laposte_xy_gm_coord') THEN
        ALTER INDEX idx_laposte_xy_gm_coord RENAME TO ix_laposte_xy_gm_coord;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_laposte_xy_gm_coord ON fr.laposte_xy USING GIST(gm_coord);
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL fr.setLaPosteIndexCoordinates();
END
$$;
