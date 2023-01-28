/***
 * RAN : add COORDINATES (XY) data
 */

/*
-- data from RAN-RA50 file
CREATE TABLE IF NOT EXISTS ran.coord_ra50(
    co_insee CHAR(5) NOT NULL,
    co_cea CHAR(10) NULL,
    va_x CHARACTER VARYING /*NOT*/ NULL,
    va_y CHARACTER VARYING /*NOT*/ NULL,
    no_type_localisation INTEGER /*NOT*/ NULL,
    lb_type_localisation CHARACTER VARYING(100) /*NOT*/ NULL,
    co_type_projection CHAR(1) /*NOT*/ NULL,
    lb_type_projection CHARACTER VARYING(100) /*NOT*/ NULL,
    fl_diffusable INTEGER
)
;

ALTER TABLE ran.coord_ra50 SET (
    AUTOVACUUM_ENABLED = FALSE
);
 */

-- address-XY with history (date & type of last change)
CREATE TABLE IF NOT EXISTS ran.coord
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

-- manual VACUUM (ran/import.sh)
ALTER TABLE ran.coord SET (
    AUTOVACUUM_ENABLED = FALSE
);

SELECT drop_all_functions_if_exists('ran', 'setIndexCoordinates');
CREATE OR REPLACE PROCEDURE ran.setIndexCoordinates()
AS
$proc$
BEGIN
    -- uniq CEA
    IF index_exists('ran', 'idx_ran_coord_co_cea') AND NOT index_exists('ran', 'iux_coord_co_cea') THEN
        ALTER INDEX idx_ran_coord_co_cea RENAME TO iux_coord_co_cea;
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS iux_coord_co_cea ON ran.coord (co_cea);
    END IF;

    -- INSEE
    IF index_exists('ran', 'idx_ran_coord_co_insee') AND NOT index_exists('ran', 'iux_coord_co_insee') THEN
        ALTER INDEX idx_ran_coord_co_insee RENAME TO iux_coord_co_insee;
    ELSE
        CREATE INDEX IF NOT EXISTS iux_coord_co_insee ON ran.coord (co_insee);
    END IF;

    -- parent
    IF index_exists('ran', 'idx_ran_coord_gm_coord') AND NOT index_exists('ran', 'ix_coord_gm_coord') THEN
        ALTER INDEX idx_ran_coord_gm_coord RENAME TO ix_coord_gm_coord;
    ELSE
        CREATE INDEX IF NOT EXISTS ix_coord_gm_coord ON ran.coord USING GIST(gm_coord);
    END IF;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    -- manage indexes
    CALL ran.setIndexCoordinates();
END
$$;
