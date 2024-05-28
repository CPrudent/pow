/***
 * FR: add INSEE municipality events
 */

CREATE TABLE IF NOT EXISTS fr.insee_municipality_event(
    mod SMALLINT,
    date_eff DATE NOT NULL,
    typecom_av VARCHAR,
    com_av CHAR(5),
    tncc_av SMALLINT,
    ncc_av VARCHAR,
    nccenr_av VARCHAR,
    libelle_av VARCHAR,
    typecom_ap VARCHAR,
    com_ap CHAR(5),
    tncc_ap SMALLINT,
    ncc_ap VARCHAR,
    nccenr_ap VARCHAR,
    libelle_ap VARCHAR
);

CREATE INDEX IF NOT EXISTS ix_insee_municipality_event_com_av ON fr.insee_municipality_event(com_av) WHERE typecom_av = 'COM' AND typecom_ap = 'COM';
CREATE INDEX IF NOT EXISTS ix_insee_municipality_event_com_ap ON fr.insee_municipality_event(com_ap) WHERE typecom_ap = 'COM' AND typecom_ap = 'COM';

/*
 * get (code, name) from events, even if multiple merges (for same municipality)
 */
SELECT public.drop_all_functions_if_exists('fr', 'get_municipalities_of_merge');
CREATE OR REPLACE FUNCTION fr.get_municipalities_of_merge(
    municipality_code VARCHAR,
    from_date VARCHAR DEFAULT '2009-01-01'
)
RETURNS SETOF fr.insee_municipality_event
LANGUAGE plpgsql AS
$func$
DECLARE
    _query TEXT;
BEGIN
    DROP TABLE IF EXISTS tmp_multiple_merges;
    CREATE TEMPORARY TABLE tmp_multiple_merges AS
        SELECT
            com_ap,
            ncc_ap,
            date_eff
        FROM
            fr.insee_municipality_event
        WHERE
            com_ap = municipality_code
            AND typecom_av = 'COM' AND typecom_ap = 'COM'
            AND mod BETWEEN 31 AND 34
            AND date_eff > from_date::DATE
        GROUP BY
            com_ap,
            ncc_ap,
            date_eff
            ;
    IF (SELECT COUNT(*) FROM tmp_multiple_merges) > 1 THEN
        /* NOTE
        exclude target municipality if merged again
        by comparing 'ncc' labels (due to difference of 'libelle')
        i.e. 28406 Eole-en-Beauce / Éole-en-Beauce
         */
        _query := '
            SELECT
                me.*
            FROM fr.insee_municipality_event me
                JOIN tmp_multiple_merges mm ON me.com_ap = mm.com_ap
            WHERE
                me.typecom_av = ''COM'' AND me.typecom_ap = ''COM''
                AND me.mod BETWEEN 31 AND 34
                AND me.date_eff = mm.date_eff
                AND me.ncc_av != mm.ncc_ap
            ';
    ELSE
        _query := '
            SELECT
                *
            FROM fr.insee_municipality_event
            WHERE
                com_ap = ''' || municipality_code || '''
                AND typecom_av = ''COM'' AND typecom_ap = ''COM''
                AND mod BETWEEN 31 AND 34
                AND date_eff > ''' || from_date || '''::DATE
            ';
    END IF;
    RETURN QUERY EXECUTE _query;
END
$func$;

/* TEST
multiple
SELECT * FROM fr.get_municipalities_of_merge('27198');
SELECT * FROM fr.get_municipalities_of_merge('28406');

simple
SELECT * FROM fr.get_municipalities_of_merge('73010');
 */
