/***
 * DDL: INSEE municipality events
 */

-- temporary code to rename
DO $$
BEGIN
    IF table_exists('insee', 'district_event') THEN
        ALTER TABLE district_event RENAME TO municipality_event;
        ALTER INDEX IF EXISTS ix_district_event_com_av RENAME TO ix_municipality_event_com_av;
        ALTER INDEX IF EXISTS ix_district_event_com_ap RENAME TO ix_municipality_event_com_ap;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS insee.municipality_event(
    mod SMALLINT
    , date_eff DATE NOT NULL
    , typecom_av VARCHAR
    , com_av CHAR(5)
    , tncc_av SMALLINT
    , ncc_av VARCHAR
    , nccenr_av VARCHAR
    , libelle_av VARCHAR
    , typecom_ap VARCHAR
    , com_ap CHAR(5)
    , tncc_ap SMALLINT
    , ncc_ap VARCHAR
    , nccenr_ap VARCHAR
    , libelle_ap VARCHAR
);

CREATE INDEX IF NOT EXISTS ix_municipality_event_com_av ON insee.municipality_event(com_av) WHERE typecom_av = 'COM' AND typecom_ap = 'COM';
CREATE INDEX IF NOT EXISTS ix_municipality_event_com_ap ON insee.municipality_event(com_ap) WHERE typecom_ap = 'COM' AND typecom_ap = 'COM';
