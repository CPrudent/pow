/***
 * FR: add INSEE municipality events
 */

CREATE TABLE IF NOT EXISTS fr.insee_municipality_event(
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

CREATE INDEX IF NOT EXISTS ix_insee_municipality_event_com_av ON fr.insee_municipality_event(com_av) WHERE typecom_av = 'COM' AND typecom_ap = 'COM';
CREATE INDEX IF NOT EXISTS ix_insee_municipality_event_com_ap ON fr.insee_municipality_event(com_ap) WHERE typecom_ap = 'COM' AND typecom_ap = 'COM';
