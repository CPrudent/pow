/***
 * FR: import INSEE municipality events
 */

TRUNCATE TABLE fr.insee_municipality_event;
SELECT public.drop_table_indexes('insee', 'municipality_event');

INSERT INTO fr.insee_municipality_event(
    mod
    , date_eff
    , typecom_av
    , com_av
    , tncc_av
    , ncc_av
    , nccenr_av
    , libelle_av
    , typecom_ap
    , com_ap
    , tncc_ap
    , ncc_ap
    , nccenr_ap
    , libelle_ap
)
(
    SELECT DISTINCT -- doubles!, 21, 1977-01-01, COM, 89344 -> 89344
        mod::SMALLINT
        , date_eff::DATE
        , typecom_av
        , LPAD(com_av, 5, '0')
        , tncc_av::SMALLINT
        , ncc_av
        , nccenr_av
        , libelle_av
        , typecom_ap
        , LPAD(com_ap, 5, '0')
        , tncc_ap::SMALLINT
        , ncc_ap
        , nccenr_ap
        , libelle_ap
    FROM fr.insee_municipality_event_tmp
);

CREATE INDEX IF NOT EXISTS ix_insee_municipality_event_com_av ON fr.insee_municipality_event(com_av) WHERE typecom_av = 'COM' AND typecom_ap = 'COM';
CREATE INDEX IF NOT EXISTS ix_insee_municipality_event_com_ap ON fr.insee_municipality_event(com_ap) WHERE typecom_ap = 'COM' AND typecom_ap = 'COM';

--https://fr.wikipedia.org/wiki/Loisey-Culey : Au 1er janvier 2014, les communes devaient retrouver leur indépendance, mais la procédure est reportée au 1er janvier 2015, ne pouvant avoir lieu dans l'année précédant une échéance électorale. Cependant, lors des élections municipales de 2014, un maire est élu dans chaque commune, et finalement, par décision du tribunal le 1er juillet 2014, les deux communes sont indépendantes.
--> on retarde l'evenement au 1er juillet 2014
UPDATE fr.insee_municipality_event SET date_eff = TO_DATE('2014-07-01', 'YYYY-MM-DD')
WHERE date_eff = TO_DATE('2014-01-01', 'YYYY-MM-DD')
AND typecom_av = 'COM' AND typecom_ap = 'COM' 
AND com_av = '55298';

--https://fr.wikipedia.org/wiki/L%27Oudon : Un nouvel arrêté préfectoral, le 7 janvier 2014, fait de la commune de Notre-Dame-de-Fresnay le nouveau chef-lieu6. Afin de prendre en compte ce transfert de chef lieu, lors de la publication du COG 2016 l'INSEE décide de modifier le code commune de L'Oudon pour reprendre l'ancien code de Notre-Dame-de-Fresnay (14472).
--> on retarde l'evenement au 1er janvier 2016
UPDATE fr.insee_municipality_event SET date_eff = TO_DATE('2016-01-01', 'YYYY-MM-DD')
WHERE date_eff = TO_DATE('2014-01-07', 'YYYY-MM-DD')
AND typecom_av = 'COM' AND typecom_ap = 'COM' 
AND com_av = '14697';

-- drop temporary import
DROP TABLE IF EXISTS fr.insee_municipality_event_tmp;
