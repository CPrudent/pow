/***
 * FR: add LAPOSTE/RAN complement words (by level)
 */

-- to store words by level
CREATE TABLE IF NOT EXISTS fr.laposte_address_complement_word_level (
    nivgeo VARCHAR NOT NULL
    , codgeo VARCHAR NOT NULL
    , word VARCHAR NOT NULL
    , count INT NOT NULL
    , rank INT
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_word_level_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_word_level_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_complement_word_level_ids_word ON fr.laposte_address_complement_word_level (nivgeo, codgeo, word);
END
$proc$ LANGUAGE plpgsql;

-- build counters, ranks for each word (by level)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_complement_word_level');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_complement_word_level()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_complement_word_descriptor') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion des mots dans les noms de complément (L3) par niveau');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_complement_word_level;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_complement_word_level');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_complement_word_level(
        nivgeo
        , codgeo
        , word
        , count
    )
    SELECT
        'ZA'
        , a.co_adr_za
        , sw.word
        , COUNT(*)
    FROM fr.address_view a
        JOIN fr.laposte_address_complement_reference sr ON sr.address_id = a.co_adr
        JOIN fr.laposte_address_complement_membership sm ON sm.name_id = sr.name_id
        JOIN fr.laposte_address_complement_word_descriptor sw ON sw.word = sm.word
    WHERE
        a.co_niveau = 'L3'
    GROUP BY
        a.co_adr_za
        , sw.word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Comptage (mot): ', _nrows));

    -- generate supra levels
    IF fr.set_territory_supra(
        table_name => 'laposte_address_complement_word_level'
        , schema_name => 'fr'
        , base_level => 'ZA'
        , columns_groupby => ARRAY['word']
        , columns_agg => ARRAY['count']
    )
    THEN
        CALL fr.set_laposte_address_complement_word_level_index();
        CALL public.log_info(' Indexation');

        WITH
        word_rank AS (
            SELECT
                nivgeo
                , codgeo
                , word
                , ROW_NUMBER() OVER (PARTITION BY nivgeo, codgeo ORDER BY count DESC) "rank"
            FROM
                fr.laposte_address_complement_word_level
        )
        UPDATE fr.laposte_address_complement_word_level w SET
            rank = r.rank
            FROM word_rank r
            WHERE
                (w.nivgeo, w.codgeo, w.word) = (r.nivgeo, r.codgeo, r.word)
        ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(' Rangs (mot) : ', _nrows));
    END IF;
END
$proc$ LANGUAGE plpgsql;

/* TEST
11:25:03.992 Gestion des mots dans les noms de complément (L3) par niveau
11:25:03.992  Purge
11:25:04.016  Initialisation
11:25:13.971  Comptage (mot): 481904
11:25:13.973 Début Traitement GEO SUPRA ZA de fr.laposte_address_complement_word_level
11:25:15.205 Traitement GEO SUPRA ZA -> COM "INSERT INTO tmp_supra_00927b76" : 00:00:00 470554 inserted
11:25:32.608 Traitement GEO SUPRA ZA -> CP "INSERT INTO tmp_supra_00927b76" : 00:00:17 447103 inserted
11:26:45.402 Traitement GEO SUPRA COM -> COM_GLOBALE_ARM "INSERT INTO tmp_supra_00927b76" : 00:01:07 7269 inserted
11:26:55.498 Traitement GEO SUPRA CP -> PDC_PPDC "INSERT INTO tmp_supra_00927b76" : 00:00:09 386789 inserted
11:26:58.457 Traitement GEO SUPRA COM -> EPCI "INSERT INTO tmp_supra_00927b76" : 00:00:02 310706 inserted
11:27:01.314 Traitement GEO SUPRA COM -> CV "INSERT INTO tmp_supra_00927b76" : 00:00:02 381015 inserted
11:27:04.197 Traitement GEO SUPRA COM -> ARR "INSERT INTO tmp_supra_00927b76" : 00:00:02 272230 inserted
11:27:07.993 Traitement GEO SUPRA PDC_PPDC -> PPDC_PDC "INSERT INTO tmp_supra_00927b76" : 00:00:03 296504 inserted
11:27:10.145 Traitement GEO SUPRA ARR -> DEP "INSERT INTO tmp_supra_00927b76" : 00:00:01 217171 inserted
11:27:11.357 Traitement GEO SUPRA PPDC_PDC -> DEX remplacé par PDC_PPDC -> DEX "SELECT public.get_bigger_suble" : 00:00:01
11:27:12.439 Traitement GEO SUPRA PDC_PPDC -> DEX "INSERT INTO tmp_supra_00927b76" : 00:00:01 141663 inserted
11:27:14.690 Traitement GEO SUPRA DEP -> REG "INSERT INTO tmp_supra_00927b76" : 00:00:00 143399 inserted
11:27:17.149 Traitement GEO SUPRA REG -> METROPOLE_DOM_TOM "INSERT INTO tmp_supra_00927b76" : 00:00:00 86603 inserted
11:27:19.693 Traitement GEO SUPRA METROPOLE_DOM_TOM -> PAYS "INSERT INTO tmp_supra_00927b76" : 00:00:00 83416 inserted
11:27:26.101 Traitement GEO SUPRA  "UPDATE tmp_supra_00927b768bd6e" : 00:00:00 0 updated
11:27:26.135 Traitement GEO SUPRA  "DELETE FROM fr.laposte_address" : 00:00:00 0 deleted
11:27:29.289 Traitement GEO SUPRA  "INSERT INTO fr.laposte_address" : 00:00:03 3244422 affected
11:27:29.290 Fin Traitement GEO SUPRA  ZA de fr.laposte_address_complement_word_level +3244422 (3244422 inserted - 0 deleted, 0 updated)
11:27:39.306  Indexation
11:29:00.956  Rangs (mot) : 3726326

Query returned successfully in 4 min 1 secs.
 */
