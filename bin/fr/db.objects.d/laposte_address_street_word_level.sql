/***
 * FR: add LAPOSTE/RAN street words (by level)
 */

-- old name
DROP TABLE IF EXISTS fr.laposte_address_municipality_word;

-- to store words by municipality
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_word_level (
    nivgeo VARCHAR NOT NULL
    , codgeo VARCHAR NOT NULL
    , word VARCHAR NOT NULL
    , count INT NOT NULL
    , rank INT
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word_level_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word_level_index()
AS
$proc$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_laposte_address_street_word_level_ids_word ON fr.laposte_address_street_word_level (nivgeo, codgeo, word);
END
$proc$ LANGUAGE plpgsql;

-- build counters, ranks for each word (by level)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_word_level');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_word_level()
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street_word_descriptor') THEN
        RAISE 'Données LAPOSTE non suffisantes';
    END IF;

    CALL public.log_info('Gestion des mots dans les noms de voies par niveau');

    CALL public.log_info(' Purge');
    TRUNCATE TABLE fr.laposte_address_street_word_level;
    PERFORM public.drop_table_indexes('fr', 'laposte_address_street_word_level');

    CALL public.log_info(' Initialisation');
    INSERT INTO fr.laposte_address_street_word_level(
        nivgeo
        , codgeo
        , word
        , count
    )
    SELECT
        'ZA'
        , s.co_adr_za
        , sw.word
        , COUNT(*)
    FROM fr.street_view s
        JOIN fr.laposte_address_street_reference sr ON sr.address_id = s.co_adr
        JOIN fr.laposte_address_street_membership sm ON sm.name_id = sr.name_id
        JOIN fr.laposte_address_street_word_descriptor sw ON sw.word = sm.word
    GROUP BY
        s.co_adr_za
        , sw.word
    ;
    GET DIAGNOSTICS _nrows = ROW_COUNT;
    CALL public.log_info(CONCAT(' Comptage (mot): ', _nrows));

    -- generate supra levels
    IF fr.set_territory_supra(
        table_name => 'laposte_address_street_word_level'
        , schema_name => 'fr'
        , base_level => 'ZA'
        , columns_groupby => ARRAY['word']
        , columns_agg => ARRAY['count']
    )
    THEN
        CALL fr.set_laposte_address_street_word_level_index();
        CALL public.log_info(' Indexation');

        WITH
        word_rank AS (
            SELECT
                nivgeo
                , codgeo
                , word
                , ROW_NUMBER() OVER (PARTITION BY nivgeo, codgeo ORDER BY count DESC) "rank"
            FROM
                fr.laposte_address_street_word_level
        )
        UPDATE fr.laposte_address_street_word_level w SET
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
17:47:59.902 Gestion des mots dans les noms de voies par niveau
17:47:59.902  Purge
17:47:59.968  Initialisation
17:48:34.921  Comptage (mot): 2881848
17:48:34.922 Début Traitement GEO SUPRA  ZA de fr.laposte_address_street_word_level
17:48:41.437 Traitement GEO SUPRA ZA -> COM "INSERT INTO tmp_supra_fdcef83c" : 00:00:03 2835335 inserted
17:52:35.407 Traitement GEO SUPRA ZA -> CP "INSERT INTO tmp_supra_fdcef83c" : 00:03:53 2273800 inserted
17:54:34.408 Traitement GEO SUPRA COM -> COM_GLOBALE_ARM "INSERT INTO tmp_supra_fdcef83c" : 00:01:57 11144 inserted
17:54:43.252 Traitement GEO SUPRA CP -> PDC_PPDC "INSERT INTO tmp_supra_fdcef83c" : 00:00:08 1850554 inserted
17:55:02.009 Traitement GEO SUPRA COM -> EPCI "INSERT INTO tmp_supra_fdcef83c" : 00:00:18 1615459 inserted
17:55:19.400 Traitement GEO SUPRA COM -> CV "INSERT INTO tmp_supra_fdcef83c" : 00:00:17 1869841 inserted
17:55:36.365 Traitement GEO SUPRA COM -> ARR "INSERT INTO tmp_supra_fdcef83c" : 00:00:16 1240833 inserted
17:55:44.789 Traitement GEO SUPRA PDC_PPDC -> PPDC_PDC "INSERT INTO tmp_supra_fdcef83c" : 00:00:08 1258566 inserted
17:55:49.806 Traitement GEO SUPRA ARR -> DEP "INSERT INTO tmp_supra_fdcef83c" : 00:00:03 932902 inserted
17:55:50.929 Traitement GEO SUPRA PPDC_PDC -> DEX remplacé par PDC_PPDC -> DEX "SELECT public.get_bigger_suble" : 00:00:01
17:55:56.896 Traitement GEO SUPRA PDC_PPDC -> DEX "INSERT INTO tmp_supra_fdcef83c" : 00:00:05 593038 inserted
17:56:02.383 Traitement GEO SUPRA DEP -> REG "INSERT INTO tmp_supra_fdcef83c" : 00:00:03 598851 inserted
17:56:07.466 Traitement GEO SUPRA REG -> METROPOLE_DOM_TOM "INSERT INTO tmp_supra_fdcef83c" : 00:00:02 378949 inserted
17:56:12.972 Traitement GEO SUPRA METROPOLE_DOM_TOM -> PAYS "INSERT INTO tmp_supra_fdcef83c" : 00:00:02 369978 inserted
17:56:47.131 Traitement GEO SUPRA  "UPDATE tmp_supra_fdcef83c28f75" : 00:00:00 0 updated
17:56:47.450 Traitement GEO SUPRA  "DELETE FROM fr.laposte_address" : 00:00:00 0 deleted
17:58:16.832 Traitement GEO SUPRA  "INSERT INTO fr.laposte_address" : 00:01:29 15829250 affected
17:58:16.948 Fin Traitement GEO SUPRA  ZA de fr.laposte_address_street_word_level +15829250 (15829250 inserted - 0 deleted, 0 updated)
18:05:26.113  Rangs (mot) : 18711098
18:06:49.868  Indexation

Query returned successfully in 18 min 50 secs.
 */
