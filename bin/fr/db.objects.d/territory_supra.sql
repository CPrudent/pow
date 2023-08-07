/***
 * FR-TERRITORY : update SUPRA territories
 *
 * by aggregating sublevel
 * hierarchy being available as column (codgeo_<level>_parent) foreach levels
 */

SELECT drop_all_functions_if_exists('fr', 'set_territory_supra');
CREATE OR REPLACE FUNCTION fr.set_territory_supra(
    table_name VARCHAR
    , columns_agg TEXT[] DEFAULT NULL            -- NULL for all else list of column(s)
    , columns_agg_func JSONB DEFAULT NULL::JSONB -- specify function other than default for column(s)
    , columns_groupby TEXT[] DEFAULT NULL        -- idem
    , where_in TEXT DEFAULT NULL
    , base_level VARCHAR DEFAULT 'COM'
    , supra_level_filter VARCHAR DEFAULT NULL    -- reduce SUPRA to this level only
    , schema_name VARCHAR DEFAULT 'public'
    , update_mode BOOLEAN DEFAULT FALSE          -- only update columns defined by columns_agg (w/ existing levels)
    , simulation BOOLEAN DEFAULT FALSE
    , drop_temporary BOOLEAN DEFAULT TRUE
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query TEXT;
    _query_where TEXT;
    _query_join TEXT := 'source.nivgeo = destination.nivgeo AND source.codgeo = destination.codgeo';
    _query_row_equal TEXT;
    _tmp_table_name VARCHAR;
    _columns_insert TEXT;
    _columns_groupby TEXT;
    _columns_select TEXT;
    _columns_select_on_groupby TEXT;
    _columns_update_set TEXT;
    _columns_onconflict TEXT := 'nivgeo, codgeo';
    _column_information information_schema.columns%ROWTYPE;
    _column_type VARCHAR;
    _column_name TEXT;
    _nrows_deleted INTEGER := 0;
    _nrows_inserted INTEGER := 0;
    _nrows_updated INTEGER := 0;
    _nrows_affected INTEGER;
    _levels VARCHAR[];
    _level VARCHAR;
    _level2 VARCHAR;
    _bigger_sublevel VARCHAR;
    _self_use BOOLEAN := FALSE;
    _geometry_column_information RECORD;
    _start_time TIMESTAMP WITHOUT TIME ZONE;
    _notice VARCHAR := 'Traitement GEO SUPRA ';
BEGIN
    CALL public.log_info(
        message => CONCAT(
            'Début ', _notice, ' ', CONCAT_WS('/', base_level, supra_level_filter)
            , ' de ', schema_name, '.', table_name
        )
    );

    _query_where := NULLIF(where_in, '');
    FOREACH _column_name IN ARRAY get_table_columns(schema_name, table_name) LOOP
        IF _column_name IN ('nivgeo', 'codgeo', 'dtrgeo' , 'dt_reference_geo', 'libgeo', 'id_histo', 'nb_histo_use') THEN
            CONTINUE;
        ELSE
            IF _column_name LIKE 'codgeo_%_parent' AND NOT update_mode THEN
                IF NOT _self_use THEN
                    _self_use := TRUE;
                    _levels := NULL::VARCHAR[];
                END IF;
                _level := UPPER(REPLACE(REPLACE(_column_name, 'codgeo_', ''), '_parent', ''));
                _levels := ARRAY_APPEND(_levels, _level);
                _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('CASE WHEN $2::VARCHAR != ''', _level, ''' THEN UNIQUE_AGG(', CONCAT('source.', _column_name), ') END AS ', _column_name));
                /* NOTE
                 A moins d'être sûr qu'il s'agit bien d'une parenté (et on considère COM_GLOBALE_ARM comme le niveau COM)
                 On ne la retient que si le parent est parent de plus d'un enfant pour éviter de proposer des parentés absurdes (exemple : CP de COM, alors que CP = COM)
                 */
                _columns_select_on_groupby := CONCAT_WS(', ', _columns_select_on_groupby, CONCAT('CASE WHEN public.is_level_below(''fr'', CASE WHEN $2::VARCHAR = ''COM_GLOBALE_ARM'' THEN ''COM'' ELSE $2::VARCHAR END, ''', _level, ''') OR COUNT(', _column_name, ') OVER(PARTITION BY nivgeo, ', _column_name, ') > 1 THEN ', _column_name, ' END'));
                _columns_update_set := CONCAT_WS(', ', _columns_update_set, CONCAT(_column_name, ' = source.', _column_name));
            ELSIF _column_name IN ('dt_reference', 'dt_reference_data') OR (_column_name = ANY(columns_groupby)) THEN
                _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('source.', _column_name));
                _columns_groupby := CONCAT_WS(', ', _columns_groupby, CONCAT('source.', _column_name));
                _columns_select_on_groupby := CONCAT_WS(', ', _columns_select_on_groupby, _column_name);
                _query_join := CONCAT_WS(' AND ', _query_join, CONCAT('source.', _column_name, ' = destination.', _column_name));
                _columns_onconflict := CONCAT_WS(', ', _columns_onconflict, _column_name);
            ELSE
                IF columns_agg IS NOT NULL AND NOT (_column_name = ANY(columns_agg)) THEN CONTINUE; END IF;
                IF columns_agg_func IS NOT NULL AND columns_agg_func[_column_name] IS NOT NULL THEN
                    _columns_select := CONCAT_WS(', ', _columns_select, CONCAT(columns_agg_func[_column_name], '(source.', _column_name, ') AS ', _column_name));
                ELSE
                    _column_information := public.get_column_information(schema_name, table_name, _column_name);
                    --TODO : faire une fonction qui donne le type, et une autre qui donne le type général (chaine, numérique, ...)
                    _column_type := LOWER(COALESCE(NULLIF(_column_information.data_type, 'USER-DEFINED'), _column_information.udt_name));
                    IF _column_type IN ('numeric', 'integer', 'real', 'smallint', 'bigint', 'double precision') THEN
                        _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('SUM(source.', _column_name, ') AS ', _column_name));
                    ELSIF _column_type IN ('geometry') THEN
                        SELECT srid, type
                        INTO _geometry_column_information
                        FROM ext_postgis.geometry_columns
                        WHERE f_table_catalog = 'pow'
                        AND f_table_schema = schema_name
                        AND f_table_name = table_name
                        AND f_geometry_column = _column_name;
                        IF _geometry_column_information.type LIKE 'MULTI%' THEN
                            _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('ST_Multi(ST_Union(source.', _column_name, ')) AS ', _column_name));
                        ELSE
                            _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('ST_Union(source.', _column_name, ') AS ', _column_name));
                        END IF;
                    ELSIF _column_type IN ('array') THEN
                        RAISE NOTICE 'Type % non géré, veuillez recalculer les valeurs NULL de la colonne % de la table %.%', _column_type, _column_name, schema_name, table_name;
                        /* _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('NULL AS ', _column_name));
                            * Contournement pour avoir une valeur nulle avec le type de la colonne, utile uniquement au _columns_select_on_groupby : */
                        _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('NULLIF(FIRST(source.', _column_name, '), FIRST(source.', _column_name, ')) AS ', _column_name));
                    ELSE
                        _columns_select := CONCAT_WS(', ', _columns_select, CONCAT('UNIQUE_AGG(source.', _column_name, ') AS ', _column_name));
                    END IF;
                END IF;
                _columns_update_set := CONCAT_WS(', ', _columns_update_set, CONCAT(_column_name, ' = source.', _column_name));
                _columns_select_on_groupby := CONCAT_WS(', ', _columns_select_on_groupby, _column_name);
            END IF;
            _query_row_equal := CONCAT_WS(' AND ', _query_row_equal, CONCAT('destination.', _column_name, ' IS NOT DISTINCT FROM source.', _column_name));
        END IF;
        _columns_insert := CONCAT_WS(', ', _columns_insert, _column_name);
    END LOOP;

    IF simulation THEN
        RAISE NOTICE '_columns_select = %', _columns_select;
        RAISE NOTICE '_columns_insert = %', _columns_insert;
        RAISE NOTICE '_columns_select_on_groupby = %', _columns_select_on_groupby;
        RAISE NOTICE '_columns_groupby = %', _columns_groupby;
        RAISE NOTICE '_query_row_equal = %', _query_row_equal;
        RAISE NOTICE '_query_join = %', _query_join;
        RAISE NOTICE '_levels = %', _levels;
        RAISE NOTICE '_self_use = %', _self_use;
    END IF;

    _levels = public.get_levels(
        country => 'fr'
        , order_in => 'ASC'
        , among_levels => _levels --en cas de self use, on ordonne les niveaux
        , subfilter => base_level
    );

    /* NOTE
     Vérification des niveaux parents à générer
     On prend parmi les niveaux possibles
     */
    FOREACH _level IN ARRAY _levels
    LOOP
        IF NOT (
            --Ceux qui sont différents du niveau de base
            _level != base_level
            --Et si filtre sur niveau, dont le niveau filtré est un sous-découpage
            AND (supra_level_filter IS NULL OR public.is_level_below('fr', supra_level_filter, _level))
        )
        THEN
            _levels := ARRAY_REMOVE(_levels, _level);
        /*
            RAISE NOTICE 'retrait niveau %', _level;
        ELSE
            RAISE NOTICE 'garde niveau %', _level;
        */
        END IF;
    END LOOP;

    IF columns_groupby IS NULL THEN
        --On ajoute automatiquement la colonne dt_reference_data si existante (données historisées)
        columns_groupby := ARRAY[]::TEXT[];
        IF column_exists(schema_name, table_name, 'dt_reference_data') THEN
            columns_groupby := ARRAY_APPEND(columns_groupby, 'dt_reference_data');
        END IF;
        --RAISE NOTICE 'ajout de dt_reference_data dans columns_groupby';
    END IF;
    IF columns_agg IS NULL THEN
        --On ajoute automatiquement la colonne dt_reference_data
        columns_agg := ARRAY[]::TEXT[];
        FOREACH _column_name IN ARRAY get_table_columns(schema_name, table_name) LOOP
            IF _column_name NOT IN ('codgeo', 'nivgeo', 'typgeo', 'dtrgeo', 'dt_reference_geo', 'dt_reference_data')
                AND NOT(_column_name = ANY(columns_groupby)) THEN
                --RAISE NOTICE 'ajout de % dans columns_agg', _column_name;
                columns_agg := ARRAY_APPEND(columns_agg, _column_name);
            END IF;
        END LOOP;
    END IF;

    IF simulation THEN
        RAISE NOTICE 'columns_groupby = %', columns_groupby;
        RAISE NOTICE 'columns_agg = %', columns_agg;
        RAISE NOTICE '_levels = %', _levels;
    END IF;

    _tmp_table_name := CONCAT('tmp_supra_', MD5(CONCAT(table_name, _columns_insert)));
    _query := CONCAT(
        'CREATE TEMPORARY TABLE IF NOT EXISTS ', _tmp_table_name, '_base AS (SELECT nivgeo, codgeo, ', _columns_insert, ' FROM ', schema_name, '.', table_name, ' LIMIT 0) WITH NO DATA;
        CREATE TEMPORARY TABLE IF NOT EXISTS ', _tmp_table_name, ' AS (SELECT nivgeo, codgeo, ', _columns_insert, ', NULL::BOOLEAN AS already_exists FROM ', schema_name, '.', table_name, ' LIMIT 0) WITH NO DATA;
        TRUNCATE TABLE ', _tmp_table_name, '_base;
        TRUNCATE TABLE ', _tmp_table_name, ';
        DROP INDEX IF EXISTS ix_', _tmp_table_name, '_base_pk;
        DROP INDEX IF EXISTS ix_', _tmp_table_name, '_pk;
        INSERT INTO ', _tmp_table_name, '_base (
            nivgeo, codgeo, ', _columns_insert, '
        )
        (
            SELECT nivgeo, codgeo, ', _columns_insert, '
            FROM ', schema_name, '.', table_name, ' AS source
            WHERE source.nivgeo = $1'
            , CASE WHEN _query_where IS NOT NULL THEN CONCAT(' AND ', _query_where) END,
        ');
        CREATE UNIQUE INDEX iux_', _tmp_table_name, '_base_pk ON ', _tmp_table_name, '_base(', _columns_onconflict, ')'
    );
    IF NOT simulation THEN
        EXECUTE _query USING base_level;
    ELSE
        RAISE NOTICE '[$1=%]', base_level;
        RAISE NOTICE '%', _query;
    END IF;

    FOREACH _level IN ARRAY _levels LOOP
        _bigger_sublevel := public.get_bigger_sublevel(
            country => 'fr'
            , level_in => _level
            , among_levels => ARRAY_APPEND(_levels, base_level)
        );
        IF simulation THEN
            RAISE NOTICE ' _level : %', _level;
            RAISE NOTICE ' _bigger_sublevel : %', _bigger_sublevel;
        END IF;

        IF _level = 'COM' AND _bigger_sublevel IN ('COM_CP', 'IRIS') THEN
            _query := CONCAT(
                '(
                    SELECT
                        $2::VARCHAR AS nivgeo
                        , LEFT(source.codgeo, 5) AS codgeo
                        , ', _columns_select, '
                    FROM ', _tmp_table_name, CASE WHEN _bigger_sublevel = base_level THEN '_base' END, ' AS source
                    WHERE source.nivgeo = $1
                    GROUP BY LEFT(source.codgeo, 5)
                    ', CASE WHEN _columns_groupby IS NOT NULL THEN CONCAT(', ', _columns_groupby) END, '
                )'
            );
        /* NOTE
        need to get INSEE code from another source (for active ZA only), due to merge of municipalities : ZA's codgeo is composed by CONCAT (INSEE, postal) codes
        but is this necessary ? below query can do it, no ?
         */
        ELSIF _level = 'COM' AND _bigger_sublevel IN ('ZA') THEN
            _query := CONCAT(
                '(
                    SELECT
                        $2::VARCHAR AS nivgeo
                        , za.co_insee_commune AS codgeo
                        , ', _columns_select, '
                    FROM ', _tmp_table_name, CASE WHEN _bigger_sublevel = base_level THEN '_base' END, ' AS source
                    JOIN fr.laposte_zone_address za ON source.codgeo = za.co_cea AND za.fl_active
                    WHERE source.nivgeo = $1
                    GROUP BY za.co_insee_commune
                    ', CASE WHEN _columns_groupby IS NOT NULL THEN CONCAT(', ', _columns_groupby) END, '
                )'
            );
        ELSE
            --On prend le niveau le plus grand, représentant le mieux le niveau à calculer, en le comparant à ce qu'on pourrait obtenir avec le niveau de base
            IF _bigger_sublevel != base_level THEN
                _query := 'SELECT public.get_bigger_sublevel(country => ''fr'', level_in => $1, among_levels => ARRAY[$2';
                FOREACH _level2 IN ARRAY _levels LOOP
                    IF simulation THEN
                        RAISE NOTICE '_level2 = %', _level2;
                    END IF;
                    IF public.is_level_below('fr', _level2, _level) THEN
                        IF simulation THEN
                            RAISE NOTICE '_level2 % is below _level %', _level2, _level;
                        END IF;
                        _query := CONCAT(_query, ',
                            CASE WHEN
                                --premier test rapide : dans le cas du calcul de DEP à partir du niveau CV, comparé au niveau de base COM : le nombre de communes ayant un département parent est le même que le nombre de communes ayant un département parent ET un canton ville parent
                                COUNT(source.codgeo_', _level, '_parent) = COUNT(CASE WHEN source.codgeo_', _level, '_parent IS NOT NULL THEN source.codgeo_', _level2, '_parent END)
                                --deuxième test plus précis et plus couteux : dans le cas du calcul de DEP à partir du niveau CV, comparé au niveau de base COM : toutes les communes de chaque canton ville sont sur le même département, et toutes les communes sont représentées dans les cantons ville
                                AND COUNT(source.codgeo_', _level, '_parent) = (
                                    SELECT SUM(CASE WHEN unique_codgeo_', _level, '_parent IS NOT NULL THEN nb ELSE 0 END) FROM (
                                        SELECT codgeo_', _level2, '_parent, UNIQUE_AGG(codgeo_', _level, '_parent) AS unique_codgeo_', _level, '_parent
                                            , COUNT(codgeo_', _level, '_parent) AS nb
                                        FROM ', CASE WHEN _self_use THEN CONCAT(_tmp_table_name, '_base WHERE 1=1') ELSE 'fr.territory WHERE nivgeo = $2' END, '
                                        AND codgeo_', _level, '_parent IS NOT NULL
                                        AND codgeo_', _level2, '_parent IS NOT NULL
                                        GROUP BY codgeo_', _level2, '_parent
                                    ) AS sous_requete
                                )
                            THEN ''', _level2, ''' ELSE ''NULL'' END'
                        );
                    END IF;
                END LOOP;
                _query := CONCAT(_query, ']) AS nivgeo_agg FROM ', CASE WHEN _self_use THEN CONCAT(_tmp_table_name, '_base AS source') ELSE 'fr.territory AS source WHERE nivgeo = $2' END);
                IF NOT simulation THEN
                    _start_time := clock_timestamp();
                    EXECUTE _query INTO _level2 USING _level, base_level;
                    IF _level2 != _bigger_sublevel THEN
                        CALL public.log_info(
                            message => CONCAT(
                                _notice
                                , _bigger_sublevel, ' -> ', _level
                                , ' remplacé par '
                                , _level2, ' -> ', _level
                                , ' "', LEFT(_query, 30), '" : '
                                , TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS')
                            )
                        );
                        -- ALL IS HERE!
                        _bigger_sublevel := _level2;

                        --RAISE NOTICE 'Traitement GEO SUPRA % -> % remplacé par % -> % "%" : %', _bigger_sublevel, _level, _level2, _level, LEFT(_query, 30), TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS');
                    --ELSE
                    --	RAISE NOTICE 'Traitement GEO SUPRA % -> % gardé "%" : %', _bigger_sublevel, _level, LEFT(_query, 30), TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS');
                    END IF;
                ELSE
                    RAISE NOTICE '[$1=%, $2=%]', _level, base_level;
                    RAISE NOTICE '%', _query;
                END IF;
            END IF;

            IF _self_use THEN
                _query := CONCAT(
                    '(
                        SELECT nivgeo, codgeo, ', _columns_select_on_groupby, '
                        FROM (
                            SELECT
                                $2::VARCHAR AS nivgeo
                                , source.codgeo_', _level, '_parent AS codgeo
                                , ', _columns_select, '
                            FROM ', _tmp_table_name, CASE WHEN _bigger_sublevel = base_level THEN '_base' END, ' AS source
                            WHERE source.nivgeo = $1 AND source.codgeo_', _level, '_parent IS NOT NULL
                            GROUP BY source.codgeo_', _level, '_parent
                            ', CASE WHEN _columns_groupby IS NOT NULL THEN CONCAT(', ', _columns_groupby) END, '
                        ) AS query_groupby
                    )'
                );
            ELSE
                _query := CONCAT(
                    '(
                        SELECT
                            $2::VARCHAR AS nivgeo
                            , territory.codgeo_', _level, '_parent AS codgeo
                            , ', _columns_select, '
                        FROM ', _tmp_table_name, CASE WHEN _bigger_sublevel = base_level THEN '_base' END, ' AS source
                        INNER JOIN fr.territory ON (territory.nivgeo, territory.codgeo) = (source.nivgeo, source.codgeo)
                        WHERE territory.nivgeo = $1 AND territory.codgeo_', _level, '_parent IS NOT NULL
                        GROUP BY territory.codgeo_', _level, '_parent
                        ', CASE WHEN _columns_groupby IS NOT NULL THEN CONCAT(', ', _columns_groupby) END, '
                    )'
                );
            END IF;
        END IF;
        _query := CONCAT(
            'INSERT INTO ', _tmp_table_name, ' (
                nivgeo
                , codgeo
                , ', _columns_insert, '
            )', _query
        );
        IF NOT simulation THEN
            _start_time := clock_timestamp();
            EXECUTE _query USING _bigger_sublevel, _level;
            GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
            CALL public.log_info(
                message => CONCAT(
                    _notice
                    , _bigger_sublevel, ' -> ', _level
                    , ' "', LEFT(_query, 30), '" : '
                    , TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS')
                    , ' ', _nrows_affected, ' inserted'
                )
            );

            --RAISE NOTICE 'Traitement GEO SUPRA % -> % "%" : % : % inserted', _bigger_sublevel, _level, LEFT(_query, 30), TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS'), _nrows_affected;
        ELSE
            RAISE NOTICE '[$1=%, $2=%]', _bigger_sublevel, _level;
            RAISE NOTICE '%', _query;
        END IF;
    END LOOP;

    _query := CONCAT('CREATE UNIQUE INDEX iux_', _tmp_table_name, '_pk ON ', _tmp_table_name, '(', _columns_onconflict, ')');
    IF NOT simulation THEN EXECUTE _query; ELSE RAISE NOTICE '%', _query; END IF;

    IF NOT update_mode THEN
        _query := CONCAT(
            'UPDATE ', _tmp_table_name, ' AS destination
            SET already_exists = TRUE
            FROM ', schema_name, '.', table_name, ' AS source
            WHERE source.nivgeo = ANY($1)
            ', CASE WHEN _query_where IS NOT NULL THEN CONCAT(' AND ', _query_where) END, '
            AND ', _query_join, '
            AND ', _query_row_equal, '
            /* plus lent ?
            AND isEqual(
                in_rec_a => destination
                , in_rec_b => source
                , in_att_ignore => array[''id_histo'', ''nb_histo_use'']
            )
            */'
        );
        IF NOT simulation THEN
            _start_time := clock_timestamp();
            EXECUTE _query USING _levels;
            GET DIAGNOSTICS _nrows_affected = ROW_COUNT;
            CALL public.log_info(
                message => CONCAT(
                    _notice
                    , ' "', LEFT(_query, 30), '" : '
                    , TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS')
                    , ' ', _nrows_affected, ' updated'
                )
            );

            --RAISE NOTICE 'Traitement GEO SUPRA "%" : % : % updated', LEFT(_query, 30), TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS'), _nrows_affected;
        ELSE
            RAISE NOTICE '[$1=%]', _levels;
            RAISE NOTICE '%', _query;
        END IF;

        --On supprime les entrées sauf celles qui existent déjà dans le nouveau jeu de données à insérer
        _query := CONCAT(
            'DELETE FROM ', schema_name, '.', table_name, ' AS source
            WHERE source.nivgeo = ANY($1)
            ', CASE WHEN _query_where IS NOT NULL THEN CONCAT(' AND ', _query_where) END, '
            AND NOT EXISTS (
                SELECT 1
                FROM ', _tmp_table_name, ' AS destination
                WHERE ', _query_join, '
                AND already_exists
            )'
        );
        IF NOT simulation THEN
            _start_time := clock_timestamp();
            EXECUTE _query USING _levels;
            GET DIAGNOSTICS _nrows_deleted = ROW_COUNT;
            CALL public.log_info(
                message => CONCAT(
                    _notice
                    , ' "', LEFT(_query, 30), '" : '
                    , TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS')
                    , ' ', _nrows_deleted, ' deleted'
                )
            );

            --RAISE NOTICE 'Traitement GEO SUPRA "%" : % : % deleted', LEFT(_query, 30), TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS'), _nrows_deleted;
        ELSE
            RAISE NOTICE '[$1=%]', _levels;
            RAISE NOTICE '%', _query;
        END IF;

        _query := CONCAT(
            'INSERT INTO ', schema_name, '.', table_name, ' (
                nivgeo
                , codgeo
                , ', _columns_insert, '
            )
            (
                SELECT
                    nivgeo
                    , codgeo
                    , ', _columns_insert, '
                FROM ', _tmp_table_name, ' AS source
                WHERE already_exists IS NULL
            )'
        );
    ELSE
        _query := CONCAT(
            'UPDATE ', schema_name, '.', table_name, ' AS destination
            SET ', _columns_update_set, '
            FROM ', _tmp_table_name, ' AS source
            WHERE ', _query_join, '
            AND NOT(', _query_row_equal, ')
            ', CASE WHEN _query_where IS NOT NULL THEN CONCAT(' AND ', _query_where) END
        );
    END IF;
    IF NOT simulation THEN
        _start_time := clock_timestamp();
        EXECUTE _query USING base_level;
        IF NOT update_mode THEN
            GET DIAGNOSTICS _nrows_inserted = ROW_COUNT;
            _nrows_affected := _nrows_inserted;
        ELSE
            GET DIAGNOSTICS _nrows_updated = ROW_COUNT;
            _nrows_affected := _nrows_updated;
        END IF;
        CALL public.log_info(
            message => CONCAT(
                _notice
                , ' "', LEFT(_query, 30), '" : '
                , TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS')
                , ' ', _nrows_affected, ' affected'
            )
        );

        --RAISE NOTICE 'Traitement GEO SUPRA "%" : % : % affected', LEFT(_query, 30), TO_CHAR((clock_timestamp() - _start_time), 'HH24:MI:SS'), _nrows_affected;
    ELSE
        RAISE NOTICE '%', _query;
    END IF;

    CALL public.log_info(
        message => CONCAT(
            'Fin ', _notice, ' ', CONCAT_WS('/', base_level, supra_level_filter)
            , ' de ', schema_name, '.', table_name
            , ' '
            , CONCAT(
                CASE WHEN (_nrows_inserted-_nrows_deleted) >=0 THEN '+' ELSE '-' END
                , ABS(_nrows_inserted-_nrows_deleted)
            )
            , ' (', _nrows_inserted, ' inserted - ', _nrows_deleted, ' deleted, ', _nrows_updated, ' updated)'
        )
    );
    --RAISE NOTICE '% : Fin traitement GEO SUPRA % de %.% : % (% inserted - % deleted, % updated)', TO_CHAR(clock_timestamp(), 'HH24:MI:SS'), CONCAT_WS('/', base_level, supra_level_filter), schema_name, table_name, CONCAT(CASE WHEN (_nrows_inserted-_nrows_deleted) >=0 THEN '+' ELSE '-' END, ABS(_nrows_inserted-_nrows_deleted)), _nrows_inserted, _nrows_deleted, _nrows_updated;

    IF drop_temporary THEN
        _query := 'DROP TABLE ' || _tmp_table_name || '_base';
        EXECUTE _query;
        _query := 'DROP TABLE ' || _tmp_table_name;
        EXECUTE _query;
    END IF;

    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;
