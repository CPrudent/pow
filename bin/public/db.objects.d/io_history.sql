/***
 * add IO history
 */

CREATE TABLE IF NOT EXISTS public.io_history (
    id SERIAL NOT NULL -- after INSERT, do: SELECT CURRVAL('io_history_id_seq')
    , co_type VARCHAR(50) NOT NULL
    , dt_exec_begin TIMESTAMP NOT NULL DEFAULT NOW()
    , dt_exec_end TIMESTAMP
    , co_status VARCHAR(10) NOT NULL DEFAULT 'EN_COURS' -- [ERREUR, SUCCES]
    , dt_data_begin TIMESTAMP NOT NULL
    , dt_data_end TIMESTAMP NOT NULL
    , nb_rows_todo INTEGER NOT NULL
    , nb_rows_processed INTEGER NOT NULL DEFAULT 0
    , nb_rows_valid INTEGER                             -- useful ?
    , co_status_integration VARCHAR(10)                 -- useful ?
    , infos_data VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_io_history_id ON public.io_history(id);
DROP INDEX IF EXISTS uix_io_history_co_type;
CREATE INDEX IF NOT EXISTS ix_io_history_co_type ON public.io_history(co_type);

COMMENT ON TABLE public.io_history IS 'Historique des Entrées/Sorties';
SELECT set_column_comment('public', 'io_history', 'id', 'Identifiant de l''Entrée/Sortie');
SELECT set_column_comment('public', 'io_history', 'co_type', 'Nom de l''Entrée/Sortie');
SELECT set_column_comment('public', 'io_history', 'dt_exec_begin', 'Début d''exécution');
SELECT set_column_comment('public', 'io_history', 'dt_exec_end', 'Fin d''exécution');
SELECT set_column_comment('public', 'io_history', 'co_status', 'Etat : EN_COURS, SUCCES OU ERREUR');
SELECT set_column_comment('public', 'io_history', 'dt_data_begin', 'Début des données');
SELECT set_column_comment('public', 'io_history', 'dt_data_end', 'Fin des données');
SELECT set_column_comment('public', 'io_history', 'nb_rows_todo', 'Nb enregistrements à traiter');
SELECT set_column_comment('public', 'io_history', 'nb_rows_processed', 'Nb enregistrements traités');
SELECT set_column_comment('public', 'io_history', 'nb_rows_valid', 'Nb enregistrements validés');
SELECT set_column_comment('public', 'io_history', 'co_status_integration', 'Etat intégration des données : NULL (à intégrer)', 'Valeurs possibles : EN_COURS, ERREUR ou SUCCES');
SELECT set_column_comment('public', 'io_history', 'infos_data', 'Informations supplémentaires');

-- get all IO
SELECT public.drop_all_functions_if_exists('public', 'get_all_io');
CREATE OR REPLACE FUNCTION public.get_all_io(
    type_in TEXT
    , date_end TIMESTAMP
    , status_in VARCHAR DEFAULT 'SUCCES'
)
RETURNS SETOF public.io_history AS
$func$
BEGIN
    RETURN QUERY SELECT *
        FROM public.io_history
        WHERE co_type = type_in
        AND dt_data_end = date_end
        AND co_status = status_in
        ORDER BY dt_data_begin
    ;
END
$func$ LANGUAGE plpgsql;

-- get last IO
SELECT public.drop_all_functions_if_exists('public', 'get_last_io');
CREATE OR REPLACE FUNCTION public.get_last_io(
    type_in TEXT
    , status_in VARCHAR DEFAULT 'SUCCES'
)
RETURNS SETOF public.io_history AS
$func$
BEGIN
    RETURN QUERY
        SELECT *
        FROM public.io_history
        WHERE co_type = type_in
            AND co_status = status_in
        ORDER BY dt_data_end DESC
        LIMIT 1
    ;
END
$func$ LANGUAGE plpgsql;

-- add IO history for LAPOSTE restored data, if not exists
DO $$
DECLARE
    _io VARCHAR;
    _ios VARCHAR[] :=
        ARRAY[
            'LAPOSTE_ADDRESS'
            , 'LAPOSTE_DELIVERY_POINT'
            , 'LAPOSTE_DELIVERY_ADDRESS'
            , 'LAPOSTE_ORGANIZATION'
        ];
    _i INT;
    _nb_rows INT[];
    _date_io TIMESTAMP;
    _io_history public.io_history%ROWTYPE;
BEGIN
    _io_history.id := 0;
    FOREACH _io IN ARRAY _ios LOOP
        -- IO already exists?
        _date_io := (public.get_last_io(type_in => _io)).dt_data_end;
        IF _date_io IS NULL THEN
            _io_history.dt_exec_begin := TIMEOFDAY()::TIMESTAMP;
            _io_history.infos_data := CONCAT(
                '{ "import" : '
                , '{ "from" : "backup" } }'
                );
            IF _io = 'LAPOSTE_ADDRESS' THEN
                SELECT
                    GREATEST(
                        MAX(dt_reference)
                        , MAX(dt_reference_l3)
                        , MAX(dt_reference_numero)
                        , MAX(dt_reference_voie)
                        , MAX(dt_reference_za)
                    )
                INTO
                    _io_history.dt_data_end
                FROM
                    fr.laposte_address
                    ;

                /*
                SELECT COUNT(*) INTO _nb_rows[1] FROM fr.laposte_address;
                SELECT COUNT(*) INTO _nb_rows[2] FROM fr.laposte_zone_address;
                SELECT COUNT(*) INTO _nb_rows[3] FROM fr.laposte_street;
                SELECT COUNT(*) INTO _nb_rows[4] FROM fr.laposte_housenumber;
                SELECT COUNT(*) INTO _nb_rows[5] FROM fr.laposte_complement;
                 */
                _nb_rows[1] := COUNT(*) FROM fr.laposte_address;
                _nb_rows[2] := COUNT(*) FROM fr.laposte_zone_address;
                _nb_rows[3] := COUNT(*) FROM fr.laposte_street;
                _nb_rows[4] := COUNT(*) FROM fr.laposte_housenumber;
                _nb_rows[5] := COUNT(*) FROM fr.laposte_complement;

                _io_history.nb_rows_todo := _nb_rows[1];
                FOR _i IN 2..5 LOOP
                    _io_history.nb_rows_todo := _io_history.nb_rows_todo + _nb_rows[_i];
                END LOOP;
                _io_history.infos_data := CONCAT(
                    '{ "import" : '
                    , '{ "from" : "backup", "items" : '
                    , '{ "address" : { "rows" : ', _nb_rows[1], ' }'
                    , ', "zone_address" : { "rows" : ', _nb_rows[2], ' }'
                    , ', "street" : { "rows" : ', _nb_rows[3], ' }'
                    , ', "housenumber" : { "rows" : ', _nb_rows[4], ' }'
                    , ', "complement" : { "rows" : ', _nb_rows[5], ' }'
                    , ' } } }'
                    );

            ELSIF _io = 'LAPOSTE_DELIVERY_POINT' THEN
                SELECT
                    MAX(pdi_dt_modification)::DATE
                INTO
                    _io_history.dt_data_end
                FROM
                    fr.laposte_delivery_point
                    ;

                _io_history.nb_rows_todo := COUNT(*) FROM fr.laposte_delivery_point;
            ELSIF _io = 'LAPOSTE_DELIVERY_ADDRESS' THEN
                _io_history.nb_rows_todo := COUNT(*) FROM fr.laposte_delivery_address;
                _io_history.dt_data_end := '2022-12-09'::DATE;
            ELSE
                _io_history.nb_rows_todo := COUNT(*) FROM fr.laposte_organization;
                _io_history.dt_data_end := '2022-12-09'::DATE;
            END IF;

            -- SERIAL not called
            SELECT
                COALESCE(MAX(id), 0) +1
            INTO
                _io_history.id
            FROM
                public.io_history;

            -- common values
            _io_history.co_type := _io;
            _io_history.co_status := 'SUCCES';
            _io_history.dt_data_begin := _io_history.dt_data_end;
            _io_history.nb_rows_processed := _io_history.nb_rows_todo;
            _io_history.dt_exec_end := TIMEOFDAY()::TIMESTAMP;

            INSERT INTO public.io_history VALUES (_io_history.*);
        END IF;
    END LOOP;

    -- reset sequence
    IF _io_history.id > 0 THEN
        --ALTER SEQUENCE io_history_id_seq RESTART WITH _io_history.id +1;
        PERFORM setval('io_history_id_seq', _io_history.id +1);
    END IF;
END $$;
