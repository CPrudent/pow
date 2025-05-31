/***
 * add IO history
 */

CREATE TABLE IF NOT EXISTS public.io_history (
    id SERIAL NOT NULL, -- after INSERT, do: SELECT CURRVAL('io_history_id_seq')
    name VARCHAR(50) NOT NULL,
    date_exec_begin TIMESTAMP NOT NULL DEFAULT NOW(),
    date_exec_end TIMESTAMP,
    status VARCHAR(10) NOT NULL DEFAULT 'EN_COURS', -- [ERREUR, SUCCES]
    date_data_begin TIMESTAMP NOT NULL,
    date_data_end TIMESTAMP NOT NULL,
    nb_rows_todo INTEGER NULL,
    nb_rows_processed INTEGER NULL,
    attributes VARCHAR
);

DO $$
BEGIN
    IF column_exists('public', 'io_history', 'co_type') THEN
        ALTER TABLE public.io_history RENAME COLUMN co_type TO name;
    END IF;
    IF column_exists('public', 'io_history', 'dt_exec_begin') THEN
        ALTER TABLE public.io_history RENAME COLUMN dt_exec_begin TO date_exec_begin;
    END IF;
    IF column_exists('public', 'io_history', 'dt_exec_end') THEN
        ALTER TABLE public.io_history RENAME COLUMN dt_exec_end TO date_exec_end;
    END IF;
    IF column_exists('public', 'io_history', 'co_status') THEN
        ALTER TABLE public.io_history RENAME COLUMN co_status TO status;
    END IF;
    IF column_exists('public', 'io_history', 'dt_data_begin') THEN
        ALTER TABLE public.io_history RENAME COLUMN dt_data_begin TO date_data_begin;
    END IF;
    IF column_exists('public', 'io_history', 'dt_data_end') THEN
        ALTER TABLE public.io_history RENAME COLUMN dt_data_end TO date_data_end;
    END IF;
    IF column_exists('public', 'io_history', 'nb_rows_valid') THEN
        ALTER TABLE public.io_history DROP COLUMN nb_rows_valid;
    END IF;
    IF column_exists('public', 'io_history', 'co_status_integration') THEN
        ALTER TABLE public.io_history DROP COLUMN co_status_integration;
    END IF;
    IF column_exists('public', 'io_history', 'infos_data') THEN
        ALTER TABLE public.io_history RENAME COLUMN infos_data TO attributes;
    END IF;

    IF (SELECT is_nullable
        FROM information_schema.columns
        WHERE table_name = 'io_history' AND column_name = 'nb_rows_todo') = 'NO' THEN
        ALTER TABLE public.io_history ALTER COLUMN nb_rows_todo DROP NOT NULL;
    END IF;
    IF (SELECT is_nullable
        FROM information_schema.columns
        WHERE table_name = 'io_history' AND column_name = 'nb_rows_processed') = 'NO' THEN
        ALTER TABLE public.io_history ALTER COLUMN nb_rows_processed DROP NOT NULL;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS iux_io_history_id ON public.io_history(id);
DROP INDEX IF EXISTS ix_io_history_co_type;
CREATE INDEX IF NOT EXISTS ix_io_history_name ON public.io_history(name);

COMMENT ON TABLE public.io_history IS 'Historique des Entrées/Sorties';
SELECT set_column_comment('public', 'io_history', 'id', 'Identifiant de l''Entrée/Sortie');
SELECT set_column_comment('public', 'io_history', 'name', 'Nom de l''Entrée/Sortie');
SELECT set_column_comment('public', 'io_history', 'date_exec_begin', 'Début d''exécution');
SELECT set_column_comment('public', 'io_history', 'date_exec_end', 'Fin d''exécution');
SELECT set_column_comment('public', 'io_history', 'status', 'Etat : EN_COURS, SUCCES OU ERREUR');
SELECT set_column_comment('public', 'io_history', 'date_data_begin', 'Début des données');
SELECT set_column_comment('public', 'io_history', 'date_data_end', 'Fin des données');
SELECT set_column_comment('public', 'io_history', 'nb_rows_todo', 'Nb enregistrements à traiter');
SELECT set_column_comment('public', 'io_history', 'nb_rows_processed', 'Nb enregistrements traités');
SELECT set_column_comment('public', 'io_history', 'attributes', 'Informations supplémentaires');

-- get IO
SELECT public.drop_all_functions_if_exists('public', 'get_all_io');
SELECT public.drop_all_functions_if_exists('public', 'get_io');
CREATE OR REPLACE FUNCTION public.get_io(
    name TEXT,
    date_end TIMESTAMP,
    status VARCHAR DEFAULT 'SUCCES'
)
RETURNS SETOF public.io_history AS
$func$
BEGIN
    RETURN QUERY SELECT *
        FROM public.io_history h
        WHERE h.name = get_io.name
        AND date_data_end = get_io.date_end
        AND h.status = get_io.status
        --ORDER BY h.date_data_begin ?
    ;
END
$func$ LANGUAGE plpgsql;

-- get last IO
SELECT public.drop_all_functions_if_exists('public', 'get_last_io');
CREATE OR REPLACE FUNCTION public.get_last_io(
    name TEXT,
    status VARCHAR DEFAULT 'SUCCES'
)
RETURNS SETOF public.io_history AS
$func$
BEGIN
    RETURN QUERY
        SELECT *
        FROM public.io_history h
        WHERE h.name = get_last_io.name
            AND h.status = get_last_io.status
        ORDER BY h.date_data_end DESC, h.date_exec_end DESC
        LIMIT 1
    ;
END
$func$ LANGUAGE plpgsql;

-- get municipality from IO name
SELECT public.drop_all_functions_if_exists('public', 'get_municipality_from_io_name');
CREATE OR REPLACE FUNCTION public.get_municipality_from_io_name(
    name IN TEXT,
    municipality OUT VARCHAR
)
AS
$func$
BEGIN
    municipality := CASE
        WHEN name ~ '^FR-BAL-[0-9]' THEN SUBSTR(name, 8, 5)
        WHEN name ~ '^FR-LAPOSTE-.{5}-IRIS_GE' THEN SUBSTR(name, 12, 5)
        END
    ;

    IF municipality IS NULL THEN
        RAISE 'extraction code Commune non prévue pour IO(%)', name;
    END IF;
END
$func$ LANGUAGE plpgsql;

-- add IO history for LAPOSTE restored data, if not exists
DO $$
DECLARE
    _io VARCHAR;
    _ios VARCHAR[] :=
        ARRAY[
            'FR-ADDRESS-LAPOSTE',
            'FR-ADDRESS-LAPOSTE-DELIVERY-POINT',
            'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION',
            'FR-TERRITORY-LAPOSTE-ORGANIZATION'
        ];
    _i INT;
    _nb_rows INT[];
    _date_io TIMESTAMP;
    _io_history public.io_history%ROWTYPE;
BEGIN
    _io_history.id := 0;
    FOREACH _io IN ARRAY _ios LOOP
        -- IO already exists?
        _date_io := (public.get_last_io(name => _io)).date_data_end;
        IF _date_io IS NULL THEN
            _io_history.date_exec_begin := TIMEOFDAY()::TIMESTAMP;
            _io_history.attributes := CONCAT(
                '{ "import" : ',
                '{ "from" : "backup" } }'
                );
            IF _io = 'FR-ADDRESS-LAPOSTE' THEN
                SELECT
                    LEAST(
                        MIN(dt_reference),
                        MIN(dt_reference_l3),
                        MIN(dt_reference_numero),
                        MIN(dt_reference_voie),
                        MIN(dt_reference_za)
                    )
                INTO
                    _io_history.date_data_begin
                FROM
                    fr.laposte_address
                    ;

                SELECT
                    GREATEST(
                        MAX(dt_reference),
                        MAX(dt_reference_l3),
                        MAX(dt_reference_numero),
                        MAX(dt_reference_voie),
                        MAX(dt_reference_za)
                    )
                INTO
                    _io_history.date_data_end
                FROM
                    fr.laposte_address
                    ;

                _nb_rows[1] := COUNT(*) FROM fr.laposte_address;
                _nb_rows[2] := COUNT(*) FROM fr.laposte_address_area;
                _nb_rows[3] := COUNT(*) FROM fr.laposte_address_street;
                _nb_rows[4] := COUNT(*) FROM fr.laposte_address_housenumber;
                _nb_rows[5] := COUNT(*) FROM fr.laposte_address_complement;

                _io_history.nb_rows_todo := _nb_rows[1];
                FOR _i IN 2..5 LOOP
                    _io_history.nb_rows_todo := _io_history.nb_rows_todo + _nb_rows[_i];
                END LOOP;
                _io_history.attributes := CONCAT(
                    '{ "import" : ',
                    '{ "from" : "backup", "items" : ',
                    '{ "address" : { "rows" : ', _nb_rows[1], ' }',
                    ', "zone_address" : { "rows" : ', _nb_rows[2], ' }',
                    ', "street" : { "rows" : ', _nb_rows[3], ' }',
                    ', "housenumber" : { "rows" : ', _nb_rows[4], ' }',
                    ', "complement" : { "rows" : ', _nb_rows[5], ' }',
                    ' } } }'
                    );

            ELSIF _io = 'FR-ADDRESS-LAPOSTE-DELIVERY-POINT' THEN
                SELECT
                    MIN(pdi_dt_modification),
                    MAX(pdi_dt_modification)
                INTO
                    _io_history.date_data_begin,
                    _io_history.date_data_end
                FROM
                    fr.laposte_delivery_point
                    ;

                _io_history.nb_rows_todo := COUNT(*) FROM fr.laposte_delivery_point;
            ELSIF _io = 'FR-ADDRESS-LAPOSTE-DELIVERY-ORGANIZATION' THEN
                _io_history.nb_rows_todo := COUNT(*) FROM fr.laposte_delivery_address;
                _io_history.date_data_end := '2022-12-09'::DATE;
                _io_history.date_data_begin := _io_history.date_data_end;
            ELSE
                _io_history.nb_rows_todo := COUNT(*) FROM fr.laposte_organization;
                SELECT
                    TO_DATE(
                        SUBSTR(
                            LEAST(
                                MIN(date_creation),
                                MIN(date_modification)
                            ),
                            3
                        ),
                        'YY-MM-DD'
                    )
                INTO
                    _io_history.date_data_begin
                FROM
                    fr.laposte_organization_all
                    ;
                SELECT
                    TO_DATE(
                        GREATEST(
                            MAX(date_creation),
                            MAX(date_modification)
                        ),
                        'YY-MM-DD'
                    )
                INTO
                    _io_history.date_data_end
                FROM
                    fr.laposte_organization_all
                WHERE
                    date_creation <= date_modification
                    ;
            END IF;

            -- SERIAL not called
            SELECT
                COALESCE(MAX(id), 0) +1
            INTO
                _io_history.id
            FROM
                public.io_history;

            -- common values
            _io_history.name := _io;
            _io_history.status := 'SUCCES';
            _io_history.nb_rows_processed := _io_history.nb_rows_todo;
            _io_history.date_exec_end := TIMEOFDAY()::TIMESTAMP;

            INSERT INTO public.io_history VALUES (_io_history.*);
        END IF;
    END LOOP;

    -- reset sequence
    IF _io_history.id > 0 THEN
        --ALTER SEQUENCE io_history_id_seq RESTART WITH _io_history.id +1;
        PERFORM setval('io_history_id_seq', _io_history.id +1);
    END IF;
END $$;
