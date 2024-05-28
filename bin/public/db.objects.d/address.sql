/***
 * ADDRESS
 */

CREATE TABLE IF NOT EXISTS public.address (
    id SERIAL NOT NULL,
    id_parent INT,
    id_territory INT,
    id_street INT,
    id_housenumber INT,
    id_complement INT
)
;

-- manual VACUUM
ALTER TABLE public.address SET (
	autovacuum_enabled = FALSE
);

SELECT drop_all_functions_if_exists('public', 'set_address_index');
CREATE OR REPLACE PROCEDURE public.set_address_index()
AS
$proc$
BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS iux_address_id ON public.address (id);
    CREATE INDEX IF NOT EXISTS ix_address_ids ON public.address (id_territory, id_street, id_housenumber, id_complement);

    /* useful ?
    DROP INDEX IF EXISTS ix_address_id_territory;
    DROP INDEX IF EXISTS ix_address_id_parent;
    DROP INDEX IF EXISTS ix_address_id_street;
    DROP INDEX IF EXISTS ix_address_id_housenumber;
    DROP INDEX IF EXISTS ix_address_id_complement;

    CREATE INDEX IF NOT EXISTS ix_address_id_parent ON public.address (id_parent);
    CREATE INDEX IF NOT EXISTS ix_address_id_street ON public.address (id_street);
    CREATE INDEX IF NOT EXISTS ix_address_id_housenumber ON public.address (id_housenumber);
    CREATE INDEX IF NOT EXISTS ix_address_id_complement ON public.address (id_complement);
     */
END
$proc$ LANGUAGE plpgsql;

SELECT drop_all_functions_if_exists('public', 'drop_address_index');
CREATE OR REPLACE PROCEDURE public.drop_address_index()
AS
$proc$
BEGIN
    DROP INDEX IF EXISTS iux_address_id;
    DROP INDEX IF EXISTS ix_address_ids;
END
$proc$ LANGUAGE plpgsql;

DO $$
BEGIN
    CALL public.set_address_index();
END
$$;

SELECT drop_all_functions_if_exists('public', 'set_address');
CREATE OR REPLACE PROCEDURE public.set_address(
    force BOOLEAN DEFAULT FALSE,
    drop_temporary BOOLEAN DEFAULT TRUE
)
AS
$proc$
DECLARE
    _schema_name VARCHAR;
    _procedure_name VARCHAR := 'push_address_to_public';
    _query TEXT;
BEGIN
    FOR _schema_name IN (
        SELECT schema_name FROM information_schema.schemata
        WHERE
            schema_name ~ '^..$'
    )
    LOOP
        IF procedure_exists(_schema_name, _procedure_name) THEN
            _query := CONCAT(
                'CALL ',
                _schema_name,
                '.',
                _procedure_name,
                '($1, $2)'
            );

            CALL public.log_info(CONCAT('Pays: ', UPPER(_schema_name)));
            EXECUTE _query USING force, drop_temporary;
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;
