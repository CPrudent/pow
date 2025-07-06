/***
 * DB: add SCHEMAS/ROLES
 */

-- drop old roles (inherited from BCAA)
SELECT public.drop_all_functions_if_exists('public', 'drop_old_roles');
CREATE OR REPLACE PROCEDURE public.drop_old_roles()
AS
$proc$
DECLARE
    _role VARCHAR;
    _query TEXT;
BEGIN
    FOR _role IN SELECT UNNEST('{bal,divers,geopad,ign,insee,ran}'::VARCHAR[])
    LOOP
        --RAISE NOTICE 'role(%) exists : %', _role, role_exists(_role);
        IF role_exists(_role) THEN
            -- revoke privileges
            _query := CONCAT(
                FORMAT('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %s;', _role),
                FORMAT('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM %s;', _role),
                FORMAT('REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM %s;', _role),
                FORMAT('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA fr FROM %s;', _role),
                FORMAT('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA fr FROM %s;', _role),
                FORMAT('REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA fr FROM %s;', _role),
                FORMAT('REVOKE USAGE ON SCHEMA public FROM %s;', _role)
            );
            EXECUTE _query;

            --EXECUTE 'DROP ROLE $1' USING _role;
            EXECUTE FORMAT('DROP ROLE %s', _role);
        END IF;
    END LOOP;
END
$proc$ LANGUAGE plpgsql;

DO $SCHEMAS_ROLES$
BEGIN
    /*
     * old schemas inherited from BCAA, only necessary to restore data, see: restore.sh
     *
    -- LAPOSTE (DELIVERY)
    IF NOT role_exists('geopad') THEN
        CREATE ROLE geopad LOGIN
            ENCRYPTED PASSWORD 'md51d88f1c6ed1072c47354f19cc39f1305'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS geopad AUTHORIZATION geopad;

    -- LAPOSTE (ADDRESS)
    IF NOT role_exists('ran') THEN
        CREATE ROLE ran LOGIN
            ENCRYPTED PASSWORD 'md51c1f24e7a573be92a0b05d38cf33014d'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS ran AUTHORIZATION ran;

    -- INSEE
    IF NOT role_exists('insee') THEN
        CREATE ROLE insee LOGIN
            ENCRYPTED PASSWORD 'md53bbfa0dabeabd50b93cf05f8b4569cb0'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS insee AUTHORIZATION insee;

    -- IGN
    IF NOT role_exists('ign') THEN
        CREATE ROLE ign LOGIN
            ENCRYPTED PASSWORD 'md51f1f199a064196af86ec69dc1139ff84'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS ign AUTHORIZATION ign;

    -- BAL
    IF NOT role_exists('bal') THEN
        CREATE ROLE bal LOGIN
            ENCRYPTED PASSWORD 'md5bb9b3e5c33fd259eb62ff40b64f73440'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS bal AUTHORIZATION bal;

    -- OTHERS
    IF NOT role_exists('divers') THEN
        CREATE ROLE divers LOGIN
            ENCRYPTED PASSWORD 'md533bd6c15a1964f2f2b66554a418ecd6e'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS divers AUTHORIZATION divers;
     */

    /*
     * DON'T forget to modify libenv.sh w/ password for the new schema
     */

    -- FR-COUNTRY
    IF NOT role_exists('fr') THEN
        CREATE ROLE fr LOGIN
            ENCRYPTED PASSWORD 'md5220680a960c20fcc5663a47dd49ccfd5'
            NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    CREATE SCHEMA IF NOT EXISTS fr AUTHORIZATION fr;

END $SCHEMAS_ROLES$;
