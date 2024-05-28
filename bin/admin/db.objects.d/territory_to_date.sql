DO $$
DECLARE
    _query VARCHAR;
BEGIN
    IF NOT type_exists('public', 'territory_to_date_t') THEN
        _query := CONCAT(
            'CREATE TYPE ',
            'public.territory_to_date_t',
            ' AS (',
            ' code VARCHAR',
            ', code_previous VARCHAR',
            ', name VARCHAR',
            ', date_geography DATE',
            ', distribution NUMERIC',
            ', information TEXT',
            ', is_new BOOLEAN',
            ')'
        );
        EXECUTE _query;
    END IF;
END $$;
