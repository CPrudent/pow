/***
 * add facilities to COMMENT
 */

-- get comment of table
SELECT public.drop_all_functions_if_exists('public', 'get_table_comment');
CREATE OR REPLACE FUNCTION get_table_comment(
    schema_name TEXT,
    table_name TEXT
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _comment CHARACTER VARYING;
BEGIN
    --NOTE : ok for table or view
    SELECT pg_description.description INTO _comment
    FROM pg_namespace
    INNER JOIN pg_class ON pg_class.relnamespace = pg_namespace.oid
    INNER JOIN pg_description ON pg_description.objoid = pg_class.oid AND pg_description.objsubid = 0
    WHERE (pg_namespace.nspname, pg_class.relname) = (schema_name, table_name);

    RETURN _comment;
END
$func$ LANGUAGE plpgsql;

-- get comment of column
SELECT public.drop_all_functions_if_exists('public', 'get_column_comment');
CREATE OR REPLACE FUNCTION get_column_comment(
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT
)
RETURNS CHARACTER VARYING AS
$func$
DECLARE
    _comment CHARACTER VARYING;
BEGIN
    SELECT pg_description.description INTO _comment
    FROM pg_namespace
    INNER JOIN pg_class ON pg_class.relnamespace = pg_namespace.oid
    INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
    INNER JOIN pg_description ON pg_description.objoid = pg_class.oid AND pg_description.objsubid = pg_attribute.attnum
    WHERE
        (pg_namespace.nspname, pg_class.relname, pg_attribute.attname) = (schema_name, table_name, column_name);

    RETURN _comment;
END
$func$ LANGUAGE plpgsql;

-- set comment of table
SELECT public.drop_all_functions_if_exists('public', 'set_table_comment');
CREATE OR REPLACE FUNCTION set_table_comment(
    schema_name TEXT,
    table_name TEXT,
    label_short TEXT,
    label_long TEXT DEFAULT NULL,
    description TEXT DEFAULT ''
)
RETURNS BOOLEAN AS
$func$
DECLARE
	_query CHARACTER VARYING;
	_comment CHARACTER VARYING;
BEGIN
    _comment := CONCAT(
        '{"libelle_court":',
        to_json(label_short),
        ', "libelle_long":',
        to_json(COALESCE(label_long, label_short)),
        ', "description":',
        to_json(description),
        '}'
    );
    _query := CONCAT(
        'COMMENT ON TABLE ',
        schema_name,
        '.',
        table_name,
        ' IS ''',
        REPLACE(_comment, '''', ''''''),
        ''';'
    );
    EXECUTE _query;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- set comment of column
SELECT public.drop_all_functions_if_exists('public', 'set_column_comment');
CREATE OR REPLACE FUNCTION set_column_comment(
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    label_short TEXT,
    label_long TEXT DEFAULT NULL,
    description TEXT DEFAULT '',
    business VARCHAR DEFAULT ''
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query CHARACTER VARYING;
    _comment CHARACTER VARYING;
BEGIN
	_comment := CONCAT(
        '{"libelle_court":',
        to_json(label_short),
        ', "libelle_long":',
        to_json(COALESCE(label_long, label_short)),
        ', "description":',
        to_json(description),
        ', "type_metier":',
        to_json(business),
        '}'
    );
	_query := CONCAT(
        'COMMENT ON COLUMN ',
        schema_name,
        '.',
        table_name,
        '.',
        column_name,
        ' IS ''',
        REPLACE(_comment, '''', ''''''),
        ''';'
    );
	EXECUTE _query;
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- set comment of column from another column
SELECT public.drop_all_functions_if_exists('public', 'copy_column_comment');
CREATE OR REPLACE FUNCTION copy_column_comment(
    schema_name_source TEXT,
    table_name_source TEXT,
    column_name_source TEXT,
    schema_name_target TEXT,
    table_name_target TEXT,
    column_name_target TEXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _query CHARACTER VARYING;
    _comment CHARACTER VARYING;
BEGIN
    _comment := get_column_comment(schema_name_source, table_name_source, column_name_source);
    IF _comment != '' THEN
        _query := CONCAT(
            'COMMENT ON COLUMN ',
            schema_name_target,
            '.',
            table_name_target,
            '.',
            column_name_target,
            ' IS ''',
            REPLACE(_comment, '''', ''''''),
            ''';'
        );
        EXECUTE _query;
    END IF;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- set comment of columns from another table
SELECT public.drop_all_functions_if_exists('public', 'copy_columns_comments');
CREATE OR REPLACE FUNCTION copy_columns_comments(
    schema_name_source TEXT,
    table_name_source TEXT,
    schema_name_target TEXT,
    table_name_target TEXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
    _record RECORD;
    _query CHARACTER VARYING;
BEGIN
	FOR _record IN (
        SELECT
            pg_attribute.attname,
            get_column_comment(
                schema_name_source,
                table_name_source,
                pg_attribute.attname
            ) AS new_comment
        FROM pg_namespace
            INNER JOIN pg_class ON pg_class.relnamespace = pg_namespace.oid
            INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
        WHERE (pg_namespace.nspname, pg_class.relname) = (schema_name_target, table_name_target)
    ) LOOP
        IF _record.new_comment != '' THEN
            _query := CONCAT(
                'COMMENT ON COLUMN ',
                schema_name_target,
                '.',
                table_name_target,
                '.',
                _record.attname,
                ' IS ''',
                REPLACE(_record.new_comment, '''', ''''''),
                ''';'
            );
            EXECUTE _query;
        END IF;
    END LOOP;
    RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- set comment of table from another table
SELECT public.drop_all_functions_if_exists('public', 'copy_table_comment');
CREATE OR REPLACE FUNCTION copy_table_comment(
    schema_name_source TEXT,
    table_name_source TEXT,
    schema_name_target TEXT,
    table_name_target TEXT,
    class_target TEXT DEFAULT 'TABLE'
)
RETURNS BOOLEAN AS
$func$
DECLARE
	_query CHARACTER VARYING;
	_comment_table CHARACTER VARYING;
	_comment_columns BOOLEAN;
BEGIN
	_comment_table := get_table_comment(schema_name_source, table_name_source);
	IF _comment_table != '' THEN
		_query := CONCAT(
            'COMMENT ON ',
            class_target,
            ' ',
            schema_name_target,
            '.',
            table_name_target,
            ' IS ''',
            REPLACE(_comment_table, '''', ''''''),
            ''';'
        );
		EXECUTE _query;
	END IF;
	_comment_columns := copy_columns_comments(schema_name_source, table_name_source, schema_name_target, table_name_target);
	RETURN _comment_columns;
END
$func$ LANGUAGE plpgsql;

-- set metadata of table (in its comment)
SELECT public.drop_all_functions_if_exists('public', 'set_table_metadata');
CREATE OR REPLACE FUNCTION set_table_metadata(
    schema_name TEXT,
    table_name TEXT,
    metadata TEXT
)
RETURNS BOOLEAN AS
$func$
DECLARE
	v_object_type VARCHAR;
BEGIN
	IF table_exists(schema_name, table_name) THEN v_object_type := 'TABLE';
	ELSIF view_exists(schema_name, table_name) THEN v_object_type := 'VIEW';
	ELSE RAISE 'Il n''existe pas de table ni de vue %.%', schema_name, table_name; END IF;

	EXECUTE CONCAT(
        'COMMENT ON ',
        v_object_type,
        ' ',
        schema_name,
        '.',
        table_name,
        ' IS ''',
        REPLACE(
            jsonb_merge(
                public.get_table_comment(schema_name, table_name)::JSONB,
                metadata::JSONB
            )::TEXT,
            '''',
            ''''''
        ),
        ''';'
    );
	RETURN TRUE;
END
$func$ LANGUAGE plpgsql;

-- get metadata of table (from its comment)
SELECT public.drop_all_functions_if_exists('public', 'get_table_metadata');
CREATE OR REPLACE FUNCTION get_table_metadata(
    schema_name TEXT,
    table_name TEXT
)
RETURNS jsonb AS
$func$
BEGIN
	RETURN public.get_table_comment(schema_name, table_name)::JSONB;
END
$func$ LANGUAGE plpgsql;

/* TEST
SELECT public.set_table_metadata('public', 'territoire_ign', '{"dtrgeo":"01/01/2019"}')
SELECT TO_DATE(public.get_table_metadata('public', 'territoire_ign')->>'dtrgeo', 'DD/MM/YYYY')
 */
