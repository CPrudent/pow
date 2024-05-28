/***
 * FR: add LAPOSTE/RAN street uniq (dictionary)
 */

/* NOTE
initialization will be done w/ constant
 */

DO $$
BEGIN
    IF table_exists(
            schema_name => 'fr',
            table_name => 'laposte_address_street_uniq'
        )
        AND
        NOT column_exists(
            schema_name => 'fr',
            table_name => 'laposte_address_street_uniq',
            column_name => 'as_words'
        ) THEN
        DROP TABLE fr.laposte_address_street_uniq;
    END IF;
END $$;

-- to store uniq name (as dictionary of street)
CREATE TABLE IF NOT EXISTS fr.laposte_address_street_uniq (
    id SERIAL NOT NULL,
    name VARCHAR NOT NULL,
    descriptors VARCHAR,
    as_words INT[],
    name_normalized VARCHAR,
    descriptors_normalized VARCHAR,
    as_words_normalized INT[],
    occurs INT,
    words TEXT[],
    nwords INT
)
;

SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_uniq_index');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_uniq_index(
    set_case VARCHAR DEFAULT 'ALL'             -- ALL | ONLY_BASE | ONLY_ATTRIBUTS
)
AS
$proc$
BEGIN
    IF set_case = ANY('{ALL,ONLY_BASE}') THEN
        CREATE UNIQUE INDEX IF NOT EXISTS ix_laposte_address_street_uniq_id ON fr.laposte_address_street_uniq (id);

        CREATE INDEX IF NOT EXISTS ix_laposte_address_street_uniq_name ON fr.laposte_address_street_uniq USING GIN(name GIN_TRGM_OPS);
    END IF;

    IF set_case = ANY('{ALL,ONLY_ATTRIBUTS}') THEN
        CREATE INDEX IF NOT EXISTS ix_laposte_address_street_uniq_name_normalized ON fr.laposte_address_street_uniq USING GIN(name_normalized GIN_TRGM_OPS)
        WHERE name_normalized IS NOT NULL;
    END IF;
END
$proc$ LANGUAGE plpgsql;

-- build street dictionnary w/ (normalized name, descriptors, words array, nof words)
SELECT drop_all_functions_if_exists('fr', 'set_laposte_address_street_uniq');
CREATE OR REPLACE PROCEDURE fr.set_laposte_address_street_uniq(
    set_case IN VARCHAR DEFAULT 'ALL'             -- ALL | DICTIONARY | ATTRIBUTS
)
AS
$proc$
DECLARE
    _nrows INT;
BEGIN
    IF NOT table_exists('fr', 'laposte_address_street') THEN
        RAISE 'Données LAPOSTE non présentes';
    END IF;

    CALL public.log_info('Dictionnaire des voies');

    /* NOTE
    1st section to initiate street-dictionary
    TODO: COUNT(DISTINCT co_voie) instead of COUNT(*) ?
    TODO: use street_view, to set fl_active AND fl_diffusable ?
     */
    IF set_case = ANY('{ALL,DICTIONARY}') THEN
        CALL public.log_info(' Purge');
        TRUNCATE TABLE fr.laposte_address_street_uniq;
        PERFORM public.drop_table_indexes('fr', 'laposte_address_street_uniq');

        CALL public.log_info(' Initialisation');
        -- reminder: words, nwords are initiated by trigger
        INSERT INTO fr.laposte_address_street_uniq(
            name,
            occurs
        )
        WITH
        name_uniq AS (
            SELECT
                lb_voie name,
                COUNT(*) occurs
            FROM
                fr.laposte_address_street s
            WHERE
                fl_active
            GROUP BY
                lb_voie
        )
        -- #1120726
        SELECT * FROM name_uniq
        ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(' Création: ', _nrows));

        CALL fr.set_laposte_address_street_uniq_index('ONLY_BASE');
        CALL public.log_info(' Indexation');
    END IF;

    /* NOTE
    this 2nd section is todo after fix of street-faults, view set_constant_address()
     */
    IF set_case = ANY('{ALL,ATTRIBUTS}') THEN
        CALL public.log_info(' Mise à jour (Attributs)');
        WITH
        name_attributs AS (
            SELECT
                u.id,
                nn.descriptors_as_words,
                nn.name_normalized_as_words,
                nn.descriptors_normalized_as_words,
                nn.as_words,
                CASE
                    WHEN nn.name_normalized_as_words IS NOT NULL THEN
                        fr.get_as_words_from_splited_value(
                            property_as_words => nn.name_normalized_as_words
                        )
                    END as_words_normalized
            FROM
                fr.laposte_address_street_uniq u
                    CROSS JOIN fr.normalize_street_name(u.name) nn
        )
        UPDATE fr.laposte_address_street_uniq u SET
            descriptors = ARRAY_TO_STRING(na.descriptors_as_words, ''),
            as_words = na.as_words,
            name_normalized = CASE
                WHEN na.name_normalized_as_words IS NOT NULL THEN
                    ARRAY_TO_STRING(na.name_normalized_as_words, ' ')
                END,
            descriptors_normalized = CASE
                WHEN na.name_normalized_as_words IS NOT NULL THEN
                    ARRAY_TO_STRING(na.descriptors_normalized_as_words, '')
                END,
            as_words_normalized = na.as_words_normalized
            FROM
                name_attributs na
            WHERE
                u.id = na.id
        ;
        GET DIAGNOSTICS _nrows = ROW_COUNT;
        CALL public.log_info(CONCAT(' Attributs: ', _nrows));

        CALL fr.set_laposte_address_street_uniq_index('ONLY_ATTRIBUTS');
        CALL public.log_info(' Indexation');
    END IF;
END
$proc$ LANGUAGE plpgsql;

/* TEST
CALL fr.set_laposte_address_street_uniq();

17:00:22.122 Dictionnaire des voies
17:00:22.122  Purge
17:00:22.146  Initialisation
17:01:01.256  Création: 1120726
17:01:06.927  Indexation

Query returned successfully in 45 secs 662 msec.
 */

SELECT drop_all_functions_if_exists('fr', 'laposte_address_street_uniq_fill_columns');
CREATE OR REPLACE FUNCTION fr.laposte_address_street_uniq_fill_columns(
)
RETURNS TRIGGER AS
$func$
DECLARE
    _words TEXT[];
BEGIN
    -- words, nwords
    IF (((TG_OP = 'UPDATE') AND (OLD.name != NEW.name))
        OR
        ((TG_OP = 'INSERT') AND (NEW.words IS NULL))) THEN
        --RAISE NOTICE 'begin % : OLD=%, NEW=%', TG_OP, OLD, NEW;
        _words := REGEXP_SPLIT_TO_ARRAY(NEW.name, '\s+');
        --RAISE NOTICE 'words=%', _words;
        NEW.words := _words;
        NEW.nwords := ARRAY_LENGTH(_words, 1);
        --RAISE NOTICE 'end % : OLD=%, NEW=%', TG_OP, OLD, NEW;
    END IF;

    RETURN NEW;
END
$func$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS before_laposte_address_street_uniq ON fr.laposte_address_street_uniq;
CREATE TRIGGER before_laposte_address_street_uniq
    BEFORE INSERT OR UPDATE OF name
        ON fr.laposte_address_street_uniq
    FOR EACH ROW
    EXECUTE PROCEDURE fr.laposte_address_street_uniq_fill_columns();
