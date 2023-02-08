/***
 * add IO history
 */

CREATE TABLE IF NOT EXISTS public.io_history (
    id SERIAL NOT NULL -- after INSERT, do: SELECT CURRVAL('io_history_id_seq')
    , co_type VARCHAR(50) NOT NULL
    , dt_debut_execution TIMESTAMP NOT NULL DEFAULT NOW()
    , dt_fin_execution TIMESTAMP
    , co_etat VARCHAR(10) DEFAULT 'EN_COURS' -- [ERREUR, SUCCES]
    , dt_debut_donnees TIMESTAMP NOT NULL
    , dt_fin_donnees TIMESTAMP NOT NULL
    , nb_enregistrements_a_traiter INTEGER NOT NULL
    , nb_enregistrements_traites INTEGER NOT NULL DEFAULT 0
    , nb_enregistrements_valides INTEGER NOT NULL DEFAULT 0
    , co_etat_integration VARCHAR(10) NULL -- useful ?
    , infos_data VARCHAR NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS iux_io_history_id ON public.io_history(id);
CREATE INDEX IF NOT EXISTS ix_io_history_co_type ON public.io_history(co_type);

COMMENT ON TABLE public.io_history IS 'Historique des Entrées/Sorties';
SELECT set_column_comment('public','io_history','id','Identifiant de l''Entrée/Sortie');
SELECT set_column_comment('public','io_history','co_type','Code du type de l''Entrée/Sortie, exemple : GEOPAD_PDI, RAN_ADRESSE, etc ...');
SELECT set_column_comment('public','io_history','dt_debut_execution','Début d''exécution');
SELECT set_column_comment('public','io_history','dt_fin_execution','Fin d''exécution');
SELECT set_column_comment('public','io_history','co_etat','Etat : EN_COURS, SUCCES OU ERREUR');
SELECT set_column_comment('public','io_history','dt_debut_donnees','Début des données');
SELECT set_column_comment('public','io_history','dt_fin_donnees','Fin des données');
SELECT set_column_comment('public','io_history','nb_enregistrements_a_traiter','Nb enregistrements à traiter');
SELECT set_column_comment('public','io_history','nb_enregistrements_traites','Nb enregistrements traités');
SELECT set_column_comment('public','io_history','nb_enregistrements_valides','Nb enregistrements validés');
SELECT set_column_comment('public','io_history','co_etat_integration','Etat intégration des données : NULL (à intégrer)','Valeurs possibles : EN_COURS, ERREUR ou SUCCES');
SELECT set_column_comment('public','io_history','infos_data','Informations supplémentaires');

-- get all IO
SELECT public.drop_all_functions_if_exists('public','get_all_io');
CREATE OR REPLACE FUNCTION public.get_all_io(
    type_in TEXT
    , date_end TIMESTAMP
    , status_in IN VARCHAR DEFAULT 'SUCCES'
    )
RETURNS SETOF public.io_history AS
$func$
BEGIN
    RETURN QUERY SELECT *
        FROM public.io_history
        WHERE co_type ~ type_in
        AND dt_fin_donnees = date_end
        AND co_etat = status_in
        ORDER BY dt_debut_donnees
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
        WHERE co_type ~ type_in
            AND co_etat = status_in
        ORDER BY dt_fin_donnees DESC
        LIMIT 1
    ;
END
$func$ LANGUAGE plpgsql;
