/***
 * FR: import INSEE administrative cuttings (municipality, district and supra)
 */

-- municipalities (except global ones, w/ districts)
INSERT INTO fr.insee_municipality
(
    millesime,
    codgeo,
    libgeo,
    dep,
    reg,
    epci,
    nature_epci,
    arr,
    cv
)
(
    SELECT
        millesime,
        "CODGEO",
        "LIBGEO",
        "DEP",
        "REG",
        "EPCI",
        "NATURE_EPCI",
        "ARR",
        "CANOV"
    FROM fr.tmp_insee_municipality
    -- "global" municipalities (w/ districts) are thought as supra-territory
    WHERE "CODGEO" NOT IN ('75056', '13055', '69123')
);

-- districts for Paris/Lyon/Marseille
INSERT INTO fr.insee_municipality
(
    millesime,
    codgeo,
    libgeo,
    com,
    dep,
    reg,
    epci,
    nature_epci,
    arr,
    cv
)
(
    SELECT
        millesime,
        "CODGEO",
        "LIBGEO",
        "COM",
        "DEP",
        "REG",
        "EPCI",
        "NATURE_EPCI",
        "ARR",
        "CANOV"
    FROM fr.tmp_insee_municipal_district
);

-- supra-territories
INSERT INTO fr.insee_supra
(
    millesime,
    nivgeo,
    codgeo,
    libgeo
)
(
    SELECT
        millesime,
        "NIVGEO",
        "CODGEO",
        "LIBGEO"
    FROM fr.tmp_insee_supra
);
INSERT INTO fr.insee_supra
(
    millesime,
    nivgeo,
    codgeo,
    libgeo
)
(
    SELECT
        millesime,
        'COM_GLOBALE_ARM',
        "CODGEO",
        "LIBGEO"
    FROM fr.tmp_insee_municipality
    WHERE "CODGEO" IN ('75056', '13055', '69123')
);
