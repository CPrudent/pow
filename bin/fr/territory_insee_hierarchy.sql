/***
 * FR: import INSEE administrative cuttings (municipality, district and supra)
 */

-- municipalities (except global ones, w/ districts)
INSERT INTO fr.insee_municipality
(
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
    WHERE
        --"CODGEO" NOT IN ('75056', '13055', '69123')
        "CODGEO" !~
            '^(' ||
            (SELECT value FROM fr.constant WHERE usecase = 'FR_ADDRESS' AND key = 'MUNICIPALITY_DISTRICT')
            || ')$'
);

-- districts for Paris/Lyon/Marseille
INSERT INTO fr.insee_municipality
(
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
    nivgeo,
    codgeo,
    libgeo
)
(
    SELECT
        "NIVGEO",
        "CODGEO",
        "LIBGEO"
    FROM fr.tmp_insee_supra
);
INSERT INTO fr.insee_supra
(
    nivgeo,
    codgeo,
    libgeo
)
(
    SELECT
        'COM_GLOBALE_ARM',
        "CODGEO",
        "LIBGEO"
    FROM fr.tmp_insee_municipality
    WHERE
        --"CODGEO" IN ('75056', '13055', '69123')
        "CODGEO" ~
            '^(' ||
            (SELECT value FROM fr.constant WHERE usecase = 'FR_ADDRESS' AND key = 'MUNICIPALITY_DISTRICT')
            || ')$'
);
