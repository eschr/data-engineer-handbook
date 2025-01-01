SELECT * from players where player_name = 'Vince Carter';

DROP TABLE players;

 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );

INSERT INTO players
WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2022
),
    today AS (
        SELECT * FROM player_seasons
        WHERE season = 2023
    )

SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
    CASE WHEN y.seasons IS NULL
        THEN ARRAY[ROW(
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats]
        WHEN t.season IS NOT NULL THEN y.seasons || ARRAY[ROW(
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats]
        ELSE y.seasons
    END as seasons,
    CASE
        WHEN t.season IS NOT NULL THEN
            CASE WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'
                ELSE 'bad'
            END::scoring_class
        ELSE y.scoring_class
    END as scoring_class,

    CASE WHEN t.season IS NOT NULL THEN 0
        ELSE y.years_since_last_active + 1
    END AS years_since_last_season,

    t.season IS NOT NULL as is_active,
    COALESCE(t.season, y.current_season + 1) as current_season
FROM today t FULL OUTER JOIN yesterday y
    ON t.player_name = y.player_name;

DROP TABLE players_scd;
-- create slowly changing dimensions type 2 for players table
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    current_season INTEGER,
    start_season INTEGER,
    end_season INTEGER,
    PRIMARY KEY (player_name, start_season)
);

INSERT INTO players_scd
WITH with_previous AS (
               SELECT
                player_name,
                current_season,
                scoring_class,
                is_active,
                LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
                LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
    FROM players
    WHERE current_season <= 2021
    ),
    with_indicators AS (
        SELECT
            *,
            CASE
                WHEN scoring_class <> previous_scoring_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END as change_indicator
    FROM with_previous
    ),
    with_streaks as (
        SELECT *,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
        FROM with_indicators
    )
SELECT
    player_name,
    scoring_class,
    is_active,
    2021 AS current_season,
    min(current_season) as start_season,
    max(current_season) as end_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class


WITH last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
    historical_scd AS (
        SELECT
            player_name,
            scoring_class,
            is_active,
            start_season,
            end_season
        FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
    ),
    this_season_data AS (
        SELECT *
        FROM players
        WHERE current_season = 2022
    ),
    unchanged_records AS (
        SELECT ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season AS end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
        WHERE ts.scoring_class = ls.scoring_class AND ts.is_active = ls.is_active
    ),
    changed_records AS (
        SELECT ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season
                        )::player_scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::player_scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
        WHERE (ts.scoring_class <> ls.scoring_class OR ts.is_active <> ls.is_active)
    ),
    unnested_changed_records AS (
        SELECT
            player_name,
            (records::player_scd_type).scoring_class,
            (records::player_scd_type).is_active,
            (records::player_scd_type).start_season,
            (records::player_scd_type).end_season
        FROM changed_records
    ),
    new_records AS (
        SELECT
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ts.current_season AS start_season,
            ts.current_season AS end_season
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
            ON ts.player_name = ls.player_name
        WHERE ls.player_name IS NULL
    )

SELECT * FROM historical_scd

UNION ALL

SELECT* FROM unnested_changed_records

UNION ALL

SELECT * FROM new_records;



CREATE TYPE player_scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
);
