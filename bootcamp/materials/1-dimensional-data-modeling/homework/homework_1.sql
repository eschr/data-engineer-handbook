-- CREATE FILM STRUCT
CREATE TYPE film_struct
    AS (
        film TEXT,
        votes INTEGER,
        rating REAL,
        filmid TEXT
    );

-- CREATE RATING AS ENUM
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- CREATE ACTORS TABLE
CREATE TABLE actors (
    actor TEXT,
    actor_id TEXT,
    films film_struct[],
    quality_class quality_class,
    is_active BOOLEAN,
    year INTEGER,
    PRIMARY KEY(actor_id, year)
);

-- Cumulative table generation query: Write a query that populates the actors table one year at a time
WITH last_year AS (
    SELECT * FROM actors
    WHERE year = 1999
),
this_year AS (
    SELECT
        actor,
        actorid,
        year,
        ARRAY_AGG(ROW(
                film,
                votes,
                rating,
                filmid
            )::film_struct) as films,
        avg(rating) as avg_rating
        FROM actor_films
        WHERE year = 2000
        GROUP BY actor, actorid, year
)
INSERT INTO actors
SELECT
    COALESCE(ty.actor, ly.actor) as actor,
    COALESCE(ty.actorid, ly.actor_id) as actor_id,
    COALESCE(ly.films, ARRAY[]::film_struct[]) ||
                     CASE WHEN ty.year IS NOT NULL THEN ty.films
                    END as films,

    CASE
        WHEN ty.year IS NOT NULL THEN
            CASE WHEN avg_rating > 8 THEN 'star'
                WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
                WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE ly.quality_class
    END as quality_class,

    CASE WHEN ty.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,

    COALESCE(ty.year, ly.year + 1) as year

FROM this_year ty FULL OUTER JOIN last_year ly
    ON ty.actorid = ly.actor_id;


-- CREATE DDL for actors history SCD
CREATE TABLE actors_history_scd (
    actor TEXT,
    actor_id TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER,
    PRIMARY KEY(actor_id, start_date, end_date)
);


-- Backfill query for actors_history_scd
WITH with_previous AS (
    SELECT
        actor,
        actor_id,
        year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY year) as previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY year) as previous_is_active
    FROM actors
    WHERE year <= 1995
),
    with_indicators AS (
        SELECT *,
            CASE
                WHEN quality_class <> previous_quality_class THEN 1
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END as change_indicator
        FROM with_previous
    ),
    with_streaks AS (
        SELECT *, SUM(change_indicator)
            OVER (PARTITION BY actor ORDER BY year) AS streak_identifier
        FROM with_indicators
    )
INSERT INTO actors_history_scd
    SELECT actor,
           actor_id,
           quality_class,
           is_active,
           MIN(year) as start_date,
           MAX(year) as end_date,
           1995 as current_year
    FROM with_streaks
    GROUP BY actor, actor_id, streak_identifier, is_active, quality_class
    ORDER BY actor

-- SCD TYPE FOR ARRAY OF VALUES
CREATE TYPE scd_type AS (
        quality_class quality_class,
        is_active BOOLEAN,
        start_date INTEGER,
        end_date INTEGER
                     )

-- Incremental building of actors_history_scd table
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 1995
    AND end_date = 1995
),
    historical_scd AS (
        SELECT
            actor,
            actor_id,
            quality_class,
            is_active,
            start_date,
            end_date
        FROM actors_history_scd
        WHERE current_year = 1995
        AND end_date < 1995
    ),

    this_year_data AS (
        SELECT * FROM actors
        WHERE year = 1996
    ),
    unchanged_records AS (
        SELECT
            tyd.actor,
            tyd.actor_id,
            tyd.quality_class,
            tyd.is_active,
            lyd.start_date,
            tyd.year as end_date
        FROM this_year_data tyd
        JOIN last_year_scd lyd
        ON lyd.actor_id = tyd.actor_id
        WHERE tyd.quality_class = lyd.quality_class
        AND tyd.is_active = lyd.is_active
    ),

    changed_records AS (
        SELECT
            tyd.actor,
            tyd.actor_id,
            UNNEST(ARRAY[
                ROW(
                    lyd.quality_class,
                    lyd.is_active,
                    lyd.start_date,
                    lyd.end_date
                    )::scd_type,
                ROW(
                    tyd.quality_class,
                    tyd.is_active,
                    tyd.year,
                    tyd.year
                    )::scd_type
                ]) AS records
        FROM this_year_data tyd
        LEFT JOIN last_year_scd lyd
        ON lyd.actor_id = tyd.actor_id
        WHERE (tyd.quality_class <> lyd.quality_class)
        OR (tyd.is_active <> lyd.is_active)
        OR lyd.actor IS NULL
    ),

    unnested_changed_records AS (
        SELECT
            actor,
            actor_id,
            (records::scd_type).quality_class,
            (records::scd_type).is_active,
            (records::scd_type).start_date,
            (records::scd_type).end_date
        FROM changed_records
    ),

    new_records AS (
        SELECT
            tyd.actor,
            tyd.actor_id,
            tyd.quality_class,
            tyd.is_active,
            tyd.year AS start_date,
            tyd.year AS end_date
        FROM this_year_data tyd
        LEFT JOIN last_year_scd lyd
            ON tyd.actor_id = lyd.actor_id
        WHERE lyd.actor_id IS NULL
    )

INSERT INTO actors_history_scd
SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * from unnested_changed_records

UNION ALL

SELECT * FROM new_records

