-- 1. dedup game_details table
WITH depuded AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id, g.game_date_est) as row_num
    FROM game_details as gd
        JOIN games g ON gd.game_id = g.game_id
)
SELECT
    game_date_est,
    season,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
    player_id,
    player_name,
    start_position,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) +
        CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60
            AS minutes,
    fgm,
    fga,
    fg3m,
    fg3a,
    ftm,
    fta,
    oreb,
    dreb,
    reb,
    ast,
    blk,
    stl,
    "TO" as turn_overs,
    pf,
    pts,
    plus_minus
FROM depuded
WHERE row_num = 1;

-- 2. A DDL for an user_devices_cumulated
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_id TEXT,
    browser_type TEXT,
    -- The list of dates in the past where the user was active
    device_activity_datelist DATE[],
    -- Current date for the user
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

-- 3. A cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH devices_row_num AS (
    SELECT
        d.device_id,
        e.user_id,
        d.browser_type,
        e.event_time::date,
        ROW_NUMBER() OVER(PARTITION BY e.user_id, browser_type, event_time::date) as row_num
    FROM events as e
        JOIN devices d ON e.device_id = d.device_id
    WHERE e.user_id IS NOT NULL
),
    dedupd_devices AS (
        SELECT *
        FROM devices_row_num
        WHERE row_num = 1
    ),
    yesterday AS (
        SELECT
            *
        FROM user_devices_cumulated
        WHERE date = DATE('2023-01-30')
    ),
        today AS (
            SELECT
                  CAST(user_id AS TEXT),
                  CAST(device_id AS TEXT),
                  browser_type,
                  DATE(CAST(event_time AS TIMESTAMP)) as date
              FROM dedupd_devices
              WHERE
                  DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
                    AND device_id IS NOT NULL
              GROUP BY user_id, device_id, browser_type, DATE(CAST(event_time AS TIMESTAMP))
    )
SELECT
    COALESCE(t.user_id, y.user_id) as user_id,
    COALESCE(t.device_id, y.device_id) as device_id,
    COALESCE(t.browser_type, y.browser_type) as browser_type,
    CASE WHEN y.device_activity_datelist IS NULL
        THEN ARRAY[t.date]
        WHEN t.date IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.date] || y.device_activity_datelist
        END as device_activity_datelist,
    COALESCE(t.date, y.date + INTERVAL '1 day') AS date
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id AND t.browser_type = y.browser_type;

-- 4. A datelist_int generation query.
-- Convert the device_activity_datelist column into a datelist_int column
WITH devices AS (
    SELECT
        *
    FROM users_devices_cumulated
    WHERE date = DATE('2023-01-31')
),
    series AS (
        SELECT *
        FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')
            as series_date
    ),
    place_holder_ints AS (
        SELECT
            CASE WHEN
                device_activity_datelist @> ARRAY [DATE(series_date)]
            THEN POW(2, 32 - (date - DATE(series_date)))
            ELSE 0
            END as placeholder_int_value,
            *
        FROM devices CROSS JOIN series
    )
SELECT
    user_id,
    SUM(placeholder_int_value) AS datelist_int,
    device_id,
    browser_type
FROM place_holder_ints
GROUP BY user_id, device_id, browser_type

-- 5. A DDL for hosts_cumulated table
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    month_start DATE,
    PRIMARY KEY (host, month_start)
);

-- 6. The incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM hosts_cumulated
    WHERE date = DATE('2023-01-03')
),
    today AS (
            SELECT
                  host,
                  DATE(CAST(event_time AS TIMESTAMP)) as date
              FROM events
              WHERE
                  DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-04')
                    AND host IS NOT NULL
              GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
    )
SELECT
    COALESCE(t.host, y.host) as host,
    CASE WHEN y.host_activity_datelist IS NULL
        THEN ARRAY[t.date]
        WHEN t.date IS NULL THEN y.host_activity_datelist
        ELSE ARRAY[t.date] || y.host_activity_datelist
        END as dates_active,
    COALESCE(t.date, y.date + INTERVAL '1 day') AS date
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host;




