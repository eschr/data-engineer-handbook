-- create the session events sink
-- flink job counts the distinct number of urls per session stored in url_count
CREATE TABLE IF NOT EXISTS sessionized_events (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    url_count BIGINT
);

-- What is the average number of web events of a session from a user on Tech Creator?
SELECT
    host,
    AVG(url_count) AS avg_events_per_session
FROM sessionized_events
WHERE host LIKE '%techcreator.io'
GROUP BY host;

-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT
    host,
    AVG(url_count) AS avg_events_per_session
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;