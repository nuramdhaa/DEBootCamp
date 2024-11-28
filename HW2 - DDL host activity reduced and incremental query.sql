CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array REAL[],
    unique_visitors REAL[],
    PRIMARY KEY(month,host)
);

INSERT INTO host_activity_reduced
WITH daily_activity AS (
    SELECT
        DATE(event_time) AS date,
        host,
        COUNT(1) AS daily_hits,
        COUNT(DISTINCT user_id) AS daily_unique_visitors
FROM events
WHERE DATE(event_time) = DATE('2023-01-01')
GROUP BY 1,2
), yesterday_array AS (
    SELECT * FROM host_activity_reduced
             WHERE month = DATE('2023-01-01')
             )
SELECT
    COALESCE(ya.month, DATE_TRUNC('month', da.date)) AS month,
    COALESCE(da.host, ya.host) AS host,
CASE WHEN ya.hit_array IS NOT NULL THEN
ya.hit_array|| ARRAY[COALESCE(da.daily_hits, 0)]
    WHEN ya.hit_array IS NULL
        THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.daily_hits, 0)]
        END AS hit_array,
CASE WHEN ya.unique_visitors IS NOT NULL THEN
ya.unique_visitors|| ARRAY[COALESCE(da.daily_unique_visitors, 0)]
    WHEN ya.unique_visitors IS NULL
        THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.daily_unique_visitors, 0)]
        END AS unique_visitors
FROM daily_activity da
FULL OUTER JOIN yesterday_array ya
    ON da.host = ya.host
ON CONFLICT(month, host)
DO UPDATE
SET hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;
