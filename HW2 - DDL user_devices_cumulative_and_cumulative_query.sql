CREATE TABLE deduped (
    user_id TEXT,
    browser_type TEXT,
    event_time DATE,
    row_num INT,
            PRIMARY KEY (user_id, browser_type, event_time)
);

INSERT INTO deduped
WITH deduped AS(
    SELECT
        e.user_id AS user_id,
         d.browser_type AS browser_type,
        DATE(CAST(e.event_time AS TIMESTAMP)) AS event_time,
        ROW_NUMBER() OVER (PARTITION BY user_id, d.browser_type ORDER BY 1) AS row_num
    FROM devices d
    JOIN events e ON e.device_id = d.device_id
    WHERE user_id IS NOT NULL
   ) SELECT
    user_id,
    browser_type,
     event_time,
     row_num
       FROM deduped
WHERE row_num = 1;


CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

DO $$
    DECLARE
       today_date DATE;
       yesterday_date DATE;
    BEGIN
       FOR yesterday_date IN
           SELECT *
               FROM GENERATE_SERIES('2023-01-20'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL)
           LOOP
           RAISE NOTICE 'Processing date:%', today_date;
            today_date :=  yesterday_date + INTERVAL '1 day';
INSERT INTO user_devices_cumulated
WITH yesterday AS (
        SELECT *
        FROM user_devices_cumulated
         WHERE date = CAST(yesterday_date AS DATE)
), today AS (
    SELECT
                        d.user_id::TEXT AS user_id,
                          d.browser_type AS browser_type,
                          DATE(CAST(d.event_time AS TIMESTAMP)) AS device_active,
                          COUNT(1)                              as num_events
                   FROM deduped d
                   WHERE DATE(CAST(d.event_time AS TIMESTAMP)) = CAST(today_date AS DATE)
                   GROUP BY 1, 2, 3
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
COALESCE(t.browser_type, y.browser_type) AS browser_type,
       COALESCE(y.device_activity_datelist, ARRAY[]::DATE[])
            || CASE WHEN t.user_id IS NOT NULL
                THEN ARRAY[CAST(t.device_active AS DATE)]
                ELSE ARRAY[]::DATE[]
                END AS device_activity_datelist,
    COALESCE(t.device_active, y.date + INTERVAL '1 day') as date
    FROM today t
   FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;
END LOOP;
END; $$;

SELECT * FROM user_devices_cumulated
ORDER BY user_id;
