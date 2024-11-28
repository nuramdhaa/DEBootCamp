CREATE TABLE host_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
     date DATE,
    PRIMARY KEY (host, date)
);

      DO $$
    DECLARE
       today_date DATE;
       yesterday_date DATE;
    BEGIN
       FOR today_date IN
           SELECT
            GENERATE_SERIES('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL)
           LOOP
            yesterday_date := today_date - INTERVAL '1 day';
    INSERT INTO host_cumulated
    WITH yesterday AS (
       SELECT
    *
    FROM host_cumulated
    WHERE date = yesterday_date
    ), today AS (
    SELECT
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS dates_active,
        COUNT(1) as num_events
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = today_date
    AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
    )
    SELECT
        COALESCE(t.host, y.host) AS host,
       COALESCE(y.host_activity_datelist,
               ARRAY[]::DATE[])
                || CASE WHEN
                    t.host IS NOT NULL
                    THEN ARRAY[t.dates_active]
                    ELSE ARRAY[]::DATE[]
                    END AS host_activity_datelist,
        COALESCE(t.dates_active, y.date + INTERVAL '1 day') AS date
        FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host;
      END LOOP;
    END; $$;


SELECT * FROM host_cumulated
order by 1;