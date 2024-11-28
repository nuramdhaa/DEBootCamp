

WITH user_devices AS (
    SELECT *
        FROM user_devices_cumulated udc
WHERE date = DATE('2023-01-31')
),
series AS (
    SELECT *
    FROM GENERATE_SERIES('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL)
        as series_date
), datelist_int AS (
SELECT
   *,
   CASE WHEN
    ud.device_activity_datelist @> ARRAY [DATE(series_date)]
        THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
        ELSE 0
            END AS datelist_int_values
FROM user_devices ud CROSS JOIN series)
SELECT
  di.user_id AS user_id,
  di.browser_type AS browser_type,
     CAST(CAST(SUM(datelist_int_values) AS BIGINT) AS BIT(32)) AS days_active,
    BIT_COUNT(CAST(CAST(SUM(datelist_int_values) AS BIGINT) AS BIT(32))) > 0
        AS dim_is_monthly_active,
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &
CAST(CAST(SUM(datelist_int_values) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active
FROM datelist_int di
GROUP BY 1,2;