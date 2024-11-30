
-- 5
CREATE TABLE hosts_cumulated (
    host text,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (host,date)
)

-- 6
WITH yesterday AS (
    select * from hosts_cumulated
    where date= DATE('2023-01-30')
), today AS (
    SELECT
        host,
        DATE(event_time) as date_active,
        ROW_NUMBER() OVER (PARTITION BY host) AS COUNTER
    FROM events
    WHERE DATE(event_time)='2023-01-31'    
)
INSERT INTO hosts_cumulated
SELECT
    COALESCE(y.host,t.host) as host,
    CASE WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE y.dates_active || ARRAY[t.date_active]
    END AS dates_active,
    COALESCE(t.date_active,y.date + INTERVAL '1 day') as date
FROM yesterday y
FULL OUTER JOIN today t
ON y.host=t.host
WHERE t.COUNTER=1