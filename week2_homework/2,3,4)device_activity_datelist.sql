--test (ignore)

select 
    count(*),
    count(device_id),
    count(DISTINCT device_id)
from devices

select 
    count(*),
    count(user_id),
    count(DISTINCT device_id)
from events
WHERE DATE(event_time)='2023-01-09'

select * from devices
where device_id=11022104064902400000
limit 100


select 
    count(device_id),
    count(user_id),
    count(*)   
from events
WHERE DATE(event_time)='2023-01-09'
AND user_id IS NOT NULL
limit 100

SELECT
    e.*,
    d.browser_type,
    d.device_type,
    d.os_type
FROM events e
JOIN devices d
ON e.device_id=d.device_id
WHERE DATE(event_time)='2023-01-09'
AND e.user_id IS NOT NULL
LIMIT 100

-- DDL FOR NOT DUPLICATE DEVICES AND POPULATION

CREATE TABLE no_duplicate_devices (
    device_id text,
    browser_type text,
    browser_version_major integer,
    browser_version_minor integer,
    browser_version_patch integer,
    device_type text,
    device_version_major text,
    device_version_minor integer,
    device_version_patch integer,
    os_type text,
    os_version_major text,
    os_version_minor integer,
    os_version_patch integer,
    PRIMARY KEY (device_id,browser_type,device_type,os_type)
)


--BEFORE THE INSERTIONS COULD BE GOOD MAKE SOME CLEANING IN THE BROWSER_TYPE COLUMN
--AND JUST WORK WITH THE MAIN BROWSERS, IN OTHER CASE IT CAN BE MAPPED TO OTHER
--USING A CASE STATEMENT, BUT I MY CASE I TAKE THE RAW BROWSER_TYPE COLUMN
INSERT INTO no_duplicate_devices
SELECT 
    CAST(device_id AS TEXT) as device_id,
    browser_type,
    browser_version_major,
    browser_version_minor,
    browser_version_patch,
    device_type,
    device_version_major,
    device_version_minor,
    device_version_patch,
    os_type,
    os_version_major,
    os_version_minor,
    os_version_patch    
FROM (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY device_id,browser_type,device_type,os_type
        ORDER BY device_id
    ) AS RANK
FROM
 devices
) T 
WHERE T.RANK=1

--2 DDL for users_devices_cumulated

CREATE TABLE user_devices_cumulated (
    user_id text,
    device_id text,
    browser_type text,
    dates_active DATE[],
    date Date,
    PRIMARY KEY (user_id,device_id,browser_type,date)
)

-- 3 device_activity_datelist

WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date='2023-01-30'
), today AS (    
    SELECT
        CAST(e.user_id AS TEXT) AS user_id,
        CAST(e.device_id AS TEXT) as device_id,
        DATE(e.event_time) as date_active,
        ndd.browser_type,
        ROW_NUMBER() OVER (PARTITION BY e.user_id,e.device_id,ndd.browser_type) AS COUNTER
    FROM events e
    JOIN no_duplicate_devices ndd
    ON CAST(e.device_id as TEXT)=ndd.device_id
    WHERE DATE(event_time)='2023-01-31'
    AND e.user_id IS NOT NULL    
)
INSERT INTO user_devices_cumulated
SELECT 
    COALESCE(y.user_id,t.user_id) as user_id,
    COALESCE(y.device_id,t.device_id) as device_id,
    COALESCE(y.browser_type,t.browser_type) as browser_type,
    CASE WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(t.date_active, y.date + Interval '1 day') as date
FROM today t
    FULL OUTER JOIN yesterday y
    ON (t.user_id=y.user_id AND t.device_id=y.device_id AND t.browser_type=y.browser_type)
WHERE t.COUNTER=1

-- 4 datelist_int 

-- ddl for user_device_datelist_int


CREATE TABLE user_device_datelist_int (
    user_id text,
    device_id text,
    browser_type text,
    datelist_int Date[],
    date Date,
    PRIMARY KEY (user_id,device_id,browser_type, date)
)

WITH starter AS (
    SELECT udc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           udc.user_id,
           udc.device_id,
           udc.browser_type
    FROM user_devices_cumulated udc
             CROSS JOIN
         (SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-01-31')
),
     bits AS (
         SELECT user_id,
                device_id,
                browser_type,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-01-31') as date
         FROM starter
         GROUP BY user_id,device_id,browser_type
     )    
SELECT * FROM bits