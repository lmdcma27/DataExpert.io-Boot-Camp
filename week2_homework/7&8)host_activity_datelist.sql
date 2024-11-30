

drop TABLE monthly_host_activity_reduced

-- 7 DDL host_activity_reduced
    CREATE TABLE monthly_host_activity_reduced (        
        host text,
        hit_array numeric[],
        unique_visitors numeric[], 
        month_start date,
        first_found_date DATE,
        date_partition DATE,
        PRIMARY KEY (host)  
    )

-- 8 Incremental query host_activity_reduced
-- here user_id null or not null doesnt care, because just count the actiity

WITH yesterday AS (
    SELECT *
    FROM monthly_host_activity_reduced
    WHERE date_partition = '2023-01-01'
),
     today AS (
         SELECT host,
                DATE_TRUNC('day', DATE(event_time)) AS today_date,
                COUNT(1) as num_hits,
                COUNT(DISTINCT user_id) as unique_visitors
         FROM events
         WHERE DATE_TRUNC('day', DATE(event_time)) = DATE('2023-01-02')         
         GROUP BY host, DATE_TRUNC('day', DATE(event_time))
     )
INSERT INTO monthly_host_activity_reduced
SELECT
    COALESCE(y.host, t.host) AS host,
    COALESCE(y.hit_array,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-02') - DATE('2023-01-01')]))
        || ARRAY[t.num_hits] AS hits_array,    
    COALESCE(y.unique_visitors,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-02') - DATE('2023-01-01')]))
        || ARRAY[t.unique_visitors] AS unique_visitors,
    DATE('2023-01-01') as month_start,
    CASE WHEN y.first_found_date < t.today_date
        THEN y.first_found_date
        ELSE t.today_date
            END as first_found_date,
    DATE('2023-01-02') AS date_partition
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.host = t.host
limit 200


--AFTER THE FIRST INSERT, THE NEXT STEP IS an update to avoid dupications
-- or the primary key can be (host,date_partition)
WITH yesterday AS (
    SELECT *
    FROM monthly_host_activity_reduced
    WHERE date_partition = '2023-01-02'
),
     today AS (
         SELECT host,
                DATE_TRUNC('day', DATE(event_time)) AS today_date,
                COUNT(1) as num_hits,
                COUNT(DISTINCT user_id) as unique_visitors
         FROM events
         WHERE DATE_TRUNC('day', DATE(event_time)) = DATE('2023-01-03')         
         GROUP BY host, DATE_TRUNC('day', DATE(event_time))
     )
UPDATE monthly_host_activity_reduced m
SET
    hit_array = COALESCE(y.hit_array,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-03') - DATE('2023-01-01')]))
        || ARRAY[t.num_hits],
    unique_visitors = COALESCE(y.unique_visitors,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-01-03') - DATE('2023-01-01')]))
        || ARRAY[t.unique_visitors],
    first_found_date = CASE WHEN y.first_found_date < t.today_date
        THEN y.first_found_date
        ELSE t.today_date
    END,
    date_partition = DATE('2023-01-03')
FROM yesterday y
FULL OUTER JOIN today t
    ON y.host = t.host
WHERE m.host = COALESCE(y.host, t.host)
    AND m.date_partition = DATE('2023-01-02');
