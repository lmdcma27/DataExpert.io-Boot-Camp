
--1) the windows function to identify a change in the quality_class it must to replicate for the is_active column
--2) the sum running windows funciton must to include an or between the change of quality_class and is_active.



CREATE TABLE actors_history_scd (    
    actorid text,    
    quality_class quality_class,
    start_year integer,
    end_year integer,
    is_active boolean,
    current_year integer,
    PRIMARY KEY (actorid,start_year)
)


WITH has_changed AS (
    select 
        actorid,
        current_year,
        quality_class,            
        is_active,
        LAG(quality_class,1) 
            OVER (PARTITION BY actorid ORDER BY current_year) <> quality_class 
            OR LAG(quality_class,1) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
        AS did_change_quality,
        LAG(is_active,1) 
            OVER (PARTITION BY actorid ORDER BY current_year) <> is_active 
            OR LAG(is_active,1) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
        AS did_change_active
    FROM actors
), change_idenfified AS (
    SELECT
        actorid,
        current_year,
        quality_class,
        is_active,
        SUM(CASE WHEN (did_change_quality OR did_change_active) THEN 1 ELSE 0 END)
            OVER (PARTITION BY actorid ORDER BY current_year) as change_idenfified
        FROM has_changed
), grouped AS (

    SELECT
        actorid,
        change_idenfified,
        quality_class,
        is_active,        
        MIN(current_year) as start_year,
        MAX(current_year) as end_year,
        2024 AS current_year
    FROM change_idenfified
    GROUP BY 1,2,3,4

)
INSERT INTO actors_history_scd
SELECT 
    actorid,
    quality_class,
    start_year,
    end_year,
    is_active,    
    current_year
FROM grouped




