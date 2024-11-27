

CREATE TYPE scd_type AS (
    quality_class quality_class,    
    start_year INTEGER,
    end_year INTEGER,
    is_active boolean
)


WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year=1991
    AND end_year=1991
), historical_scd AS (
    SELECT
        actorid,
        quality_class,
        start_year,
        end_year,
        is_active
    FROM actors_history_scd
    WHERE current_year=1991
    AND end_year<1991
), this_year_records AS (
    SELECT * FROM ACTORS
    WHERE current_year=1992
), unchanged_records AS (
    SELECT
        ty.actorid,
        ty.quality_class,
        ly.start_year,
        ty.current_year as end_year,
        ty.is_active
    FROM this_year_records ty
    JOIN last_year_scd ly
    ON ly.actorid=ty.actorid
    WHERE ty.quality_class=ly.quality_class
    AND ty.is_active=ly.is_active
), changed_records AS (
    SELECT 
        ty.actorid,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.start_year,
                ly.end_year,
                ly.is_active
            )::scd_type,
            ROW(
                ty.quality_class,
                ty.current_year,
                ty.current_year,
                ty.is_active
            )::scd_type        
        ]) as records
    FROM this_year_records ty
    LEFT JOIN last_year_scd ly
    ON ty.actorid=ly.actorid
    WHERE (ty.quality_class<>ly.quality_class OR ty.is_active<>ly.is_active)
), unnested_changed_records AS (
    SELECT 
        actorid,
        (records::scd_type).quality_class,
        (records::scd_type).start_year,
        (records::scd_type).end_year,
        (records::scd_type).is_active
    FROM changed_records
), new_records AS (
    SELECT 
        ty.actorid,
        ty.quality_class,
        ty.current_year as start_year,
        ty.current_year as end_year,
        ty.is_active
    FROM this_year_records ty
    LEFT JOIN last_year_scd ly
    ON ty.actorid = ly.actorid
    WHERE ly.actorid IS NULL
)

--union the tables historical, unchanged_records, unnested_changed_records and new records





