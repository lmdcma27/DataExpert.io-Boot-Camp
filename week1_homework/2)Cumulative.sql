
WITH last_year AS (
    SELECT * FROM actors
    WHERE current_year=1991
), ungrouped AS (
    SELECT
    actor,
    actorid,    
    year as current_year,
    CASE 
        WHEN year IS NOT NULL THEN
                ARRAY[ROW(
                year,
                film,
                votes,
                rating,
                filmid)::films_type]
        ELSE ARRAY[]::films_type[] END as films
    FROM actor_films
    WHERE year=1992
), this_year AS (select 
        MAX(actor) as actor,
        actorid,
        MAX(current_year) as current_year,
        ARRAY_AGG(films) as films,
        AVG(rating) as rating        
    from ungrouped,
    LATERAL unnest(films) as rating    
    group by actorid,current_year
) 
INSERT INTO ACTORS
select 
    COALESCE(ly.actor,ty.actor) as actor,    
    COALESCE(ly.actorid,ty.actorid) as actorid,    
    COALESCE(ly.films, ARRAY[]::films_type[]
    ) || ty.films as films,
    CASE 
        WHEN ty.current_year IS NOT NULL THEN
            (CASE WHEN rating>8 THEN 'star'
                WHEN (rating> 7 AND rating <= 8) THEN 'good'
                WHEN (rating> 6 AND rating <= 7) THEN 'average'
                WHEN (rating <= 6) THEN 'bad'
        END
            )::quality_class
        ELSE ly.quality_class
        END AS "quality_class",
    ty.current_year IS NOT NULL as is_active,
    COALESCE(ly.current_year + 1,ty.current_year) as   current_year
from this_year ty
FULL OUTER JOIN last_year ly
on (ty.actorId=ly.actorId) 



select max(current_year) from actors


