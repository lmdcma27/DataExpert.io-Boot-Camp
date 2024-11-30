--##################TEST QUERIES########################
select count(*) from actor_films
limit 1000;
--#############################################

CREATE TYPE films_type AS (
    year integer,
    film TEXT,
    votes Integer,
    rating REAL,
    filmdid text
)



CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE ACTORS (
    actor TEXT,
    actorId Text,         
    films films_type[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year integer,
    PRIMARY KEY (actorId,current_year)
);







