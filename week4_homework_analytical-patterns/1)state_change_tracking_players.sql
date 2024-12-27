

CREATE TABLE state_change_tracking_players (
    player_name TEXT,
    status TEXT,
    season INTEGER,
    PRIMARY KEY (player_name,season)
)



-- delete from state_change_tracking_players

WITH last_season as (
    select * from state_change_tracking_players
    where season=2020
), this_season as (
    select * from player_seasons
    where season=2021
    AND player_name is not null
)  
INSERT INTO state_change_tracking_players
SELECT 
    COALESCE(ls.player_name,ts.player_name) as player_name,
    CASE 
        WHEN ls.status is null THEN 'New'
        WHEN (ls.status='Retired' or ls.status='Stayed Retired') and ts.season is null THEN 'Stayed Retired'
        WHEN (ls.status='New' or ls.status='Continued Playing' or ls.status='Returned from Retirement') and ts.season is null THEN 'Retired'
        WHEN (ls.status='Retired' or ls.status='Stayed Retired') and ts.season is not null THEN 'Returned from Retirement'
        WHEN (ls.status='New' or ls.status='Continued Playing' or ls.status='Returned from Retirement') and ts.season is not null THEN 'Continued Playing'
        ELSE NULL
    END AS status,
    COALESCE(ls.season + 1,ts.season)
FROM last_season  ls
FULL OUTER JOIN this_season  ts
ON ls.player_name=ts.player_name


-- queries for testing (ignoring)
select * from state_change_tracking_players

select * from state_change_tracking_players
where player_name='Xavier Silas'
ORDER BY season

select * from state_change_tracking_players
where status is null

select * from player_seasons where player_name='Xavier Silas'

    John Coker
    Terry Davis
    Hot Rod Williams
    Xavier Silas