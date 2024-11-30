

WITH no_duplicate_records AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id,team_id,player_id ORDER BY g.game_date_est) AS counter
    FROM game_details gd
    JOIN games g 
    ON gd.game_id =g.game_id    
)
SELECT
    game_date_est,
    season,
    team_id,    
    team_id=home_team_id AS dim_is_playing_at_home,
    player_id,
    player_name,
    start_position,
    COALESCE(POSITION('DNP' in comment),0)>0 AS dim_did_not_play,
    COALESCE(POSITION('DND' in comment),0)>0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment),0)>0 AS dim_not_with_team,
    CAST(SPLIT_PART(min,':',1) AS REAL) +
    CAST(SPLIT_PART(min,':',2) AS REAL)/60 AS minutes,    
    no_duplicate_records.min,
    fgm,
    fga,
    fg3m,
    fg3a,
    ftm,
    fta,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    "TO" AS turnovers,
     pf,
     pts,
     plus_minus
FROM no_duplicate_records
WHERE counter=1
limit 10