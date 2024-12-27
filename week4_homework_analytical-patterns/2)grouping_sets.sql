

-- for this problem is posible get the names instead of _id just by doing a join with the game table

WITH cte_grouping_sets AS (
    SELECT     
        gd.player_id,
        gd.team_id,
        g.season,
        SUM(CASE WHEN gd.pts IS NULL THEN 0 ELSE gd.pts END) as total_points,
        SUM(CASE WHEN g.home_team_wins IS NULL THEN 0 ELSE g.home_team_wins END) as total_wins
    FROM game_details gd
    LEFT JOIN games g ON  gd.game_id=g.game_id
    GROUP BY GROUPING SETS (
        (player_id,team_id),
        (player_id,season),
        (team_id)
    )
), 
--2.1 who scored the most points playing for one team?
scored_most_points_by_team_id AS (
    SELECT player_id,total_points FROM cte_grouping_sets
    WHERE player_id IS NOT NULL AND team_id IS NOT NULL
    ORDER BY total_points DESC
    LIMIT 1
),
--2.2 who scored the most points in one season?
scored_most_points_by_season AS (
    SELECT player_id,total_points FROM cte_grouping_sets
    WHERE player_id IS NOT NULL AND season IS NOT NULL
    ORDER BY total_points DESC
    LIMIT 1
),--2.3 which team has won the most games?
team_has_won_most_games AS (
    SELECT team_id,total_wins FROM cte_grouping_sets    
    WHERE team_id IS NOT NULL and player_id IS NULL AND season IS NULL
    ORDER BY total_wins DESC
    LIMIT 1
)select * from team_has_won_most_games

