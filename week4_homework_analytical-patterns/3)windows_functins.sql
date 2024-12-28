
SELECT 

FROM game_details gd 
LEFT JOIN games g on gd.game_id=g.game_id
LIMIT 2000

-- 3.1 - What is the most games a team has won in a 90 game stretch?
-- comment: the solution it's easy using two separeted windows function, but im not 
-- sure or i dont know a solution yet for this using only one windows function
WITH cte_cumulated_points AS (
    SELECT
        game_date_est,
        home_team_id,
        home_team_wins,
        SUM(home_team_wins) OVER (PARTITION BY home_team_id ORDER BY game_date_est, home_team_id) AS running_total_points
    FROM games
), cte_points_in_stretch_period AS (
    SELECT
        game_date_est,        
        home_team_id,
        home_team_wins,
        running_total_points-COALESCE(LAG(running_total_points,90) OVER (PARTITION BY home_team_id ORDER BY game_date_est, home_team_id),0) as points_in_stretch_period
    FROM cte_cumulated_points
) 
-- Here i use the home_team_id instead the team id, but i can be retrive with a join with game_details table
SELECT home_team_id,MAX(points_in_stretch_period) as max_won_games_per_team FROM cte_points_in_stretch_period
GROUP BY home_team_id

--3.2 - How many games in a row did LeBron James score over 10 points a game?

WITH cte_more_then_10_points as (
    SELECT 
        g.game_date_est,
        gd.player_id, 
        gd.player_name,
        gd.pts,
        CASE 
            WHEN gd.pts IS NULL OR gd.pts<11 THEN 1
            ELSE 0
        END as points_over_10
    FROM game_details gd LEFT JOIN
    games g ON gd.game_id=g.game_id
    WHERE gd.player_name='LeBron James'
), cte_cumulated_points AS (
    SELECT 
        *,
        CASE 
            WHEN points_over_10=1 THEN -1
            ELSE SUM(points_over_10) OVER (ORDER BY game_date_est)
        END as cumulative_sum
    FROM cte_more_then_10_points
) 
SELECT 
    MIN(game_date_est) AS start_date,
    MAX(game_date_est) As end_date,
    MAX(player_id) AS player_id,
    MAX(player_id) AS player_name,
    COUNT(cumulative_sum) AS consecutive_games
FROM cte_cumulated_points
WHERE cumulative_sum>=0
GROUP BY cumulative_sum
HAVING COUNT(cumulative_sum)>1
ORDER BY COUNT(cumulative_sum) desc


-- Solution explanation: about the question of the problem 'How many games in a row did LeBron James score over 10 points a game?'
-- it can exists many sequence of matches that fill that requiremente, so i calculated all the sequence of games where Lebron james scored
-- over 10 points in a match, each row of the output table gives the start_date,end_date and number of consecutive matches.
-- If the null value in the points column (pts) must be part of the count you just need to remove the case statement from cte_cumulated_points
-- table and left the windows function only.