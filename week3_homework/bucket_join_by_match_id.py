from pyspark.sql import SparkSession


query='''select 
        a.player_gamertag as player_gamertag_medal, 
        a.medal_id, 
        a.count, 
        b.*, 
        c.is_team_game, 
        c.playlist_id, 
        c.game_variant_id, 
        c.is_match_over, 
        c.completion_date, 
        c.match_duration, 
        c.game_mode, 
        c.map_variant_id
    from 
    bucketed.medals_matches_players a 
    left join bucketed.match_details b 
    on a.match_id=b.match_id 
    left join bucketed.matches c 
    on b.match_id=c.match_id'''


average_most_kills_per_game_query='''
    WITH cte_player_stats AS  (SELECT 
        player_gamertag, 
        count(match_id) as num_of_matches, 
        sum(player_total_kills) as total_kills 
    FROM bucket_join_df 
    GROUP BY player_gamertag) 
    SELECT *,total_kills/num_of_matches as kills_per_match from cte_player_stats 
    ORDER BY total_kills/num_of_matches DESC LIMIT 1'''

most_played_playlist_query='''
    WITH cte_count_duplicates AS  (SELECT 
    playlist_id, 
    match_id, 
    ROW_NUMBER() OVER (PARTITION BY match_id,playlist_id ORDER BY playlist_id) AS COUNTER
    FROM bucket_join_df) 
    SELECT playlist_id as reproductions FROM cte_count_duplicates 
    WHERE COUNTER=1 
    GROUP BY playlist_id
    ORDER BY count(match_id) desc 
    LIMIT 1'''

most_played_map_query='''
    WITH cte_count_duplicates AS  (SELECT 
    map_variant_id, 
    match_id, 
    ROW_NUMBER() OVER (PARTITION BY match_id,map_variant_id ORDER BY map_variant_id) AS COUNTER
    FROM bucket_join_df) 
    SELECT map_variant_id as most_played FROM cte_count_duplicates 
    WHERE COUNTER=1 
    GROUP BY map_variant_id 
    ORDER BY COUNT(map_variant_id) DESC 
    LIMIT 1'''

#3 - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
def bucket_join(spark_instance):

    #load datasets
    match_details=spark_instance.read.option("header","true").option("inferSchema","true").csv("/home/iceberg/data/match_details.csv")
    matches=spark_instance.read.option("header","true").option("inferSchema","true").csv("/home/iceberg/data/matches.csv")
    medals_matches_players=spark_instance.read.option("header","true").option("inferSchema","true").csv("/home/iceberg/data/medals_matches_players.csv")

    #save buckets as tables
    match_details.write.bucketBy(16,"match_id").saveAsTable("bucketed.match_details")
    matches.write.bucketBy(16,"match_id").saveAsTable("bucketed.matches")
    medals_matches_players.write.bucketBy(16,"match_id").saveAsTable("bucketed.medals_matches_players")

    #save the joined dataframe in a variable
    bucket_join_df=spark_instance.sql(query)

    #create temporal view
    bucket_join_df.createOrReplaceTempView("bucket_join_df")    

#4 - Aggregate the joined data frame to figure out questions like:

#Note: i know is not good idea use collect(), but the response is short
#Its possible use take too.

# - Which player averages the most kills per game?

def kills_per_game(spark_instance):    

    average_most_kills_per_game=spark.sql(average_most_kills_per_game_query)
    return average_most_kills_per_game.collect()[0]['player_gamertag']

# - Which playlist gets played the most?

def played_playlist(spark_instance):
    
    ## the query consider that can be duplicate registers, so it deduplicated
    ## i return the playlist_id instead the explicit playlist name
    most_played_playlist=spark_instance.sql(most_played_playlist_query)
    return most_played_playlist.collect()[0]['playlist_id']

# - Which map gets played the most?
def the_most_played_map(spark_instance):

    ## the query consider that can be duplicate registers, so it deduplicated
    ## i return the map_id instead the explicit map name
    most_played_map=spark_instance.sql(most_played_map_query)
    return most_played_map.collect()[0]['map_variant_id']

# - Which map do players get the most Killing Spree medals on?

def map_with_more_killing_spree_medals(saprk_instance):

    #load medal dataset
    medals=spark_instance.read.option("header","true").option("inferSchema","true").csv("/home/iceberg/data/medals.csv")

    #In this problem i dont make a join, so i just get the medal_if fro  Killing Spree from the medal dataset to filter in the dataframe required.
    medal_id_killing_spree=medals.select('medal_id').where(col("name")=='Killing Spree').collect()[0]['medal_id']

    most_killing_spree_medals_map_query=f''' 
            WITH cte_medals_per_map AS (SELECT  
           map_variant_id,
           medal_id, 
           sum(count) as cumulative_sum 
           FROM bucket_join_df 
           WHERE map_variant_id is not null 
           GROUP BY map_variant_id,medal_id)
           SELECT map_variant_id FROM cte_medals_per_map 
           WHERE medal_id={medal_id_killing_spree} 
           ORDER BY cumulative_sum DESC LIMIT 1'''
    
    most_killing_spree_medals_map=spark_instance.sql(most_killing_spree_medals_map_query)

    return most_killing_spree_medals_map.collect()[0]['map_variant_id']


def main():

    spark=SparkSession.builder\
        .master("local") \
        .appName("bucket_join_and_agreggations") \
        .getOrCreate()

    #load the bucket joind dataframe
    bucket_join(spark)

    #for these problems i will return a tuple of dataframes as response just for simplify
    result1=kills_per_game(spark)
    result2=played_playlist(spark)
    result3=the_most_played_map(spark)
    result4=map_with_more_killing_spree_medals(spark)

    return (result1,result2,result3,result4)

