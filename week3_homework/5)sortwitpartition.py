
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from bucket_join_by_match_id import bucket_join


sp_average_most_kills_per_game_query='''
    WITH cte_player_stats AS  (SELECT 
        player_gamertag, 
        count(match_id) as num_of_matches, 
        sum(player_total_kills) as total_kills 
    FROM SP_average_most_kills_per_game 
    GROUP BY player_gamertag) 
    SELECT *,total_kills/num_of_matches as kills_per_match from cte_player_stats 
    ORDER BY total_kills/num_of_matches DESC LIMIT 1'''

sp_most_played_playlist_query='''
    WITH cte_count_duplicates AS  (SELECT 
    playlist_id, 
    match_id, 
    ROW_NUMBER() OVER (PARTITION BY match_id,playlist_id ORDER BY playlist_id) AS COUNTER
    FROM SP_most_played_playlist_query) 
    SELECT playlist_id as reproductions FROM cte_count_duplicates 
    WHERE COUNTER=1 
    GROUP BY playlist_id
    ORDER BY count(match_id) desc 
    LIMIT 1'''

sp_most_played_map_query='''
    WITH cte_count_duplicates AS  (SELECT 
    map_variant_id, 
    match_id, 
    ROW_NUMBER() OVER (PARTITION BY match_id,map_variant_id ORDER BY map_variant_id) AS COUNTER
    FROM SP_most_played_map_query) 
    SELECT map_variant_id as most_played FROM cte_count_duplicates 
    WHERE COUNTER=1 
    GROUP BY map_variant_id 
    ORDER BY COUNT(map_variant_id) DESC 
    LIMIT 1'''



def try_sort_partitions(spark_instance,subsection):

    bucket_join_df=spark_instance.sql("SELECT * FROM bucket_join_df")
    #here i user repartition, but if you use want to reduce the number of partition is
    #better use COALESCE to avoid full schuffle

    if subsection=='a':

        #sort partition for average_most_kills_per_game
        bucket_join_df.repartition(16,col('player_gamertag'))\
            .sortWithinPartitions('player_gamertag')\
            .createOrReplaceTempView("SP_average_most_kills_per_game")
        
        #Now calculate the performance after sortWithPartition
        df1=spark_instance.sql(sp_average_most_kills_per_game_query)
        return df1

    elif subsection=='b':
        #sort partition for most_played_playlist_query
        bucket_join_df.repartition(16,col('playlist_id'))\
            .sortWithinPartitions('playlist_id')\
            .createOrReplaceTempView("SP_most_played_playlist_query")
        df2=spark_instance.sql(sp_most_played_playlist_query)
        return df2

    elif subsection=='c':
        #sort partition for most_played_map_query
        bucket_join_df.repartition(16,col('map_variant_id'))\
            .sortWithinPartitions('map_variant_id')\
            .createOrReplaceTempView("SP_most_played_map_query")
    
        df3=spark_instance.sql(sp_most_played_map_query)
        return df3

    else:
        pass


def main():

    spark=SparkSession.builder\
        .master("local") \
        .appName("bucket_join_and_agreggations") \
        .getOrCreate()

    try_sort_partitions(spark,'a')

    try_sort_partitions(spark,'b')

    try_sort_partitions(spark,'c')
