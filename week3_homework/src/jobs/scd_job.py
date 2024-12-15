
from pyspark.sql import SparkSession


query_scd = """ 
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
), change_identified AS (
    SELECT
        actorid,
        current_year,
        quality_class,
        is_active,
        SUM(CASE WHEN (did_change_quality OR did_change_active) THEN 1 ELSE 0 END)
            OVER (PARTITION BY actorid ORDER BY current_year) as change_identified
        FROM has_changed
), grouped AS (

    SELECT
        actorid,
        change_identified,
        quality_class,
        is_active,        
        MIN(current_year) as start_year,
        MAX(current_year) as end_year,
        2024 AS current_year
    FROM change_identified
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
"""



def do_actors_scd_transformation(spark,dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query_scd)


def main():
    spark.SparkSession.builder\
        .master("local") \
        .appName("actors_scd")\
        .getOrCreate()
    output_df=do_actors_scd_transformation(spark,spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")

