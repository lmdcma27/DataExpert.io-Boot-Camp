
from pyspark.sql import SparkSession


def do_hosts_cumulated_transformation(spark,dataframe,yesterday_date,today_date):

    dataframe.createOrReplaceTempView("hosts_cumulated")
    query_hosts_cumulated = f"""
        WITH yesterday AS (
            select * from hosts_cumulated
            where date= DATE({yesterday_date})
        ), today AS (
            SELECT
                host,
                DATE(event_time) as date_active,
                ROW_NUMBER() OVER (PARTITION BY host) AS COUNTER
            FROM events
            WHERE DATE(event_time)={today_date}
        )
        INSERT INTO hosts_cumulated
        SELECT
            COALESCE(y.host,t.host) as host,
            CASE WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
                WHEN t.date_active IS NULL THEN y.dates_active
                ELSE y.dates_active || ARRAY[t.date_active]
            END AS dates_active,
            COALESCE(t.date_active,y.date + INTERVAL '1 day') as date
        FROM yesterday y
        FULL OUTER JOIN today t
        ON y.host=t.host
        WHERE t.COUNTER=1
    """
    return spark.sql(query_hosts_cumulated)

def main():
    spark=SparkSession.builder \
        .master("local") \
        .appName("hosts_cumulated") \
        .getOrCreate() 
    output_df=do_hosts_cumulated_transformation(spark,spark.table("hosts_cumulated"),'2023-01-30','2023-01-31')
    output_df.write.mode("overwrite").insertInto("hosts_cumulated")