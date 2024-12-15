from pyspark.sql import SparkSession


def turn_off_automatic_broadcast_join(saprk_instance : SparkSession) -> None:
    saprk_instance.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


def main():
    spark=SparkSession.builder\
        .master("local") \
        .appName("turn_off_automatic_broadcast_join") \
        .getOrCreate()
    turn_off_automatic_broadcast_join(spark)