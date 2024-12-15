from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast



def make_explicit_broadcast_join(spark_instance):
    #load datasets
    medals=spark.read.option("header","true").option("inferSchema","true").csv("/home/iceberg/data/medals.csv")
    maps=spark.read.option("header","true").option("inferSchema","true").csv("/home/iceberg/data/maps.csv")
    #return result

    #in this case i take the entity columne to make the explicit broadcastjoin practice. 
    # Because between medals and maps there's not exist a key join column.
    # But it's possible use a intermediate cross table(s).
    #However i make this because the exersice indicate that, the important is understand the tools
    return medals.join(broadcast(maps), on="name", how="left")
    

def main():
    spark=SparkSession.builder\
        .master("local") \
        .appName("explicit_broadcast_join") \
        .getOrCreate()
    result=make_explicit_broadcast_join(spark)    