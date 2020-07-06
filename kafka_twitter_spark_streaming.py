from pyspark import SparkContext
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#
#sc = SparkContext('local')
#spark = SparkSession(sc)
#
#df = spark \
#  .readStream \
#  .format("kafka") \
#  .option("kafka.bootstrap.servers", "localhost:9092") \
#  .option("subscribe", "twitter") \
#  .option("startingOffsets", "earliest") \
#  .option("inferSchema", "true") \
#  .load()
#
#opdf = df \
#  .writeStream \
#  .format("memory") \
#  .outputMode("append") \
#  .queryName("tweets") \
#  .start()
#
#
#tweets = spark.sql("select cast(key as string), cast(value as string) from tweets")
#print(tweets.show())
#
#opdf.awaitTermination()

#query.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import sys
from pyspark.sql.types import *

def fun(avg_senti_val):
	try:
		if avg_senti_val < 0: return 'NEGATIVE'
		elif avg_senti_val == 0: return 'NEUTRAL'
		else: return 'POSITIVE'
	except TypeError:
		return 'NEUTRAL'

if __name__ == "__main__":
    schema = StructType([                                                                                          
		    StructField("id", StringType(), True),
		    StructField("text", StringType(), True)    
	    ])
    spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()
    kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()
    kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING)")
    tweets_table = kafka_df_string.select(from_json(col("value"), schema).alias("data")).select("data.*")
#    sum_val_table = tweets_table.select(avg('senti_val').alias('avg_senti_val'))
#    udf_avg_to_status = udf(fun, StringType())
#    new_df = sum_val_table.withColumn("status", udf_avg_to_status("avg_senti_val"))
    query = tweets_table.writeStream.outputMode("append").format("console").start()
#    print(spark.sql("select cast(text as STRING) from query"))
    query.awaitTermination()
