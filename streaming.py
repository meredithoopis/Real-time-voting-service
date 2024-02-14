from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json,col 
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging 


if __name__ == "__main__": 
    spark = (SparkSession.builder.appName("Voting Analysis")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.jars", "postgresql-42.7.1.jar")
            .config("spark.sql.adaptive.enabled", "false").
            getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    
#Kafka topics
    vote_schema = StructType(
    [
        StructField("voter_id", StringType(), True), 
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True),
    ]
)

    #Read data from "Votes_topic"
    votes_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "Votes_topic").option("startingOffsets", "earliest").load().selectExpr("CAST(value AS STRING)").select(from_json(col("value"), vote_schema).alias("data")).select("data.*")

    #Preprocessing 
    votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())).withColumn("vote", col("vote").cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")
    # Aggregate votes per candidate 
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party", "photo_url").agg(_sum("vote").alias("total_votes"))
    # Write to Kafka topics
    votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "aggregated_votes_per_candidate").option("checkpointLocation", "checkpoint").outputMode("update").start()
    votes_per_candidate_to_kafka.awaitTermination()
    