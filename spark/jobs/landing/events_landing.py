from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession 
    .builder 
    .appName("Streaming from Kafka") 
    .config("spark.streaming.stopGracefullyOnShutdown", True) 
    .master("spark://spark-master:7077")
    .enableHiveSupport()
    .getOrCreate()
)

kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "hsl_stream")
    .option("startingOffsets", "earliest")
    .load()
)

df_parsed = (
    kafka_df
    .selectExpr(
        "CAST(value AS STRING) as json_str",
        "CAST(key AS STRING) as key",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )
)

df_landing = df_parsed.withColumn("insert_time", current_timestamp())


def hsl_data_output(batch_df, batch_id):
    print("Batch id: "+ str(batch_id))  

    batch_df.show(5, truncate=False)
    
    # Write to Delta table
    (
        batch_df.write
        .mode("append")
        .format("delta")
        .saveAsTable("hdw_ld.events_ld")
    
    )
    

(
    df_landing
    .writeStream
    .foreachBatch(hsl_data_output)
    .trigger(processingTime='10 seconds')
    .option("checkpointLocation", "chk/landing/hsl_stream")
    .start()
    .awaitTermination()
)