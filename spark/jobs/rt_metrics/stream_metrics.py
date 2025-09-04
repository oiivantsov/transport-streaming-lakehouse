import os, json
from datetime import timedelta
from prometheus_client import Gauge, start_http_server
from pyspark.sql import SparkSession, functions as F

# HTTP server so Prometheus can scrape metrics
PORT = int(os.getenv("PROM_PORT", "9108"))
start_http_server(PORT)

# gauges as we monitor values which can go up or down
TOTAL_ACTIVE = Gauge("total_active_vehicles", "Total number of active vehicles across all routes")
AVG_SPEED_ALL = Gauge("avg_speed_all", "Average speed across all active vehicles (km/h)")
PCT_ROUTES_ON_TIME = Gauge("pct_routes_on_time", "Share of routes on time (<1 min delay)")

spark = (
    SparkSession.builder
    .appName("HSL RT Metrics")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", 16)
    .enableHiveSupport()
    .getOrCreate()
)

kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "hsl_stream")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = kafka_stream.selectExpr("CAST(value AS STRING) AS v").select(
    F.get_json_object("v", "$.topic.route_id").alias("route_id"),
    F.get_json_object("v", "$.topic.vehicle_number").alias("vehicle_number"),
    F.get_json_object("v", "$.payload.VP.tst").alias("tst"),
    F.get_json_object("v", "$.payload.VP.spd").cast("double").alias("speed"),
    F.get_json_object("v", "$.payload.VP.dl").cast("double").alias("delay")
).withColumn("timestamp", F.to_timestamp("tst"))

windowed_df = (
    parsed_df
    .withWatermark("timestamp", "1 minutes")
    .groupBy(F.window(F.col("timestamp"), "5 minutes", "20 seconds"))
    .agg(
        F.approx_count_distinct("vehicle_number").alias("active_vehicles"),
        F.avg("speed").alias("avg_speed"),
        F.avg(F.when(F.abs(F.col("delay")) <= 60, 1).otherwise(0)).alias("on_time_ratio")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "active_vehicles", "avg_speed", "on_time_ratio"
    )
)
def update_prometheus(batch_df, batch_id):

    # send to prometheus only the freshest data (with newest window)
    if batch_df.rdd.isEmpty():
        return

    latest_end = batch_df.agg(F.max("window_end").alias("m")).collect()[0]["m"]    
    latest = (batch_df
            .filter(F.col("window_end") == F.lit(latest_end))
            .limit(1)
            .collect())
    
    if not latest:
        return
    
    row = latest[0]

    TOTAL_ACTIVE.set(int(row["active_vehicles"] or 0))
    AVG_SPEED_ALL.set(float(row["avg_speed"] or 0.0))
    PCT_ROUTES_ON_TIME.set(float(row["on_time_ratio"] or 0.0))

(
    windowed_df.writeStream
    .outputMode("append")
    .foreachBatch(update_prometheus)
    .option("checkpointLocation", "/home/jobs/checkpoint_data/realtime/metrics")
    .trigger(processingTime="15 seconds")
    .start()
    .awaitTermination()
)
