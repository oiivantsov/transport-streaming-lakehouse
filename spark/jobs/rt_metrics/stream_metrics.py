import os, json
from datetime import timedelta
from prometheus_client import Gauge, start_http_server
from pyspark.sql import SparkSession, functions as F

# HTTP server so Prometheus can scrape metrics
PORT = int(os.getenv("PROM_PORT", "9108"))
start_http_server(PORT)

# gauges as we monitor values which can go up or down
ACTIVE_VEHICLES = Gauge("active_vehicles_total", "Active vehicles in the last window", ["route_id"])
AVG_SPEED = Gauge("avg_speed_kmh_by_route", "Average speed (km/h) for the last window", ["route_id"])
ON_TIME_RATIO = Gauge("on_time_ratio_by_route", "Share of on-time events in the last window", ["route_id"])

spark = (
    SparkSession.builder
    .appName("HSL RT Metrics")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", 16)
    .enableHiveSupport()
    .getOrCreate()
)

kdf = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "hsl_stream")
    .option("startingOffsets", "latest")
    .load()
)

df = kdf.selectExpr("CAST(value AS STRING) AS v").select(
    F.get_json_object("v", "$.topic.route_id").alias("route_id"),
    F.get_json_object("v", "$.topic.vehicle_number").alias("vehicle_number"),
    F.get_json_object("v", "$.payload.VP.tst").alias("tst"),
    F.get_json_object("v", "$.payload.VP.spd").cast("double").alias("spd"),
    F.get_json_object("v", "$.payload.VP.dl").cast("double").alias("dl")
).withColumn("tst_ts", F.to_timestamp("tst"))

win_df = (
    df.withWatermark("tst_ts", "10 minutes")  # allow 10 min late data
    .groupBy(
        F.window(F.col("tst_ts"), "5 minutes", "1 minute"),  # sliding window: 5 min, slide 1 min
        F.col("route_id")
    )
    .agg(
        F.approx_count_distinct("vehicle_number").alias("active_vehicles"),  # unique vehicles
        F.avg("spd").alias("avg_spd"),  # avg speed
        
        # for each row: 1 if delay within +-60 sec, else 0, average gives the % on-time
        F.avg(F.when(F.abs(F.col("dl")) <= 60, 1).otherwise(0)).alias("on_time_ratio")
    )
)

def update_prometheus(batch_df, batch_id):
    ACTIVE_VEHICLES.clear()
    AVG_SPEED.clear()
    ON_TIME_RATIO.clear()
    rows = batch_df.select("route_id", "active_vehicles", "avg_spd", "on_time_ratio").collect()
    for r in rows:
        rid = r["route_id"] or "unknown"
        ACTIVE_VEHICLES.labels(rid).set(float(r["active_vehicles"] or 0))
        if r["avg_spd"] is not None:
            AVG_SPEED.labels(rid).set(float(r["avg_spd"]))
        if r["on_time_ratio"] is not None:
            ON_TIME_RATIO.labels(rid).set(float(r["on_time_ratio"]))

(
    win_df.writeStream
    .outputMode("update")
    .foreachBatch(update_prometheus)
    .option("checkpointLocation", "chk/realtime/metrics")
    .trigger(processingTime="30 seconds")
    .start()
    .awaitTermination()
)
