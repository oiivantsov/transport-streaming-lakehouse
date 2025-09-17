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
PCT_ROUTES_ON_TIME_1 = Gauge("pct_routes_on_time_1", "Share of routes on time (<1 min delay)")
PCT_ROUTES_ON_TIME_2 = Gauge("pct_routes_on_time_2", "Share of routes on time (<2 min delay)")
PCT_ROUTES_ON_TIME_3 = Gauge("pct_routes_on_time_3", "Share of routes on time (<3 min delay)")

ROUTE_DELAY_EXTREME = Gauge(
    "route_delay_extreme_seconds",
    "Extreme average route delay (best=most ahead, worst=most delayed) within the latest window",
    ["type", "route_id"]  # type in {"best","worst"}
)

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
    .groupBy(F.window(F.col("timestamp"), "5 minutes", "30 seconds"))
    .agg(
        F.approx_count_distinct("vehicle_number").alias("active_vehicles"),
        F.avg("speed").alias("avg_speed"),
        F.avg(F.when(F.abs(F.col("delay")) <= 60, 1).otherwise(0)).alias("on_time_ratio_1"),
        F.avg(F.when(F.abs(F.col("delay")) <= 120, 1).otherwise(0)).alias("on_time_ratio_2"),
        F.avg(F.when(F.abs(F.col("delay")) <= 180, 1).otherwise(0)).alias("on_time_ratio_3")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "active_vehicles", "avg_speed", "on_time_ratio_1", "on_time_ratio_2", "on_time_ratio_3"
    )
)


route_windowed_df = (
    parsed_df
    .withWatermark("timestamp", "1 minutes")
    .groupBy(F.window(F.col("timestamp"), "5 minutes", "30 seconds"), F.col("route_id"))
    .agg(
        F.avg("delay").alias("avg_delay_sec"),
        F.count("*").alias("events_cnt")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "route_id", "avg_delay_sec", "events_cnt"
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
    PCT_ROUTES_ON_TIME_1.set(float(row["on_time_ratio_1"] or 0.0))
    PCT_ROUTES_ON_TIME_2.set(float(row["on_time_ratio_2"] or 0.0))
    PCT_ROUTES_ON_TIME_3.set(float(row["on_time_ratio_3"] or 0.0))


def update_prometheus_route_extremes(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    latest_end = batch_df.agg(F.max("window_end").alias("m")).collect()[0]["m"]
    latest = batch_df.filter(F.col("window_end") == F.lit(latest_end))
    if latest.rdd.isEmpty():
        return

    # anti-noise
    latest = latest.filter(F.col("events_cnt") >= 5)

    # to not collect old values
    ROUTE_DELAY_EXTREME.clear()

    worst_row = latest.orderBy(F.desc("avg_delay_sec")).limit(1).collect()
    if worst_row:
        wr = worst_row[0]
        ROUTE_DELAY_EXTREME.labels("worst", wr["route_id"] or "unknown").set(float(wr["avg_delay_sec"] or 0.0))

    best_row = latest.orderBy(F.asc("avg_delay_sec")).limit(1).collect()
    if best_row:
        br = best_row[0]
        ROUTE_DELAY_EXTREME.labels("best", br["route_id"] or "unknown").set(float(br["avg_delay_sec"] or 0.0))


(
    windowed_df.writeStream
    .outputMode("append")
    .foreachBatch(update_prometheus)
    .option("checkpointLocation", "/home/jobs/checkpoint_data/realtime/metrics_totals")
    .trigger(processingTime="15 seconds")
    .start()
)

(
    route_windowed_df.writeStream
    .outputMode("append")
    .foreachBatch(update_prometheus_route_extremes)
    .option("checkpointLocation", "/home/jobs/checkpoint_data/realtime/metrics_extremes")
    .trigger(processingTime="15 seconds")
    .start()
    .awaitTermination()
)

