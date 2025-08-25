import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from jobs.lib.job_control import get_max_timestamp, insert_log
from jobs.lib.utils import get_rundate
from datetime import datetime
from pyspark.sql.window import Window
from jobs.lib import data_quality as dq
from delta import DeltaTable

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw_stg"
table_name = "routes_stg"
table_full_name = f"{schema_name}.{table_name}"
landing_table_full_name = "hdw_ld.routes_ld"
print("SPARK_APP: JOB triggered for rundate - " + rundate)

spark = (
    SparkSession
    .builder
    .appName("Routes Staging")
    .enableHiveSupport()
    .getOrCreate()
)


print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)

# Spark Configs
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.parquet.mergeSchema", True)

# Get max timestamp for incremental load
max_timestamp = get_max_timestamp(spark, schema_name, table_name)
print("SPARK_APP: Max timestamp for staging data load - " + max_timestamp)

# Read landing data (already structured)
df_ld = spark.read.table(landing_table_full_name) \
    .where(f"insert_time > to_timestamp('{max_timestamp}')")

print("SPARK_APP: Landing Data Count - " + str(df_ld.count()))
df_ld.printSchema()

# Cast and clean columns
df_transformed = df_ld \
    .withColumn("route_short_name", F.coalesce("route_short_name", F.lit("NA"))) \
    .drop("route_desc", "route_url") \
    .withColumn("route_type", F.col("route_type").cast(IntegerType()))

# De-dupe based on business key
w = Window.partitionBy(F.col("route_id")).orderBy(F.col("insert_time").desc())
df_dedupe = df_transformed.withColumn("rnk", F.row_number().over(w)) \
    .filter(F.col("rnk") == 1).drop("rnk")

print("SPARK_APP: Data Count after de-dupe - " + str(df_dedupe.count()))

# DQ
dq.check_not_empty(df_dedupe, "Staging routes")
dq.check_no_duplicates(df_dedupe, ["route_id"], "Staging routes")

# Add audit columns
df_stg = df_dedupe \
    .withColumn("effective_start_dt", F.current_timestamp()) \
    .withColumn("effective_end_dt", F.to_timestamp(F.lit("9999-12-31 00:00:00.000000"))) \
    .withColumn("active_flg", F.lit(1)) \
    .withColumn("insert_dt", F.current_timestamp()) \
    .withColumn("update_dt", F.current_timestamp())

print("SPARK_APP: Staging Data Count - " + str(df_stg.count()))
df_stg.printSchema()

# Write to staging table
df_stg.write.format("delta").mode("overwrite").saveAsTable(table_full_name)
print("SPARK_APP: Data written to staging table")

# Log to JOB CONTROL
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")

spark.sql(f"select * from hdw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)

# Get the logs from delta table version
dt = DeltaTable.forName(spark, table_full_name)
dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numTargetRowsInserted",
                                "operationMetrics.numTargetRowsUpdated",
                                "operationMetrics.numOutputRows").show(1, False)

spark.stop()
