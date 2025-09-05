import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from jobs.lib.job_control import get_max_timestamp, insert_log
from jobs.lib.utils import get_rundate
from datetime import datetime
from jobs.lib import data_quality as dq
from delta import DeltaTable

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw_ld"
table_name = "routes_ld"
table_full_name = f"{schema_name}.{table_name}"
print("SPARK_APP: JOB triggered for rundate - " + rundate)

spark = (
    SparkSession
    .builder
    .appName("Routes Landing")
    .enableHiveSupport()
    .getOrCreate()
)

print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)

# Spark Configs
spark.conf.set("spark.sql.shuffle.partitions", 8)

# Read raw file
raw_df = spark.read.format("text").load("/home/jobs/dim_raw/routes.txt")
print("SPARK_APP: Printing Raw Schema --")
raw_df.printSchema()
print("SPARK_APP: Raw data count - " + str(raw_df.count()))

# Extract header row
header_row = raw_df.orderBy(F.col("value").desc()).limit(1).collect()[0][0]
cols = header_row.split(",")

# Split value into columns
data_df = raw_df.filter(F.col("value") != header_row)
split_df = data_df.withColumn("splitted", F.split("value", ",")).drop("value")
df_landing = split_df.select(
    [F.col("splitted")[i].alias(col) for i, col in enumerate(cols)]
)

# DQ
dq.check_not_empty(df_landing, "Landing routes")
dq.check_columns_exist(df_landing, ["route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_url"], "Landing routes")
dq.check_no_nulls_in_column(df_landing, "route_id", "Landing routes")

# Add audit columns
df_landing = df_landing \
    .withColumn("insert_time", F.current_timestamp()) \
    .withColumn("rundate", F.lit(rundate))

print("SPARK_APP: Final landing data count - " + str(df_landing.count()))
df_landing.printSchema()

# Write to landing table
df_landing.write.format("delta").mode("append").saveAsTable(table_full_name)

print("SPARK_APP: Data written to landing layer")

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