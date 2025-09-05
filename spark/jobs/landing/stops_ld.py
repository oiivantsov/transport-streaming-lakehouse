import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from jobs.lib.job_control import get_max_timestamp, insert_log
from jobs.lib.utils import get_rundate
from datetime import datetime
from jobs.lib import data_quality as dq
from pyspark.sql.types import StructType, StructField, StringType
from delta import DeltaTable

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw_ld"
table_name = "stops_ld"
table_full_name = f"{schema_name}.{table_name}"
print("SPARK_APP: JOB triggered for rundate - " + rundate)

spark = (
    SparkSession
    .builder
    .appName("Stops Landing")
    .enableHiveSupport()
    .getOrCreate()
)

print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)

# Spark Configs
spark.conf.set("spark.sql.shuffle.partitions", 8)

# Read CSV
landing_schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("stop_code", StringType(), True),
    StructField("stop_name", StringType(), True),
    StructField("stop_desc", StringType(), True),
    StructField("stop_lat", StringType(), True),
    StructField("stop_lon", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("stop_url", StringType(), True),
    StructField("location_type", StringType(), True),
    StructField("parent_station", StringType(), True),
    StructField("wheelchair_boarding", StringType(), True),
    StructField("platform_code", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("radius", StringType(), True),
    StructField("_corrupt_record", StringType(), True)  # store malformed rows
])

df_landing = spark.read \
    .option("header", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", False) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(landing_schema) \
    .csv("/home/jobs/dim_raw/stops.txt")

print("SPARK_APP: Initial landing data count - " + str(df_landing.count()))
df_landing.printSchema()

# QC: check number of columns matches expectation
expected_cols = [
    "stop_id", "stop_code", "stop_name", "stop_desc",
    "stop_lat", "stop_lon", "zone_id", "stop_url",
    "location_type", "parent_station", "wheelchair_boarding",
    "platform_code", "vehicle_type", "radius"
]
dq.check_not_empty(df_landing, "Landing stops")
dq.check_columns_exist(df_landing, expected_cols, "Landing stops")
dq.check_no_nulls_in_column(df_landing, "stop_id", "Landing stops")

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
