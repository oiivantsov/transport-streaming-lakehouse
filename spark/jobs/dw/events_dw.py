import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession, functions as F
from jobs.lib.job_control import insert_log
from jobs.lib.utils import get_rundate
from datetime import datetime

spark = (
    SparkSession
    .builder
    .appName("Events from Staging to DW")
    .config("spark.sql.shuffle.partitions", 16)
    .master("spark://spark-master:7077")
    .enableHiveSupport()
    .getOrCreate()
)

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw"
table_name = "fact_vehicle_position"
table_full_name = f"{schema_name}.{table_name}"
staging_table = "hdw_stg.events_stg"
print("SPARK_APP: JOB triggered for rundate - " + rundate)

df_stg = spark.read.table(staging_table)

# Cast to correct types and select only business-relevant columns
df_selected = (
    df_stg
    .select(
        "oday", "tst", "tsi",
        "route_id", "route", "stop",
        "operator_id", "oper",
        "vehicle_number", "veh",
        "transport_mode", "direction_id", "dir",
        "headsign", "lat", "long", "spd",
        "dl", "occu", "event_type"
    )
)

# audit columns
df_dw = df_selected \
    .withColumn("insert_time", F.current_timestamp()) \
    .withColumn("update_time", F.current_timestamp()) 

print("SPARK_APP: DW Data Count - " + str(df_dw.count()))
print("SPARK_APP: Printing DW Schema --")
df_dw.printSchema()


(
    df_dw.write
    .format("delta")
    .mode("append")
    .partitionBy("oday")
    .saveAsTable(table_full_name)
)

print("SPARK_APP: Data written to DW table")

# Add job details in JOB CONTROL
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")

spark.sql(f"select * from hdw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)

spark.stop()