import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from jobs.lib.job_control import get_max_timestamp, insert_log
from jobs.lib.utils import get_rundate
from pyspark.sql import functions as F
from jobs.lib.parsing_utils import parse_and_flatten
from datetime import datetime
from pyspark.sql.window import Window

spark = (
    SparkSession 
    .builder 
    .appName("Events from Landing to Staging") 
    .config("spark.sql.shuffle.partitions", 16)
    .master("spark://spark-master:7077")
    .enableHiveSupport()
    .getOrCreate()
)

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw_stg"
table_name = "events_stg"
table_full_name = f"{schema_name}.{table_name}"
landing_table_full_name = "hdw_ld.events_ld"
print("SPARK_APP: JOB triggered for rundate - " + rundate)

# load only new data
max_timestamp = get_max_timestamp(spark, schema_name, table_name)

# Read data from landing based on max timestamp
df_ld = spark \
    .read \
    .table(landing_table_full_name) \
    .where(f"insert_time > to_timestamp('2025-09-05 12:09:10.112 +0000')")

print("SPARK_APP: Landing Data Count - " + str(df_ld.count()))
print("SPARK_APP: Printing Landing Schema --")
df_ld.printSchema()

# json_str (kafka value) to columns
df_flat = parse_and_flatten(df_ld)

# delete duplicate events
df_dedupe = df_flat.withColumn(
    "row_num",
    F.row_number().over(Window.partitionBy("vehicle_number", "tst").orderBy(F.col("insert_time").desc()))
).where("row_num = 1").drop("row_num")

# Cast columns + add audit metadata
df_stg = (
    df_dedupe
    .withColumn("tst", F.to_timestamp("tst"))
    .withColumn("oday", F.to_date("oday")) 
    .withColumn("insert_time", F.current_timestamp())
    .withColumn("update_time", F.current_timestamp())
)

print("SPARK_APP: Staging Data Count - " + str(df_stg.count()))
print("SPARK_APP: Printing Staging Schema --")
df_stg.printSchema()

# Write to Staging (overwrite = truncate + reload)
spark.sql(f"DROP TABLE IF EXISTS {table_full_name}")
df_stg.write.format("delta").saveAsTable(table_full_name)

print("SPARK_APP: Data written to staging table")

# Add job details in JOB CONTROL
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")

spark.sql(f"select * from hdw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)

spark.stop()