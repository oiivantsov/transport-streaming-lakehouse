import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from jobs.lib.utils import get_rundate
from jobs.lib.job_control import insert_log, get_max_timestamp
from jobs.lib import data_quality as dq
from delta import DeltaTable

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw"
table_name = "dim_routes"
table_full_name = f"{schema_name}.{table_name}"
staging_table_full_name = "hdw_stg.routes_stg"
print(f"SPARK_APP: JOB triggered for rundate - {rundate}")

# Spark Session
spark = (
    SparkSession.builder
    .appName(f"Dimension load - {table_full_name}")
    .enableHiveSupport()
    .getOrCreate()
)
print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)

# Read staging and add SCD2 helper columns
df_stg = spark.read.table(staging_table_full_name) \
    .withColumn("row_wid", F.expr("uuid()")) \
    .withColumn("hist_record_end_timestamp", F.expr("cast(effective_start_dt as TIMESTAMP) - INTERVAL 1 seconds")) \
    .withColumn("hist_record_active_flg", F.lit(0)) \
    .withColumn("hist_record_update_dt", F.current_timestamp())

# Register temp views
df_stg.createOrReplaceTempView("dim_temp")

# Check for full load
is_empty = get_max_timestamp(spark, schema_name, table_name) == "1900-01-01 00:00:00.000000"

if is_empty:
    print("SPARK_APP: Table is set for full load")
    spark.sql(f"""
        INSERT INTO {table_full_name}
        SELECT route_id, agency_id, route_short_name, route_long_name, route_type,
               insert_time, rundate,
               effective_start_dt, effective_end_dt, active_flg, insert_dt, update_dt, row_wid
        FROM dim_temp
    """)

    # log - rows inserted
    dt = DeltaTable.forName(spark, table_full_name)
    dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numTargetRowsInserted").show(1, False)
else:
    # Close old records where data changed (SCD2 update)
    spark.sql(f"""
        MERGE INTO {table_full_name} AS dim
        USING dim_temp AS stg
        ON dim.route_id = stg.route_id
           AND dim.active_flg = 1
           AND (
               dim.route_short_name <> stg.route_short_name OR
               dim.route_long_name <> stg.route_long_name OR
               dim.route_type <> stg.route_type
           )
        WHEN MATCHED THEN UPDATE SET
            dim.effective_end_dt = stg.hist_record_end_timestamp,
            dim.active_flg = stg.hist_record_active_flg,
            dim.update_dt = stg.hist_record_update_dt
    """)
    print("SPARK_APP: Updated History Records")

    # log - rows updated
    dt = DeltaTable.forName(spark, table_full_name)
    dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                "operationMetrics.numTargetRowsUpdated").show(1, False)

    # Insert only new or changed records (SCD2 insert)
    spark.sql(f"""
        INSERT INTO {table_full_name}
        SELECT stg.route_id, stg.agency_id, stg.route_short_name, stg.route_long_name, stg.route_type,
               stg.insert_time, stg.rundate,
               stg.effective_start_dt, stg.effective_end_dt, stg.active_flg, stg.insert_dt, stg.update_dt, stg.row_wid
        FROM dim_temp stg
        LEFT JOIN {table_full_name} dim
            ON stg.route_id = dim.route_id 
            AND dim.active_flg = 1
        WHERE dim.route_id IS NULL
    """)
    print("SPARK_APP: Active Records inserted into Dimension Table")

    # log - rows inserted
    dt = DeltaTable.forName(spark, table_full_name)
    dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numTargetRowsInserted").show(1, False)

# QC: Ensure only one active record per route_id
df_final = spark.read.table(table_full_name)
dq.check_active_unique(df_final, ["route_id"], table_full_name)

# Log job
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")

spark.sql(f"select * from hdw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)

spark.stop()
