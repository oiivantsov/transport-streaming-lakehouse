import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
from jobs.lib.utils import get_rundate
from jobs.lib.job_control import insert_log, get_max_timestamp
from jobs.lib import data_quality as dq
from delta import DeltaTable

# JOB Parameters
rundate = get_rundate()
schema_name = "hdw"
table_name = "dim_stops"
table_full_name = f"{schema_name}.{table_name}"
staging_table_full_name = "hdw_stg.stops_stg"
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

# Register temp view
df_stg.createOrReplaceTempView("stops_temp")

# Full vs incremental
is_empty = get_max_timestamp(spark, schema_name, table_name) == "1900-01-01 00:00:00.000000"

if is_empty:
    print("SPARK_APP: dim_stops is empty -> full load")
    spark.sql(f"""
        INSERT INTO {table_full_name}
        SELECT
            stop_id, stop_code, stop_name, stop_desc,
            stop_lat, stop_lon, zone_id, parent_station,
            wheelchair_boarding, platform_code, vehicle_type,
            radius,
            insert_time, rundate,
            effective_start_dt, effective_end_dt, active_flg, insert_dt, update_dt, row_wid
        FROM stops_temp
    """)
    
    # log - rows inserted
    dt = DeltaTable.forName(spark, table_full_name)
    dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numOutputRows").show(1, False)
    
else:
    print("SPARK_APP: dim_stops -> SCD2 incremental")

    change_predicate = """
    (
            dim.stop_name <> stg.stop_name OR
            dim.stop_desc <> stg.stop_desc OR
            dim.stop_lat <> stg.stop_lat OR
            dim.stop_lon <> stg.stop_lon OR
            dim.zone_id <> stg.zone_id OR
            dim.parent_station <> stg.parent_station OR
            dim.wheelchair_boarding <> stg.wheelchair_boarding OR
            dim.platform_code <> stg.platform_code OR
            dim.vehicle_type <> stg.vehicle_type OR
            dim.radius <> stg.radius
    )
    """
    
    # 1) Close history for changed active rows
    spark.sql(f"""
        MERGE INTO {table_full_name} AS dim
        USING stops_temp AS stg
            ON dim.stop_id = stg.stop_id
            AND dim.active_flg = 1
            AND {change_predicate}
        WHEN MATCHED THEN UPDATE SET
            dim.effective_end_dt = stg.hist_record_end_timestamp,
            dim.active_flg = stg.hist_record_active_flg,
            dim.update_dt = stg.hist_record_update_dt;
    """)
    print("SPARK_APP: Closed previous active versions for changed rows")

    # log - rows updated
    dt = DeltaTable.forName(spark, table_full_name)
    dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                "operationMetrics.numTargetRowsUpdated").show(1, False)

    # 2) Insert new active versions (new stops or changed attributes)
    spark.sql(f"""
        INSERT INTO {table_full_name}
        SELECT
            stg.stop_id, stg.stop_code, stg.stop_name, stg.stop_desc,
            stg.stop_lat, stg.stop_lon, stg.zone_id, stg.parent_station,
            stg.wheelchair_boarding, stg.platform_code, stg.vehicle_type,
            stg.radius,
            stg.insert_time, stg.rundate,
            stg.effective_start_dt, stg.effective_end_dt, stg.active_flg, stg.insert_dt, stg.update_dt, stg.row_wid
        FROM stops_temp stg
        LEFT JOIN {table_full_name} dim
            ON dim.stop_id = stg.stop_id
            AND dim.active_flg = 1
        WHERE dim.stop_id IS NULL
    """)
    print("SPARK_APP: Inserted new active versions")

    # log - rows inserted
    dt = DeltaTable.forName(spark, table_full_name)
    dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numOutputRows").show(1, False)

df_final = spark.read.table(table_full_name)
dq.check_active_unique(df_final, ["stop_id"], table_full_name)

# Log job
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")

spark.sql(f"select * from hdw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)

spark.stop()
