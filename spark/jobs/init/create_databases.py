import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Create Databases")
    .enableHiveSupport()
    .getOrCreate()
)


spark.sql("create database if not exists hdw")
spark.sql("create database if not exists hdw_stg")
spark.sql("create database if not exists hdw_ld")

spark.sql("show databases").show()

spark.sql("""drop table if exists hdw.job_control""")
spark.sql("""
create table hdw.job_control (
    schema_name string,
    table_name string,
    max_timestamp timestamp,
    rundate string,
    insert_dt timestamp
)
USING delta
;
""")

print("SPARK-APP: JOB Control table created")

spark.sql("""drop table if exists hdw.dim_routes""")
spark.sql("""
create table hdw.dim_routes (
    route_id STRING,
    agency_id STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type INT,
    insert_time TIMESTAMP,
    rundate STRING,
    effective_start_dt TIMESTAMP,
    effective_end_dt TIMESTAMP,
    active_flg INT,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP,
    row_wid STRING
)
USING DELTA
""")


print("SPARK-APP: Dim Routes table created")

spark.sql("""
DROP TABLE IF EXISTS hdw.dim_stops
""")
spark.sql("""
CREATE TABLE hdw.dim_stops (
    stop_id STRING,
    stop_code STRING,
    stop_name STRING,
    stop_desc STRING,
    stop_lat DOUBLE,
    stop_lon DOUBLE,
    zone_id STRING,
    parent_station STRING,
    wheelchair_boarding INT,
    platform_code STRING,
    vehicle_type INT,
    radius INT,
    insert_time TIMESTAMP,
    rundate STRING,
    effective_start_dt TIMESTAMP,
    effective_end_dt TIMESTAMP,
    active_flg INT,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP,
    row_wid STRING
)
USING DELTA
""")

print("SPARK-APP: Dim Stops table created")

spark.sql("show tables in hdw").show()

spark.stop()