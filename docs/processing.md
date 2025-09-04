# Processing and Data Flow

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

This document describes how data is **processed with Spark** from ingestion to analytics:  
- [Real-time streaming from Kafka](#real-time-events-and-kafka-producer)  
- [Batch ETL with Slowly Changing Dimensions (SCD2)](#batch-etl-with-slowly-changing-dimensions-scd2)  
- [Continuous metric computation with Prometheus](#real-time-metrics-with-spark-and-prometheus)  
- [Built-in Data Quality (DQ) safeguards](#data-quality-dq-framework)

---

## Real-Time Events and Kafka Producer

> You can view the full script [here](/hsl-transport-service/hsl_producer.py).

The **fact table** in the Gold layer ultimately originates from **real-time events** ingested via a Kafka producer.

The producer connects to HSL’s MQTT feed (`mqtt.hsl.fi`), parses incoming vehicle position messages, and forwards them into Kafka (`hsl_stream` topic).  
It has configurable parameters:

* **Message rate control** – the `TIME_SLEEP` variable regulates how fast events are produced (e.g. 1s delay between messages).
* **Filtering by transport mode** – currently, the producer only processes **bus events**, but the code can be easily adapted to include trams, metro, trains, or ferries.
* **Parallelism** – the topic is created with multiple partitions (`PARTITIONS = 6`), enabling scaling of Spark Structured Streaming consumers.

Example snippet from the producer:

```python
if (journey_type != 'journey' or temporal_type != 'ongoing' or transport_mode != 'bus'):
    logging.info(f"Skipping non-bus or non-ongoing message: journey_type={journey_type}, temporal_type={temporal_type}, transport_mode={transport_mode}")
    return

producer.send(TOPIC, key=kafka_key, value=event_body)
print(f"Produced: key={kafka_key} -> {event_body}")
```

---

## Batch ETL with Slowly Changing Dimensions (SCD2)

> The full dimension processing script for `dim_routes` is available [here](/spark/jobs/dw/dim_routes.py)

The **routes dimension** is managed as a Slowly Changing Dimension Type 2 (SCD2).
This means that every time a route attribute changes (e.g., name, type), the old record is **closed** and a new one is **inserted**, preserving history.

Using SCD2 ensures:

* Full **historical tracking** (e.g., if a bus line is renamed or rebranded, old and new versions coexist).
* Queries can reproduce the exact state of dimensions for any historical date.
* Business users don’t lose context when dimensions evolve over time.

The same logic applies to **`dim_stops`**, enabling historical traceability of stop names, zones, and accessibility.

---

### Implementation Steps (Example: `dim_routes`)

#### Step 0: Silver Layer Preparation

When data moves from Landing to Staging, we enrich it with **audit and SCD2 helper columns**, which are later used by dimension jobs:

```python
df_stg = df_dedupe \
    .withColumn("effective_start_dt", F.current_timestamp()) \
    .withColumn("effective_end_dt", F.to_timestamp(F.lit("9999-12-31 00:00:00.000000"))) \
    .withColumn("active_flg", F.lit(1)) \
    .withColumn("insert_dt", F.current_timestamp()) \
    .withColumn("update_dt", F.current_timestamp())
```

These fields ensure:

* **`effective_start_dt`** – when the record became valid.
* **`effective_end_dt`** – defaulted to *infinity* until record is closed.
* **`active_flg`** – marks the currently valid row.
* **`insert_dt` / `update_dt`** – technical timestamps for auditing.

---

#### Step 1: Read staging and prepare temporary table

We first read staging data, add technical helper columns, and register it as a temp view:

```python
df_stg = spark.read.table("hdw_stg.routes_stg") \
    .withColumn("row_wid", F.expr("uuid()")) \
    .withColumn("hist_record_end_timestamp", F.expr("cast(effective_start_dt as TIMESTAMP) - INTERVAL 1 seconds")) \
    .withColumn("hist_record_active_flg", F.lit(0)) \
    .withColumn("hist_record_update_dt", F.current_timestamp())

df_stg.createOrReplaceTempView("dim_temp")
```

Here we prepare:

* **`row_wid`** – surrogate key for uniqueness.
* **`hist_record_end_timestamp`** – used to close active records when updates are found.
* **`hist_record_active_flg`** – 0 for closed records.
* **`hist_record_update_dt`** – timestamp of the update action.

---

#### Step 2: Full load (only first run)

If the dimension is empty (fresh load), we simply insert all records:

```sql
INSERT INTO hdw.dim_routes
SELECT * FROM dim_temp
```

---

#### Step 3: Detect changes and update history

For incremental runs, we use a **Delta Lake MERGE** to close old records:

```sql
MERGE INTO hdw.dim_routes AS dim
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
```

This ensures that **previous active records are closed** when attributes change.

---

#### Step 4: Insert new or changed records

After closing old rows, we insert the **new active versions**:

```sql
INSERT INTO hdw.dim_routes
SELECT stg.*
FROM dim_temp stg
LEFT JOIN hdw.dim_routes dim
    ON stg.route_id = dim.route_id AND dim.active_flg = 1
WHERE dim.route_id IS NULL
```

---

## Real-Time Metrics with Spark and Prometheus

> Full Spark streaming job available at [stream\_metrics.py](/spark/jobs/rt_metrics/stream_metrics.py)

In addition to the batch ETL into Bronze, Silver, and Gold layers, the pipeline includes a **real-time metrics job** that exposes global KPIs for monitoring system health and traffic dynamics.

### How It Works

1. **Read from Kafka**
   Spark Structured Streaming consumes the raw `hsl_stream` Kafka topic and extracts relevant JSON fields (`route_id`, `vehicle_number`, `timestamp`, `speed`, `delay`).

2. **Apply Sliding Windows**
   Events are grouped in **5-minute windows sliding every 20 seconds**, with a **1-minute watermark** to handle late data.

3. **Aggregate Global KPIs**
   For each window, Spark computes:

   * **Total active vehicles** -> distinct vehicle count across all routes
   * **Average speed** -> mean speed across all active vehicles
   * **On-time ratio** -> share of events with delay ≤ 60 seconds

   ```python
   windowed_df = (
       parsed_df
       .withWatermark("timestamp", "1 minutes")
       .groupBy(F.window(F.col("timestamp"), "5 minutes", "20 seconds"))
       .agg(
           F.approx_count_distinct("vehicle_number").alias("active_vehicles"),
           F.avg("speed").alias("avg_speed"),
           F.avg(F.when(F.abs(F.col("delay")) <= 60, 1).otherwise(0)).alias("on_time_ratio")
       )
   )
   ```

4. **Push Only the Latest Window to Prometheus**
   Each micro-batch is reduced to the **freshest completed window**. Metrics are updated using the Python `prometheus_client` library, which serves them via an HTTP endpoint (`PORT=9108`) for scraping:

   ```python
   TOTAL_ACTIVE = Gauge("total_active_vehicles", "Total number of active vehicles across all routes")
   AVG_SPEED_ALL = Gauge("avg_speed_all", "Average speed across all active vehicles (km/h)")
   PCT_ROUTES_ON_TIME = Gauge("pct_routes_on_time", "Share of routes on time (<1 min delay)")
   ```

5. **Grafana Dashboards**
   Prometheus metrics are visualized in Grafana, providing near real-time visibility into:

   * The total number of vehicles currently on the road
   * Average speed across the region
   * Share of buses arriving on time

    Example dashboard:
   ![Grafana Dashboard Screenshot](/docs/img/stream/grafana_v1.png)

---

### Why This Job Matters

* **Operational monitoring** – instead of waiting for batch jobs, traffic KPIs update every 30 seconds.
* **Early anomaly detection** – delays or unusual patterns can be spotted live.
* **Integration with DevOps stack** – the same monitoring tools (Prometheus + Grafana) used for infrastructure also show business-level KPIs.
* **Scalability** – Spark handles large event volumes, while Prometheus/Grafana make metrics accessible to users instantly.

---

### Example Metrics

* `active_vehicles_total{route_id="550"} 12`
  -> 12 vehicles active on route 550 in the last window.

* `avg_speed_kmh_by_route{route_id="550"} 28.7`
  -> average speed on route 550 is 28.7 km/h.

* `on_time_ratio_by_route{route_id="550"} 0.92`
  -> 92% of events on route 550 are within +-60 seconds of schedule.


---

## Data Quality (DQ) Framework

> The complete script with all data quality check functions is available [here](/spark/jobs/lib/data_quality.py).

Every Spark job in this Lakehouse pipeline includes **data quality (DQ) checks** to ensure that only valid and consistent data flows through the system.  
If any check fails, the job **raises an exception and stops execution**. This prevents corrupted, incomplete, or unexpected data from propagating into downstream layers.

The DQ utilities are implemented in a shared module (`jobs/lib/data_quality.py`) and applied across **Landing, Staging, and Warehouse** jobs.

### Available Checks

* **check_not_empty** – verifies that a DataFrame is not empty before insertion.
* **check_columns_exist** – ensures all required columns are present.
* **check_no_nulls_in_column** – ensures critical columns contain no null or empty values.
* **check_no_duplicates** – checks that business key columns contain no duplicates.
* **check_values_in_set** – validates that a column’s values are restricted to an allowed set.
* **check_active_unique** – enforces that only one active record exists per business key in SCD2 dimensions.

### Example Usage

```python
from jobs.lib import data_quality as dq

df = spark.read.table("hdw_stg.routes_stg")

# Apply quality checks
dq.check_not_empty(df, "hdw_stg.routes_stg")
dq.check_columns_exist(df, ["route_id", "route_short_name", "route_long_name"], "hdw_stg.routes_stg")
dq.check_no_nulls_in_column(df, "route_id", "hdw_stg.routes_stg")
dq.check_no_duplicates(df, ["route_id"], "hdw_stg.routes_stg")
```

If a check fails, the Spark job raises an error such as:

```
ValueError: QC FAILED: hdw_stg.routes_stg missing columns: ['route_short_name']
```

Execution **stops immediately**, ensuring that invalid data never makes it into Silver or Gold layers. In practice, this means if a **schema changes unexpectedly**, the job fails fast and alerts instead of silently corrupting the dataset.
