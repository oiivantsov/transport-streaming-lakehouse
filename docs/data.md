# Data and Storage

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

This document describes the **data model, storage design, and processing flow** of the HSL Transport Streaming Lakehouse.
It illustrates how public transport events are ingested, stored, and transformed across the **Bronze (Landing), Silver (Staging), and Gold (Warehouse)** layers.

---

## Table of Contents

- [Medallion Architecture Overview](#medallion-architecture-overview)  
- [File Organization in S3](#file-organization-in-s3)  
- [Streaming Data Flow (Events)](#streaming-data-flow-events)  
  - [1. Landing Layer (Bronze)](#1-landing-layer-bronze)  
  - [2. Staging Layer (Silver)](#2-staging-layer-silver)  
  - [3. Data Warehouse Layer (Gold)](#3-data-warehouse-layer-gold)  
- [Stops Data (≈8,500 records)](#stops-data-≈8500-records)  
  - [1. Landing Layer (Bronze) – hdw_ld.stops_ld](#landing-layer-bronze--hdw_ldstops_ld)  
  - [2. Staging Layer (Silver) – hdw_stg.stops_stg](#staging-layer-silver--hdw_stgstops_stg)  
  - [3. Data Warehouse Layer (Gold) – hdw.dim_stops](#data-warehouse-layer-gold--hdwdim_stops)  
- [Routes Data (≈530 records)](#routes-data-≈530-records)  
  - [1. Landing Layer (Bronze) – hdw_ld.routes_ld](#landing-layer-bronze--hdw_ldroutes_ld)  
  - [2. Staging Layer (Silver) – hdw_stg.routes_stg](#staging-layer-silver--hdw_stgroutes_stg)  
  - [3. Data Warehouse Layer (Gold) – hdw.dim_routes](#data-warehouse-layer-gold--hdwdim_routes)  
- [Star Schema in the Gold Layer](#star-schema-in-the-gold-layer)  
  - [Example Query: Average Delay per Route](#example-query-average-delay-per-route)  

---

## Medallion Architecture Overview

The project follows the **Delta Lake Medallion pattern**:

* **Bronze (Landing)** – append-only raw data from Kafka
* **Silver (Staging)** – cleaned, deduplicated, and normalized data
* **Gold (Warehouse)** – curated analytics layer (star schema)

Each layer has its own dedicated **database namespace** in Hive Metastore:

* `hdw_ld` -> Landing layer
* `hdw_stg` -> Staging layer
* `hdw` -> Data Warehouse layer

S3 is used as the **underlying object store**, with Delta Lake providing ACID transactions, schema enforcement, and time travel.

---

## File Organization in S3

The S3 structure reflects the three-layer design:

![S3 schemas](/docs/img/data/s3_schemas.png)

For example, the **events landing database** is stored in:

```
s3://my-hsl-lakehouse-bucket/warehouse/hdw_ld/events_ld/
```

Each table is represented as a **Delta Lake folder**, containing:

* **Parquet files** -> actual data blocks
* **\_delta\_log JSON files** -> transaction log and schema evolution

Example raw event files:

![New stream data in S3](/docs/img/data/new_stream_data_s3.png)

Example Delta JSON log:

![Delta JSON log](/docs/img/data/delta_json_log.png)

---

## Streaming Data Flow (Events)

The most critical dataset is the **vehicle events stream** (topic: `hsl_stream`), published via MQTT and ingested into Kafka.

The processing pipeline consists of **three Spark jobs**, which gradually transform the data.

---

### 1. Landing Layer (Bronze)

Spark Structured Streaming consumes Kafka records and appends them to `hdw_ld.events_ld`.

Schema is not parsed yet – raw JSON is preserved with minimal metadata:

* `key`, `topic`, `partition`, `offset`
* `json_str` (raw Kafka payload)
* `insert_time` (when the event was stored)

**Code excerpt:**

```python
(
    df_landing
    .writeStream
    .foreachBatch(hsl_data_output)
    .trigger(processingTime='10 seconds')
    .option("checkpointLocation", "chk/landing/hsl_stream")
    .start()
    .awaitTermination()
)
```

**Landing table preview:**

![events landing](/docs/img/data/events_ld.png)

---

### 2. Staging Layer (Silver)

A batch Spark job loads new landing data and **flattens JSON** into columns using the defined schema:

* **Topic metadata** (prefix, journey\_type, event\_type, route\_id, headsign, etc.)
* **Payload VP fields** (lat, long, speed, delay, occupancy, etc.)

Additional transformations:

* Deduplication (`row_number` over `(vehicle_number, tst)`)
* Type casting (`tst -> timestamp`, `oday -> date`)
* Audit metadata (`insert_time`, `update_time`)

**Code excerpt:**

```python
df_dedupe = df_flat.withColumn(
    "row_num",
    F.row_number().over(Window.partitionBy("vehicle_number", "tst")
                        .orderBy(F.col("insert_time").desc()))
).where("row_num = 1").drop("row_num")
```

**Staging table preview:**

![events staging](/docs/img/data/events_stg.png)

---

### 3. Data Warehouse Layer (Gold)

The curated analytics layer `hdw.fact_vehicle_position` keeps **only business-relevant fields**, partitioned by `oday` for efficient queries.

Columns include:

* `oday`, `tst`, `tsi`
* Route and stop IDs (`route_id`, `route`, `stop`)
* Vehicle/operator identifiers
* Position (`lat`, `long`, `spd`, `dl`, `occu`)
* Event type
* Audit fields

**Code excerpt:**

```python
df_dw.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("oday") \
    .saveAsTable("hdw.fact_vehicle_position")
```

**Warehouse table preview:**

![events warehouse](/docs/img/data/events_fact.png)

---

## Stops Data (≈8,500 records)

The **stops dataset** is derived from the official **GTFS `stops.txt` file**, which contains all physical and logical stops across the HSL network (bus, tram, metro, train, ferry).
It includes \~8,500 records, each describing the location, accessibility, and hierarchy of a stop.

---

### Landing Layer (Bronze) – `hdw_ld.stops_ld`

* **Source** -> GTFS `stops.txt` (CSV)

* **Fields** (raw as delivered):

  * `stop_id` – unique GTFS stop identifier
  * `stop_code` – public-facing code (short numeric/alpha code)
  * `stop_name` – human-readable name of stop or station
  * `stop_desc` – additional description (optional)
  * `stop_lat` / `stop_lon` – geographic coordinates (string type at this stage)
  * `zone_id` – fare zone
  * `stop_url` – optional URL with stop info
  * `location_type` – indicates whether stop is a platform, station, etc.
  * `parent_station` – parent hub (if applicable)
  * `wheelchair_boarding` – accessibility flag (0=unknown, 1=accessible, 2=not accessible)
  * `platform_code` – platform number (if relevant)
  * `vehicle_type` – type of transport served (bus, tram, metro, etc.)
  * `radius` – radius for stop grouping
  * `_corrupt_record` – holds malformed input rows

* **Transformations** -> none, except adding `insert_time` and `rundate`.

* **QC checks**:

  * Table not empty
  * `stop_id` not null
  * Expected columns exist

**Screenshot:**
![Stops landing](/docs/img/data/stops_ld.png)

---

### Staging Layer (Silver) – `hdw_stg.stops_stg`

Landing data is cleaned, typed, and deduplicated.

* **Transformations:**

  * Cast `stop_lat` / `stop_lon` -> Double
  * Cast `wheelchair_boarding` -> Integer
  * Cast `radius` -> Integer
  * Drop unused columns (`stop_url`, `location_type`, `_corrupt_record`)
  * Replace nulls with `"N/A"` for `parent_station`, `platform_code`, `vehicle_type`
  * Deduplication on `stop_id` (keep latest by `insert_time`)

* **Audit / SCD2 helper columns added:**

  * `effective_start_dt`, `effective_end_dt`
  * `active_flg`
  * `insert_dt`, `update_dt`

* **QC checks:**

  * No duplicates by `stop_id`
  * Allowed values in `wheelchair_boarding` {0,1,2}

**Screenshot:**
![Stops staging](/docs/img/data/stops_stg.png)

---

### Data Warehouse Layer (Gold) – `hdw.dim_stops`

The final **dimension table** contains \~8,500 active stop records, managed via **Slowly Changing Dimension Type 2 (SCD2)**.

* **Change detection logic:**

  * Compares staging vs active warehouse records.
  * If attributes differ (name, description, lat/lon, zone, parent station, accessibility, platform, vehicle type, radius), the old record is closed (`effective_end_dt`, `active_flg=0`) and a new active row is inserted.

* **Columns:**

  * Business attributes (`stop_id`, `stop_name`, `stop_lat`, `stop_lon`, zone, accessibility, etc.)
  * Technical audit attributes (`insert_time`, `rundate`)
  * SCD2 fields (`effective_start_dt`, `effective_end_dt`, `active_flg`, `row_wid`)

This ensures **historical tracking of stop changes**, e.g., when a stop is renamed, moved, or reclassified.

**Screenshot:**
![Stops dim](/docs/img/data/stops_dim.png)

---

## Routes Data (≈530 records)

The **routes dataset** is derived from the GTFS `routes.txt` file. It contains all HSL transport routes across bus, tram, metro, train, and ferry modes.
Each route defines the logical grouping of trips, with both a short and long name and an associated agency/operator.
The dataset includes roughly **530 unique routes** in the Helsinki region.

---

### Landing Layer (Bronze) – `hdw_ld.routes_ld`

* **Source** -> GTFS `routes.txt` (CSV), initially read as raw text and split into columns.

* **Fields (raw):**

  * `route_id` – unique identifier for the route (matches GTFS)
  * `agency_id` – operator agency code
  * `route_short_name` – public-facing short code (bus number, line ID, etc.)
  * `route_long_name` – full descriptive route name
  * `route_desc` – additional description (unused)
  * `route_type` – numeric GTFS route type (e.g. 3 = bus, 0 = tram, 1 = subway, 2 = train, 4 = ferry, extended HSL types also supported)
  * `route_url` – optional link to agency info

* **Transformations** -> none, data is preserved as-is with `insert_time` and `rundate`.

* **QC checks:**

  * Table not empty
  * `route_id` not null
  * All expected columns exist

**Screenshot:**
![Routes landing](/docs/img/data/routes_ld.png)

---

### Staging Layer (Silver) – `hdw_stg.routes_stg`

Landing data is cleaned and deduplicated.

* **Transformations:**

  * Drop unused columns (`route_desc`, `route_url`)
  * Cast `route_type` -> Integer
  * Replace nulls in `route_short_name` with `"NA"`
  * Deduplicate by `route_id` (keep latest by `insert_time`)

* **Audit / SCD2 helper columns added:**

  * `effective_start_dt`, `effective_end_dt`
  * `active_flg`
  * `insert_dt`, `update_dt`

* **QC checks:**

  * No duplicates by `route_id`
  * Not empty

**Screenshot:**
![Routes staging](/docs/img/data/routes_stg.png)

---

### Data Warehouse Layer (Gold) – `hdw.dim_routes`

The final **dimension table** contains \~530 active routes, managed as an **SCD2 dimension**.

* **Change detection logic:**

  * Compares staging vs active warehouse records.
  * If attributes differ (`route_short_name`, `route_long_name`, `route_type`), the old record is closed (`effective_end_dt`, `active_flg=0`) and a new active row is inserted.

* **Columns:**

  * Business attributes (`route_id`, `agency_id`, `route_short_name`, `route_long_name`, `route_type`)
  * Technical audit attributes (`insert_time`, `rundate`)
  * SCD2 fields (`effective_start_dt`, `effective_end_dt`, `active_flg`, `row_wid`)

This allows for **full historical tracking** of route changes — for example, when a bus line is renamed, rebranded, or converted into another transport mode.

**Screenshot:**
![Routes dim](/docs/img/data/routes_dim.png)

---

## Star Schema in the Gold Layer

The Gold layer forms a **star schema** for analytical queries:

* **Fact table:** `fact_vehicle_position`
* **Dimension tables:** `dim_routes`, `dim_stops`

This enables analysis such as:

* On-time performance per route
* Vehicle occupancy trends
* Stop-level bottleneck analysis

### Example Query: Average Delay per Route

```sql
SELECT 
    f.oday,
    r.route_short_name,
    r.route_long_name,
    AVG(f.dl) AS avg_delay_seconds
FROM hdw.fact_vehicle_position f
JOIN hdw.dim_routes r
    ON f.route_id = r.route_id
    AND r.active_flg = 1
WHERE f.oday = DATE '2025-08-31'
GROUP BY f.oday, r.route_short_name, r.route_long_name
ORDER BY avg_delay_seconds ASC
LIMIT 10;
```
