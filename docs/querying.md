# Querying Data

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

This project stores data across **Bronze**, **Silver**, and **Gold** layers in the **Delta Lakehouse** architecture.
With **Trino** (formerly PrestoSQL) connected to the **Hive Metastore**, all layers can be queried using standard SQL.

---

## Table of Contents

* [Querying with Trino + Hive Metastore](#querying-with-trino--hive-metastore)
* [Data Collection Window](#data-collection-window)
* [Example Query: Route Density - Vehicles vs Unique Stops](#example-query-route-density---vehicles-vs-unique-stops-gold-layer)
* [Example Query: Average Delay per Route](#example-query-average-delay-per-route-gold-layer)
* [Example Query: Partition Distribution Check](#example-query-partition-distribution-check-bronze-layer)
* [Example Query: Check Kafka–Spark Ingestion for Data Loss](#example-query-check-kafka-to-spark-ingestion-for-data-loss)
* [Summary & Other Queries](#summary--other-queries)

---

### Querying with Trino + Hive Metastore

[Trino](https://trino.io/) is a **distributed SQL query engine** designed for fast, interactive analytics over large datasets. It allows you to query multiple data sources using standard SQL, including object stores like **Amazon S3** and data lakes built with **Delta Lake**.

Trino integrates with the **Hive Metastore**, which provides:

* **Unified metadata**: Table definitions, partitions, and schema details are stored centrally in the Hive Metastore.
* **SQL interoperability**: Any tool connected to Trino can query Delta tables in S3 via Hive Metastore metadata. In this project, I used CloudBeaver as the SQL client.
* **Delta Lake support**: Trino reads Delta tables through the Hive catalog, so updates in Delta (e.g., merges, new partitions) are instantly reflected in query results.

 While the examples below use SQL queries via Trino and Hive Metastore, note that Trino can also be connected to [**visualization tools**](https://trino.io/ecosystem/index.html) such as Grafana, Tableau or Power BI, enabling interactive dashboards directly on top of the Bronze, Silver, and Gold layers.

---

## Data Collection Window

All queries in this section are based on data collected over a **two-day period** from **12:00 on September 7, 2025** to **14:00 on September 9, 2025**.

* **Total events ingested:** approximately **1,850,000** real-time vehicle events from HSL MQTT feeds (including about 850,000 events on Monday, September 8).
* **Coverage:** buses across all routes in the Helsinki region
* **Reasoning:** this time window includes both a weekend and busy weekdays (Monday and Tuesday), ensuring sufficient data for each route in **both directions**.

Since all routes complete multiple trips during this period, the dataset offers a **representative snapshot** of operational performance, delays, and vehicle counts.

---

### Example Query: Route Density - Vehicles vs Unique Stops (Gold Layer)

This query ranks routes by **veh\_to\_stops\_ratio** (how many distinct vehicles served a route compared to the number of unique stops that day). Higher values indicate denser vehicle coverage per stop - useful for spotting replacement services or surge capacity.

```sql
SELECT 
    f.oday,
    f.route,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT f.vehicle_number) AS bus_count,
    COUNT(DISTINCT f.stop) AS unique_stops,
    ROUND(
        CAST(COUNT(DISTINCT f.vehicle_number) AS DOUBLE) 
        / CAST(COUNT(DISTINCT f.stop) AS DOUBLE),
      2
    ) AS veh_to_stops_ratio
FROM hdw.fact_vehicle_position f
JOIN hdw.dim_routes r
    ON f.route_id = r.route_id
    AND r.active_flg = 1
WHERE f.oday = DATE '2025-09-08'
GROUP BY f.oday, f.route_id, f.route, r.route_short_name, r.route_long_name, r.route_type
ORDER BY veh_to_stops_ratio DESC
LIMIT 10;
```

![99v_query](/docs/img/sql/99v.png)

Route **99V** tops the list with an unusually high ratio. This bus actually is a metro replacement bus between Itäkeskus–Rastila–Vuosaari, added due to the temporary suspension of Metro service to Vuosaari and Rastila during the bridge renovation. The line ran as frequently as every 2.5 minutes at peak, which explains the high bus-to-stops density on that date.

Source: [https://www.hsl.fi/en/hsl/news/service-updates/2025/03/no-metro-services-to-vuosaari-or-rastila-from-5-may--we-will-increase-bus-services-in-the-area](https://www.hsl.fi/en/hsl/news/service-updates/2025/03/no-metro-services-to-vuosaari-or-rastila-from-5-may--we-will-increase-bus-services-in-the-area)

![route_on_map](/docs/img/sql/99v_map.png)

---

### Example Query: Average Delay per Route (Gold Layer)

The following query computes the **top 10 most delayed routes** on September 8 (Monday), 2025:

```sql
SELECT 
    f.oday,
    r.route_short_name,
    r.route_long_name,
    ROUND(AVG(f.dl) / 60, 2) AS avg_delay_min
FROM hdw.fact_vehicle_position f
JOIN hdw.dim_routes r
    ON f.route_id = r.route_id
    AND r.active_flg = 1
WHERE f.oday = DATE '2025-09-08'
GROUP BY f.oday, r.route_short_name, r.route_long_name
ORDER BY avg_delay_min DESC
LIMIT 10
```

![Delay Top-10](/docs/img/sql/top_delays.png)

The data indicates that on September 8, 2025, **Route 574**, which operates through the Peijas-Ruskeasanta-Aviapolis-Ylästö-Myyrmäki areas, experienced the highest average delay among all routes (~ 2.2 minutes).

Below is a screenshot from the [official HSL website](https://www.hsl.fi/), showing the location of Route 574 to provide a clearer picture of its operations.

![Routes 574](/docs/img/sql/574.png)

---

### Example Query: Partition Distribution Check (Bronze Layer)

```sql
SELECT
  partition AS kafka_partition,
  COUNT(*) AS event_count
FROM
  hdw_ld.events_ld
GROUP BY
  partition
ORDER BY
  partition;
```

![alt text](/docs/img/sql/part_events.png)

The results show that the event counts are roughly balanced across all Kafka partitions, indicating no major skew in data distribution.

---

### Example Query: Check Kafka to Spark Ingestion for Data Loss
Ensures no messages are missing when streaming data from Kafka to Spark in the Bronze layer.

```sql
WITH renamed_t AS (
  SELECT
    partition AS kafka_partition,
    offset AS kafka_offset
  FROM
    hdw_ld.events_ld
),
ordered AS (
  SELECT
    kafka_partition,
    kafka_offset,
    LEAD(kafka_offset) OVER (
      PARTITION BY kafka_partition ORDER BY kafka_offset
    ) AS next_offset
  FROM
    renamed_t
),
gaps AS (
  SELECT
    kafka_partition,
    kafka_offset + 1 AS missing_start,
    next_offset - 1 AS missing_end,
    (next_offset - kafka_offset - 1) AS missing_count
  FROM
    ordered
  WHERE
    next_offset IS NOT NULL
    AND next_offset > kafka_offset + 1
)
SELECT
  *
FROM
  gaps;
```

This ensures data completeness in the raw layer before processing downstream transformations. Over the two days, there were no missing messages.

---

## Summary & Other Queries

The SQL examples above demonstrate how Trino + Hive Metastore enables seamless querying across all layers of the Delta Lakehouse: from raw Kafka ingestion checks in the Bronze layer to aggregated KPIs in the Gold layer.

Similar analytics can be extended to include geo-analytics, stop-level analysis, and time-window analysis to detect peak hours - similar to the real-time monitoring I implemented via Prometheus and Grafana, but applied over longer time horizons such as monthly or yearly reports for trend analysis and capacity planning.
