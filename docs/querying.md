# Querying Data

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

This project stores data across **Bronze**, **Silver**, and **Gold** layers in the **Delta Lakehouse** architecture.
With **Trino** (formerly PrestoSQL) connected to the **Hive Metastore**, all layers can be queried using standard SQL.

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

### Partition Distribution Check (Bronze Layer)

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

### Example Query: Identify whether Ingestion by Kafka -> Spark Structured Streaming is operating without any Data Loss (Bronze Layer)

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

