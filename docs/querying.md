# Querying Data

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

This project stores data across **Bronze**, **Silver**, and **Gold** layers in the **Delta Lakehouse** architecture.
With **Trino** (formerly PrestoSQL) connected to the **Hive Metastore**, all layers can be queried using standard SQL.

### Querying with Trino + Hive Metastore

[Trino](https://trino.io/) is a **distributed SQL query engine** designed for fast, interactive analytics over large datasets. It allows you to query multiple data sources using standard SQL, including object stores like **Amazon S3** and data lakes built with **Delta Lake**.

Trino integrates with the **Hive Metastore**, which provides:

* **Unified metadata**: Table definitions, partitions, and schema details are stored centrally in the Hive Metastore.
* **SQL interoperability**: Any tool connected to Trino can query Delta tables in S3 via Hive Metastore metadata. In our case, we use CloudBeaver as the SQL client.
* **Delta Lake support**: Trino reads Delta tables through the Hive catalog, so updates in Delta (e.g., merges, new partitions) are instantly reflected in query results.

 While the examples below use SQL queries via Trino and Hive Metastore, note that Trino can also be connected to [**visualization tools**](https://trino.io/ecosystem/index.html) such as Grafana, Tableau or Power BI, enabling interactive dashboards directly on top of the Bronze, Silver, and Gold layers.

---

## Data Collection Window

All queries in this section are based on data collected over a **two-day period** from **12:00 on September 7, 2025** to **14:00 on September 9, 2025**.

* **Total events ingested:** approximately **1,850,000** real-time vehicle events from HSL MQTT feeds (including about 850,000 events on Monday, September 8).
* **Coverage:** buses across all routes in the Helsinki region
* **Reasoning:** this time window includes both a weekend and busy weekdays (Monday and Tuesday), ensuring sufficient data for each route in **both directions**.

Since all routes complete multiple trips during this period, the dataset offers a **representative snapshot** of operational performance, delays, and vehicle counts without requiring full-day data ingestion.

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

### Example Query: Checking Missing Partitions (Bronze Layer)

In the **Bronze** layer, raw events are partitioned by operational day (`oday`). This query finds any days in September 2025 **with no data** in the landing table:

```sql
SELECT d.day::DATE AS missing_day
FROM UNNEST(SEQUENCE(DATE '2025-09-01', DATE '2025-09-30', INTERVAL '1' DAY)) AS t(d)
LEFT JOIN (
    SELECT DISTINCT oday
    FROM hdw_ld.vehicle_events_bronze
) b ON d.day = b.oday
WHERE b.oday IS NULL
ORDER BY missing_day;
```

This ensures **data completeness** in the raw layer before processing downstream transformations.

---

### Example Query: Event Counts per Partition (Bronze Layer)

To monitor data ingestion volumes:

```sql
SELECT 
    oday, 
    COUNT(*) AS event_count
FROM hdw_ld.vehicle_events_bronze
GROUP BY oday
ORDER BY oday;
```

This query quickly highlights **days with unusually low or high event counts** in the raw data.

---