# Architecture Rationale

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

This document explains why the HSL Streaming Lakehouse architecture was designed the way it is. It covers cost constraints, technology choices, trade-offs, and possible future improvements.

---

## Table of Contents

[Overall Architecture Principles](#1-overall-architecture-principles)
[Cost Constraints](#2-cost-constraints)
[Real-Time Requirements & Why Spark Structured Streaming](#3-real-time-requirements--why-spark-structured-streaming)
[Why Lakehouse Instead of Traditional Databases](#4-why-lakehouse-instead-of-traditional-databases)
[Data Ingestion – Why Kafka for Streaming](#5-data-ingestion--why-kafka-for-streaming)
[Real-Time Metrics – Why Prometheus Instead of ClickHouse](#6-real-time-metrics--why-prometheus-instead-of-clickhouse)
[Query Engine – Trino vs Athena](#7-query-engine--trino-vs-athena)
[Orchestration – Airflow](#8-orchestration--airflow)
[Future Improvements](#9-future-improvements)

---

## 1. Overall Architecture Principles

The design combines concepts from both:

1. **Classic Hadoop architecture**:

   * **HDFS** as storage
   * **Hive Metastore** for schema & metadata
   * **Spark** for processing
   * **Airflow** for orchestration
   * **Kafka** for ingestion

2. **Databricks Lakehouse platform**:
   I see how popular Databricks has become in modern data engineering. Its stack (Spark, Delta Lake, streaming + batch, orchestration, notebooks, etc.) solves most real-world scenarios.
   My goal was to **build a simplified on-premise Databricks alternative** using open-source tools.

Once this core vision was defined, I adapted the design step by step to the project constraints and real-time requirements.

---

## 2. Cost Constraints

One of the main requirements was to **keep the project cost close to zero**, so that I could test and run experiments as often as needed.

* **Streaming workloads** require frequent job runs (micro-batches every 30–60 sec), so running the entire stack in the cloud would quickly become expensive.
* When processing up to **1M+ events per day** with 30-sec micro-batches, it was important that I could scale experiments without worrying about cost.
* The only paid component here is **Amazon S3 storage**. Everything else runs on local Docker containers.

While it’s possible to replace S3 with **HDFS** to go completely free, modern object stores like S3/Azure Blob/GCS are cheap, reliable, and widely used in production, so keeping S3 made sense.

---

## 3. Real-Time Requirements & Why Spark Structured Streaming

This project processes **high-velocity IoT data** from HSL vehicles, producing thousands of events per minute with position, speed, delay, and trip updates.
The requirements were:

* **Low latency** for real-time monitoring (seconds, not minutes)
* **Windowed aggregations** (e.g., 5-min average speed, on-time ratio)
* **Seamless integration** with batch ETL for historical analytics
* **Scalability** for both development and future production environments
* **Cost efficiency** -> no expensive managed services, everything open-source

Because of these needs, I required a **unified processing engine** for both real-time and batch workloads.

### Why Spark Structured Streaming

I chose **Spark Structured Streaming** because:

* **Unified engine:** uses the same core as batch Spark -> one codebase for both real-time and offline pipelines
* **Reliability:** provides **exactly-once semantics** with checkpoints and **micro-batch processing**, so jobs can recover after failures
* **Scalability:** runs on a **standalone cluster** locally but can later move to **Kubernetes** or **Databricks**

This keeps the whole architecture consistent: **one engine for streaming, batch ETL, and analytics**, reducing complexity and operational overhead.

---

## 4. Why Lakehouse Instead of Traditional Databases

Traditional databases have limitations when dealing with **big data**:

* Scaling beyond single-node storage gets expensive and complex.
* Many DBs lack **schema evolution**, **ACID transactions**, and **time travel** for historical data.
* Real-time + batch workloads often require different solutions.

A **Lakehouse** solves all of this:

* **Infinite storage** on S3
* **Delta Lake** brings ACID, versioning, and schema evolution
* Cost efficiency -> storage is cheap, compute is separate
* Flexibility to handle both **streaming** and **batch** workloads

---

## 5. Data Ingestion – Why Kafka for Streaming

Kafka was the natural choice because it:

* **Decouples producers & consumers** – acts as a durable buffer, so ingestion never overloads processing.
* **Scales easily** – partitions map directly to Spark tasks for parallel processing.
* **Handles high throughput & low latency** – millions of events/sec with millisecond delays.
* **Enables replay & fault tolerance** – stored events can be reprocessed anytime if jobs fail.
* **Integrates natively** with Spark and Prometheus for minimal glue code.

---

## 6. Real-Time Metrics – Why Prometheus Instead of ClickHouse

The real-time metrics part was the hardest decision:

* Initially, I considered **ClickHouse** because it’s a powerful OLAP database, ideal for aggregations.
* But this would require **separate storage** for metrics, while my **Lakehouse** already stores all raw and processed data.
* I also didn’t need long-term storage for metrics — just real-time monitoring.

The final solution:

* Use **Prometheus** for metric scraping + **Grafana** for visualization
* A Spark job computes **aggregates on the fly** using time windows -> Prometheus stores only the results as **Gauge metrics**

This avoided extra services and reused existing monitoring infrastructure (Kafka already integrated with Prometheus).

---

## 7. Query Engine – Trino vs Athena

I needed an **SQL engine** over Delta tables stored in S3:

* **Athena** works well, but it’s paid + vendor lock-in.
* **Trino** (open-source) + **Hive Metastore** provide the same capabilities without cost.
* Trino also supports many connectors, can scale horizontally, and handles partitioned big data well.

---

## 8. Orchestration – Airflow

For batch ETL jobs (Bronze -> Silver -> Gold), **Apache Airflow** is the open-source standard:

* Clear DAG definitions
* Scheduling, monitoring, retry policies
* Easy integration with Spark jobs via SSH or custom scripts

The only challenge was triggering Spark jobs on a **standalone cluster** from Airflow containers, but mounting credentials and using the SSH operator solved it.

---

## 9. Future Improvements

Several next steps are planned:

* **Kubernetes** -> to dynamically scale Spark nodes
* **dbt** -> to better manage SQL transformations between layers
* **Schema enforcement & validation** -> using tools like **Great Expectations**
