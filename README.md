# HSL Transport Streaming Lakehouse

👉 [Jump straight to the Architecture Diagram](#architecture-and-workflow)

## Overview

HSL (Helsingin seudun liikenne) is the Helsinki Regional Transport Authority, responsible for planning and operating public transport across the capital region of Finland (Helsinki, Espoo, Vantaa, and surrounding municipalities).

It provides open data via MQTT and GTFS feeds with real-time events like vehicle positions, trip progress, and stop arrivals.
Buses, trams, metro, trains, and ferries emit these messages continuously, creating a fast-changing transport data stream.

This makes it a perfect use case for streaming pipelines, supporting both real-time monitoring and historical analysis.

And to achieve this, I built a **streaming Lakehouse** with:

* Kafka for ingestion
* Spark Structured Streaming for processing
* Delta Lake + Hive Metastore for storage (raw → staging → DW)
* Airflow for batch orchestration
* Trino for SQL queries
* Prometheus + Grafana for monitoring and dashboards

This project puts its main focus on Apache Spark and Delta Lakehouse. These technologies are also the core building blocks of the Databricks platform, and working with them directly helped me prepare for and pass the [**Databricks Data Engineer Associate exam**](https://www.databricks.com/learn/certification/data-engineer-associate).

---

## Business Value

This automated transport pipeline delivers clear value:

* **Operational visibility in real time:** operators can instantly monitor vehicle activity and delays, enabling faster responses to incidents and service disruptions.
* **Capacity and demand planning:** integrating real-time with historical data supports forecasting passenger loads, optimizing schedules, and balancing fleet capacity.
* **Bottleneck detection:** continuous monitoring reveals recurring delays, overloaded stops, and underperforming routes, guiding targeted infrastructure or scheduling improvements.
* **Innovation and future use cases:** the GTFS-based model lays the foundation for advanced analytics and ML, such as predictive maintenance, demand forecasting, and route optimization.

---

## Stack and Technologies

This project uses the following technologies and services:

* Apache Spark (Structured Streaming, batch jobs)
* Delta Lake (transactional data lakehouse format)
* Apache Kafka with ZooKeeper (real-time ingestion)
* Hive Metastore with PostgreSQL (metadata and catalog)
* Apache Airflow (batch orchestration, SCD2 merges)
* Trino (SQL queries over Delta tables)
* Prometheus (metrics scraping)
* Grafana (real-time dashboards)
* Docker Compose (containerized local environment)
* Python (data quality checks, producer, utilities)

---

## Architecture and Workflow

![HSL Streaming Lakehouse Architecture](docs/streaming_s3.png)

The pipeline begins with **HSL Public Transport**, which publishes real-time vehicle data (positions, trip updates, delays) via MQTT. A custom **Python Kafka Producer** ingests these messages and pushes them into the `hsl_stream` Kafka topic.

From there, the data flows into multiple layers following the **Medallion Lakehouse architecture**:

* **Bronze (Landing) – raw data ingestion:**
  Spark Structured Streaming consumes Kafka events and appends them to the **Delta Lake landing layer**. At this stage, the data is raw and unprocessed, serving as a reliable source of truth. In parallel, another Spark job computes **real-time business metrics** (active vehicles, average speed, on-time ratio) and exposes them to **Prometheus**, with live dashboards in **Grafana**.

* **Silver (Staging) – cleaned and normalized data:**
  Batch jobs orchestrated by **Apache Airflow** overwrite the staging tables with cleaned, normalized data.

* **Gold (Data Warehouse) – curated analytics layer:**
  Additional Spark jobs perform **SCD2 merges** into the **Delta Lake DW layer**.

* **Storage and metadata (Delta Lake + Hive Metastore):**
  All three layers (Bronze, Silver, Gold) are managed in **Delta Lake**, which provides ACID transactions, schema evolution, and time travel. The **Hive Metastore** (with PostgreSQL backend) maintains metadata, enabling query engines to access the tables.

* **Query and analytics (Trino):**
  Analysts can query any layer (Bronze, Silver, or Gold) via **Trino**, which integrates with the Hive Metastore.

* **Monitoring and observability (Prometheus + Grafana):**
  Kafka broker metrics (via JMX exporter) and custom Spark streaming metrics are collected by **Prometheus** and visualized in **Grafana**. This provides end-to-end visibility into ingestion, processing, and transport KPIs.

All services run in **Docker Compose**, making the setup reproducible locally while still following modern Lakehouse design principles.

---

## Data Model and Warehouse

In the **Gold layer (Data Warehouse)** three main tables are created:

* **dim\_routes**

  * route\_id, agency\_id, short/long name, route type
  * SCD2 fields: effective\_start\_dt, effective\_end\_dt, active\_flg, row\_wid

* **dim\_stops**

  * stop\_id, stop\_name, coordinates (lat, lon), zone, parent station, platform, vehicle\_type
  * SCD2 fields: effective\_start\_dt, effective\_end\_dt, active\_flg, row\_wid

* **fact\_vehicle\_position**

  * event-level data: date (oday), timestamp, route\_id, stop, vehicle\_number, operator, transport mode, direction, headsign, lat/long, speed, delay, occupancy, event\_type
  * partitioned by operational day (`oday`) for performance

All tables include **audit fields** (insert\_time, update\_time, rundate).

Together, these form a small **star schema**:

* **2 dimensions (routes, stops)**
* **1 fact (vehicle events)**

This allows analytical queries such as:

* On-time performance per route
* Average speed by route and vehicle
* Vehicle usage and occupancy trends over time

If you want to know more about the underlying data definitions, see the official **GTFS documentation**:

* [GTFS Overview](https://gtfs.org/documentation/overview/)
* [Extended route types](https://developers.google.com/transit/gtfs/reference/extended-route-types)

---

## Quickstart

1. **Start all services with Docker Compose**

   ```bash
   docker compose up --build
   ```

   This will build custom images (Airflow, Spark, Hive Metastore, Trino) and start all containers (Kafka, Prometheus, Grafana, Postgres, etc.).

2. **Wait until services are ready**

   * Airflow webserver → `http://localhost:8080` (user: `airflow`, pass: `airflow`)
   * Spark master UI → `http://localhost:8081`
   * Grafana → `http://localhost:3000` (user: `admin`, pass: `admin`)
   * See [UIs](#uis) section for full list.

3. **Run initial setup**

   * Airflow will auto-initialize on first run (`airflow-init` service).
   * Spark job `spark-db-init` will create the required Delta databases (`hdw_ld`, `hdw_stg`, `hdw`) and control tables.

4. **Start the pipeline**

   * The **producer** (`hsl-transport-service`) will push HSL MQTT messages into Kafka.
   * Spark streaming jobs (`hsl-spark-streaming-landing`, `hsl-spark-streaming-metrics`) will consume data, write to Delta Lake, and expose metrics to Prometheus.
   * Batch jobs (triggered in Airflow) will move data from Bronze → Silver → Gold.

5. **Explore the system**

   * Query Delta tables in **Trino** using **CloudBeaver** (`http://localhost:8978`) – convenient SQL client for Trino.
   * Manage and inspect the **PostgreSQL** metadata database with **pgAdmin** (`http://localhost:5050`).
   * Open **Grafana** (`http://localhost:3000`) for real-time dashboards.


## UIs

* Airflow `http://localhost:8080` — username: `airflow`, password: `airflow`
* Spark UI (master) `http://localhost:8081` — no auth
* Grafana `http://localhost:3000` — username: `admin`, password: `admin`
* Prometheus `http://localhost:9090` — no auth
* Trino `http://localhost:8083` — no auth (connect via Hive catalog)
* pgAdmin `http://localhost:5050` — email: `admin@admin.com`, password: `admin`
* CloudBeaver `http://localhost:8978` — no auth on first login, workspace persisted in volume

---

## Feedback

I'm always happy to hear any feedback, suggestions, or ideas on how this project can be further improved.

Feel free to open an issue or contact me on [LinkedIn](https://www.linkedin.com/in/oleg-ivantsov/).

---

## License

This project is released under the MIT License.
