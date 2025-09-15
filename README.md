# HSL Transport Streaming Lakehouse

👉 [Jump straight to the Architecture Diagram](#architecture-and-workflow)

> Additional materials: [Architecture Rationale](docs/architecture_rationale.md) · [Data and Storage](docs/data.md) · [Processing and Data Flow](docs/processing.md) · [Querying Data](docs/querying.md) · [Some Facts about HSL](docs/results.md)

---

## Table of Contents

<!-- - [Overview](#overview)  
- [Target Audience](#target-audience)  
- [Business Value](#business-value)
- [Cost](#cost)  
- [Stack and Technologies](#stack-and-technologies)  
- [Architecture and Workflow](#architecture-and-workflow)  
- [Data Model and Warehouse](#data-model-and-warehouse)
- [Key Challenges and Debugging Notes](#key-challenges-and-debugging-notes) 
- [Scalability and Future Growth](#scalability-and-future-growth)
- [How to run - Recommended Order](#how-to-run---recommended-order)
- [Feedback](#feedback)  
- [Disclaimer](#disclaimer)  
- [License](#license) -->

---

## Overview

HSL (Helsingin seudun liikenne) is the Helsinki Regional Transport Authority, responsible for planning and operating public transport across the capital region of Finland (Helsinki, Espoo, Vantaa, and surrounding municipalities).

It provides open data via MQTT and GTFS feeds with real-time events like vehicle positions, trip progress, and stop arrivals. Buses, trams, metro, trains, and ferries emit these messages continuously, creating a fast-changing transport data stream.

To harness these high-volume streams, I built this project — **a unified real-time Lakehouse on open-source technologies: Kafka for ingestion, Spark for streaming and batch processing, Delta Lake + Hive for storage and catalog, Airflow for orchestration, Trino for SQL queries, and Prometheus + Grafana for monitoring**.

As a result, this project delivers a single open-source streaming Lakehouse that provides real-time operational visibility and durable historical analytics for smarter decision-making — all without vendor lock-in.

---

## Target Audience

While this project is demonstrated using public transport data, the underlying streaming Lakehouse design is applicable to a wide range of industries, particularly those that need to process millions of events per day in a cost-effective, open-source environment:

* **Startups and mid-sized businesses** – building scalable products without committing to expensive managed platforms.  
* **IoT and mobility** – processing millions of device and vehicle events per day in real time.  
* **Gaming companies** – tracking player telemetry, in-game transactions, and live events at scale.  
* **Financial services and fintech** – streaming transactions, fraud detection, and real-time risk monitoring on high-volume event streams.  
* **Telecom and streaming providers** – handling continuous event streams such as usage data, sessions, or content delivery metrics.

---

## Business Value

This automated transport pipeline delivers clear value:

* **Operational visibility in real time:** operators can instantly monitor vehicle activity and delays, enabling faster responses to incidents and service disruptions.
* **Capacity and demand planning:** integrating real-time with historical data supports forecasting passenger loads, optimizing schedules, and balancing fleet capacity.
* **Bottleneck detection:** continuous monitoring reveals recurring delays, overloaded stops, and underperforming routes, guiding targeted infrastructure or scheduling improvements.
* **Innovation and future use cases:** the GTFS-based model lays the foundation for advanced analytics and ML, such as predictive maintenance, demand forecasting, and route optimization.

---

## Cost

To illustrate the cost efficiency, I ran the full pipeline for two days (Sunday, September 7 – Tuesday, September 9, 2025), covering the entire Monday peak period. The pipeline processed **\~2 million real-time events** from HSL using **6 partitions** and a **10-second micro-batch interval**.

The resulting data across **Bronze, Silver, and Gold** layers in **Parquet** format totaled only **\~500 MB** in **Amazon S3**. The combined cost for S3 storage and PUT operations was approximately **\$0.05** for the entire run.

Extrapolated to **30 million events per month**, the cost would stay **under \$1/month**, while all streaming, batch processing, orchestration, SQL queries, and dashboards run **free of charge** on open-source technologies.

> Costs can be reduced even further by lowering the frequency of PUT requests (e.g., using longer micro-batch intervals).

---

## Stack and Technologies

This project uses the following technologies and services:

* Apache Spark (Structured Streaming, batch jobs)
* Delta Lake (transactional data lakehouse format)
* Apache Kafka with ZooKeeper (real-time ingestion)
* Hive Metastore with PostgreSQL (metadata and catalog)
* Apache Airflow (batch orchestration, SCD2 merges)
* Trino (SQL queries over Delta tables)
* Amazon S3 (cloud object storage for Bronze/Silver/Gold layers)
* Prometheus (metrics scraping)
* Grafana (real-time dashboards)
* Docker Compose (containerized local environment)
* Python (data quality checks, producer, utilities)

---

## Architecture and Workflow

![HSL Streaming Lakehouse Architecture](docs/img/streaming_s3.png)

The pipeline begins with **HSL Public Transport**, which publishes real-time vehicle data (positions, trip updates, delays) via MQTT. A custom **Python Kafka Producer** ingests these messages and pushes them into the `hsl_stream` Kafka topic.

From there, the data flows into multiple layers following the **Medallion Lakehouse architecture**:

* **Bronze (Landing) – raw data ingestion:**
  Spark Structured Streaming consumes Kafka events and appends them to the **Delta Lake landing layer**. At this stage, the data is raw and unprocessed, serving as a reliable source of truth.

* **Silver (Staging) – cleaned and normalized data:**
  Batch jobs orchestrated by **Apache Airflow** overwrite the staging tables with cleaned, normalized data.

* **Gold (Data Warehouse) – curated analytics layer:**
  In the events pipeline, Spark jobs **append fact records** into the Gold layer partitioned by operational day. For [dimension data](/docs/data.md#medallion-architecture-overview), separate batch jobs perform **SCD2 merges** to manage historical changes (name updates, stop relocations, etc.).

* **Storage and metadata (Delta Lake + Hive Metastore):**
  All three layers (Bronze, Silver, Gold) are managed in **Delta Lake**, which provides ACID transactions, schema evolution, and time travel. The **Hive Metastore** (with PostgreSQL backend) maintains metadata, enabling query engines to access the tables.

* **Query and analytics (Trino):**
  Analysts can query any layer (Bronze, Silver, or Gold) via **Trino**, which integrates with the Hive Metastore.

In addition to the Medallion data flow, the architecture also includes dedicated components for **real-time metrics** and **system monitoring**:

* **Real-time metrics (Prometheus + Grafana):**
  In parallel with landing ingestion, a dedicated Spark streaming job computes **business KPIs** (active vehicles, average speed, on-time ratio) with **sub-minute latency**. These metrics are exposed via the Python `prometheus_client` library and scraped by **Prometheus**, then visualized in near real time with **Grafana** dashboards.

  ![Grafana Dashboard Screenshot](/docs/img/stream/grafana_v6.png)

* **Monitoring and observability:**
  - **Kafka** is monitored through JMX exporters, with metrics scraped by Prometheus and visualized in Grafana.

    ![Kafka Dashboard Screenshot](/docs/img/kafka_monitoring.png)

    <details><summary>Attribution</summary>
    Based on this template: <a href="https://grafana.com/grafana/dashboards/11962-kafka-metrics/">Kafka Metrics</a>.
    </details>
      
  - **Spark** provides its own monitoring via the **Spark UI** (per job/streaming application).  

For cloud storage, this demo uses **Amazon S3**, but the same design works with old-school **HDFS** or other object stores such as Azure Blob Storage and Google Cloud Storage (GCS), depending on the deployment environment.

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

For more details on the data flows, schemas, and transformations, see [Data and Storage](docs/data.md).

---

## Key Challenges and Debugging Notes

### 1. Running Multiple Spark Jobs in Parallel

By default, when running multiple Spark jobs in parallel, one job can grab all available cores, leaving no resources for the others.

![Two Spark jobs running at the same time](/docs/img/debug/spark_parallel_1.png)

Solution: explicitly configure `spark.executor.cores` and `spark.cores.max` for each job in `docker-compose.yml`, so that resources are shared fairly:

```yaml
command: >
  /spark/bin/spark-submit
  --master spark://spark-master:7077
  --conf spark.ui.port=4046
  --conf spark.executor.memory=2g
  --conf spark.executor.cores=6
  --conf spark.cores.max=6
  /home/jobs/rt_metrics/stream_metrics.py
```

Both jobs can then run concurrently without interference:
![Two Spark jobs running at the same time](/docs/img/debug/spark_parallel_2.png)

Additionally, make sure the Kafka producer creates enough partitions to match the parallelism of the consumers:

```python
topic = NewTopic(name=TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
```

I eventually kept the default setup (without per-job resource configs), since my computer didn’t handle running multiple Spark jobs at once very well, and in practice I rarely needed them simultaneously.

---

### 2. Real-Time Metrics Windows

Configuring time windows for the [real-time metrics Spark job](/spark/jobs/rt_metrics/stream_metrics.py) was one of the most time-consuming tasks.

Current setup:

* Watermark = 1 minute
* Window = 5 minutes with 30-second slide
* Output = append mode (only closed windows are written to Prometheus)

```python
    .withWatermark("timestamp", "1 minutes")
    .groupBy(F.window(F.col("timestamp"), "5 minutes", "30 seconds"))
```

Despite using a 30-second slide, new data points appear on the Grafana dashboard only about every 2 minutes. Tuning the trigger interval and windowing logic is still in progress.

---

### 3. SCD2 Merge Logic

Implementing Slowly Changing Dimensions (SCD2) without rewriting entire tables required careful SQL design.

Final approach:

1. **MERGE** step marks outdated records as inactive.
2. **INSERT** step adds new records with updated values.

See detailed SQL patterns in [Processing docs](/docs/processing.md#batch-etl-with-slowly-changing-dimensions-scd2).

---

### 4. Hive Metastore Setup

Getting the Delta Lake – Hive – PostgreSQL stack working together was also challenging.

Main issues:

* Version mismatches between PostgreSQL and Hive
* Initialization scripts failing on first run

Fix: introduced a custom [entrypoint script](/hive-metastore/entrypoint.sh) to bootstrap the schema and ensure Hive Metastore connects reliably.

---

### 5. AWS S3 Integration

Connecting Spark, Hive, and Trino to S3 required multiple rounds of trial and error.

Key point: every service accessing S3 must have AWS credentials mounted (not just Spark). Once configured consistently, the pipeline was able to write/read from the same bucket without errors.

---

### 6. Airflow and Spark Separation

Initially, I tried using the Airflow Spark operator, but it caused issues in cluster mode (requiring Spark to run inside the Airflow container).

```text
[2025-08-25, 08:21:59 UTC] {spark_submit.py:644} INFO - Exception in thread "main" org.apache.spark.SparkException: Cluster deploy mode is currently not supported for python applications on standalone clusters.
```

So I decided to use the SSH operator instead, which allows Airflow to trigger Spark jobs running in their own containers.

One caveat: Airflow needed AWS credentials mounted in spark containers explicitly for its user (`sparkuser`):

```yml
- ./aws:/home/sparkuser/.aws:ro
```

This ensures Spark jobs triggered via Airflow can still access S3.

---

## Scalability and Future Growth

This project is built as a technical demo, but the architecture is **scalable by design**.  
Several clear growth paths exist:

* **Cluster scaling**  
  - The current Spark setup runs on Docker Compose, but scaling is best achieved by adding more Spark nodes.  
  - In production, this would typically be managed via **Kubernetes**, enabling elastic resource allocation and auto-scaling.

* **Data coverage**  
  - At the moment, only **bus events** are ingested from HSL.  
  - The pipeline can be extended to include **trams, metro, trains, and ferries**, greatly expanding the event volume.  
  - Additional GTFS dimensions beyond routes and stops can also be integrated.

* **Analytics potential**  
  - With more data, advanced analytics use cases become possible:  
    - **Geospatial queries** (vehicle positions, route coverage, hot-spot detection)  
    - **Deviation analysis** (detecting off-route driving, unusual patterns)
    - **Predictive modeling** (forecasting congestion, optimizing fleet allocation)

This demonstrates how an open-source Lakehouse approach can grow into an enterprise-grade platform capable of processing tens of millions of events per day while supporting increasingly sophisticated analytics.

### Raw S3 backup & decoupled landing

To improve durability and simplify reprocessing, a **Raw layer in S3** can be added to store the exact Kafka payloads as immutable files.

- **Ingestion:** Kafka -> (Kafka Connect S3 Sink) -> `s3://<bucket>/raw/hsl_stream/...`
- **Landing job:** Spark Structured Streaming reads from the Raw S3 path (instead of directly from Kafka) and writes to bronze Delta table.
- **Reprocessing:** If schemas change or logic needs fixes, I can replay from Raw S3 deterministically without relying on Kafka retention.

---

## How to run - Recommended Order

⚠️ **Note**: This project was originally designed for a single machine. Running all services at once with
`docker compose up` may overload your system. Instead, it’s better to start services gradually.

### 0. Configure AWS credentials & S3 bucket

1. **Create an S3 bucket** (must be globally unique). Example:

   ```bash
   aws s3 mb s3://my-hsl-lakehouse-bucket
   ```

   Replace `my-hsl-lakehouse-bucket` with your own unique name.

2. **Add AWS credentials** to `./aws/credentials` and `./aws/config`:

   ```
   [default]
   aws_access_key_id=YOUR_KEY
   aws_secret_access_key=YOUR_SECRET
   region=eu-north-1
   ```

3. **Update Spark configuration** (`spark/Dockerfile`) – set the default bucket name:

   ```dockerfile
   ENV S3_BUCKET=my-hsl-lakehouse-bucket
   ```

4. **Update Hive Metastore configuration** (`hive-metastore/hive-site.xml`) – point to the same S3 bucket:

   ```xml
    <property>
      <name>hive.metastore.warehouse.dir</name>
      <value>s3a://my-hsl-lakehouse-bucket/warehouse</value>
    </property>
   ```

This ensures Spark, Hive Metastore, and downstream jobs all write to the same S3 bucket.

---

### 1. Start Spark cluster

```bash
docker compose up -d spark-master spark-worker
```

* Spark Master UI -> [http://localhost:8081](http://localhost:8081)

---

### 2. Start core services

```bash
docker compose up -d zookeeper kafka grafana prometheus postgres hive-metastore dbeaver trino pgadmin airflow-webserver airflow-scheduler airflow-init
```

* Kafka -> `localhost:9092`
* Prometheus -> [http://localhost:9090](http://localhost:9090)
* Grafana -> [http://localhost:3000](http://localhost:3000) (user: `admin`, pass: `admin`)
* Airflow -> [http://localhost:8080](http://localhost:8080) (user: `airflow`, pass: `airflow`)
* Trino (via CloudBeaver) -> [http://localhost:8978](http://localhost:8978)
* PostgreSQL (via pgAdmin) -> [http://localhost:5050](http://localhost:5050)

`airflow-webserver` and `kafka` may be needed to re-run

---

### 3. Initialize Delta databases

Run short Spark job once (no need to repeat):

```bash
docker compose up spark-db-init
```

* Spark job UI -> [http://localhost:4043](http://localhost:4043)

* Creates the required Delta databases (hdw_ld, hdw_stg, hdw)
* Initializes control tables (used by batch ETL jobs and Airflow DAGs)
* Ensures the S3 warehouse folder structure exists

---

### 4. Start real-time ingestion

```bash
docker compose up -d hsl-transport-service
```

* Produces HSL events via MQTT -> Kafka collects them into `hsl_stream` topic.
* Monitor Kafka with Grafana dashboards.
* You can also increase the velocity of events by decreasing the `TIME_SLEEP` parameter in [HSL producer script](/hsl-transport-service/hsl_producer.py)

---

### 5. Process landing layer

Run Spark streaming job to collect some data, then stop:

```bash
docker compose up hsl-spark-streaming-landing
```

* Spark job UI -> [http://localhost:4044](http://localhost:4044)

* Consumes Kafka events.
* Stores them into **Delta Lake (Bronze)**.

```bash
docker compose down hsl-spark-streaming-landing
```

---

### 6. Run real-time metrics

```bash
docker compose up hsl-spark-streaming-metrics
```

* Spark job UI -> [http://localhost:4046](http://localhost:4046)

* Pushes metrics to **Prometheus**.
* View live dashboards in **Grafana**.
* Stop `hsl-spark-streaming-metrics` when done.

```bash
docker compose down hsl-spark-streaming-metrics
```

---

### 7. Batch processing (ETL)

* Open **Airflow** -> trigger DAGs to move data Bronze -> Silver -> Gold.

![Airflow DAGs](/docs/img/debug/airflow.png)

---

### 8. Explore the Data Warehouse

* Open **CloudBeaver** -> connect to **Trino** -> query `hdw.fact_*` or `hdw.dim_*` tables.
* Explore PostgreSQL metadata in **pgAdmin**.

---


## Feedback

I'm always happy to hear any feedback, suggestions, or ideas on how this project can be further improved.

Feel free to open an issue or contact me on [LinkedIn](https://www.linkedin.com/in/oleg-ivantsov/).

---

## Disclaimer

This project is built purely as a technical demo of open-source data engineering practices.  
The results and statistics shown here are based on publicly available HSL open data feeds (MQTT and GTFS), processed through the custom Lakehouse pipeline.  

* The numbers (bus counts, delays, speeds, route rankings) are illustrative and may not exactly match official HSL operational reports.  
* This repository is not affiliated with or endorsed by HSL.  
* All visualizations and metrics are provided for educational and portfolio purposes only.  

Official source: [HSL Open Data](https://www.hsl.fi/en/hsl/open-data).
Data is licensed under [Creative Commons BY 4.0 International Licence](https://creativecommons.org/licenses/by/4.0/).

---

## License

This project is released under the MIT License.
