# HSL Data Highlights

> This document is part of the [HSL Transport Streaming Lakehouse](../README.md). See the main README for architecture, business value, and setup instructions.

During two days (Sep 7–9, 2025), around 1.85M HSL vehicle events were collected across all routes, capturing both weekend and weekday traffic for a representative view of delays and performance.

Here are some observations from real-time monitoring and analytics in the Lakehouse Delta Gold layer.

---

## Table of Contents

* [Real-Time Monitoring Insights](#real-time-monitoring-insights)
  * [Total Active Vehicles Over Time](#total-active-vehicles-over-time)
  * [Average Bus Speed](#average-bus-speed)
  * [On-Time Arrival Performance](#on-time-arrival-performance)
* [Route-Level Analytics (Gold Layer)](#route-level-analytics-gold-layer)
  * [Most Popular Routes (by Bus Count)](#1-most-popular-routes-by-bus-count)
  * [Routes with the Most Unique Stops](#2-routes-with-the-most-unique-stops)
  * [Busiest Hubs](#3-busiest-hubs)
* [Extending the Analysis with More Dimensions](#extending-the-analysis-with-more-dimensions)

---

## Real-Time Monitoring Insights

### Total Active Vehicles Over Time

![amount](/docs/img/stream/grafana_v10_amount.png)

* Sunday traffic: We see only about half the number of active buses compared to weekdays, with the curve peaking well below weekday levels.
* Weekday traffic (Monday): Two clear peaks occur during the morning (≈7–9 AM) and afternoon (≈3–6 PM) rush hours, each reaching about 890 simultaneously active buses.
* Night hours: Between 3–4 AM, the number of active buses drops to just about 10 vehicles, reflecting minimal overnight service.
* Tuesday morning: The rising trend continues, following a similar pattern to Monday, with the morning peak forming by around 9 AM.

---

### Average Bus Speed

![speed](/docs/img/stream/grafana_v10_speed.png)

* Typical speed: Around 6–7 km/h most of the day.
* Rush hours: Speeds drop during peaks as buses stop more often and traffic is heavier.
* Night: Higher speeds (up to 9–10 km/h) with fewer buses and lighter traffic.
* Weekend: Sunday is less congested, so speeds stay slightly higher than weekdays.

---

### On-Time Arrival Performance

The chart shows the on-time arrival ratio for buses within 1, 2, and 3 minutes of the schedule:

![punctuality](/docs/img/stream/grafana_v10_delay.png)

* Within 3 min: About 80–85% of buses arrive on time, showing strong punctuality across the network.
* Within 2 min: Around 70–75% stay within two minutes of schedule.
* Within 1 min: About 40–50% manage to stay within one minute, reflecting tight time management.
* Night hours: Slight fluctuations appear overnight with fewer buses, but daytime punctuality remains consistently high despite traffic.

This highlights the strong time management and overall efficiency of Helsinki’s public transport system.

---

## Route-Level Analytics (Gold Layer)

In addition to real-time metrics, the Delta Gold layer enables deeper analytics at the route and stop levels:

### 1. Most Popular Routes (by Bus Count)

Routes with the highest number of buses indirectly reflect route popularity and passenger demand.
For example, Route 611 (Rautatientori -> Suutarila -> Tikkurila) had 45 buses in a single day, making it one of the busiest.

![most\_buses](/docs/img/sql/top_buses.png)

![611\_map](/docs/img/sql/611_most_buses.png)

---

### 2. Routes with the Most Unique Stops

Routes with the highest number of unique stops often indicate broader coverage across multiple neighborhoods, potentially reflecting higher demand.

Route 736 (Tikkurila -> Nikinmäki -> Korso) had 136 unique stops, making it the route with the largest stop coverage in the dataset.

![most\_stops](/docs/img/sql/top_stops_both_direction.png)

![736\_map](/docs/img/sql/736_most_stops.png)

---

### 3. Busiest Hubs

No surprise here: the Helsinki Central Railway Station and Helsinki Airport emerged as the busiest hubs, with the highest event density in the entire dataset.

![hot\_spots](/docs/img/sql/hot_spots.png)

![hot\_spots\_map](/docs/img/sql/hot_spots_map.png)

---

## Extending the Analysis with More Dimensions

By enriching the model with additional dimension tables, such as weather conditions, demographic data, or fare attributes, analysis can become both broader and more granular. This would enable insights like:

* Correlating delays with weather patterns.
* Understanding peak demand based on population density.
* Analyzing fare revenue versus route coverage.

This multi-dimensional view would significantly enhance both operational efficiency and strategic planning.

![dims\_all](/docs/img/sql/dims_all.png)
