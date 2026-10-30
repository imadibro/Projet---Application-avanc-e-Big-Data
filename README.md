SmartCity -- Real-Time Transport & Air Quality Monitoring

Project Overview

SmartCity is a Big Data mini-project that simulates a city through
real-time events related to public transport, air quality, and urban
incidents.

The project specification describes a streaming data pipeline that
collects events, processes them in real time, stores the resulting data
in PostgreSQL, and presents operational information through Power BI.

City used in the example data: Marrakech.

Objectives

The project aims to demonstrate the following Big Data concepts:

Generate real-time data using Python producers.

Ingest streaming events through Apache Kafka topics and partitions.

Process streaming data with Spark Structured Streaming.

Parse JSON events, enrich records, calculate indicators, and
aggregate data using time windows.

Store current-state data and calculated KPIs in PostgreSQL.

Visualize real-time monitoring information using Power BI
DirectQuery.

Architecture

The proposed architecture follows the Kappa pattern:

Python Producers (JSON)
          |
          v
        Kafka
          |
          v
Spark Structured Streaming
          |
          v
      PostgreSQL
          |
          v
       Power BI

Main components

Component                           Responsibility

Python Producers                    Generate simulated transport,
air-quality, and incident events in
JSON format.

Apache Kafka                        Receive and distribute streaming
events through dedicated topics.

Spark Structured Streaming          Read, parse, transform, enrich, and
aggregate incoming events.

PostgreSQL                          Store the latest entity states and
one-minute KPI results.

Data Streams

The specification defines three Kafka topics.

1. transport_events

Contains public transport information, such as bus or tram location,
speed, and delay.

Example event:

{
  "vehicle_id": "BUS_12",
  "line": "L3",
  "city": "Marrakech",
  "lat": 31.63,
  "lon": -8.00,
  "speed": 32.5,
  "delay_min": 4,
  "ts": "2026-03-03T13:40:12Z"
}

2. air_quality

Contains measurements collected by air-quality sensors.

Example event:

{
  "sensor_id": "AQ_5",
  "city": "Marrakech",
  "pm25": 18.2,
  "co2": 520,
  "ts": "2026-03-03T13:40:12Z"
}

3. incidents

Contains information about urban incidents, such as accidents and road
closures.

Example event:

{
  "incident_id": "INC_77",
  "city": "Marrakech",
  "type": "ACCIDENT",
  "severity": 3,
  "ts": "2026-03-03T13:40:12Z"
}

Spark Streaming Processing

The project specification identifies the following minimum processing
requirements.

JSON parsing and calculated indicators

Convert the event timestamp into an event_time timestamp column and
calculate these indicators:

Indicator       Condition

is_late       delay_min > 5
is_polluted   pm25 > 35 OR co2 > 900
is_severe     severity >= 3

Each indicator applies to its corresponding event stream.

One-minute KPIs

Calculate the following metrics using one-minute windows.

Transport

Average speed by city.

Number of delayed buses.

Top lines with the most delays.

Air quality

Average PM2.5 by city.

Number of pollution alerts.

Incidents

Number of incidents by city.

Maximum incident severity by city.

Latest state

Maintain the most recent known state for each:

Transport vehicle, identified by vehicle_id.

Air-quality sensor, identified by sensor_id.

PostgreSQL Data Model

The specification identifies two latest-state tables and three KPI
tables.

Latest-state tables

transport_latest

Column         Description

vehicle_id   Vehicle identifier (primary key)
line         Transport line
city         City
lat          Latitude
lon          Longitude
speed        Current speed
delay_min    Delay in minutes
event_time   Event timestamp

air_latest

Column         Description

sensor_id    Sensor identifier (primary key)
city         City
pm25         PM2.5 measurement
co2          CO2 measurement
event_time   Event timestamp

KPI tables

transport_kpi_1min

city

window_start

window_end

avg_speed

late_count

air_kpi_1min

city

window_start

window_end

avg_pm25

pollution_count

incidents_kpi_1min

city

window_start

window_end

incident_count

max_severity

Power BI Dashboard

The specification describes two dashboard pages using PostgreSQL
DirectQuery.

Page 1 -- Real-Time Monitoring

Table showing the latest transport state, including bus, delay, and
speed.

Map showing delayed buses.

Card showing the latest update.

Card showing active severe incidents.

Page 2 -- One-Minute KPIs

Line chart showing average speed by city.

Line chart showing average PM2.5 by city.

Bar chart showing incident count by city.

Red indicator when pollution_count > 0.

Expected Deliverables

According to the project specification, the expected deliverables are:

Python producer code for the three Kafka topics.

Spark Structured Streaming code for reading, transformations, and
PostgreSQL writes.

SQL script for creating the database tables.

Power BI dashboard file (.pbix).

A two-page report describing the architecture, screenshots, and KPI
interpretation.

Project Status and Scope

This README documents the project scope and requirements described in
the supplied mini-project specification. It does not assert that any
component has already been implemented or deployed.

Implementation details such as repository structure, installation
commands, environment variables, Kafka configuration, Spark
configuration, and database connection settings should be added once
they are defined by the project.

Reference

Mini-projet : SmartCity Temps Réel -- Transport & Qualité de l'air
ISGA -- Marrakech | Applications avancées Big Data | 2CI ISI
