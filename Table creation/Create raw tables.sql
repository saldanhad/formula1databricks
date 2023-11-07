-- Databricks notebook source
-- DBTITLE 1,Create circuits table

--create database
CREATE DATABASE IF NOT EXISTS f1_raw;

--create table
DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
                                           circuitRef STRING,
                                           name STRING,
                                           location STRING,
                                           country STRING,
                                           lat DOUBLE,
                                           lng DOUBLE,
                                           alt INT,
                                           url STRING)
                                           
USING csv
OPTIONS (path "/mnt/instablobcart/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)

USING CSV
OPTIONS (path "/mnt/instablobcart/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create table for JSON files

-- COMMAND ----------

--for constructors file
DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING)

USING JSON
OPTIONS (path "/mnt/instablobcart/raw/constructors.json")

-- COMMAND ----------

SELECT * from f1_raw.constructors;

-- COMMAND ----------

--for drivers file

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING)

using json
OPTIONS (path "/mnt/instablobcart/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

--for results table

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT, grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING)

using json
OPTIONS (path "/mnt/instablobcart/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

--for pit stops file 

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
using JSON
OPTIONS(path "/mnt/instablobcart/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pitstops;

-- COMMAND ----------

-- DBTITLE 1,Lap Times Table
--lap times table
DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)

USING csv
OPTIONS (path "/mnt/instablobcart/raw/lap_times")

-- COMMAND ----------

select count(*) from f1_raw.lap_times

-- COMMAND ----------

--qualifying file 

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT)

USING JSON
OPTIONS (path "/mnt/instablobcart/raw/qualifying", multiLine True)

-- COMMAND ----------

select * from f1_raw.qualifying
