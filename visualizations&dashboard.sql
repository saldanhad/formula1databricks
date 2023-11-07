-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style= "color:Black; text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------


USE f1_processed;

-- COMMAND ----------

select * from f1_processed.drivers

-- COMMAND ----------

DROP TABLE f1_presentation.calculated_race_results

-- COMMAND ----------

-- DBTITLE 1,calculated race resullts
--crate table calculated_race_results
CREATE TABLE f1_presentation.calculated_race_results
USING parquet
SELECT races.race_year,
  constructors.name as team_name,
  drivers.name as driver_name,
  results.position,
  results.points,
  11 - results.position as calculated_points
FROM results
JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
JOIN f1_processed.races ON (results.race_id = races.race_id)
where results.position <=10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

-- DBTITLE 1,dominant drivers
select driver_name,
count(1) as total_races,
 sum(calculated_points) as total_points,
 avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
group by driver_name
HAVING count(1) >=50
order by avg_points DESC;

-- COMMAND ----------

--dominated in the last decade

select driver_name,
count(1) as total_races,
 sum(calculated_points) as total_points,
 avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by driver_name
HAVING count(1) >=50
order by avg_points DESC;

-- COMMAND ----------

-- DBTITLE 1,dominant teams
SELECT team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

--last 10 years

SELECT team_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year between 2001 and 2011
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- DBTITLE 1,Dominant Drivers Visualization
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
select driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points,
  RANK() OVER(order by avg(calculated_points) DESC) driver_rank
 from f1_presentation.calculated_race_results
group by driver_name
HAVING count(1) >=50
order by avg_points DESC;

-- COMMAND ----------

select race_year,
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers WHERE driver_rank <=10)
group by race_year,driver_name
order by race_year,avg_points DESC;

-- COMMAND ----------

-- DBTITLE 1,Dominant Teams
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
  COUNT(team_name) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points,
  RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(team_name) >=100
ORDER BY avg_points DESC

-- COMMAND ----------

select * from v_dominant_teams

-- COMMAND ----------

select race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_teams WHERE team_rank <=5)
group by race_year,team_name
order by race_year,avg_points DESC;

-- COMMAND ----------


