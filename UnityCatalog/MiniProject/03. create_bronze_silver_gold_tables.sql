-- Databricks notebook source
drop table if exists formula_dev.bronze.drivers;
create table if not exists formula_dev.bronze.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dbo DATE,
  nationality STRING,
  url STRING
) 
using json
options(path "abfss://bronze@ucadls0412ext.dfs.core.windows.net/drivers.json");

-- COMMAND ----------

drop table if exists formula_dev.bronze.results;
create table if not exists formula_dev.bronze.results
(
  resultId INT,
  raceId int,
  driverId int,
  constructionId int,
  number int,
  grid int,
  position int,
  positionText STRING,
  positionOrder INT,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed float,
  statusId string
) 
using json
options(path "abfss://bronze@ucadls0412ext.dfs.core.windows.net/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### silver managed

-- COMMAND ----------

drop table if exists formula_dev.silver.drivers;

create table if not exists formula_dev.silver.drivers
AS
select 
  driverId as driver_id,
  driverRef as driver_ref,
  number,
  code,
  concat(name.forename,' ',name.surname) as name,
  dbo,
  nationality,
  current_timestamp() as ingestion_date
from formula_dev.bronze.drivers;

-- COMMAND ----------

drop table if exists formula_dev.silver.results;
create table if not exists formula_dev.silver.results
AS
select 
  resultId as result_id,
  raceId as race_id,
  driverId as driver_id,
  constructionId as construction_id,
  number,
  grid,
  position,
  positionText as position_text,
  positionOrder as position_order,
  points,
  laps,
  time,
  milliseconds,
  fastestLap as fastest_lap,
  rank,
  fastestLapTime as fastest_lap_time,
  fastestLapSpeed as fastest_lap_speed,
  statusId as status_id,
  current_timestamp() as ingestion_date
from formula_dev.bronze.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### gold tables

-- COMMAND ----------


drop table if exists formula_dev.gold.driver_wins;

create table formula_dev.gold.driver_wins
AS
select d.name,count(1) as number_of_wins
  from formula_dev.silver.drivers d
  join formula_dev.silver.results r
    on (d.driver_id = r.driver_id)
  where r.position =1
  group by d.name;

select * from formula_dev.gold.driver_wins order by number_of_wins desc;

-- COMMAND ----------

