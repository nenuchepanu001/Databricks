-- Databricks notebook source
create catalog if not exists formula_dev;

-- COMMAND ----------

use catalog formula_dev;

-- COMMAND ----------

create schema if not exists bronze
managed location 'abfss://bronze@ucadls0412ext.dfs.core.windows.net/'

-- COMMAND ----------

create schema if not exists silver
managed location 'abfss://silver@ucadls0412ext.dfs.core.windows.net/'

-- COMMAND ----------

create schema if not exists gold
managed location 'abfss://gold@ucadls0412ext.dfs.core.windows.net/'

-- COMMAND ----------

show schemas;

-- COMMAND ----------

