-- Databricks notebook source
create external location if not exists uc_ext_bronze
url 'abfss://bronze@ucadls0412ext.dfs.core.windows.net/'
with (storage credential `databricks-ext-storage`);

-- COMMAND ----------

create external location if not exists uc_ext_silver
url 'abfss://silver@ucadls0412ext.dfs.core.windows.net/'
with (storage credential `databricks-ext-storage`);

-- COMMAND ----------

create external location if not exists uc_ext_gold
url 'abfss://gold@ucadls0412ext.dfs.core.windows.net/'
with (storage credential `databricks-ext-storage`);

-- COMMAND ----------

desc external location uc_ext_bronze;

-- COMMAND ----------

