# Databricks notebook source
# MAGIC %md
# MAGIC ###### 1. Data skipping information is collected automatically when you write data into delta table.
# MAGIC ###### 2. Delta lake on azure databrick takes advantages of this information (min and max values, null counts and total records per file ) at query timeto provide faster queries

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.format('csv')\
    .option('header','true')\
        .option('inferSchema','true')\
            .load('dbfs:/FileStore/Streaming/')

display(df)

# COMMAND ----------

df.write.format('parquet').saveAsTable('country_parquet')
df.write.format('delta').saveAsTable('country_delta')

# COMMAND ----------

dbutils.fs.head('dbfs:/user/hive/warehouse/country_delta/_delta_log/00000000000000000000.json')

# COMMAND ----------

country_parquet = spark.table('hive_metastore.default.country_parquet')
count_parquet= country_parquet.filter("Country=='USA'")
count_parquet.display()

# COMMAND ----------

