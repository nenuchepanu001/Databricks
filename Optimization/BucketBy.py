# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.format('csv')\
    .option('header','true')\
        .option('inferSchema','true')\
            .load('dbfs:/FileStore/Streaming/')

display(df)

# COMMAND ----------

df.write.format('parquet').saveAsTable('country_unbucketed')

# COMMAND ----------

df.write.format('parquet').bucketBy(2,"Country").saveAsTable('country_bucketed')

# COMMAND ----------

df.write.format('parquet').bucketBy(2,"Country").saveAsTable('country_bucketed2')

# COMMAND ----------

c_bucketed = spark.table('hive_metastore.default.country_bucketed')
c_bucketed2 = spark.table('hive_metastore.default.country_bucketed2')
c_unbucketed = spark.table('hive_metastore.default.country_unbucketed')

# COMMAND ----------

display(c_unbucketed.join(c_bucketed,"Country"))

# COMMAND ----------

display(c_unbucketed.repartition(2,'Country').join(c_bucketed,"Country"))

# COMMAND ----------

