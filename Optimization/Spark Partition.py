# Databricks notebook source
from pyspark.sql.functions import col

df = spark.read.format('csv')\
    .option('header','true')\
        .option('inferSchema','true')\
            .load('dbfs:/FileStore/Streaming/')

display(df)

# COMMAND ----------

df1 = df.filter("Country=='USA'")
df1.display()

# COMMAND ----------

df.repartition(col('Country'))\
    .write.mode('overwrite')\
        .parquet('dbfs:/FileStore/repartitioned_data/Country')


# COMMAND ----------

repartitioned_data =  spark.read.parquet('dbfs:/FileStore/repartitioned_data/Country')
repartitioned_data_filtered=repartitioned_data.filter("Country=='USA'")
repartitioned_data_filtered.display()

# COMMAND ----------

df.repartition(col('Country'))\
    .write.partitionBy('Country').mode('overwrite')\
        .parquet('dbfs:/FileStore/repartitioned_data/Country')


# COMMAND ----------

partitioned_data =  spark.read.parquet('dbfs:/FileStore/repartitioned_data/Country')
partitioned_data_filtered=partitioned_data.filter("Country=='USA'")
partitioned_data_filtered.display()

# COMMAND ----------

