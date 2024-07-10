# Databricks notebook source
from pyspark.sql.functions import col

df = spark.read.format('csv')\
    .option('header','true')\
        .option('inferSchema','true')\
            .load('dbfs:/FileStore/Streaming/')

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table country;
# MAGIC create table country(
# MAGIC     Country string,
# MAGIC     Citizens int,
# MAGIC     val int
# MAGIC )
# MAGIC partitioned By (Country,val)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into country 
# MAGIC values 
# MAGIC ( 'IND',100,1),
# MAGIC ('PAK',11,99),
# MAGIC ( 'IND',1000,2),
# MAGIC ('PAK',13,55),
# MAGIC ( 'IND',100,1),
# MAGIC ('PAK',12,1),
# MAGIC ( 'IND',100,2),
# MAGIC ('PAK',1,55);

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/country/Country=IND/val=1'))

# COMMAND ----------

# MAGIC %sql
# MAGIC Optimize country where Country='IND'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize country

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark,'country')
deltaTable.optimize().where('val=2').executeCompaction()

# COMMAND ----------

