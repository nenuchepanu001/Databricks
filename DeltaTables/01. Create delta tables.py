# Databricks notebook source
# MAGIC %md
# MAGIC ## Create delta table and Insert Values

# COMMAND ----------

# Delete the managed table or directory
dbutils.fs.rm('dbfs:/user/hive/warehouse/users_managed', recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/users_managed_python', recurse=True)



# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table users_managed(
# MAGIC     id int,
# MAGIC     name string
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Insert values to table

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into default.users_managed values(1,'chris');
# MAGIC insert into default.users_managed values(2,'maddy');
# MAGIC insert into default.users_managed values(3,'kate');

# COMMAND ----------

# MAGIC %md
# MAGIC ### managed delta table with python

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

DeltaTable.create(spark)\
    .tableName('default.users_managed_python')\
        .addColumn('id','string')\
        .addColumn('name','string')\
        .execute()


# COMMAND ----------

data = [('1','lucy'),('2', 'bruce')]

df =  spark.createDataFrame(data,['id','name'])
df.write.format('delta').mode('append').saveAsTable('users_managed_python')

# COMMAND ----------

# MAGIC %md
# MAGIC ### create partitioned table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table users_managed_partitioned(
# MAGIC     id int,
# MAGIC     name string,
# MAGIC     state string
# MAGIC ) using delta
# MAGIC partitioned by (state);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into default.users_managed_partitioned values('1','chris','NY');
# MAGIC insert into default.users_managed_partitioned values('2','maddy','Paris');
# MAGIC insert into default.users_managed_partitioned values('3','kate','Ohio');

# COMMAND ----------

# MAGIC %md
# MAGIC ### partitioned with python

# COMMAND ----------

DeltaTable.create(spark)\
    .tableName('default.users_managed_python_partitioned')\
        .addColumn('id','string')\
        .addColumn('name','string')\
        .addColumn('state','string')\
        .partitionedBy('state')\
        .execute()


# COMMAND ----------

data = [('1','lucy','AP'),('2', 'bruce','TN')]

df =  spark.createDataFrame(data,['id','name','state'])
df.write.format('delta').mode('append').saveAsTable('users_managed_python_partitioned')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Unmanaged delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table users_unmanaged(
# MAGIC   id int,
# MAGIC   name string
# MAGIC ) using delta
# MAGIC location 'dbfs:/user/hive/warehouse/users_managed';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.users_unmanaged;

# COMMAND ----------

df = spark.read.format('csv').option('header','true').load("dbfs:/FileStore/Streaming/Countries1.csv")

df =  df.withColumn('Citizens',df['Citizens'].cast('INT'))
df.write.format('delta').save('/FileStore/users_unmanaged_python')

# COMMAND ----------

spark.read.format('delta').load('/FileStore/users_unmanaged_python').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### describe details

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail default.users_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ### describe history

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.users_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ### show table extended

# COMMAND ----------

# MAGIC %sql
# MAGIC show table extended like 'users_managed';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy into Command

# COMMAND ----------

