# Databricks notebook source
# MAGIC %fs
# MAGIC rm -r /FileStore/Schema_Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet Schema Validation
# MAGIC

# COMMAND ----------

users = [('1','John'),('2','John')]

users_df = spark.createDataFrame(users,['id','f_name'])

users_df.write.format('parquet').save('/FileStore/Schema_Validation/Parquet')

# COMMAND ----------

spark.read.parquet('/FileStore/Schema_Validation/Parquet').show()

# COMMAND ----------

users = [('3','John'),('4','John')]

users_df = spark.createDataFrame(users,['id','f_name2'])

users_df.write.format('parquet').mode('append').save('/FileStore/Schema_Validation/Parquet')

# COMMAND ----------

spark.read.parquet('/FileStore/Schema_Validation/Parquet').show()

# COMMAND ----------

# MAGIC %md
# MAGIC In above schema validation is not working 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Schema Validation

# COMMAND ----------

users = [('1','John'),('2','John')]
users_df = spark.createDataFrame(users,['id','f_name'])
users_df.write.format('delta').save('/FileStore/Schema_Validation/Delta')

users = [('3','John'),('4','John')]
users_df = spark.createDataFrame(users,['id','f_name2'])
users_df.write.format('delta').option("mergeSchema", "true").mode('append').save('/FileStore/Schema_Validation/Delta')


# COMMAND ----------

spark.read.format('delta').load('/FileStore/Schema_Validation/Delta').show()

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

dt = DeltaTable.forPath(spark,'/FileStore/Schema_Validation/Delta')
df = dt.toDF()
display(df)

# COMMAND ----------

