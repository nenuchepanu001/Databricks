# Databricks notebook source

dbutils.fs.mkdirs('/FileStore/autoloader/csv')
dbutils.fs.mkdirs('/FileStore/autoloader/json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load CSV

# COMMAND ----------


df = (spark.readStream.format('cloudFiles')
.option('cloudFiles.format','csv')
.option('cloudFiles.schemaLocation','/FileStore/autoloader/csv')
.load('/FileStore/autoloader/csv'))

# COMMAND ----------

df =  spark.read.format('csv').option('header','true').load('/FileStore/autoloader/csv')
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC #### _rescued data

# COMMAND ----------


df = (spark.readStream.format('cloudFiles')
.option('cloudFiles.format','csv')
.option('cloudFiles.schemaLocation','/FileStore/autoloader/csv')
.option('cloudFiles.inferColumnTypes','true')
.option('cloudFiles.schemaEvolutionMode','rescue')
.load('/FileStore/autoloader/csv'))

display(df)

# COMMAND ----------

df.writeStream.option('mergeSchema','true').option('checkpointLocation','/tmp/checkpoint').trigger(processingTime='10 seconds').toTable('hve_metastore.default.countries')

# COMMAND ----------

