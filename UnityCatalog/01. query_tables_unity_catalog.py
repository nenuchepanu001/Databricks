# Databricks notebook source
# MAGIC %sql
# MAGIC select * from demo_catalog.demo_schema.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC #### To see current Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_schema()

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema demo_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuits limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

display(spark.sql('show tables'))

# COMMAND ----------

df =  spark.table('demo_catalog.demo_schema.circuits')

# COMMAND ----------

display(df)

# COMMAND ----------

