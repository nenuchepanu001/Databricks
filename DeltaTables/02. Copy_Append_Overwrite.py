# Databricks notebook source
# MAGIC %md
# MAGIC ### remove folders from hive warehouse

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/users_managed',recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/users_managed_python',recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### create table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table users_managed(
# MAGIC   Country string, 
# MAGIC   Citizens int
# MAGIC ) using delta;
# MAGIC
# MAGIC insert into default.users_managed values('Ind', 111);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy into command - sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.copyInto.formatCheck.enabled = false;
# MAGIC copy into default.users_managed from 'dbfs:/FileStore/users_delta' FILEFORMAT=DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### copy into command - python

# COMMAND ----------

table_name = 'default.users_managed_python'
source_data='/FileStore/users_delta'
source_format='DELTA'


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table users_managed_python;

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session if not already created
spark = SparkSession.builder.appName("Disable Delta Copy Format Check") \
    .config("spark.databricks.delta.copyInto.formatCheck.enabled", "false") .getOrCreate()

# Define the table name
table_name = 'default.users_managed_python'

# Create the managed Delta table
spark.sql(f"""
    CREATE TABLE {table_name} (
        Country STRING,
        Citizens INT
    )
    USING DELTA
""")

# Define the source data path
source_data = '/FileStore/users_delta'

# Construct the COPY INTO SQL query
sql_query = f"""
    COPY INTO {table_name}
    FROM '{source_data}'
    FILEFORMAT = DELTA
"""

# COMMAND ----------

# Execute the COPY INTO command
spark.sql("SET spark.databricks.delta.copyInto.formatCheck.enabled=false")
spark.conf.set("spark.databricks.delta.copyInto.formatCheck.enabled", "false")
spark.sql(sql_query)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed_python;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append mode - sql

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Step 1: Read the CSV file into a Temporary View
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_countries2
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   path 'dbfs:/FileStore/Streaming/Countries2.csv',
# MAGIC   header 'true',
# MAGIC   inferSchema 'true'
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Create the Table from the Temporary View
# MAGIC CREATE TABLE users_managed_rows AS
# MAGIC SELECT * FROM temp_view_countries2
# MAGIC LIMIT 3;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed_python;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into users_managed_python select * from users_managed_rows;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed_python;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table users_managed_python select * from default.users_managed_rows;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed_python;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append python

# COMMAND ----------

df =  spark.sql("select * from users_managed_rows")
df.write.format('delta').mode('append').saveAsTable('default.users_managed_python')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite

# COMMAND ----------

df =  spark.sql("select * from users_managed_rows")
df.write.format('delta').mode('overwrite').saveAsTable('default.users_managed_python')
display(df)

# COMMAND ----------

