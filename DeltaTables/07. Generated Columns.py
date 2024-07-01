# Databricks notebook source
# MAGIC %md
# MAGIC ### Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists employee1(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   birthdate timestamp,
# MAGIC   dateofbirth date generated always as (cast(birthdate as date))
# MAGIC ) using delta;
# MAGIC
# MAGIC INSERT INTO employee1(id,name,birthdate) VALUES (1, 'john', '1999-01-01T11:00:00.000+00:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### using python

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

DeltaTable.create(spark) \
    .tableName('emp') \
    .addColumn('id', 'INT') \
    .addColumn('name', 'STRING') \
    .addColumn('birthdate', 'TIMESTAMP') \
    .addColumn('dateofbirth', 'DATE', generatedAlwaysAs="CAST(birthdate AS DATE)") \
    .execute()

# COMMAND ----------

