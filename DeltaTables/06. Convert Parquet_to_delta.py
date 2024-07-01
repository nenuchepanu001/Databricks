# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create table users_parquet(
# MAGIC   id int,
# MAGIC   name string
# MAGIC
# MAGIC )using parquet;
# MAGIC
# MAGIC INSERT INTO default.users_parquet VALUES (1, 'a');
# MAGIC INSERT INTO default.users_parquet VALUES (2, 'b');
# MAGIC INSERT INTO default.users_parquet VALUES (3, 'c');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC convert to delta default.users_parquet

# COMMAND ----------

