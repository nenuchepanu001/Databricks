# Databricks notebook source
# MAGIC %sql
# MAGIC drop table employee;
# MAGIC
# MAGIC create table employee(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   birthdate timestamp,
# MAGIC   dateofbirth date generated always as(cast(birthdate as 
# MAGIC    date)),
# MAGIC    yearofdate int generated always as(year(birthdate))
# MAGIC ) using delta 
# MAGIC partitioned by (dateofbirth);
# MAGIC
# MAGIC insert into default.employee (id,name,birthdate) values (1,'john','1999-01-01 11:00:00');

# COMMAND ----------

# MAGIC %md
# MAGIC ### check using describe history

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into default.employee (id,name,birthdate) values (1,'kyra','1991-01-01 11:00:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into default.employee (id,name,birthdate) values (3,'lov','1891-01-01 11:00:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history default.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Timestamp and version

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee timestamp as of '2024-07-01T08:44:17.000+00:00'

# COMMAND ----------

# MAGIC %md
# MAGIC #### using syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee@v2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee@202407010844170000000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update employee set id =2
# MAGIC where name='kyra'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee@v4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get deleted data back

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from employee where id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employee;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee version as of 4 where id =1

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into employee select * from employee version as of 4 where id =1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee;

# COMMAND ----------

