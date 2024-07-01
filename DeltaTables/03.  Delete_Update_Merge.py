# Databricks notebook source
# MAGIC %sql
# MAGIC select * from users_managed_rows;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from users_managed_rows where Citizens=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from users_managed_rows;

# COMMAND ----------

# MAGIC %sql
# MAGIC update users_managed_rows set Citizens=202 where  Country='USA';
# MAGIC select * from users_managed_rows;

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

deltaTable = DeltaTable.forName(spark,'default.users_managed_python')
deltaTable.delete("Country='China'")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed_python;

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark,'default.users_managed_python')
deltaTable.update(
    condition="Country ='India'",
    set={"Citizens": '222'}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_managed_python;

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   city string
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into default.employee values (1,'john','Hamilton');
# MAGIC insert into default.employee values (2,'scarlett','New York');
# MAGIC insert into default.employee values (3,'kyra','Sydney');

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employee_incremental(
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   city string
# MAGIC ) using delta;
# MAGIC insert into default.employee_incremental values (1,'john','kenya');
# MAGIC insert into default.employee_incremental values (4,'symonds','Aus');

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into employee
# MAGIC using employee_incremental
# MAGIC on
# MAGIC employee.id =employee_incremental.id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ### merge with python

# COMMAND ----------

deltaTableemp =  DeltaTable.forName(spark,'default.employee')
deltaTableempIncr = DeltaTable.forName(spark,'employee_incremental')

dfUpdates =  deltaTableempIncr.toDF()

# COMMAND ----------

deltaTableemp.alias('emp')\
    .merge(
        dfUpdates.alias('empIncr'),
        'emp.id=empIncr.id'
    )\
        .whenMatchedUpdate(set={
            'name':"empIncr.name",
            'city':"empIncr.city"

        })\
            .whenNotMatchedInsert(values={
                'name':"empIncr.name",
                'id':'empIncr.id',
                'city':'empIncr.city'

            })\
                .execute()


# COMMAND ----------

