# Databricks notebook source
# MAGIC %md
# MAGIC ####
# MAGIC **PERMISSIVE**: Default mode. It allows records to be partially parsed even if they contain corrupted data. The corrupted records are processed and the data is returned, filling in with nulls if necessary.

# COMMAND ----------

filePath = 'dbfs:/FileStore/AgentRecords.csv'

Agent_df = spark.read.format('csv')\
            .option('header','true')\
            .option('inferSchema','true')\
            .option('mode','PERMISSIVE')\
            .load(filePath)
Agent_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **DROPMALFORMED:** In this mode, any records that are malformed (not conforming to the schema) are dropped

# COMMAND ----------

filePath = 'dbfs:/FileStore/AgentRecords_1.csv'
Agent_df = spark.read.format('csv')\
            .option('header','true')\
            .option('inferSchema','true')\
            .option('mode','DROPMALFORMED')\
            .load(filePath)
Agent_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **FAILFAST:** If any record is malformed, the entire read operation will fail immediately.

# COMMAND ----------

filePath = 'dbfs:/FileStore/AgentRecords_1.csv'
Agent_df = spark.read.format('csv')\
            .option('header','true')\
            .option('inferSchema','true')\
            .option('mode','FAILFAST')\
            .load(filePath)
Agent_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Store corrupted records.**
# MAGIC For this first, we need to make change in the Agent schema, which we defined earlier. We need to add StructField('_corrupt_record', StringType(), True). Below is the new schema

# COMMAND ----------

Agent_schema = StructType(
[
StructField( "Agent_ID", IntegerType(), True),
StructField( "Name", StringType(), True),
StructField( "Age", IntegerType(), True),
StructField( "Salary", IntegerType(), True),
StructField( "Address", StringType(), True),
StructField( "Insurance_Type", StringType(), True)
]
)


# COMMAND ----------

file_path = 'dbfs:/FileStore/AgentRecords_1.csv'
# Use the defined schema in the read operation
Agent_df = spark.read.format('csv') \
                .option('header', 'true') \
                .option('inferschema', 'false') \
                .option('mode', 'DROPMALFORMED') \
                .schema(Agent_schema) \
                .load(file_path)

Agent_df.write.format('csv').save('dbfs:/FileStore/tables/AgentRecord/Record')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

Agent_schema = StructType(
                            [
                            StructField( "Agent_ID", IntegerType(), True),
                            StructField( "Name", StringType(), True),
                            StructField( "Age", IntegerType(), True),
                            StructField( "Salary", IntegerType(), True),
                            StructField( "Address", StringType(), True),
                            StructField( "Insurance_Type", StringType(), True),
                            StructField('_corrupt_record', StringType(), True)
                            ]
                        )

# COMMAND ----------

file_path = 'dbfs:/FileStore/AgentRecords_1.csv'
bad_record_file_path='dbfs:/FileStore/badrecords'
bad_record_file_path = 'dbfs:/FileStore/tables/bad_records'

Agent_df = spark.read.format('csv')\
                    .option('header','true')\
                    .option('inferschema','false')\
                    .schema(Agent_schema)\
                    .option('badRecordsPath', bad_record_file_path)\
                    .load(file_path)
Agent_df.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/bad_records/20240725T070636/bad_records/

# COMMAND ----------

filePath='dbfs:/FileStore/tables/bad_records/20240725T070636/bad_records/part-00000-bbd614df-58c5-4e62-9275-39171757e828'
bad_df = spark.read.format('json').load(filePath)
display(bad_df)

# COMMAND ----------

# Correct the path to your source data and the path to store bad records
source_data_path = "dbfs:/FileStore/AgentRecords_1.csv"
bad_records_path = "dbfs:/FileStore/tables/bad_records/20240725T070636/bad_records/part-00000-bbd614df-58c5-4e62-9275-39171757e828"

# Read the bad records as JSON files from the badRecordsPath
bad_records_df = spark.read.json(bad_records_path)

# Display the bad records DataFrame
display(bad_records_df)


# COMMAND ----------


