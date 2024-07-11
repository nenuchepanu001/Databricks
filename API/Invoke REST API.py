# Databricks notebook source
# MAGIC %md
# MAGIC ### Using CURL

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC export DATABRICKS_TOKEN=<token>
# MAGIC
# MAGIC curl -X GET --header "Authorization:Bearer $DATABRICKS_TOKEN"  https://adb-4396980242210818.18.azuredatabricks.net/api/2.0/clusters/list

# COMMAND ----------

import requests

instance_id = 'adb-4396980242210818.18.azuredatabricks.net'
api_version = '/api/2.0'
api_command = '/clusters/list'
url = f"https://{instance_id}{api_version}{api_command}"

headers = {
    'Authorization': 'Bearer <token>'
}

try:
    response = requests.get(url=url, headers=headers)
    response.raise_for_status()  # Check for any HTTP errors
    # print(response.text)
    print(json.dumps(json.loads(response.text),indent=2))
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Job API

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC export DATABRICKS_TOKEN=<token>
# MAGIC
# MAGIC curl -X GET --header "Authorization:Bearer $DATABRICKS_TOKEN"  https://adb-4396980242210818.18.azuredatabricks.net/api/2.0/jobs/list

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC export DATABRICKS_TOKEN=<token>
# MAGIC
# MAGIC curl -X GET --header "Authorization:Bearer $DATABRICKS_TOKEN"  https://adb-4396980242210818.18.azuredatabricks.net/api/2.0/jobs/get?job_id=

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC export DATABRICKS_TOKEN=<token>
# MAGIC
# MAGIC curl --request POST --header "Authorization:Bearer $DATABRICKS_TOKEN"  https://adb-4396980242210818.18.azuredatabricks.net/api/2.0/jobs/create\
# MAGIC --data  '{'name':'Test_job'}'

# COMMAND ----------

import requests

instance_id = 'adb-4396980242210818.18.azuredatabricks.net'
api_version = '/api/2.0'
api_command = '/jobs/create'
url = f"https://{instance_id}{api_version}{api_command}"

headers = {
    'Authorization': 'Bearer <token>'
}

BODY = """{
  "name": "TEST_JOB",
  "new_cluster": {
    "spark_version": "7.3.x-scala2.12",
    "node_type_id": "Standard_D3_v2",
    "num_workers": 10
  },
  "libraries": [
    {
      "jar": "dbfs:/my-jar.jar"
    },
    {
      "maven": {
        "coordinates": "org.jsoup:jsoup:1.7.2"
      }
    }
  ],
  "spark_jar_task": {
    "main_class_name": "com.databricks.ComputeModels"
  },
  "timeout_seconds": 3600,
  "max_retries": 3
}"""


try:
    response = requests.post(url=url, headers=headers,json=json.loads(BODY))
    response.raise_for_status()  # Check for any HTTP errors
    print(response)
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

import json 
instance_id = 'adb-4396980242210818.18.azuredatabricks.net'
api_version = '/api/2.0'
api_command = '/jobs/list'
url = f"https://{instance_id}{api_version}{api_command}"

try:
    response = requests.get(url=url, headers=headers)
    response.raise_for_status()  # Check for any HTTP errors
    print(json.dumps(json.loads(response.text),indent=2))
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

import json 
instance_id = 'adb-4396980242210818.18.azuredatabricks.net'
api_version = '/api/2.0'
api_command = '/jobs/delete'
url = f"https://{instance_id}{api_version}{api_command}"

BODY="""{"job_id":504838035996150}"""
try:
    response = requests.post(url=url, headers=headers,json=json.loads(BODY))
    response.raise_for_status()  # Check for any HTTP errors
    print(json.dumps(json.loads(response.text),indent=2))
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Token rest api

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC export DATABRICKS_TOKEN=dapi54feade33b588f6c2f742fa87b76b1f9-3
# MAGIC
# MAGIC curl --request POST --header "Authorization: Bearer $DATABRICKS_TOKEN" https://adb-1734799944317394.14.azuredatabricks.net/api/2.0/token/create --data '{ "comment": "TEST_TOKEN", "lifetime_seconds": 7776000 }'
# MAGIC

# COMMAND ----------

