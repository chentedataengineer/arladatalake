# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# COMMAND ----------

# MAGIC %run "/code/global/ETL/BoilerPlate/mainBoiler"

# COMMAND ----------

#TODO: paramterized json payload read
payload = {
"A": {
    "file_format": "csv",
    "file_name": "ventasNielsen",
    "file_input_path": "/FileStore/tables/datalakearla",
    "file_output_path": "/FileStore/tables/datalakearla/deltaTables",
    "file_output_name": "ventasNielsenDeltaTest",
    "file_header": "True"
    },
"B": {
    "file_format": "json",
    "file_name": "ventasTesco",
    "file_input_path": "/FileStore/tables/datalakearla",
    "file_output_path": "/FileStore/tables/datalakearla/deltaTables",
    "file_output_name": "ventasTescoDeltaTest"
    }
}

# COMMAND ----------

payload = payload["B"]
print(payload)

# COMMAND ----------

ss = SparkSession.builder \
    .appName("MiAplicacionSpark") \
    .getOrCreate()

# COMMAND ----------

dfTesco = fileReader(payload, ss)
