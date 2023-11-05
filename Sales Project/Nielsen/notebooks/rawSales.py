# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import json

# COMMAND ----------

payload_file = dbutils.widgets.get("payloadFile")

# COMMAND ----------

# MAGIC %run "/code/global/ETL/BoilerPlate/mainBoiler"

# COMMAND ----------

ss = SparkSession.builder \
    .appName("MiAplicacionSpark") \
    .getOrCreate()

# COMMAND ----------

# Open the JSON file for reading
with open("/dbfs/FileStore/datalakearla/data/payload/raw/{0}".format(payload_file), 'r') as json_file:
    # Parse the JSON data and store it in a dictionary
    data = json.load(json_file)

# COMMAND ----------

print(data)

# COMMAND ----------

dfTesco = fileReader(data, ss)

# COMMAND ----------

dtNielsen = dataframeToDeltaTable(df=dfTesco, payload=data, spark_session=ss, writing_mode="overwrite")
