# Databricks notebook source
#Set up rama develop en el repo arladatalake

# COMMAND ----------

#TODO: Technology and file format to be created as input parameters
def fileReader(payload, spark):
    # Leer dataframe
    df = spark.read.option("header",payload["file_header"]).format(payload["file_format"]).load(payload["file_input_path"]+"/"+payload["file_name"]+"."+payload["file_format"])
    return df

def dataframeToDeltaTable(payload, df, spark_session, writing_mode):
    # Guarda el DataFrame como tabla Delta en el path de salida
    try:
        df = df.write.format("delta").mode(writing_mode).save(payload["file_output_path"]+"/"+payload["file_output_name"])
        print("delta table correctamente creada")
    except Exception as e:
        print(e)

