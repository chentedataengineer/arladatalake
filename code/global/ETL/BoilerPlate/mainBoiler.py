# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

#TODO: Technology and file format to be created as input parameters
def fileReader(payload, spark):
    # Leer dataframe
    if (payload["file_format"] == "csv"):
        df = spark.read.option("header",payload["file_header"]).format(payload["file_format"]).load(payload["file_input_path"]+"/"+payload["file_name"]+"."+payload["file_format"])
        return df
    elif (payload["file_format"] == "json"):
        df = spark.read.format(payload["file_format"]).load(payload["file_input_path"]+"/"+payload["file_name"]+"."+payload["file_format"])
        return df

def dataframeToDeltaTable(payload, df, spark_session, writing_mode):
    # Guarda el DataFrame como tabla Delta en el path de salida
    try:
        df = df.write.format("delta").mode(writing_mode).save(payload["file_output_path"]+"/"+payload["file_output_name"])
        print("delta table correctamente creada")
    except Exception as e:
        print(e)


# COMMAND ----------

#TODO: deduplication function
def dataframeDeduplication(df, primary_key_fields):
    for field in primary_key_fields:
        if field not in df.columns:
            raise ValueError(f"El campo '{field}' no está presente en el DataFrame.")

    deduplicated_df = df.dropDuplicates(subset=primary_key_fields)

    return deduplicated_df
#TODO: schema parsing function
def dataframeParsing(df, type_dict):
    """
    Convierte las columnas del DataFrame a los tipos de datos especificados en el diccionario type_dict.

    :param df: DataFrame de PySpark.
    :param type_dict: Diccionario que mapea nombres de columnas a tipos de datos deseados.
    :return: DataFrame con las columnas convertidas a los tipos de datos especificados.
    """
    for col_name, col_type in type_dict.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df
