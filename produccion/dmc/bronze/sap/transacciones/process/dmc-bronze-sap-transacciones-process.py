# Databricks notebook source
#Importación
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,  StructField,  StringType

# COMMAND ----------

#Variables
spark = SparkSession.builder.getOrCreate()
#Archivo en Cloud Storage - Google Cloud Platform
nombre_bucket = "dmc_datalake_dde_11_jacondex"
path_lakehouse = f"gs://{nombre_bucket}/produccion/dmc"
path_landing = f"{path_lakehouse}/landing/transacciones/transacciones.data"
path_bronze = f"{path_lakehouse}/bronze/transacciones/"






# COMMAND ----------

print(ruta_persona_bronze)

# COMMAND ----------

# Definicion de la columna
# Todo debe estar en String
# 6.1 Estructura del dataframe.
schema = StructType([
StructField("ID_PERSONA", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
StructField("MONTO", StringType(),True),
StructField("FECHA", StringType(),True)
])

# COMMAND ----------

#Leer el archivo de origen
df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(schema).load(path_landing)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").save(path_bronze)