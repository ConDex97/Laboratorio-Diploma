# Databricks notebook source
#Importación
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,  StructField,  StringType, IntegerType, DoubleType
from pyspark.sql.functions import date_format, when, col, to_date, year, month, dayofmonth

# COMMAND ----------

#Variables
spark = SparkSession.builder.getOrCreate()
#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_jacondex"
path_lakehouse = f"gs://{name_bucket}/produccion/dmc"
path_bronze = f"{path_lakehouse}/bronze/transacciones/"
path_silver = f"{path_lakehouse}/silver/transacciones/"



# COMMAND ----------



# COMMAND ----------

#Leer el archivo de origen
df = spark.read.format("delta").load(path_bronze)
display(df)

# COMMAND ----------

#La Transformación de datos

#Casteo de datos
df_c = df.withColumn("ID_PERSONA",col("ID_PERSONA").cast(IntegerType())) \
  .withColumn("ID_EMPRESA",col("ID_EMPRESA").cast(IntegerType())) \
  .withColumn("MONTO",col("MONTO").cast(DoubleType())) \
  .withColumn("FECHA",to_date(col("FECHA"),"yyyy-MM-dd")) \
  .withColumn("ANIO",year(col("FECHA"))) \
  .withColumn("MES",month(col("FECHA"))) \
  .withColumn("DIA",dayofmonth(col("FECHA"))) 
  
     

# COMMAND ----------

display(df_c)

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("ANIO","MES","DIA").format("delta").save(path_silver)