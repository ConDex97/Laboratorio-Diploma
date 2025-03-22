# Databricks notebook source
#Importación
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType 
from pyspark.sql.functions import upper, col, date_format, current_date, add_months


# COMMAND ----------

#Variables
spark = SparkSession.builder.getOrCreate()
#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_jacondex"
path_lakehouse = f"gs://{name_bucket}/produccion/dmc"
path_bronze = f"{path_lakehouse}/bronze/empresas/"
path_silver = f"{path_lakehouse}/silver/empresas/"


# COMMAND ----------



# COMMAND ----------

#Leer el archivo de origen
df = spark.read.format("delta").option("header","true").load(path_bronze)
display(df)

# COMMAND ----------

#La Transformación de datos
df_t = df.withColumn('EMPRESA_NAME',upper(col('EMPRESA_NAME'))) \
 .withColumn("PERIODO",date_format(add_months(current_date(),-1),"yyyyMM"))

#Casteo de datos
df_c = df_t.withColumn("ID",col("ID").cast(IntegerType())) 



display(df_t)
     

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("PERIODO").format("delta").save(path_silver)