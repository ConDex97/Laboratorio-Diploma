-- Databricks notebook source
create external table bronze.transacciones(
  ID_PERSONA string,
  ID_EMPRESA STRING,
  MONTO STRING,
  FECHA STRING
)
using delta
location "gs://dmc_datalake_dde_11_jacondex/produccion/dmc/bronze/transacciones/"

-- COMMAND ----------

select * from bronze.transacciones