-- Databricks notebook source
create external table bronze.empresas(
  ID string,
  EMPRESA_NAME STRING
)
using delta
location "gs://dmc_datalake_dde_11_jacondex/produccion/dmc/bronze/empresas/"

-- COMMAND ----------

SELECT * FROM bronze.empresas