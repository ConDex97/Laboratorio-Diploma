-- Databricks notebook source
show databases;

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/bronze';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/silver';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/gold';