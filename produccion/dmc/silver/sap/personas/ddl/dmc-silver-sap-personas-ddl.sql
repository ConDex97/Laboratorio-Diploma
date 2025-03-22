-- Databricks notebook source
CREATE TABLE IF NOT EXISTS silver.personas (
    ID INT,
    NOMBRE STRING,
    EDAD INT,
    TELEFONO STRING,
    CORREO STRING,
    SALARIO DOUBLE,
    ID_EMPRESA INT,
    PERIODO STRING,
    SEGMENTO STRING,
    FECHA_INGRESO DATE,
    ANIO INT,
    MES INT,
    DIA INT
)

USING DELTA

PARTITIONED BY (periodo)

LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/silver/personas/'


-- COMMAND ----------

DESCRIBE DETAIL silver.personas;