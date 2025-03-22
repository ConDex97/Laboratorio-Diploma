-- Databricks notebook source
CREATE EXTERNAL TABLE silver.transacciones (

    ID_PERSONA INT COMMENT 'Identificador único de la persona',

    ID_EMPRESA INT COMMENT 'Identificador único de la empresa',

    MONTO DECIMAL(18,2) COMMENT 'Monto de la transacción',

    FECHA DATE COMMENT 'Fecha de la transacción',

    ANIO INT COMMENT 'Año de la transacción',

    MES INT COMMENT 'Mes de la transacción',

    DIA INT COMMENT 'Día de la transacción'

)

USING DELTA

PARTITIONED BY (ANIO, MES, DIA)

LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/silver/transacciones/';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.transacciones
USING DELTA
LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/silver/transacciones/';


-- COMMAND ----------

select * from silver.transacciones