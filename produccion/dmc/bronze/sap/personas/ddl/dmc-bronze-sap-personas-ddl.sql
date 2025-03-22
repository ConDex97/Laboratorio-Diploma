-- Databricks notebook source
create external table bronze.personas(
  ID string,
  NOMBRE STRING,
  TELEFONO STRING,
  CORREO string,
  FECHA_INGRESO string,
  EDAD STRING,
  SALARIO STRING,
  ID_EMPRESA string
)
using delta
location "gs://dmc_datalake_dde_11_jacondex/produccion/dmc/bronze/personas/"

-- COMMAND ----------

select id_empresa, count(1) from bronze.personas group by ID_EMPRESA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.personas (
    ID INT NOT NULL,
    ID_EMPRESA INT NOT NULL,
    NOMBRE STRING NOT NULL,
    APELLIDO STRING NOT NULL,
    EDAD INT,
    TELEFONO STRING NOT NULL,
    EMAIL STRING NOT NULL,
    SALARIO DOUBLE,
    FECHA_INGRESO DATE NOT NULL,
    ANIO INT,
    MES INT,
    DIA INT,
    SEGMENTO STRING NOT NULL,
    PERIODO STRING NOT NULL
)
USING DELTA
PARTITIONED BY (PERIODO)
LOCATION 'gs://dmc_datalake_dde_11_jacondex/produccion/dmc/silver/personas'

-- COMMAND ----------

/*  Para transacciones
*/
CREATE EXTERNAL TABLE silver.txn (
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
LOCATION 'gs://datalake/silver/transacciones/';

-- COMMAND ----------

/* Para compañias
*/
CREATE EXTERNAL TABLE slv.companies (
    ID INT COMMENT 'Identificador único de la empresa',
    EMPRESA_NAME STRING COMMENT 'Nombre de la empresa en mayúsculas',
    PERIODO STRING COMMENT 'Periodo de actualización (YYYYMM)'
)
USING DELTA
PARTITIONED BY (PERIODO)
LOCATION '${bucket}slv/companies/';