
--  ===============================================
--      #   Create a Master-Key with the password
--  ===============================================

CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''<<Password>>'

--  ===========================
--      #   Creating a Scope 
--  ============================

CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';

--  =========================================================================
--      #   Define the datasource : Gold Container in our storage account
--  =========================================================================

CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    TYPE = HADOOP, 
    LOCATION = 'abfss://gold@<<Storageaccount_name>>.dfs.core.windows.net/',
    CREDENTIAL = storage_credential
)

--  ==========================
--      #   Define Format
--  ==========================
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH(
    FORMAT_TYPE = PARQUET
);

--  ===============================
--      # Create External Tables
--  ===============================

--  =================================
--      1. Patient Dimention Table
--  =================================

SELECT *
FROM sys.external_tables
WHERE name = 'dim_patient';

SELECT *
FROM sys.external_data_sources
WHERE name = 'gold_data_source';


CREATE EXTERNAL TABLE dbo.dim_patient
(
    patient_id VARCHAR(100),
    gender VARCHAR(20),
    age INT,
    effective_from DATETIME2,
    surrogate_key BIGINT,
    effective_to DATETIME2,
    is_current BIT
)
WITH
(
    LOCATION = 'dim_patient_parquet/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

SELECT TOP 10 *
FROM dbo.dim_patient;

--  ===================================
--      2. Department Dimention Table
--  ===================================

CREATE EXTERNAL TABLE dbo.dim_department
(
    surrogate_key BIGINT,
    department VARCHAR(200),
    hospital_id INT
)
WITH
(
    LOCATION = 'dim_department_parquet/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

SELECT TOP 10 *
FROM dbo.dim_department;

--  ======================
--      3. Fact Table
--  ======================
CREATE EXTERNAL TABLE dbo.fact_patient_flow
(
    fact_id BIGINT,
    patient_sk BIGINT,
    department_sk BIGINT,
    admission_time DATETIME2,
    discharge_time DATETIME2,
    admission_date DATE,
    length_of_stay_hours FLOAT,
    is_currently_admitted INT,
    bed_id INT,
    event_ingestion_time DATETIME2
)
WITH
(
    LOCATION = 'fact_patient_flow_parquet/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

SELECT TOP 10 *
FROM dbo.fact_patient_flow;


-- DROP EXTERNAL TABLE dbo.dim_patient;
-- DROP EXTERNAL TABLE dbo.dim_patient_test;


-- Now our Synapse Warehouse is successfully connected to the Storage Account and ready to retrive the data.
-- Now Our SQL Pool is ready and we can query form gold storage which act as a Data Warehouse.
-- Now we can connect to Power BI to make some insights reports.

-- ====================================================================  || ======================================================================