
--  ======================
--    1. Schema Creation
--  ======================

CREATE SCHEMA GOLD;    

-- -----------------------------------------------------------------------------------------------------------------------

--  ======================
--      2. Create View 
--  ======================

-- 1. View Creation =>   1. dim_department

CREATE view GOLD.dim_patient
AS 
SELECT * 
FROM dbo.dim_patient;

SELECT COUNT(*) FROM dbo.dim_patient;
-- -----------------------------------------------------------------------------------------------------------------------

-- 2. View Creation  => 2. dim_department

CREATE view GOLD.dim_department
AS 
SELECT * 
FROM dbo.dim_department;

SELECT COUNT(*) FROM dbo.dim_department;
-- -----------------------------------------------------------------------------------------------------------------------

-- 3. View Creation =>  3. fact_patient_flow

CREATE view GOLD.fact_patient_flow
AS 
SELECT * 
FROM dbo.fact_patient_flow;

SELECT COUNT(*) FROM dbo.fact_patient_flow;
-- -----------------------------------------------------------------------------------------------------------------------

