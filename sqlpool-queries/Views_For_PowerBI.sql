
-- 	                                =============
--		                               # KPI's
--	                                =============

-- 1. Bed's occupied total

CREATE VIEW dbo.vw_bed_occupancy AS
SELECT
    p.gender,
    CAST(
    COUNT(CASE
            WHEN f.is_currently_admitted = 1
            THEN f.bed_id
        END) * 1.0
    / COUNT(f.bed_id)
        AS DECIMAL(10,4)
    ) AS bed_occupancy_percent
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;

DROP VIEW vw_bed_occupancy;

SELECT * FROM vw_bed_occupancy;

--  2. Total bed turnover

CREATE VIEW vw_bed_turnover_rate AS
		SELECT 
				p.gender,
                CAST(
                    ROUND(
					    COUNT( DISTINCT f.fact_id ) * 1.0 / COUNT( DISTINCT f.bed_id), 2 ) AS DECIMAL(10,2)
                    )  AS bed_turnover_rate
		FROM dbo.fact_patient_flow f 
		JOIN dbo.dim_patient p 
					ON f.patient_sk = p.surrogate_key
		GROUP BY p.gender;

SELECT * FROM dbo.vw_bed_turnover_rate;

DROP VIEW dbo.vw_bed_turnover_rate;
					
-- 3. Total patients

CREATE VIEW vw_patient_demographics AS 
  SELECT
				p.gender,
				COUNT( CASE WHEN f.is_currently_admitted = 1 THEN
											f.fact_id
							    END ) AS total_patients
  FROM dbo.fact_patient_flow f 
  JOIN dbo.dim_patient p 
			ON f.patient_sk = p.surrogate_key
  GROUP BY p.gender;

SELECT * FROM vw_patient_demographics;

-- 4. Avg treatment duration

CREATE VIEW dbo.vw_avg_length_of_stay
AS
SELECT
    d.department,
    p.gender,
    ROUND(AVG(f.length_of_stay_hours), 2) AS avg_treatment_duration
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY
    d.department,
    p.gender;
			
SELECT * FROM dbo.vw_avg_length_of_stay;

DROP VIEW vw_avg_length_of_stay;

-- ==========================================================================================================
	
--	                                ==================
--		                               # For Chart's
--	                                ==================
		
--	1. Total patients count over time	

CREATE VIEW vw_patient_volume_trend AS 
	SELECT 
					f.admission_time,
					p.gender,
					COUNT( DISTINCT f.fact_id ) AS patient_count
	FROM dbo.fact_patient_flow f 
	JOIN dbo.dim_patient p 
			ON f.patient_sk = p.surrogate_key
	GROUP BY f.admission_time, p.gender;

SELECT * FROM dbo.vw_patient_volume_trend;

-- 	2. Total patients over department

 CREATE VIEW vw_department_inflow AS
  SELECT
				d.department,
				p.gender,
				COUNT( DISTINCT f.fact_id ) AS patient_count
	FROM dbo.fact_patient_flow f 
		JOIN dbo.dim_patient p 
				ON f.patient_sk = p.surrogate_key
		JOIN dbo.dim_department d 
				ON  f.department_sk = d.surrogate_key
	GROUP BY p.gender, d.department;

SELECT * FROM dbo.vw_department_inflow;

-- 	3. Total overstay patients count

	CREATE VIEW vw_overstay_patients AS
	SELECT 
			d.department,
			p.gender,
			COUNT( f.fact_id ) AS overstay_count
	FROM dbo.fact_patient_flow f 
	JOIN dbo.dim_patient p 
			ON f.patient_sk = p.surrogate_key
	JOIN dbo.dim_department d
			ON f.department_sk = d.surrogate_key
	WHERE f.length_of_stay_hours >50
	GROUP BY p.gender, d.department;

SELECT * FROM vw_overstay_patients;

-- 4. Avg treatment duration

CREATE VIEW dbo.vw_avg_treatment_duration
AS
SELECT
    d.department,
    p.gender,
    ROUND(AVG(f.length_of_stay_hours), 2) AS avg_treatment_duration
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY
    d.department,
    p.gender;

SELECT *
FROM dbo.vw_avg_treatment_duration;

DROP VIEW dbo.vw_avg_treatment_duration;




