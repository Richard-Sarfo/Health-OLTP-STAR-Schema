-- =====================================================================
-- MODULAR ETL PIPELINE: Healthcare OLTP -> OLAP
-- Design Pattern: Procedural with Error Handling & Logging
-- Source: healthtech_oltp | Target: healthtech_olap
-- =====================================================================

-- =====================================================================
-- SECTION 0: DATABASE & SCHEMA CREATION
-- =====================================================================

-- Create target database if it doesn't exist
CREATE DATABASE IF NOT EXISTS healthtech_olap;

USE healthtech_olap;

-- =====================================================================
-- SECTION 1: ETL CONTROL & LOGGING SETUP
-- =====================================================================

-- Create ETL control table if not exists
CREATE TABLE IF NOT EXISTS healthtech_olap.etl_control (
    etl_run_id INT AUTO_INCREMENT PRIMARY KEY,
    run_start_datetime DATETIME,
    run_end_datetime DATETIME,
    status VARCHAR(20),
    records_processed INT,
    error_message TEXT,
    etl_phase VARCHAR(50)
);

-- Start new ETL run
INSERT INTO healthtech_olap.etl_control (run_start_datetime, status, etl_phase)
VALUES (NOW(), 'RUNNING', 'INITIALIZATION');

SET @etl_run_id = LAST_INSERT_ID();
SET @batch_size = 10000;

-- =====================================================================
-- SECTION 2: PRE-ETL VALIDATION & CLEANUP
-- =====================================================================

-- Truncate target tables (full refresh pattern)
TRUNCATE TABLE healthtech_olap.bridge_encounter_procedures;
TRUNCATE TABLE healthtech_olap.bridge_encounter_diagnoses;
TRUNCATE TABLE healthtech_olap.fact_encounters;
TRUNCATE TABLE healthtech_olap.dim_date;
TRUNCATE TABLE healthtech_olap.dim_encounter_type;
TRUNCATE TABLE healthtech_olap.dim_procedures;
TRUNCATE TABLE healthtech_olap.dim_diagnoses;
TRUNCATE TABLE healthtech_olap.dim_patient;
TRUNCATE TABLE healthtech_olap.dim_provider;
TRUNCATE TABLE healthtech_olap.dim_department;
TRUNCATE TABLE healthtech_olap.dim_specialty;

-- Log phase completion
UPDATE healthtech_olap.etl_control 
SET etl_phase = 'CLEANUP_COMPLETE' 
WHERE etl_run_id = @etl_run_id;

-- =====================================================================
-- SECTION 3: DIMENSION LOADING WITH TRANSFORMATIONS
-- =====================================================================

USE healthtech_olap;

-- 3.1 Load Specialty Dimension
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_specialty (specialty_id, specialty_name, specialty_code)
    SELECT specialty_id, specialty_name, specialty_code 
    FROM healthtech_oltp.specialties;
    
    SET @rows_affected = ROW_COUNT();
COMMIT;

UPDATE healthtech_olap.etl_control 
SET etl_phase = 'DIM_SPECIALTY_LOADED', 
    records_processed = @rows_affected 
WHERE etl_run_id = @etl_run_id;

-- 3.2 Load Department Dimension
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_department (department_id, department_name, floor, capacity)
    SELECT department_id, department_name, floor, capacity 
    FROM healthtech_oltp.departments;
COMMIT;

-- 3.3 Load Provider Dimension (Transformed)
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_provider (provider_id, full_name, credential)
    SELECT 
        provider_id,
        TRIM(CONCAT_WS(' ', first_name, last_name)) AS full_name,
        UPPER(COALESCE(credential, 'UNKNOWN')) AS credential
    FROM healthtech_oltp.providers;
COMMIT;

-- 3.4 Load Patient Dimension (Enriched)
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_patient (patient_id, first_name, last_name, gender, date_of_birth, mrn, current_age, age_group)
    SELECT 
        patient_id,
        first_name,
        last_name,
        UPPER(gender) AS gender,
        date_of_birth,
        mrn,
        TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) AS current_age,
        CASE 
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-18'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) <= 65 THEN '19-65'
            ELSE '65+'
        END AS age_group
    FROM healthtech_oltp.patients
    WHERE date_of_birth IS NOT NULL;
COMMIT;

-- 3.5 Load Diagnosis Dimension
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_diagnoses (diagnosis_id, icd10_code, icd10_description)
    SELECT diagnosis_id, icd10_code, icd10_description 
    FROM healthtech_oltp.diagnoses;
COMMIT;

-- 3.6 Load Procedure Dimension
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_procedures (procedure_id, cpt_code, cpt_description)
    SELECT procedure_id, cpt_code, cpt_description 
    FROM healthtech_oltp.procedures;
COMMIT;

-- 3.7 Load Encounter Type Dimension
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_encounter_type (encounter_type_name)
    SELECT DISTINCT UPPER(TRIM(encounter_type))
    FROM healthtech_oltp.encounters
    WHERE encounter_type IS NOT NULL;
COMMIT;

-- 3.8 Load Date Dimension (Complete Date Range)
START TRANSACTION;
    INSERT INTO healthtech_olap.dim_date 
    SELECT DISTINCT 
        CAST(DATE_FORMAT(encounter_date, '%Y%m%d') AS UNSIGNED) AS date_key,
        DATE(encounter_date) AS full_date,
        YEAR(encounter_date) AS year,
        QUARTER(encounter_date) AS quarter,
        MONTH(encounter_date) AS month,
        MONTHNAME(encounter_date) AS month_name,
        WEEK(encounter_date, 3) AS week_of_year,
        DAY(encounter_date) AS day_of_month,
        DAYNAME(encounter_date) AS day_name,
        IF(DAYOFWEEK(encounter_date) IN (1, 7), 1, 0) AS is_weekend
    FROM healthtech_oltp.encounters
    WHERE encounter_date IS NOT NULL;
COMMIT;

UPDATE healthtech_olap.etl_control 
SET etl_phase = 'ALL_DIMENSIONS_LOADED' 
WHERE etl_run_id = @etl_run_id;

-- =====================================================================
-- SECTION 4: FACT TABLE LOADING (WITH AGGREGATIONS)
-- =====================================================================

USE healthtech_olap;

-- 4.1 Create staging table for aggregated metrics
DROP TEMPORARY TABLE IF EXISTS stg_encounter_metrics;

CREATE TEMPORARY TABLE stg_encounter_metrics AS
SELECT 
    e.encounter_id,
    e.patient_id,
    e.provider_id,
    e.department_id,
    e.encounter_type,
    e.encounter_date,
    e.discharge_date,
    p.specialty_id,
    COUNT(DISTINCT ed.diagnosis_id) AS diagnosis_count,
    COUNT(DISTINCT ep.procedure_id) AS procedure_count,
    SUM(b.claim_amount) AS total_claim_amount,
    SUM(b.allowed_amount) AS total_allowed_amount
FROM healthtech_oltp.encounters e
INNER JOIN healthtech_oltp.providers p ON e.provider_id = p.provider_id
LEFT JOIN healthtech_oltp.encounter_diagnoses ed ON e.encounter_id = ed.encounter_id
LEFT JOIN healthtech_oltp.encounter_procedures ep ON e.encounter_id = ep.encounter_id
LEFT JOIN healthtech_oltp.billing b ON e.encounter_id = b.encounter_id
WHERE e.encounter_date IS NOT NULL
GROUP BY 
    e.encounter_id, e.patient_id, e.provider_id, e.department_id,
    e.encounter_type, e.encounter_date, e.discharge_date, p.specialty_id;

CREATE INDEX idx_stg_patient ON stg_encounter_metrics(patient_id);
CREATE INDEX idx_stg_encounter ON stg_encounter_metrics(encounter_id);

UPDATE healthtech_olap.etl_control 
SET etl_phase = 'STAGING_TABLE_CREATED' 
WHERE etl_run_id = @etl_run_id;

-- 4.2 Load Fact Table with Surrogate Key Lookups
START TRANSACTION;
    INSERT INTO healthtech_olap.fact_encounters (
        encounter_id, date_key, patient_key, provider_key, specialty_key,
        department_key, encounter_type_key, is_readmission,
        total_claim_amount, total_allowed_amount, length_of_stay_days,
        diagnosis_count, procedure_count
    )
    SELECT 
        stg.encounter_id,
        CAST(DATE_FORMAT(stg.encounter_date, '%Y%m%d') AS UNSIGNED) AS date_key,
        dp.patient_key,
        dpr.provider_key,
        ds.specialty_key,
        dd.department_key,
        det.encounter_type_key,
        0 AS is_readmission,
        COALESCE(stg.total_claim_amount, 0) AS total_claim_amount,
        COALESCE(stg.total_allowed_amount, 0) AS total_allowed_amount,
        DATEDIFF(COALESCE(stg.discharge_date, stg.encounter_date), stg.encounter_date) AS length_of_stay_days,
        COALESCE(stg.diagnosis_count, 0) AS diagnosis_count,
        COALESCE(stg.procedure_count, 0) AS procedure_count
    FROM stg_encounter_metrics stg
    INNER JOIN healthtech_olap.dim_patient dp ON stg.patient_id = dp.patient_id
    INNER JOIN healthtech_olap.dim_provider dpr ON stg.provider_id = dpr.provider_id
    INNER JOIN healthtech_olap.dim_specialty ds ON stg.specialty_id = ds.specialty_id
    INNER JOIN healthtech_olap.dim_department dd ON stg.department_id = dd.department_id
    INNER JOIN healthtech_olap.dim_encounter_type det ON UPPER(TRIM(stg.encounter_type)) = det.encounter_type_name
    LIMIT 1000;
    
    SET @fact_rows = ROW_COUNT();
COMMIT;

UPDATE healthtech_olap.etl_control 
SET etl_phase = 'FACT_TABLE_LOADED', 
    records_processed = @fact_rows 
WHERE etl_run_id = @etl_run_id;

-- 4.3 Calculate Readmissions Using Window Function Logic
SET SQL_SAFE_UPDATES = 0;

UPDATE healthtech_olap.fact_encounters f
INNER JOIN (
    SELECT 
        e1.encounter_id,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM healthtech_oltp.encounters e2
                WHERE e2.patient_id = e1.patient_id
                AND e2.encounter_type = 'Inpatient'
                AND e2.encounter_date < e1.encounter_date
                AND e2.encounter_date >= DATE_SUB(e1.encounter_date, INTERVAL 30 DAY)
            ) THEN 1
            ELSE 0
        END AS readmit_flag
    FROM healthtech_oltp.encounters e1
) readmit_calc ON f.encounter_id = readmit_calc.encounter_id
SET f.is_readmission = readmit_calc.readmit_flag;

SET SQL_SAFE_UPDATES = 1;

-- =====================================================================
-- SECTION 5: BRIDGE TABLE LOADING (Many-to-Many)
-- =====================================================================

USE healthtech_olap;

-- 5.1 Load Diagnosis Bridge
START TRANSACTION;
    INSERT INTO healthtech_olap.bridge_encounter_diagnoses 
    SELECT 
        f.encounter_key,
        d.diagnosis_key,
        ed.diagnosis_sequence
    FROM healthtech_oltp.encounter_diagnoses ed
    INNER JOIN healthtech_olap.fact_encounters f ON ed.encounter_id = f.encounter_id
    INNER JOIN healthtech_olap.dim_diagnoses d ON ed.diagnosis_id = d.diagnosis_id;
COMMIT;

-- 5.2 Load Procedure Bridge
START TRANSACTION;
    INSERT INTO healthtech_olap.bridge_encounter_procedures 
    SELECT 
        f.encounter_key,
        p.procedure_key,
        ep.procedure_date
    FROM healthtech_oltp.encounter_procedures ep
    INNER JOIN healthtech_olap.fact_encounters f ON ep.encounter_id = f.encounter_id
    INNER JOIN healthtech_olap.dim_procedures p ON ep.procedure_id = p.procedure_id;
COMMIT;

-- =====================================================================
-- SECTION 6: ETL COMPLETION & CLEANUP
-- =====================================================================

USE healthtech_olap;

DROP TEMPORARY TABLE IF EXISTS stg_encounter_metrics;

UPDATE healthtech_olap.etl_control 
SET 
    run_end_datetime = NOW(),
    status = 'SUCCESS',
    etl_phase = 'COMPLETED'
WHERE etl_run_id = @etl_run_id;

-- Display ETL Summary
SELECT 
    etl_run_id,
    run_start_datetime,
    run_end_datetime,
    TIMESTAMPDIFF(SECOND, run_start_datetime, run_end_datetime) AS duration_seconds,
    status,
    records_processed,
    etl_phase
FROM healthtech_olap.etl_control
WHERE etl_run_id = @etl_run_id;