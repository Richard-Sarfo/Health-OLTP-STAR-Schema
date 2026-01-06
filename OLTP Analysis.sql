-- ============================================================
-- Monthly Encounters by Specialty
-- For each month and specialty, show total encounters 
-- and unique patients by encounter type
-- ============================================================

SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS encounter_month,
    s.specialty_name,
    e.encounter_type,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
INNER JOIN providers p ON e.provider_id = p.provider_id
INNER JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 
    DATE_FORMAT(e.encounter_date, '%Y-%m'),
    s.specialty_name,
    e.encounter_type
ORDER BY encounter_month, specialty_name, encounter_type;

-- ================================================================
-- Top Diagnosis-Procedure Pairs
-- Most common diagnosis-procedure combinations
-- Show ICD code, procedure code, and encounter count
-- ================================================================

SELECT 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description,
    COUNT(DISTINCT ed.encounter_id) AS encounter_count
FROM encounter_diagnoses ed
INNER JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
INNER JOIN encounter_procedures ep ON ed.encounter_id = ep.encounter_id
INNER JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description
HAVING COUNT(DISTINCT ed.encounter_id) >= 1
ORDER BY encounter_count DESC
LIMIT 20;

-- ==================================================================
-- 30-Day Readmission Rate
-- Which specialty has highest readmission rate?
-- Definition: inpatient discharge, then return within 30 days
-- ==================================================================

WITH inpatient_discharges AS (
    SELECT 
        e.encounter_id,
        e.patient_id,
        e.provider_id,
        e.discharge_date
    FROM encounters e
    WHERE e.encounter_type = 'Inpatient'
        AND e.discharge_date IS NOT NULL
),
readmissions AS (
    SELECT 
        id.encounter_id AS initial_encounter_id,
        id.patient_id,
        id.provider_id,
        e2.encounter_id AS readmit_encounter_id,
        DATEDIFF(e2.encounter_date, id.discharge_date) AS days_to_readmit
    FROM inpatient_discharges id
    INNER JOIN encounters e2 
        ON id.patient_id = e2.patient_id
        AND e2.encounter_type = 'Inpatient'
        AND e2.encounter_date > id.discharge_date
        AND DATEDIFF(e2.encounter_date, id.discharge_date) <= 30
)
SELECT 
    s.specialty_name,
    COUNT(DISTINCT id.encounter_id) AS total_discharges,
    COUNT(DISTINCT r.initial_encounter_id) AS readmissions,
    ROUND(100.0 * COUNT(DISTINCT r.initial_encounter_id) / 
          COUNT(DISTINCT id.encounter_id), 2) AS readmission_rate_pct
FROM inpatient_discharges id
LEFT JOIN readmissions r ON id.encounter_id = r.initial_encounter_id
INNER JOIN providers p ON id.provider_id = p.provider_id
INNER JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY s.specialty_name
ORDER BY readmission_rate_pct DESC;


-- ===========================================================
-- Revenue by Specialty & Month
-- Total allowed amounts by specialty and month
-- ===========================================================

SELECT 
    DATE_FORMAT(b.claim_date, '%Y-%m') AS billing_month,
    s.specialty_name,
    COUNT(DISTINCT b.billing_id) AS total_claims,
    SUM(b.claim_amount) AS total_claimed,
    SUM(b.allowed_amount) AS total_allowed,
    ROUND(AVG(b.allowed_amount), 2) AS avg_allowed
FROM billing b
INNER JOIN encounters e ON b.encounter_id = e.encounter_id
INNER JOIN providers p ON e.provider_id = p.provider_id
INNER JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 
    DATE_FORMAT(b.claim_date, '%Y-%m'),
    s.specialty_name
ORDER BY billing_month, total_allowed DESC;

