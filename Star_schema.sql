-- DIMENSION TABLES

-- Date Dimension
-- Handles all temporal rollups (Year, Month, Day)
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,              -- Format: YYYYMMDD (e.g., 20240510)
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week_of_year INT,
    day_of_month INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- Patient Dimension
-- Slowly Changing Dimension (SCD) Type 1 or 2 potential
CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate Key
    patient_id INT,                             -- Natural Key (from OLTP)
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    current_age INT,
    age_group VARCHAR(20),                      -- Pre-calculated: '0-18', '19-65', '65+'
    mrn VARCHAR(20)
);

-- Provider Dimension
CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,-- Surrogate Key
    provider_id INT,                            -- Natural Key
    full_name VARCHAR(200),
    credential VARCHAR(20),
    npi VARCHAR(20)                             -- National Provider Identifier placeholder
);

-- Specialty Dimension
CREATE TABLE dim_specialty (
    specialty_key INT AUTO_INCREMENT PRIMARY KEY,
    specialty_id INT,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10)
);

-- Department Dimension
CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT,
    department_name VARCHAR(100),
    floor INT,
    capacity INT
);

-- Encounter Type Dimension
-- Small lookup table for filtering (Inpatient vs Outpatient)
CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type_name VARCHAR(50),
    encounter_type_description VARCHAR(200)
);

-- Diagnosis Dimension (for Bridge Table)
CREATE TABLE dim_diagnoses (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT,
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200)
);

-- Procedure Dimension (for Bridge Table)
CREATE TABLE dim_procedures (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT,
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200)
);

-- FACT TABLE

-- Fact Encounters
-- Grain: One row per encounter
CREATE TABLE fact_encounters (
    encounter_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_id INT,                  -- Degenerate Dimension (link back to source)
    
    -- Foreign Keys to Dimensions
    date_key INT,
    patient_key INT,
    provider_key INT,
    specialty_key INT,
    department_key INT,
    encounter_type_key INT,
    
    -- Pre-Aggregated Metrics (The Performance Boosters)
    is_readmission BOOLEAN DEFAULT 0,  -- 1 if readmission within 30 days
    total_claim_amount DECIMAL(12,2),  -- Sum from billing
    total_allowed_amount DECIMAL(12,2),-- Sum from billing
    length_of_stay_days INT,           -- Calculated duration
    diagnosis_count INT,               -- Count of associated diagnoses
    procedure_count INT,               -- Count of associated procedures
    
    -- Foreign Key Constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key)
);

-- Indexes for Fact Table Performance
CREATE INDEX idx_fact_date ON fact_encounters(date_key);
CREATE INDEX idx_fact_provider ON fact_encounters(provider_key);
CREATE INDEX idx_fact_specialty ON fact_encounters(specialty_key);

-- 4. BRIDGE TABLES

-- Bridge for Encounter <-> Diagnoses (Many-to-Many)
CREATE TABLE bridge_encounter_diagnoses (
    bridge_diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    diagnosis_key INT,
    diagnosis_sequence INT,  -- 1 = Primary, 2 = Secondary, etc.
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnoses(diagnosis_key)
);

-- Bridge for Encounter <-> Procedures (Many-to-Many)
CREATE TABLE bridge_encounter_procedures (
    bridge_procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_key INT,
    procedure_key INT,
    procedure_date DATE,
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedures(procedure_key)
);
