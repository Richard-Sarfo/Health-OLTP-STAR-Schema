import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration
NUM_RECORDS = 10000
BATCH_SIZE = 1000

# Starting IDs
ID_OFFSETS = {
    'specialties': 1,
    'departments': 1,
    'providers': 101,
    'patients': 1001,
    'diagnoses': 3001,
    'procedures': 4001,
    'encounters': 7001,
    'encounter_diagnoses': 8001,
    'encounter_procedures': 9001,
    'billing': 14001
}

# --- REALISTIC DATA LISTS ---

DEPARTMENTS_LIST = [
    'Cardiology Unit', 'Intensive Care Unit (ICU)', 'Emergency Room (ER)', 
    'Pediatrics Wing', 'Neurology Center', 'Oncology Ward', 'Orthopedics',
    'Maternity Ward', 'Psychiatry', 'Radiology', 'General Surgery',
    'Gastroenterology', 'Urology', 'Dermatology Clinic', 'Ophthalmology'
]

DIAGNOSES_LIST = [
    ('I10', 'Essential (primary) hypertension'),
    ('E11.9', 'Type 2 diabetes mellitus without complications'),
    ('J45.909', 'Unspecified asthma, uncomplicated'),
    ('I50.9', 'Heart failure, unspecified'),
    ('M54.5', 'Low back pain'),
    ('J06.9', 'Acute upper respiratory infection, unspecified'),
    ('E78.5', 'Hyperlipidemia, unspecified'),
    ('K21.9', 'Gastro-esophageal reflux disease without esophagitis'),
    ('N39.0', 'Urinary tract infection, site not specified'),
    ('F41.1', 'Generalized anxiety disorder'),
    ('F32.9', 'Major depressive disorder, single episode, unspecified'),
    ('R51', 'Headache'),
    ('R05', 'Cough'),
    ('M17.9', 'Osteoarthritis of knee, unspecified'),
    ('Z00.00', 'Encounter for general adult medical exam')
]

PROCEDURES_LIST = [
    ('99213', 'Office or other outpatient visit for established patient'),
    ('99214', 'Office visit, est patient, moderate complexity'),
    ('93000', 'Electrocardiogram, routine ECG with at least 12 leads'),
    ('71020', 'Radiologic examination, chest, 2 views, frontal and lateral'),
    ('85025', 'Blood count; complete (CBC), automated'),
    ('80053', 'Comprehensive metabolic panel'),
    ('36415', 'Collection of venous blood by venipuncture'),
    ('99283', 'Emergency department visit, moderate severity'),
    ('99285', 'Emergency department visit, high severity'),
    ('70450', 'CT head or brain; without contrast material'),
    ('72148', 'MRI lumbar spine; without contrast material'),
    ('30000', 'Drainage of nasal abscess'),
    ('10060', 'Incision and drainage of abscess')
]

SPECIALTIES_LIST = [
    ('CARD', 'Cardiology'), ('IM', 'Internal Medicine'), ('EM', 'Emergency Medicine'),
    ('PED', 'Pediatrics'), ('SURG', 'General Surgery'), ('NEURO', 'Neurology'),
    ('ORTHO', 'Orthopedics'), ('PSYCH', 'Psychiatry'), ('DERM', 'Dermatology'),
    ('RAD', 'Radiology'), ('ONC', 'Oncology'), ('OBGYN', 'Obstetrics and Gynecology')
]

# --- HELPER FUNCTIONS ---

def escape_sql(text):
    return str(text).replace("'", "''")

def batch_write(filename, table_name, data_generator_func):
    print(f"Generating {filename}...")
    with open(filename, 'w') as f:
        buffer = []
        for i in range(NUM_RECORDS):
            row_data = data_generator_func(i)
            formatted_values = []
            for val in row_data:
                if val is None:
                    formatted_values.append("NULL")
                elif isinstance(val, str):
                    formatted_values.append(f"'{escape_sql(val)}'")
                elif isinstance(val, (datetime,)):
                    formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                else:
                    formatted_values.append(str(val))
            
            buffer.append(f"({', '.join(formatted_values)})")
            
            if len(buffer) >= BATCH_SIZE or i == NUM_RECORDS - 1:
                statement = f"INSERT INTO {table_name} VALUES\n" + ",\n".join(buffer) + ";\n\n"
                f.write(statement)
                buffer = []

# --- GENERATORS ---

def gen_specialty(i):
    # IDs: 1 to 10000
    pk = ID_OFFSETS['specialties'] + i
    # Randomly pick a realistic specialty
    code, name = random.choice(SPECIALTIES_LIST)
    return (pk, name, code)

def gen_department(i):
    # IDs: 1 to 10000
    pk = ID_OFFSETS['departments'] + i
    # Randomly pick a realistic department name
    dept_name = random.choice(DEPARTMENTS_LIST)
    floor = random.randint(1, 10)
    capacity = random.randint(10, 100)
    return (pk, dept_name, floor, capacity)

def gen_diagnosis(i):
    # IDs: 3001 to 13000
    pk = ID_OFFSETS['diagnoses'] + i
    # Randomly pick a realistic ICD code/desc
    code, name = random.choice(DIAGNOSES_LIST)
    return (pk, code, name)

def gen_procedure(i):
    # IDs: 4001 to 14000
    pk = ID_OFFSETS['procedures'] + i
    # Randomly pick a realistic CPT code/desc
    code, name = random.choice(PROCEDURES_LIST)
    return (pk, code, name)

# --- STANDARD GENERATORS (Remaining same logic) ---

def gen_provider(i):
    pk = ID_OFFSETS['providers'] + i
    first = fake.first_name()
    last = fake.last_name()
    title = random.choice(['MD', 'DO', 'NP', 'PA'])
    dept_id = random.randint(ID_OFFSETS['departments'], ID_OFFSETS['departments'] + NUM_RECORDS - 1)
    spec_id = random.randint(ID_OFFSETS['specialties'], ID_OFFSETS['specialties'] + NUM_RECORDS - 1)
    return (pk, first, last, title, dept_id, spec_id)

def gen_patient(i):
    pk = ID_OFFSETS['patients'] + i
    first = fake.first_name()
    last = fake.last_name()
    dob = fake.date_of_birth(minimum_age=1, maximum_age=90).strftime('%Y-%m-%d')
    gender = random.choice(['M', 'F'])
    mrn = f"MRN{pk}"
    return (pk, first, last, dob, gender, mrn)

def gen_encounter(i):
    pk = ID_OFFSETS['encounters'] + i
    pat_id = random.randint(ID_OFFSETS['patients'], ID_OFFSETS['patients'] + NUM_RECORDS - 1)
    prov_id = random.randint(ID_OFFSETS['providers'], ID_OFFSETS['providers'] + NUM_RECORDS - 1)
    dept_id = random.randint(ID_OFFSETS['departments'], ID_OFFSETS['departments'] + NUM_RECORDS - 1)
    enc_type = random.choice(['Outpatient', 'Inpatient', 'ER'])
    start_date = fake.date_time_between(start_date='-1y', end_date='now')
    end_date = start_date + timedelta(minutes=random.randint(15, 300))
    return (pk, pat_id, prov_id, enc_type, start_date, end_date, dept_id)

def gen_billing(i):
    pk = ID_OFFSETS['billing'] + i
    enc_id = ID_OFFSETS['encounters'] + i
    total = random.randint(100, 50000)
    covered = int(total * random.uniform(0.5, 0.9))
    bill_date = fake.date_between(start_date='-6m', end_date='today').strftime('%Y-%m-%d')
    status = random.choice(['Paid', 'Pending', 'Denied'])
    return (pk, enc_id, total, covered, bill_date, status)

def gen_enc_diagnosis(i):
    pk = ID_OFFSETS['encounter_diagnoses'] + i
    enc_id = random.randint(ID_OFFSETS['encounters'], ID_OFFSETS['encounters'] + NUM_RECORDS - 1)
    diag_id = random.randint(ID_OFFSETS['diagnoses'], ID_OFFSETS['diagnoses'] + NUM_RECORDS - 1)
    rank = random.randint(1, 3)
    return (pk, enc_id, diag_id, rank)

def gen_enc_procedure(i):
    pk = ID_OFFSETS['encounter_procedures'] + i
    enc_id = random.randint(ID_OFFSETS['encounters'], ID_OFFSETS['encounters'] + NUM_RECORDS - 1)
    proc_id = random.randint(ID_OFFSETS['procedures'], ID_OFFSETS['procedures'] + NUM_RECORDS - 1)
    proc_date = fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
    return (pk, enc_id, proc_id, proc_date)

if __name__ == "__main__":
    batch_write("1_specialties.sql", "specialties", gen_specialty)
    batch_write("2_departments.sql", "departments", gen_department)
    batch_write("3_providers.sql", "providers", gen_provider)
    batch_write("4_patients.sql", "patients", gen_patient)
    batch_write("5_diagnoses.sql", "diagnoses", gen_diagnosis)
    batch_write("6_procedures.sql", "procedures", gen_procedure)
    batch_write("7_encounters.sql", "encounters", gen_encounter)
    batch_write("8_billing.sql", "billing", gen_billing)
    batch_write("9_encounter_diagnoses.sql", "encounter_diagnoses", gen_enc_diagnosis)
    batch_write("10_encounter_procedures.sql", "encounter_procedures", gen_enc_procedure)

    print("\nDone! 10 separate text files generated with realistic data.")