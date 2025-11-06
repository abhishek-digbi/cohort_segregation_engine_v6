# Cohort Segregation Engine - Server Setup

## Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd cohort_segregation_v4
```

### 2. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Database
```bash
cp configs/db_connection.yaml.example configs/db_connection.yaml
```

Then edit `configs/db_connection.yaml` with your database credentials.

### 4. Test Connection
```bash
python test_postgres_connection.py
```

### 5. Run Cohorts

**All cohorts:**
```bash
python scripts/run_cohorts.py --output_dir outputs/my_analysis
```

**Specific cohorts:**
```bash
python scripts/run_cohorts.py --cohorts HTN_Conservative Diabetes_General --output_dir outputs/my_analysis
```

**Output files:**
- Individual: `outputs/my_analysis/{CohortName}.parquet`
- Combined: `outputs/my_analysis/combined_cohorts.csv`
- Metadata: `outputs/my_analysis/{CohortName}_metadata.json`

## Available Cohorts

HTN_Conservative, HTN_Sensitive, Diabetes_General, PreDiabetes, GDM, CAD_Conservative, CAD_Sensitive, Dyslipidemia_Conservative, Dyslipidemia_Sensitive, IBS_General, IBS_D, IBS_M, IBS_U, Metabolic_Syndrome, PCOS, Obesity, Overweight, Normal_Weight

## Project Structure
```
cohort_segregation_v4/
├── src/
├── configs/
│   ├── db_connection.yaml
│   └── cohorts.yaml
├── scripts/
│   └── run_cohorts.py
├── outputs/
└── requirements.txt
```

## Notes
- Ensure VPN is connected before running
- Database credentials must have read access
- Output files are saved in the specified output directory

