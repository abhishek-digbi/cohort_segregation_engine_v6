# Cohort Segregation Engine - Server Setup

## Installation

### 1. Clone Repository
```bash
git clone https://github.com/abhishek-digbi/cohort_segregation_engine_v6.git
cd cohort_segregation_engine_v6
```

### 2. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Test with One Cohort
```bash
python scripts/run_cohorts.py --cohorts HTN_Conservative --output_dir outputs/test
```

### 4. Run All Cohorts
```bash
python scripts/run_cohorts.py --output_dir outputs/my_analysis
```

**Output files:**
- Individual: `outputs/my_analysis/{CohortName}.parquet`
- Combined: `outputs/my_analysis/combined_cohorts.csv`
- Metadata: `outputs/my_analysis/{CohortName}_metadata.json`

## Available Cohorts

HTN_Conservative, HTN_Sensitive, Diabetes_General, PreDiabetes, GDM, CAD_Conservative, CAD_Sensitive, Dyslipidemia_Conservative, Dyslipidemia_Sensitive, IBS_General, IBS_D, IBS_M, IBS_U, Metabolic_Syndrome, PCOS, Obesity, Overweight, Normal_Weight

## Project Structure
```
cohort_segregation_engine_v6/
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
