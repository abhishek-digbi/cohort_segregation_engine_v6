import ibis
import yaml
import duckdb
import pandas as pd
from ibis import _
import os
import sqlalchemy


class DBConnector:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.engine = sqlalchemy.create_engine(f"duckdb:///{self.config.get('duckdb', {}).get('path', 'claims.db')}")
        self.tables = {}
        self.load_tables()

    def load_config(self, path):
        """Load YAML configuration file"""
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def load_tables(self):
        """Load required tables from database using SQLAlchemy engine"""
        import sqlalchemy
        
        # Required tables
        required_tables = {
            'claims_entries': 'SELECT * FROM claims_entries',
            'claims_diagnoses': 'SELECT * FROM claims_diagnoses',
            'claims_procedures': 'SELECT * FROM claims_procedures',
            'claims_drugs': 'SELECT * FROM claims_drugs',
            'members': 'SELECT * FROM members',
        }
        
        # Optional tables (will skip if not present)
        optional_tables = {
            'claims_members_monthly_utilization': 'SELECT * FROM claims_members_monthly_utilization',
        }
        
        self.tables = {}
        
        # Load required tables
        insp = sqlalchemy.inspect(self.engine)
        for table_name, query in required_tables.items():
            if not insp.has_table(table_name):
                raise RuntimeError(f"Missing required table: {table_name}")
            self.tables[table_name] = pd.read_sql(query, self.engine)
        
        # Load optional tables (skip if missing)
        for table_name, query in optional_tables.items():
            if insp.has_table(table_name):
                self.tables[table_name] = pd.read_sql(query, self.engine)
            else:
                print(f"Warning: Optional table '{table_name}' not found in database. Skipping...")