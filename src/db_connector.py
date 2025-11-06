import yaml
import pandas as pd
import os
import sqlalchemy


class DBConnector:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.engine = self._create_postgres_engine()
        self.tables = {}
        self.load_tables()

    def load_config(self, path):
        """Load YAML configuration file"""
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _create_postgres_engine(self):
        """Create PostgreSQL SQLAlchemy engine from config"""
        pg_config = self.config.get('postgres', {})
        
        # Build connection string
        connection_string = (
            f"postgresql+psycopg2://{pg_config.get('user')}:{pg_config.get('password')}"
            f"@{pg_config.get('host', 'localhost')}:{pg_config.get('port', 5432)}"
            f"/{pg_config.get('database')}"
        )
        
        # Create engine with connection pooling
        engine = sqlalchemy.create_engine(
            connection_string,
            pool_pre_ping=True,  # Verify connections before using
            pool_size=5,  # Number of connections to maintain
            max_overflow=10,  # Additional connections if needed
            echo=False  # Set to True for SQL query debugging
        )
        
        return engine

    def load_tables(self):
        """Load required tables from database using SQLAlchemy engine.
        
        OPTIMIZED: Only loads 'members' table into memory (small, needed for joins).
        Other tables (claims_entries, claims_diagnoses, etc.) are queried directly 
        from database when needed, avoiding loading millions of rows into memory.
        """
        import sqlalchemy
        
        # Get schema from config (default to 'public' for PostgreSQL)
        schema = self.config.get('postgres', {}).get('schema', 'public')
        
        # Required table names (for validation)
        required_table_names = [
            'claims_entries',
            'claims_diagnoses',
            'claims_procedures',
            'claims_drugs',
            'members'
        ]
        
        # Optional tables (will skip if not present)
        optional_table_names = [
            'claims_members_monthly_utilization'
        ]
        
        self.tables = {}
        
        # Verify required tables exist (but don't load large tables)
        insp = sqlalchemy.inspect(self.engine)
        for table_name in required_table_names:
            if not insp.has_table(table_name, schema=schema):
                raise RuntimeError(f"Missing required table: {schema}.{table_name}")
        
        # Only load 'members' table (small, needed for joins)
        # Other tables are queried directly from database when needed
        print(f"Loading members table...")
        self.tables['members'] = pd.read_sql(
            sqlalchemy.text(f'SELECT * FROM {schema}.members'), 
            self.engine
        )
        print(f"Loaded {len(self.tables['members']):,} members")
        
        # Check optional tables (skip if missing)
        for table_name in optional_table_names:
            if not insp.has_table(table_name, schema=schema):
                print(f"Info: Optional table '{schema}.{table_name}' not found in database. Skipping...")