#!/usr/bin/env python3
"""
Test PostgreSQL Database Connection Script

This script tests the PostgreSQL connection and discovers:
- Available schemas in the database
- Location of required tables
- Sample data retrieval
"""

import yaml
import sqlalchemy
import pandas as pd
import sys
import os

def load_config(config_path='configs/db_connection.yaml'):
    """Load database configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_connection_string(pg_config):
    """Create PostgreSQL connection string"""
    return (
        f"postgresql+psycopg2://{pg_config.get('user')}:{pg_config.get('password')}"
        f"@{pg_config.get('host', 'localhost')}:{pg_config.get('port', 5432)}"
        f"/{pg_config.get('database')}"
    )

def test_connection(config_path='configs/db_connection.yaml'):
    """Test PostgreSQL connection and discover schema information"""
    print("=" * 60)
    print("PostgreSQL Connection Test Script")
    print("=" * 60)
    
    # Load configuration
    print("\n1. Loading configuration...")
    try:
        config = load_config(config_path)
        pg_config = config.get('postgres', {})
        if not pg_config:
            print("ERROR: No 'postgres' section found in config file!")
            return False
        print(f"   ✓ Configuration loaded from {config_path}")
        print(f"   Host: {pg_config.get('host')}")
        print(f"   Database: {pg_config.get('database')}")
        print(f"   User: {pg_config.get('user')}")
    except FileNotFoundError:
        print(f"ERROR: Config file not found: {config_path}")
        return False
    except Exception as e:
        print(f"ERROR: Failed to load config: {e}")
        return False
    
    # Create connection
    print("\n2. Creating database connection...")
    try:
        connection_string = create_connection_string(pg_config)
        engine = sqlalchemy.create_engine(
            connection_string,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10}
        )
        print("   ✓ Engine created successfully")
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"   ✓ Connected successfully!")
            print(f"   PostgreSQL Version: {version[:50]}...")
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        return False
    
    # Discover schemas
    print("\n3. Discovering schemas...")
    try:
        insp = sqlalchemy.inspect(engine)
        schemas = insp.get_schema_names()
        print(f"   ✓ Found {len(schemas)} schema(s):")
        for schema in schemas:
            print(f"      - {schema}")
        
        # Use configured schema or default to 'public'
        configured_schema = pg_config.get('schema', 'public')
        if configured_schema not in schemas:
            print(f"\n   ⚠ WARNING: Configured schema '{configured_schema}' not found!")
            print(f"   Available schemas: {', '.join(schemas)}")
            if schemas:
                configured_schema = schemas[0]
                print(f"   Using first available schema: {configured_schema}")
        else:
            print(f"\n   ✓ Using configured schema: {configured_schema}")
    except Exception as e:
        print(f"   ✗ Failed to discover schemas: {e}")
        return False
    
    # Check required tables
    print("\n4. Checking required tables...")
    required_tables = {
        'claims_entries': ['claim_entry_id', 'member_id_hash', 'date_of_service', 'claim_type'],
        'claims_diagnoses': ['claim_entry_id', 'icd_code'],
        'claims_procedures': ['claim_entry_id'],
        'claims_drugs': ['claim_entry_id'],
        'members': ['member_id_hash'],
    }
    
    optional_tables = {
        'claims_members_monthly_utilization': ['member_id_hash'],
    }
    
    found_tables = {}
    missing_tables = []
    
    for table_name in required_tables.keys():
        try:
            if insp.has_table(table_name, schema=configured_schema):
                columns = [col['name'] for col in insp.get_columns(table_name, schema=configured_schema)]
                print(f"   ✓ Found table: {configured_schema}.{table_name}")
                print(f"      Columns ({len(columns)}): {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                found_tables[table_name] = columns
            else:
                print(f"   ✗ Missing table: {configured_schema}.{table_name}")
                missing_tables.append(table_name)
        except Exception as e:
            print(f"   ✗ Error checking table {table_name}: {e}")
            missing_tables.append(table_name)
    
    # Check optional tables
    print("\n5. Checking optional tables...")
    for table_name in optional_tables.keys():
        try:
            if insp.has_table(table_name, schema=configured_schema):
                columns = [col['name'] for col in insp.get_columns(table_name, schema=configured_schema)]
                print(f"   ✓ Found optional table: {configured_schema}.{table_name}")
                found_tables[table_name] = columns
            else:
                print(f"   ⚠ Optional table not found: {configured_schema}.{table_name} (this is OK)")
        except Exception as e:
            print(f"   ⚠ Error checking optional table {table_name}: {e}")
    
    # Test sample queries
    print("\n6. Testing sample queries...")
    if found_tables and 'claims_entries' in found_tables:
        try:
            # Test a simple query
            query = f'SELECT COUNT(*) as count FROM {configured_schema}.claims_entries'
            result = pd.read_sql(query, engine)
            row_count = result.iloc[0]['count']
            print(f"   ✓ Sample query successful")
            print(f"      claims_entries row count: {row_count:,}")
        except Exception as e:
            print(f"   ✗ Sample query failed: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if missing_tables:
        print(f"\n✗ FAILED: Missing {len(missing_tables)} required table(s):")
        for table in missing_tables:
            print(f"   - {table}")
        print("\n⚠ Please verify:")
        print(f"   1. Schema name is correct (currently: {configured_schema})")
        print(f"   2. Tables exist in the database")
        print(f"   3. User has read permissions")
        return False
    else:
        print("\n✓ SUCCESS: All required tables found!")
        print(f"\nRecommended configuration:")
        print(f"  schema: \"{configured_schema}\"")
        print(f"\nYou can now run the cohort segregation application.")
        return True

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else 'configs/db_connection.yaml'
    success = test_connection(config_path)
    sys.exit(0 if success else 1)
