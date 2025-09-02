# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Table Migration Utility
# MAGIC
# MAGIC This notebook provides a modular solution for migrating Hive metastore tables to Unity Catalog.
# MAGIC Supports both SYNC (external tables) and DEEP CLONE (managed tables) operations.
# MAGIC
# MAGIC ## Configuration
# MAGIC Update the `MIGRATION_CONFIG` dictionary below to add new tables/schemas for migration.

# COMMAND ----------

from typing import Dict, List, Literal, Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Configuration
# MAGIC
# MAGIC Edit this configuration to add your tables for migration:
# MAGIC - `migration_type`: "SYNC" for external tables, "DEEP_CLONE" for managed tables
# MAGIC - `source_schema`: Source schema in hive_metastore
# MAGIC - `source_table`: Source table name (optional for schema-level operations)
# MAGIC - `destination_catalog`: Target Unity Catalog catalog
# MAGIC - `destination_schema`: Target schema name
# MAGIC - `destination_table`: Target table name (optional, defaults to source_table)
# MAGIC - `owner`: Table owner (user or group)
# MAGIC - `sync_as_external`: For SYNC only - sync managed tables as external (default: False)

# COMMAND ----------

MIGRATION_CONFIG = {
    "migrations": [
        {
            "name": "sales_data_sync",
            "migration_type": "SYNC",
            "source_schema": "default",
            "source_table": "sales_fact",
            "destination_catalog": "production",
            "destination_schema": "sales",
            "destination_table": "sales_fact",
            "owner": "data_team",
            "sync_as_external": False
        },
        {
            "name": "customer_clone",
            "migration_type": "DEEP_CLONE",
            "source_schema": "default", 
            "source_table": "customers",
            "destination_catalog": "production",
            "destination_schema": "crm",
            "destination_table": "customers",
            "owner": "crm_team"
        },
        {
            "name": "analytics_schema_sync",
            "migration_type": "SYNC",
            "source_schema": "analytics",
            "destination_catalog": "production",
            "destination_schema": "analytics_uc",
            "owner": "analytics_team",
            "sync_as_external": True
        }
    ],
    "global_settings": {
        "dry_run": False,
        "add_deprecation_comments": True,
        "comment_template": "This table is deprecated. Please use {destination} instead of {source}."
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Functions

# COMMAND ----------

class UnityMigrator:
    def __init__(self, config: Dict):
        self.config = config
        self.results = []
        
    def validate_config(self, migration: Dict) -> bool:
        """Validate migration configuration"""
        required_fields = ['migration_type', 'source_schema', 'destination_catalog', 'destination_schema', 'owner']
        
        for field in required_fields:
            if field not in migration:
                logger.error(f"Missing required field: {field} in migration {migration.get('name', 'unnamed')}")
                return False
                
        if migration['migration_type'] not in ['SYNC', 'DEEP_CLONE']:
            logger.error(f"Invalid migration_type: {migration['migration_type']}")
            return False
            
        return True
    
    def build_sync_command(self, migration: Dict) -> str:
        """Build SYNC SQL command"""
        source_path = f"hive_metastore.{migration['source_schema']}"
        dest_path = f"{migration['destination_catalog']}.{migration['destination_schema']}"
        
        if 'source_table' in migration:
            # Table-level sync
            source_table = f"{source_path}.{migration['source_table']}"
            dest_table = f"{dest_path}.{migration.get('destination_table', migration['source_table'])}"
            
            if migration.get('sync_as_external', False):
                cmd = f"SYNC TABLE {dest_table} AS EXTERNAL FROM {source_table}"
            else:
                cmd = f"SYNC TABLE {dest_table} FROM {source_table}"
        else:
            # Schema-level sync
            if migration.get('sync_as_external', False):
                cmd = f"SYNC SCHEMA {dest_path} AS EXTERNAL FROM {source_path}"
            else:
                cmd = f"SYNC SCHEMA {dest_path} FROM {source_path}"
        
        cmd += f" SET OWNER `{migration['owner']}`"
        return cmd
    
    def build_clone_command(self, migration: Dict) -> str:
        """Build DEEP CLONE SQL command"""
        if 'source_table' not in migration:
            raise ValueError("DEEP_CLONE requires source_table to be specified")
            
        source_table = f"hive_metastore.{migration['source_schema']}.{migration['source_table']}"
        dest_table = f"{migration['destination_catalog']}.{migration['destination_schema']}.{migration.get('destination_table', migration['source_table'])}"
        
        return f"CREATE OR REPLACE TABLE {dest_table} DEEP CLONE {source_table}"
    
    def execute_migration(self, migration: Dict) -> Dict:
        """Execute a single migration"""
        result = {
            'name': migration.get('name', 'unnamed'),
            'migration_type': migration['migration_type'],
            'status': 'pending',
            'command': '',
            'error': None,
            'start_time': datetime.now()
        }
        
        try:
            if not self.validate_config(migration):
                result['status'] = 'validation_failed'
                return result
            
            # Build command
            if migration['migration_type'] == 'SYNC':
                command = self.build_sync_command(migration)
            else:
                command = self.build_clone_command(migration)
            
            result['command'] = command
            logger.info(f"Executing migration '{result['name']}': {command}")
            
            # Execute command (or dry run)
            if self.config['global_settings'].get('dry_run', False):
                logger.info(f"DRY RUN - Would execute: {command}")
                result['status'] = 'dry_run_success'
            else:
                spark.sql(command)
                result['status'] = 'completed'
                logger.info(f"Migration '{result['name']}' completed successfully")
                
                # Set ownership for DEEP_CLONE (SYNC sets owner in command)
                if migration['migration_type'] == 'DEEP_CLONE':
                    dest_table = f"{migration['destination_catalog']}.{migration['destination_schema']}.{migration.get('destination_table', migration['source_table'])}"
                    owner_cmd = f"ALTER TABLE {dest_table} SET OWNER `{migration['owner']}`"
                    if not self.config['global_settings'].get('dry_run', False):
                        spark.sql(owner_cmd)
                    logger.info(f"Set owner for {dest_table} to {migration['owner']}")
                
        except Exception as e:
            logger.error(f"Migration '{result['name']}' failed: {str(e)}")
            result['status'] = 'failed'
            result['error'] = str(e)
        
        result['end_time'] = datetime.now()
        return result
    
    def add_deprecation_comment(self, migration: Dict):
        """Add deprecation comment to source table"""
        if not self.config['global_settings'].get('add_deprecation_comments', True):
            return
            
        if 'source_table' not in migration:
            return  # Schema-level migrations don't need table comments
            
        try:
            source_table = f"hive_metastore.{migration['source_schema']}.{migration['source_table']}"
            dest_table = f"{migration['destination_catalog']}.{migration['destination_schema']}.{migration.get('destination_table', migration['source_table'])}"
            
            comment = self.config['global_settings']['comment_template'].format(
                destination=dest_table,
                source=source_table
            )
            
            comment_cmd = f"COMMENT ON TABLE {source_table} IS '{comment}'"
            
            if not self.config['global_settings'].get('dry_run', False):
                spark.sql(comment_cmd)
            logger.info(f"Added deprecation comment to {source_table}")
            
        except Exception as e:
            logger.warning(f"Failed to add deprecation comment: {str(e)}")
    
    def run_migrations(self) -> List[Dict]:
        """Run all configured migrations"""
        logger.info(f"Starting migration batch with {len(self.config['migrations'])} migrations")
        
        for migration in self.config['migrations']:
            result = self.execute_migration(migration)
            self.results.append(result)
            
            # Add deprecation comment if migration succeeded
            if result['status'] == 'completed':
                self.add_deprecation_comment(migration)
        
        return self.results
    
    def print_summary(self):
        """Print migration results summary"""
        print("\n" + "="*80)
        print("MIGRATION SUMMARY")
        print("="*80)
        
        total = len(self.results)
        completed = len([r for r in self.results if r['status'] == 'completed'])
        failed = len([r for r in self.results if r['status'] == 'failed'])
        dry_run = len([r for r in self.results if r['status'] == 'dry_run_success'])
        
        print(f"Total migrations: {total}")
        print(f"Completed: {completed}")
        print(f"Failed: {failed}")
        print(f"Dry run: {dry_run}")
        print()
        
        for result in self.results:
            status_icon = "✅" if result['status'] == 'completed' else "❌" if result['status'] == 'failed' else "🔍"
            duration = (result.get('end_time', datetime.now()) - result['start_time']).total_seconds()
            print(f"{status_icon} {result['name']} ({result['migration_type']}) - {result['status']} ({duration:.1f}s)")
            
            if result.get('error'):
                print(f"   Error: {result['error']}")
            
        print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Migrations

# COMMAND ----------

# Initialize migrator
migrator = UnityMigrator(MIGRATION_CONFIG)

# Run migrations
results = migrator.run_migrations()

# Print summary
migrator.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions for Manual Operations

# COMMAND ----------

def check_table_exists(catalog: str, schema: str, table: str) -> bool:
    """Check if a table exists in Unity Catalog"""
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.{table}")
        return True
    except:
        return False

def get_table_info(catalog: str, schema: str, table: str) -> Dict:
    """Get detailed information about a table"""
    try:
        df = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table}")
        return {"exists": True, "info": df.collect()}
    except Exception as e:
        return {"exists": False, "error": str(e)}

def list_hive_tables(schema: str = None) -> List[str]:
    """List tables in hive_metastore"""
    if schema:
        df = spark.sql(f"SHOW TABLES IN hive_metastore.{schema}")
    else:
        df = spark.sql("SHOW TABLES IN hive_metastore")
    return [row.tableName for row in df.collect()]

def validate_permissions(catalog: str, schema: str) -> Dict:
    """Validate permissions on Unity Catalog objects"""
    try:
        # Try to create and drop a temporary table to test permissions
        test_table = f"{catalog}.{schema}.permission_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        spark.sql(f"CREATE TABLE {test_table} (test_col STRING) USING DELTA")
        spark.sql(f"DROP TABLE {test_table}")
        return {"has_permissions": True}
    except Exception as e:
        return {"has_permissions": False, "error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Testing and Validation

# COMMAND ----------

# Example usage of utility functions
print("=== VALIDATION EXAMPLES ===")

# Check if a specific table exists
exists = check_table_exists("production", "sales", "sales_fact")
print(f"Table production.sales.sales_fact exists: {exists}")

# List tables in a Hive schema
hive_tables = list_hive_tables("default")
print(f"Tables in hive_metastore.default: {hive_tables[:5]}...")  # Show first 5

# Validate permissions
perm_check = validate_permissions("production", "sales")
print(f"Permissions check: {perm_check}")