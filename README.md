# Unity Catalog Migration Utility

A modular Databricks notebook utility for migrating Hive metastore tables to Unity Catalog. Supports both SYNC (external tables) and DEEP CLONE (managed tables) operations with configuration-driven table mapping.

## Overview

This utility provides a systematic approach to Unity Catalog migration with:
- **Configuration-driven migrations** - Define all your table migrations in a single config
- **Batch processing** - Migrate multiple tables/schemas in one execution
- **Error handling** - Comprehensive validation and error reporting
- **Dry run mode** - Test migrations before execution
- **Progress tracking** - Detailed logging and summary reports

## Features

### Migration Types
- **SYNC**: Migrate external tables or managed tables (outside DBFS) to UC external tables
- **DEEP CLONE**: Migrate managed Delta tables to UC managed tables
- **Schema-level operations**: Migrate entire schemas at once

### Additional Features
- Automatic owner assignment
- Deprecation comments on source tables
- Permission validation utilities
- Table existence checking
- Comprehensive error handling and logging

## Quick Start

1. **Upload the notebook** to your Databricks workspace
2. **Configure your migrations** in the `MIGRATION_CONFIG` section
3. **Run the notebook** to execute migrations

## Configuration

### Basic Migration Config

```python
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
        }
    ],
    "global_settings": {
        "dry_run": False,
        "add_deprecation_comments": True
    }
}
```

### Configuration Fields

#### Required Fields
- `migration_type`: `"SYNC"` or `"DEEP_CLONE"`
- `source_schema`: Source schema in hive_metastore
- `destination_catalog`: Target Unity Catalog catalog
- `destination_schema`: Target schema name
- `owner`: Table owner (user or group)

#### Optional Fields
- `name`: Migration identifier (for logging)
- `source_table`: Source table name (omit for schema-level operations)
- `destination_table`: Target table name (defaults to source_table)
- `sync_as_external`: For SYNC - treat managed tables as external (default: False)

#### Global Settings
- `dry_run`: Preview operations without execution (default: False)
- `add_deprecation_comments`: Add migration comments to source tables (default: True)
- `comment_template`: Template for deprecation comments

## Usage Examples

### Individual Table Migration

```python
{
    "name": "customer_data_clone",
    "migration_type": "DEEP_CLONE",
    "source_schema": "crm",
    "source_table": "customers",
    "destination_catalog": "production",
    "destination_schema": "customer_data",
    "destination_table": "customers_uc",
    "owner": "crm_team"
}
```

### Schema-Level Migration

```python
{
    "name": "analytics_schema_sync",
    "migration_type": "SYNC",
    "source_schema": "analytics",
    "destination_catalog": "production",
    "destination_schema": "analytics_uc",
    "owner": "analytics_team",
    "sync_as_external": True
}
```

### External Table Migration

```python
{
    "name": "external_logs_sync",
    "migration_type": "SYNC",
    "source_schema": "logs",
    "source_table": "application_logs",
    "destination_catalog": "production",
    "destination_schema": "logging",
    "owner": "platform_team",
    "sync_as_external": True
}
```

## Prerequisites

### Unity Catalog Setup
- Unity Catalog metastore enabled
- Target catalogs and schemas created
- Appropriate storage credentials and external locations (for external tables)

### Permissions Required

#### Unity Catalog Permissions
- `USE CATALOG` and `USE SCHEMA` on target catalog/schema
- `CREATE TABLE` on target schema
- `CREATE EXTERNAL TABLE` on external locations (for SYNC operations)

#### Hive Metastore Access
- Table access control privileges on source tables (if using standard access mode)
- Or use dedicated access mode compute

#### Grant Hive Access (if needed)
```sql
GRANT ALL PRIVILEGES ON CATALOG hive_metastore TO `<user>`;
```

### Compute Requirements
- SQL warehouse or compute with Unity Catalog support
- Access to both Hive metastore and Unity Catalog

## Migration Types Guide

### When to Use SYNC
- **External tables** in any supported format
- **Managed tables** stored outside DBFS root
- Need to **maintain data location** (external tables)
- Want **scheduled syncing** to keep tables updated
- **Schema-level bulk migrations**

```python
# External table example
{
    "migration_type": "SYNC",
    "source_schema": "external_data",
    "source_table": "s3_logs",
    "destination_catalog": "analytics",
    "destination_schema": "logs",
    "owner": "data_engineers"
}
```

### When to Use DEEP_CLONE
- **Managed Delta tables** in DBFS
- Want **Unity Catalog managed tables** (recommended)
- Need **complete data governance**
- Tables with **complex metadata** (partitions, constraints, etc.)

```python
# Managed table example
{
    "migration_type": "DEEP_CLONE",
    "source_schema": "warehouse",
    "source_table": "fact_sales",
    "destination_catalog": "production",
    "destination_schema": "sales",
    "owner": "business_intelligence"
}
```

## Best Practices

### Pre-Migration Planning
1. **Inventory your tables** using the utility functions
2. **Validate permissions** on target locations
3. **Test with dry_run = True** first
4. **Plan owner assignments** (preferably groups)

### Migration Strategy
1. **Start with external tables** (faster, less data movement)
2. **Migrate critical tables first** during maintenance windows
3. **Use schema-level operations** for bulk migrations
4. **Validate results** before dropping source tables

### Post-Migration
1. **Update workloads** to use new table references
2. **Test applications** thoroughly
3. **Monitor deprecated table usage** via comments
4. **Drop old tables** only after validation

## Utility Functions

The notebook includes helper functions for validation and exploration:

```python
# Check if table exists
exists = check_table_exists("production", "sales", "customers")

# List Hive tables
tables = list_hive_tables("default")

# Validate permissions
perms = validate_permissions("production", "sales")

# Get table details
info = get_table_info("production", "sales", "customers")
```

## Troubleshooting

### Common Issues

#### Permission Errors
- Verify Unity Catalog permissions on target catalog/schema
- Check Hive metastore access (grant privileges or use dedicated access mode)
- Ensure storage credentials are configured for external locations

#### Table Format Issues
- **DEEP_CLONE**: Source must be Delta format
- **SYNC**: Check supported external table formats
- Convert non-Delta tables using CREATE TABLE AS SELECT first

#### Storage Location Issues
- External tables require configured external locations
- Managed tables need Unity Catalog managed storage
- Verify storage credentials have proper IAM permissions

### Error Messages

#### "Table not found"
- Verify source table exists in hive_metastore
- Check schema and table names in configuration

#### "Permission denied"
- Review Unity Catalog permissions
- Check compute access mode settings
- Verify storage credential configuration

#### "Invalid table format"
- Ensure Delta format for DEEP_CLONE operations
- Check external table format support for SYNC

## Monitoring and Validation

### During Migration
- Monitor notebook output for real-time progress
- Check for permission or format errors
- Review generated SQL commands in dry run mode

### Post-Migration Validation
1. **Data integrity**: Compare row counts and sample data
2. **Metadata**: Verify schema, partitions, and properties
3. **Permissions**: Test access with different user roles
4. **Performance**: Monitor query performance on new tables

### Deprecation Management
- Source tables get deprecation comments automatically
- Use Databricks Assistant Quick Fix to update code references
- Monitor usage before dropping old tables

## Support and Maintenance

### Regular Tasks
- **Update configurations** as new tables are created
- **Monitor migration logs** for any issues
- **Review deprecated table usage** periodically
- **Update documentation** with new migration patterns

### Customization
The utility is designed to be extended:
- Add custom validation logic
- Modify SQL command templates
- Extend logging and reporting
- Add integration with external systems

## Example Complete Configuration

```python
MIGRATION_CONFIG = {
    "migrations": [
        # Individual table migrations
        {
            "name": "sales_fact_table",
            "migration_type": "DEEP_CLONE",
            "source_schema": "warehouse",
            "source_table": "sales_fact",
            "destination_catalog": "production",
            "destination_schema": "sales",
            "owner": "sales_team"
        },
        {
            "name": "customer_external_table",
            "migration_type": "SYNC", 
            "source_schema": "external",
            "source_table": "customer_profiles",
            "destination_catalog": "production",
            "destination_schema": "customers",
            "owner": "customer_team"
        },
        # Schema-level migration
        {
            "name": "analytics_schema",
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
        "comment_template": "DEPRECATED: Use {destination} instead of {source}"
    }
}
```

This configuration will:
1. Deep clone a managed sales table
2. Sync an external customer table  
3. Sync an entire analytics schema as external tables
4. Add deprecation comments to all source tables