# Sybase to Snowflake Data Migration Toolkit

## Overview
Comprehensive data migration solution for moving large tables from Sybase to Snowflake via S3 staging.

## Features
- Parallel data extraction
- Configurable partitioning
- Comprehensive logging
- Error tracking
- Progress monitoring
- Multi-table migration support

## Prerequisites
- Python 3.8+
- Libraries:
  - pyodbc
  - pandas
  - boto3
  - snowflake-connector
  - streamlit

## Configuration
1. Edit `migration_config.json`
   - Set Sybase connection string
   - Configure S3 bucket
   - Add Snowflake credentials
   - Define tables to migrate

## Usage
### Migration Script
```bash
python sybase-s3-snowflake-migration.py
```

### Monitoring Dashboard
```bash
streamlit run migration_monitor_dashboard.py
```

## Monitoring Capabilities
- Real-time migration progress
- Partition-level tracking
- Error visualization
- Performance metrics

## Best Practices
- Test with small datasets first
- Use secure credential management
- Monitor system resources
