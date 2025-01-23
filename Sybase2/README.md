# Sybase ETL Pipeline

## Prerequisites

### Required Libraries
- jaydebeapi
- pandas
- sqlalchemy
- configparser

### Database Drivers
- Sybase jConnect JDBC Driver (jconn4.jar)

## Installation
```bash
pip install jaydebeapi pandas sqlalchemy configparser
```

## Configuration
Create a `config.ini` file with database connection details:
```ini
[database]
host = your_sybase_host
port = your_port
database = your_database
username = your_username
password = your_password
```

## Notes
- Requires Java Runtime Environment (JRE)
- Manually download jconn4.jar from Sybase/SAP
