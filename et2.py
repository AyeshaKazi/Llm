# Data Extraction Layer Components Explained

## 1. Connection Pool Management

The connection pool manages database connections to Sybase. Here's how we configure and use it:

```python
from sqlalchemy import create_engine
from contextlib import contextmanager

class SybaseConnectionPool:
    def __init__(self, connection_params):
        self.engine = create_engine(
            f"sybase+pyodbc://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}/{connection_params['database']}",
            pool_size=20,               # Maximum connections in pool
            max_overflow=5,             # Additional connections if pool is full
            pool_timeout=30,            # Wait time for available connection
            pool_recycle=3600          # Connection lifetime in seconds
        )

# Usage example:
connection_params = {
    "user": "etl_user",
    "password": "secure_pass",
    "host": "sybase.example.com",
    "database": "source_db"
}
pool = SybaseConnectionPool(connection_params)
```

## 2. Data Extraction Methods

### 2.1 Cursor-based Pagination
For large tables, we use cursor-based pagination to extract data in manageable chunks:

```python
def extract_paginated_data(connection, table_name, batch_size=10000, last_id=0):
    """
    Extracts data in batches using ID-based pagination
    
    Input:
        table_name: str - Name of source table
        batch_size: int - Number of records per batch
        last_id: int - Last processed ID
        
    Output:
        List of dictionaries containing record data
    """
    query = f"""
    SELECT TOP {batch_size} *
    FROM {table_name}
    WHERE id > :last_id
    ORDER BY id ASC
    """
    
    return connection.execute(
        query,
        {"last_id": last_id}
    ).fetchall()

# Example usage:
batch = extract_paginated_data(
    connection=pool.engine.connect(),
    table_name="customers",
    batch_size=5000,
    last_id=1000
)
```

### 2.2 Materialized Views
For complex data joins and transformations:

```python
def create_materialized_view(connection, view_name, source_query):
    """
    Creates materialized view for complex data extraction
    
    Input:
        view_name: str - Name for the materialized view
        source_query: str - Complex query to materialize
        
    Output:
        None - Creates view in database
    """
    view_query = f"""
    CREATE VIEW {view_name}
    AS
    {source_query}
    """
    
    connection.execute(view_query)

# Example usage:
complex_query = """
SELECT 
    o.order_id,
    c.customer_name,
    SUM(oi.quantity * oi.price) as total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.order_id, c.customer_name
"""

create_materialized_view(
    connection=pool.engine.connect(),
    view_name="order_summaries",
    source_query=complex_query
)
```

### 2.3 Change Data Capture Configuration

```python
def setup_cdc(connection, table_name):
    """
    Configures CDC for a table
    
    Input:
        table_name: str - Table to enable CDC on
        
    Output:
        None - Enables CDC on table
    """
    cdc_config = {
        "capture_interval": 1000,  # milliseconds
        "retention_period": 72,    # hours
        "include_columns": "*"     # all columns
    }
    
    enable_cdc_query = f"""
    EXEC sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name = '{table_name}',
        @capture_interval = {cdc_config['capture_interval']},
        @retention = {cdc_config['retention_period']},
        @columns = '{cdc_config['include_columns']}'
    """
    
    connection.execute(enable_cdc_query)

# Example usage:
setup_cdc(
    connection=pool.engine.connect(),
    table_name="orders"
)
```

## 3. Error Handling and Retries

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),           # Maximum retry attempts
    wait=wait_exponential(multiplier=1),  # Exponential backoff
    retry_error_callback=log_failure      # Error logging
)
def extract_with_retry(connection, table_name):
    """
    Extracts data with automatic retry on failure
    
    Input:
        table_name: str - Source table name
        
    Output:
        List of extracted records or None on failure
    """
    try:
        return connection.execute(f"SELECT * FROM {table_name}").fetchall()
    except Exception as e:
        log_error(f"Extraction failed for {table_name}: {str(e)}")
        raise

# Example usage:
try:
    data = extract_with_retry(
        connection=pool.engine.connect(),
        table_name="customers"
    )
except Exception as e:
    handle_extraction_failure(e)
```

## Configuration Parameters Summary

Key configuration parameters for the extraction layer:

```python
EXTRACTION_CONFIG = {
    "connection": {
        "pool_size": 20,
        "max_overflow": 5,
        "pool_timeout": 30,
        "connection_lifetime": 3600
    },
    "pagination": {
        "default_batch_size": 10000,
        "max_batch_size": 50000,
        "min_batch_size": 1000
    },
    "cdc": {
        "capture_interval": 1000,
        "retention_period": 72,
        "cleanup_interval": 24
    },
    "retry": {
        "max_attempts": 3,
        "initial_delay": 1,
        "max_delay": 10
    }
}
```

Each component is designed to handle specific aspects of data extraction while maintaining performance and reliability. The configuration parameters can be adjusted based on:
- Source table sizes
- Available system resources
- Network conditions
- Business requirements

Would you like me to explain any specific component in more detail or provide additional configuration examples?
