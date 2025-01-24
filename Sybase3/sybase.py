export SYBASE=/path/to/sybase/installation
export PATH=$SYBASE/bin:$PATH
export LD_LIBRARY_PATH=$SYBASE/lib:$LD_LIBRARY_PATH

import sqlalchemy as sa
from sqlalchemy import create_engine, text

# Sybase Connection Configuration
sybase_config = {
    'server': 'your_server_name',
    'port': 5000,  # Typical Sybase port
    'database': 'your_database',
    'username': 'your_username',
    'password': 'your_password'
}

# Construct Connection String
connection_string = (
    f"sybase+pyodbc://{sybase_config['username']}:{sybase_config['password']}"
    f"@{sybase_config['server']}:{sybase_config['port']}/{sybase_config['database']}"
)

# Create SQLAlchemy Engine
try:
    engine = create_engine(
        connection_string, 
        connect_args={'driver': 'Adaptive Server Enterprise'}
    )

    # Test Connection
    with engine.connect() as connection:
        # Version Check
        version_query = text("SELECT @@version")
        result = connection.execute(version_query)
        print("Server Version:", result.fetchone()[0])

    # Example Query
    with engine.connect() as connection:
        query = text("SELECT * FROM your_table LIMIT 10")
        results = connection.execute(query)
        for row in results:
            print(row)

except Exception as e:
    print(f"Connection Error: {e}")
