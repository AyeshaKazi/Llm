import pyodbc

# Connection Parameters
conn_params = {
    'driver': 'Sybase',  # Likely the default Sybase driver
    'server': 'your_server_hostname',
    'port': 5000,
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password'
}

# Construct Connection String
conn_string = (
    f"DRIVER={conn_params['driver']};"
    f"SERVER={conn_params['server']};"
    f"PORT={conn_params['port']};"
    f"DATABASE={conn_params['database']};"
    f"UID={conn_params['username']};"
    f"PWD={conn_params['password']}"
)

try:
    # Establish Connection
    connection = pyodbc.connect(conn_string)
    
    # Create Cursor
    cursor = connection.cursor()
    
    # Test Query
    cursor.execute("SELECT @@version")
    print("Server Version:", cursor.fetchone()[0])
    
    # Example Query
    cursor.execute("SELECT * FROM your_table LIMIT 5")
    for row in cursor.fetchall():
        print(row)
    
    # Close Connection
    cursor.close()
    connection.close()

except pyodbc.Error as e:
    print(f"Connection Error: {e}")
