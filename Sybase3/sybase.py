import sybase

def sybase_connection():
    try:
        # Establish connection
        conn = sybase.connect(
            host='hostname',
            port=5000,
            user='username', 
            password='password',
            database='dbname'
        )
        
        # Create cursor
        cursor = conn.cursor()
        
        # Execute query
        cursor.execute("SELECT * FROM your_table")
        
        # Fetch results
        results = cursor.fetchall()
        
        # Process results
        for row in results:
            print(row)
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
    except sybase.Error as e:
        print(f"Database connection error: {e}")
