from sqlalchemy import create_engine, text

# Connection String
connection_string = (
    'sybase+sqlalchemy_sybase://username:password@hostname:port/database'
)

# Create Engine
engine = create_engine(connection_string)

# Test Connection
with engine.connect() as connection:
    result = connection.execute(text("SELECT @@version"))
    print("Server Version:", result.fetchone()[0])

# Example Query
with engine.connect() as connection:
    query = text("SELECT * FROM your_table LIMIT 5")
    results = connection.execute(query)
    for row in results:
        print(row)
