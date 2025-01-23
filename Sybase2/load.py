import jaydebeapi
import pandas as pd
import configparser
import os
from sqlalchemy import create_engine

class SybaseETL:
    def __init__(self, config_path='config.ini'):
        self.config = self._load_config(config_path)
        self.connection = None
        self.jdbc_driver_path = 'jconn4.jar'  # Update with actual path
        
    def _load_config(self, config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        return config['database']
    
    def connect(self):
        jdbc_url = f"jdbc:sybase:Tds:{self.config['host']}:{self.config['port']}/{self.config['database']}"
        jdbc_driver_class = "com.sybase.jdbc4.jdbc.SybDriver"
        
        try:
            self.connection = jaydebeapi.connect(
                jdbc_driver_class,
                [jdbc_url, self.config['username'], self.config['password']],
                [self.jdbc_driver_path]
            )
        except Exception as e:
            print(f"Connection error: {e}")
            raise
    
    def get_all_tables(self):
        query = "SELECT name FROM sysobjects WHERE type = 'U'"
        return pd.read_sql(query, self.connection)
    
    def extract_table(self, table_name, chunk_size=10000):
        """
        Extract table data with chunking for large tables
        """
        query = f"SELECT * FROM {table_name}"
        
        # Use pandas to read in chunks
        for chunk in pd.read_sql(query, self.connection, chunksize=chunk_size):
            yield chunk
    
    def load_to_destination(self, table_name, destination_engine):
        """
        Load extracted data to destination (e.g., PostgreSQL, CSV)
        """
        for chunk in self.extract_table(table_name):
            chunk.to_sql(table_name, destination_engine, if_exists='append', index=False)
    
    def close_connection(self):
        if self.connection:
            self.connection.close()

def main():
    etl = SybaseETL()
    try:
        etl.connect()
        
        # Get all tables
        tables = etl.get_all_tables()
        print("Available tables:", tables)
        
        # Example: Load to PostgreSQL
        pg_engine = create_engine('postgresql://username:password@host:port/database')
        
        # Extract and load specific tables
        for table in tables['name']:
            print(f"Processing table: {table}")
            etl.load_to_destination(table, pg_engine)
    
    except Exception as e:
        print(f"ETL Process Error: {e}")
    
    finally:
        etl.close_connection()

if __name__ == "__main__":
    main()
