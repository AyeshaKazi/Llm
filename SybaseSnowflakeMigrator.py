import multiprocessing
import pyodbc
import snowflake.connector
import pandas as pd

class SybaseSnowflakeMigrator:
    def __init__(self, sybase_conn_str, snowflake_config):
        self.sybase_conn = pyodbc.connect(sybase_conn_str)
        self.snowflake_conn = snowflake.connector.connect(**snowflake_config)
        
    def get_table_partitions(self, table_name, partition_column):
        """Dynamically partition large table"""
        query = f"""
        SELECT 
            MIN({partition_column}) as min_val,
            MAX({partition_column}) as max_val
        FROM {table_name}
        """
        df = pd.read_sql(query, self.sybase_conn)
        min_val, max_val = df.iloc[0]['min_val'], df.iloc[0]['max_val']
        
        # Create 10-20 roughly equal partitions
        partition_ranges = self._create_range_partitions(min_val, max_val, 15)
        return partition_ranges
    
    def _create_range_partitions(self, min_val, max_val, num_partitions):
        """Generate partition ranges"""
        step = (max_val - min_val) / num_partitions
        return [
            (min_val + i*step, min_val + (i+1)*step) 
            for i in range(num_partitions)
        ]
    
    def migrate_partition(self, table_name, partition_column, partition_range):
        """Migrate single data partition"""
        min_val, max_val = partition_range
        
        # Extract partition
        extract_query = f"""
        SELECT * FROM {table_name}
        WHERE {partition_column} >= {min_val} 
          AND {partition_column} < {max_val}
        """
        partition_df = pd.read_sql(extract_query, self.sybase_conn)
        
        # Load to Snowflake
        self._bulk_snowflake_load(partition_df, table_name)
        
    def migrate_table(self, table_name, partition_column):
        """Orchestrate parallel table migration"""
        partitions = self.get_table_partitions(table_name, partition_column)
        
        with multiprocessing.Pool(processes=8) as pool:
            pool.starmap(
                self.migrate_partition, 
                [(table_name, partition_column, partition) for partition in partitions]
            )

def main():
    sybase_conn_str = '...'
    snowflake_config = {
        'account': '...',
        'user': '...',
        'password': '...'
    }
    
    migrator = SybaseSnowflakeMigrator(sybase_conn_str, snowflake_config)
    migrator.migrate_table('large_transactions', 'transaction_date')

if __name__ == '__main__':
    main()
