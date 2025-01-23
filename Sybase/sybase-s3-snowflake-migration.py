import os
import json
import logging
import multiprocessing
import pyodbc
import boto3
import snowflake.connector
import pandas as pd
from datetime import datetime

class DataMigrationTracker:
    def __init__(self, config_path='migration_config.json'):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.s3_client = boto3.client('s3')
        self.migration_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.migration_status_file = f'migration_status_{self.migration_id}.json'

    def _load_config(self, config_path):
        """Load migration configuration"""
        with open(config_path, 'r') as f:
            return json.load(f)

    def _setup_logging(self):
        """Configure comprehensive logging"""
        log_file = f'migration_{self.migration_id}.log'
        logging.basicConfig(
            filename=log_file, 
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        return logging.getLogger()

    def get_sybase_table_stats(self, table_name):
        """Get total record count and data size for Sybase table"""
        with pyodbc.connect(self.config['sybase_connection_string']) as conn:
            cursor = conn.cursor()
            record_count_query = f"SELECT COUNT(*) FROM {table_name}"
            size_query = f"sp_spaceused {table_name}"
            
            record_count = cursor.execute(record_count_query).fetchone()[0]
            cursor.execute(size_query)
            size_info = cursor.fetchall()
            
            return {
                'total_records': record_count,
                'total_size_mb': float(size_info[0][1])
            }

    def partition_table(self, table_name, num_partitions=10):
        """Create table partitions for parallel processing"""
        table_stats = self.get_sybase_table_stats(table_name)
        
        # Determine partition strategy based on primary key or date column
        partition_column = self.config['tables'][table_name].get('partition_column', 'id')
        
        with pyodbc.connect(self.config['sybase_connection_string']) as conn:
            query = f"""
            SELECT 
                MIN({partition_column}) as min_val,
                MAX({partition_column}) as max_val
            FROM {table_name}
            """
            df = pd.read_sql(query, conn)
            min_val, max_val = df.iloc[0]['min_val'], df.iloc[0]['max_val']
        
        # Create partitions
        step = (max_val - min_val) / num_partitions
        partitions = [
            {
                'min': min_val + i*step, 
                'max': min_val + (i+1)*step,
                'partition_id': f'partition_{i+1}'
            } 
            for i in range(num_partitions)
        ]
        
        return partitions, table_stats

    def extract_partition(self, table_name, partition):
        """Extract data partition to S3"""
        try:
            with pyodbc.connect(self.config['sybase_connection_string']) as conn:
                partition_column = self.config['tables'][table_name].get('partition_column', 'id')
                query = f"""
                SELECT * FROM {table_name}
                WHERE {partition_column} >= {partition['min']} 
                  AND {partition_column} < {partition['max']}
                """
                df = pd.read_sql(query, conn)
                
                # Convert to Parquet for efficient storage
                parquet_file = f"{table_name}_{partition['partition_id']}.parquet"
                df.to_parquet(parquet_file, index=False)
                
                # Upload to S3
                s3_key = f"migration/{self.migration_id}/{table_name}/{parquet_file}"
                self.s3_client.upload_file(
                    parquet_file, 
                    self.config['s3_bucket'], 
                    s3_key
                )
                
                return {
                    'partition_id': partition['partition_id'],
                    'records_count': len(df),
                    's3_key': s3_key
                }
        except Exception as e:
            self.logger.error(f"Extraction error for {table_name}: {e}")
            return None

    def load_to_snowflake(self, s3_keys):
        """Load data from S3 to Snowflake"""
        try:
            with snowflake.connector.connect(**self.config['snowflake_connection']) as conn:
                cursor = conn.cursor()
                for s3_key in s3_keys:
                    copy_command = f"""
                    COPY INTO {self.config['snowflake_target_table']}
                    FROM @{self.config['snowflake_stage']}/{s3_key}
                    FILE_FORMAT = (TYPE = PARQUET)
                    """
                    cursor.execute(copy_command)
                return True
        except Exception as e:
            self.logger.error(f"Snowflake load error: {e}")
            return False

    def migrate_table(self, table_name):
        """Complete migration workflow for a table"""
        partitions, table_stats = self.partition_table(table_name)
        
        # Parallel extraction to S3
        with multiprocessing.Pool(processes=8) as pool:
            s3_upload_results = pool.starmap(
                self.extract_partition, 
                [(table_name, partition) for partition in partitions]
            )
        
        # Filter out any failed partitions
        successful_uploads = [
            result for result in s3_upload_results 
            if result is not None
        ]
        
        # Load to Snowflake
        s3_keys = [upload['s3_key'] for upload in successful_uploads]
        snowflake_load_success = self.load_to_snowflake(s3_keys)
        
        # Track and log migration details
        migration_summary = {
            'table': table_name,
            'total_records': table_stats['total_records'],
            'total_size_mb': table_stats['total_size_mb'],
            'partitions': len(successful_uploads),
            's3_upload_success': len(successful_uploads),
            'snowflake_load_success': snowflake_load_success
        }
        
        with open(self.migration_status_file, 'w') as f:
            json.dump(migration_summary, f)
        
        self.logger.info(f"Migration completed for {table_name}")
        return migration_summary

def main():
    migrator = DataMigrationTracker()
    tables_to_migrate = migrator.config['tables'].keys()
    
    for table in tables_to_migrate:
        migrator.migrate_table(table)

if __name__ == '__main__':
    main()
