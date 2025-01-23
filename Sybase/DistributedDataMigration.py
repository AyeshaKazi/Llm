from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
import snowflake.connector
import json
from datetime import datetime
import logging

class DistributedDataMigration:
    def __init__(self, config_path='migration_config.json'):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Generate migration ID
        self.migration_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("DataMigration") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        # S3 Client
        self.s3_client = boto3.client('s3')

    def _setup_logging(self):
        """Configure logging"""
        log_file = f'migration_{self.migration_id}.log'
        logging.basicConfig(
            filename=log_file, 
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        return logging.getLogger()

    def read_source_data(self, table_name, partition_column=None):
        """Read source data using JDBC"""
        jdbc_url = (
            f"jdbc:{self.config['source_db_type']}://"
            f"{self.config['source_host']}:"
            f"{self.config['source_port']}/"
            f"{self.config['source_database']}"
        )
        
        # Read options
        read_options = {
            "url": jdbc_url,
            "dbtable": table_name,
            "user": self.config['source_username'],
            "password": self.config['source_password']
        }
        
        # Read dataframe
        df = self.spark.read \
            .format("jdbc") \
            .options(**read_options) \
            .load()
        
        # Repartition if partition column specified
        if partition_column:
            df = df.repartition(self.config.get('num_partitions', 10), partition_column)
        
        return df

    def write_to_s3(self, df, table_name):
        """Write dataframe to S3 in Parquet format"""
        s3_path = f"s3a://{self.config['s3_bucket']}/migration/{self.migration_id}/{table_name}"
        
        df.write \
            .mode("overwrite") \
            .parquet(s3_path)
        
        return s3_path

    def load_to_snowflake(self, s3_path, target_table):
        """Load data from S3 to Snowflake"""
        snowflake_options = {
            "sfURL": self.config['snowflake_account'],
            "sfUser": self.config['snowflake_username'],
            "sfPassword": self.config['snowflake_password'],
            "sfDatabase": self.config['snowflake_database'],
            "sfSchema": self.config['snowflake_schema'],
            "sfWarehouse": self.config['snowflake_warehouse']
        }
        
        # Read Parquet from S3
        df = self.spark.read.parquet(s3_path)
        
        # Write to Snowflake
        df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", target_table) \
            .mode("overwrite") \
            .save()

    def migrate_table(self, table_name):
        """Migrate entire table"""
        try:
            # Read source data
            partition_column = self.config['tables'][table_name].get('partition_column')
            df = self.read_source_data(table_name, partition_column)
            
            # Write to S3
            s3_path = self.write_to_s3(df, table_name)
            
            # Load to Snowflake
            self.load_to_snowflake(s3_path, table_name)
            
            # Log migration details
            migration_summary = {
                'table': table_name,
                'total_records': df.count(),
                's3_path': s3_path
            }
            
            self.logger.info(f"Migration completed for {table_name}")
            return migration_summary
        
        except Exception as e:
            self.logger.error(f"Migration failed for {table_name}: {str(e)}")
            raise

    def migrate_all_tables(self):
        """Migrate all configured tables"""
        migration_results = {}
        for table in self.config['tables']:
            result = self.migrate_table(table)
            migration_results[table] = result
        return migration_results

def main():
    migrator = DistributedDataMigration()
    migrator.migrate_all_tables()

if __name__ == '__main__':
    main()
