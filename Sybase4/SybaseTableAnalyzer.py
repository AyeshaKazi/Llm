import pyspark
from pyspark.sql import SparkSession
import logging
import traceback
from typing import Dict, List, Any

class SybaseTableAnalyzer:
    def __init__(self, 
                 jdbc_url: str, 
                 username: str, 
                 password: str, 
                 driver: str = "com.sybase.jdbc4.jdbc.SybDriver"):
        """
        Initialize Sybase connection and Spark session
        
        :param jdbc_url: JDBC connection URL for Sybase
        :param username: Database username
        :param password: Database password
        :param driver: JDBC driver class name
        """
        # Configure logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Create Spark session with Sybase JDBC driver
        try:
            self.spark = (
                SparkSession.builder
                .appName("SybaseTableAnalyzer")
                .config("spark.jars", "/path/to/sybase/jdbc/driver.jar")
                .getOrCreate()
            )
            
            # Sybase connection properties
            self.conn_properties = {
                "url": jdbc_url,
                "user": username,
                "password": password,
                "driver": driver
            }
            
            self.logger.info("Spark session and Sybase connection initialized successfully")
        
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def get_database_tables(self, database_name: str) -> List[str]:
        """
        Retrieve all tables in the specified database
        
        :param database_name: Name of the database
        :return: List of table names
        """
        try:
            # Query to get all tables in the database
            query = f"""
            SELECT name 
            FROM {database_name}..sysobjects 
            WHERE type = 'U'
            """
            
            # Execute query
            tables_df = self.spark.read.jdbc(
                url=self.conn_properties['url'],
                table=f"({query}) as tables",
                properties=self.conn_properties
            )
            
            # Collect table names
            tables = [row.name for row in tables_df.collect()]
            
            self.logger.info(f"Retrieved {len(tables)} tables from database {database_name}")
            return tables
        
        except Exception as e:
            self.logger.error(f"Error retrieving tables: {e}")
            self.logger.error(traceback.format_exc())
            return []

    def analyze_table(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of a specific table
        
        :param database_name: Name of the database
        :param table_name: Name of the table
        :return: Dictionary of table metrics
        """
        try:
            # Read the table
            table_df = self.spark.read.jdbc(
                url=self.conn_properties['url'],
                table=f"{database_name}..{table_name}",
                properties=self.conn_properties
            )
            
            # Collect metrics
            metrics = {
                "table_name": table_name,
                "row_count": table_df.count(),
                "columns": table_df.columns,
                "column_types": dict(table_df.dtypes),
                "null_counts": {
                    col: table_df.filter(table_df[col].isNull()).count() 
                    for col in table_df.columns
                },
                "estimated_size_mb": self._estimate_table_size(table_df),
                "potential_partition_keys": self._suggest_partition_keys(table_df)
            }
            
            self.logger.info(f"Analyzed table {table_name} successfully")
            return metrics
        
        except Exception as e:
            self.logger.error(f"Error analyzing table {table_name}: {e}")
            self.logger.error(traceback.format_exc())
            return {}

    def _estimate_table_size(self, df):
        """
        Estimate table size in MB
        
        :param df: Spark DataFrame
        :return: Estimated size in MB
        """
        try:
            # Rough estimation based on DataFrame
            size_in_bytes = df.rdd.map(lambda row: len(str(row))).sum()
            size_in_mb = size_in_bytes / (1024 * 1024)
            return round(size_in_mb, 2)
        except Exception as e:
            self.logger.warning(f"Size estimation failed: {e}")
            return 0

    def _suggest_partition_keys(self, df):
        """
        Suggest potential partition keys
        
        :param df: Spark DataFrame
        :return: List of potential partition keys
        """
        try:
            # Simple heuristics for partition key suggestion
            potential_keys = []
            
            # Look for columns with high cardinality but not too high
            for col, dtype in df.dtypes:
                if dtype in ['int', 'long', 'timestamp', 'date']:
                    distinct_count = df.select(col).distinct().count()
                    total_count = df.count()
                    
                    # Heuristic: good partition key if distinct count is between 10-1000
                    if 10 <= distinct_count <= 1000:
                        potential_keys.append({
                            "column": col,
                            "distinct_count": distinct_count,
                            "distinct_percentage": round(distinct_count/total_count * 100, 2)
                        })
            
            return potential_keys
        
        except Exception as e:
            self.logger.warning(f"Partition key suggestion failed: {e}")
            return []

    def estimate_transfer_time(self, table_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Estimate S3 transfer time based on table metrics
        
        :param table_metrics: Table metrics dictionary
        :return: Transfer time estimation
        """
        try:
            # Assumptions for transfer
            network_bandwidth_mbps = 100  # 100 Mbps connection
            s3_write_overhead_factor = 1.2  # 20% overhead
            
            # Calculate transfer time
            size_mb = table_metrics.get('estimated_size_mb', 0)
            transfer_time_seconds = (size_mb * 8) / network_bandwidth_mbps * s3_write_overhead_factor
            
            return {
                "estimated_size_mb": size_mb,
                "network_bandwidth_mbps": network_bandwidth_mbps,
                "estimated_transfer_time_seconds": round(transfer_time_seconds, 2),
                "estimated_transfer_time_minutes": round(transfer_time_seconds / 60, 2)
            }
        
        except Exception as e:
            self.logger.warning(f"Transfer time estimation failed: {e}")
            return {}

    def close(self):
        """
        Close Spark session
        """
        try:
            self.spark.stop()
            self.logger.info("Spark session closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing Spark session: {e}")
