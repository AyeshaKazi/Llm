from pyspark.sql import SparkSession
from typing import List, Dict, Any
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

class SparkTableAnalyzer:
    def __init__(self, logger, num_workers=2):
        """
        Initialize Spark Session.
        
        Args:
            logger (logging.Logger): Logger instance
            num_workers (int): Number of Spark workers
        """
        self.logger = logger
        self.spark = SparkSession.builder \
            .appName("SybaseDatabaseAnalyzer") \
            .master(f"local[{num_workers}]") \
            .getOrCreate()
        
    def analyze_tables(self, connector, selected_tables: List[str]) -> List[Dict[str, Any]]:
        """
        Analyze selected tables using PySpark.
        
        Args:
            connector (SybaseDatabaseConnector): Database connector
            selected_tables (List[str]): Tables to analyze
        
        Returns:
            List[Dict[str, Any]]: Analysis results for each table
        """
        results = []
        
        for table in selected_tables:
            try:
                # Read table via JDBC
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", f"jdbc:sybase:Tds:{connector.connection_params['hostname']}:{connector.connection_params['port']}/{connector.connection_params['database']}") \
                    .option("dbtable", table) \
                    .option("user", connector.connection_params['username']) \
                    .option("password", connector.connection_params['password']) \
                    .option("driver", "com.sybase.jdbc4.jdbc.SybDriver") \
                    .load()
                
                # Collect metadata
                metadata = connector.get_table_metadata(table)
                
                # Column distribution visualization
                column_types = self._get_column_distribution(df)
                
                results.append({
                    **metadata,
                    'column_types': column_types
                })
                
                self.logger.info(f"Analyzed table: {table}")
            except Exception as e:
                self.logger.error(f"Error analyzing table {table}: {str(e)}")
        
        return results
    
    def _get_column_distribution(self, df):
        """
        Get column type distribution.
        
        Args:
            df (pyspark.sql.DataFrame): Spark DataFrame
        
        Returns:
            Dict: Column type distribution
        """
        return {col: str(dtype) for col, dtype in df.dtypes}
