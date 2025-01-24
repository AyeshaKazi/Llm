import pyspark
from pyspark.sql import SparkSession
import logging
import traceback
import os
from typing import Dict, List, Any

# Custom log formatter to include filename and line number
class DetailedLogFormatter(logging.Formatter):
    def format(self, record):
        # Get the filename and line number
        filename = os.path.basename(record.pathname)
        
        # Format includes: timestamp, filename, line number, log level, message
        return (
            f"{record.asctime} | {filename}:{record.lineno} | "
            f"{record.levelname} | {record.getMessage()}"
        )

class SybaseTableAnalyzer:
    def __init__(self, 
                 jdbc_url: str, 
                 username: str, 
                 password: str, 
                 driver: str = "com.sybase.jdbc4.jdbc.SybDriver"):
        """
        Initialize Sybase connection and Spark session with detailed logging
        """
        # Configure logging with detailed formatter
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create console handler with detailed formatter
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create file handler for persistent logging
        log_filename = 'sybase_table_analyzer.log'
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO)
        
        # Use custom formatter
        detailed_formatter = DetailedLogFormatter(
            fmt='%(asctime)s | %(filename)s:%(lineno)d | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(detailed_formatter)
        file_handler.setFormatter(detailed_formatter)
        
        # Add handlers to logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

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
            # Log full traceback with filename and line number
            self.logger.error(f"Failed to initialize Spark session: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def get_database_tables(self, database_name: str) -> List[str]:
        """
        Retrieve all tables in the specified database with detailed error logging
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
            # Log error with full traceback, including filename and line number
            self.logger.error(f"Error retrieving tables: {e}")
            self.logger.error(traceback.format_exc())
            return []

    # ... (rest of the methods remain the same)
