from pyspark.sql import SparkSession
from typing import List, Dict, Any

class SybaseDatabaseConnector:
    def __init__(self, logger, hostname, port, database, username, password):
        """
        Initialize Sybase database connection parameters.
        
        Args:
            logger (logging.Logger): Logger instance
            hostname (str): Database server hostname
            port (int): Database server port
            database (str): Database name
            username (str): Database username
            password (str): Database password
        """
        self.logger = logger
        self.connection_params = {
            'hostname': hostname,
            'port': port,
            'database': database,
            'username': username,
            'password': password
        }
        self.spark = None
        
    def connect(self):
        """
        Establish Spark session with Sybase JDBC connection.
        """
        try:
            jdbc_url = f"jdbc:sybase:Tds:{self.connection_params['hostname']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            self.spark = SparkSession.builder \
                .appName("SybaseDatabaseAnalyzer") \
                .config("spark.driver.extraClassPath", "/path/to/jconn4.jar") \
                .getOrCreate()
            
            # Test connection by reading a system table
            self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "sysobjects") \
                .option("user", self.connection_params['username']) \
                .option("password", self.connection_params['password']) \
                .option("driver", "com.sybase.jdbc4.jdbc.SybDriver") \
                .load()
            
            self.logger.info(f"Connected to database: {self.connection_params['database']}")
        except Exception as e:
            self.logger.error(f"Database connection error: {str(e)}")
            raise
        
    def get_tables(self) -> List[str]:
        """
        Retrieve list of tables in the database.
        
        Returns:
            List[str]: List of table names
        """
        try:
            jdbc_url = f"jdbc:sybase:Tds:{self.connection_params['hostname']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            tables_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "(SELECT name FROM sysobjects WHERE type = 'U') as tables") \
                .option("user", self.connection_params['username']) \
                .option("password", self.connection_params['password']) \
                .option("driver", "com.sybase.jdbc4.jdbc.SybDriver") \
                .load()
            
            tables = [row.name for row in tables_df.collect()]
            self.logger.info(f"Retrieved {len(tables)} tables")
            return tables
        except Exception as e:
            self.logger.error(f"Error retrieving tables: {str(e)}")
            raise
        
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Retrieve metadata for a specific table.
        
        Args:
            table_name (str): Name of the table
        
        Returns:
            Dict[str, Any]: Table metadata
        """
        try:
            jdbc_url = f"jdbc:sybase:Tds:{self.connection_params['hostname']}:{self.connection_params['port']}/{self.connection_params['database']}"
            
            # Row count
            row_count_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(SELECT COUNT(*) as row_count FROM {table_name}) as row_count") \
                .option("user", self.connection_params['username']) \
                .option("password", self.connection_params['password']) \
                .option("driver", "com.sybase.jdbc4.jdbc.SybDriver") \
                .load()
            
            row_count = row_count_df.collect()[0].row_count
            
            # Primary key query
            pk_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"""
                    (SELECT column_name 
                    FROM syscolumns sc 
                    JOIN sysobjects so ON sc.id = so.id 
                    WHERE so.name = '{table_name}' 
                    AND sc.colid IN (
                        SELECT colid 
                        FROM syskeys sk 
                        WHERE sk.id = so.id AND sk.keyno = 1
                    )) as primary_keys
                """) \
                .option("user", self.connection_params['username']) \
                .option("password", self.connection_params['password']) \
                .option("driver", "com.sybase.jdbc4.jdbc.SybDriver") \
                .load()
            
            primary_keys = [row.column_name for row in pk_df.collect()]
            
            # Table size query (approximation)
            size_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(EXEC sp_spaceused '{table_name}') as size_info") \
                .option("user", self.connection_params['username']) \
                .option("password", self.connection_params['password']) \
                .option("driver", "com.sybase.jdbc4.jdbc.SybDriver") \
                .load()
            
            table_size = size_df.collect()[0]['data']
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'table_size': table_size,
                'primary_keys': primary_keys
            }
        except Exception as e:
            self.logger.error(f"Error retrieving metadata for {table_name}: {str(e)}")
            raise
