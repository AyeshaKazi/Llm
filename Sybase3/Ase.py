from pyspark.sql import SparkSession

# Initialize Spark session with Kyuubi connection
spark = SparkSession.builder \
    .appName("SybaseDataTransfer") \
    .config("spark.jars", "~/jconn4.jar") \
    .getOrCreate()

# Define Sybase connection properties
jdbc_url = "jdbc:sybase:Tds://sybase-server:5000/database"
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.sybase.jdbc4.jdbc.SybDriver"
}

# Example query to analyze table heuristics (row count)
df = spark.read.jdbc(url=jdbc_url, table="(SELECT COUNT(*) AS row_count FROM misdbo.Totoro_bdd) AS tmp", properties=connection_properties)
df.show()

# Collect table row count
row_count = df.collect()[0]["row_count"]
print(f"Total rows in table: {row_count}")
