from pyspark.sql import SparkSession

# Generate SparkSession
def get_spark_session(_appName="Default AppName") -> SparkSession:
    
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName(_appName) \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark