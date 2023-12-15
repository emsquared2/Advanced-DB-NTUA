from pyspark.sql import SparkSession

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()
