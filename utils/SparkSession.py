from pyspark.sql import SparkSession

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_spark_session_rdd(app_name):
    return SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate() \
        .sparkContext
