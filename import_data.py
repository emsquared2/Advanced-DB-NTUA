from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType, TimestampType
from pyspark.sql.functions import col, to_date

def import_crime_data():
    spark = SparkSession \
        .builder \
        .appName("Import crime data 2010 - present") \
        .getOrCreate()

    crime_schema = StructType([
        StructField("DR_NO", StringType()),
        StructField("Date Rptd", StringType()),
        StructField("DATE OCC", StringType()),
        StructField("TIME OCC", StringType()),
        StructField("AREA", StringType()),
        StructField("AREA ΝΑΜΕ", StringType()),
        StructField("Rpt Dist No", StringType()),
        StructField("Part 1-2", StringType()),
        StructField("Crm Cd", StringType()),
        StructField("Crm Cd Desc", StringType()),
        StructField("Mocodes", StringType()),
        StructField("Vict Age", IntegerType()),
        StructField("Vict Sex", StringType()),
        StructField("Vict Descent", StringType()),
        StructField("Premis Cd", StringType()),
        StructField("Premis Desc", StringType()),
        StructField("Weapon Used Cd", StringType()),
        StructField("Weapon Desc", StringType()),
        StructField("Status", StringType()),
        StructField("Status Desc", StringType()),
        StructField("Crm Cd 1", StringType()),
        StructField("Crm Cd 2", StringType()),
        StructField("Crm Cd 3", StringType()),
        StructField("Crm Cd 4", StringType()),
        StructField("LOCATION", StringType()),
        StructField("Cross Street", StringType()),
        StructField("LAT", DoubleType()),
        StructField("LON", DoubleType()),
    ])

    crime_df1 = spark.read.csv("hdfs://okeanos-master:54310/user/user/advDB_LACrimes/crime-data/crime-data-from-2010-to-2019.csv", header=True, schema=crime_schema) 
    crime_df2 = spark.read.csv("hdfs://okeanos-master:54310/user/user/advDB_LACrimes/crime-data/crime-data-from-2020-to-present.csv", header=True, schema=crime_schema)

    # make a single dataframe for crime data from 2010 to present
    crime_df = crime_df1.union(crime_df2)

    # transformation from string type (csv column) to DateType
    crime_df = crime_df.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
    crime_df = crime_df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))

    return crime_df
