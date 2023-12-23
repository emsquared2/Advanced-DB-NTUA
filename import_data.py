from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.functions import to_date
import csv

def import_crime_data(spark):

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

def import_crime_data_rdd(spark):
    # Use csv parser so that each cell is parsed as an item inside of the RDD
    # We didnt use simple split as it caused problem with the cell contents that contained comma
    parse_csv = lambda line: next(csv.reader([line]))

    crime_rdd1 = spark.textFile("hdfs://okeanos-master:54310/user/user/advDB_LACrimes/crime-data/crime-data-from-2010-to-2019.csv").map(lambda x: parse_csv(x))
    # Filter the headers
    header1 = crime_rdd1.take(1)[0]
    crime_rdd1 = crime_rdd1.filter(lambda line: line != header1)

    crime_rdd2 = spark.textFile("hdfs://okeanos-master:54310/user/user/advDB_LACrimes/crime-data/crime-data-from-2020-to-present.csv").map(lambda x: parse_csv(x)) 
    # Filter the headers
    header2 = crime_rdd2.take(1)[0]
    crime_rdd2 = crime_rdd2.filter(lambda line: line != header2)

    # Make a single rdd for crime data from 2010 to present
    crime_rdd = crime_rdd1.union(crime_rdd2)

    return crime_rdd