from pyspark.sql.functions import count, desc, col, udf
from pyspark.sql.types import StringType
from import_data import import_crime_data
from SparkSession import create_spark_session


spark = create_spark_session("Crime Rates Across Daytime Segments - DataFrame API")

crime_df = import_crime_data(spark)

# Function that identifies the day segment of a certain time
def daytime_segment(time):
    hours = int(time[0:2])
    if 5 <= hours < 12:
        return 'MORNING'
    elif 12 <= hours < 16:
        return 'AFTERNOON'
    elif 16 <= hours < 21:
        return 'EVENING'
    else:
        return 'NIGHT'
    
daytime_segment_udf = udf(daytime_segment, StringType())

# Select only STREET Crimes
street_crime_df = crime_df.filter(crime_df['Premis Desc'] == 'STREET')

# Add DAY SEGMENT column
street_crime_df = street_crime_df.withColumn("DAY SEGMENT", daytime_segment_udf(col("TIME OCC")))

# Group by day segment - find number of crimes per day segment and order them in descending order
grouped_street_crime_df = street_crime_df.groupBy("DAY SEGMENT").agg(count("*").alias("crime_total")).orderBy(desc("crime_total"))

grouped_street_crime_df.show()

# Saves output to hdfs
grouped_street_crime_df.write.csv("./query2-DF_output.csv", header=True, mode="overwrite")

spark.stop()