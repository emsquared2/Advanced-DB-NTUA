from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from import_data import import_crime_data
from SparkSession import create_spark_session

# Create Spark session
spark = create_spark_session("Crime Rates Across Daytime Segments - SQL API")

# Import crime data
crime_df = import_crime_data(spark)

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
spark.udf.register("daytime_segment_udf", daytime_segment, StringType())

# To utilize as SQL tables
crime_df.createOrReplaceTempView("crimes")

# Prepare the SQL query
query = "SELECT `DAYTIME SEGMENT`, COUNT(*) AS crime_total \
        FROM ( \
            SELECT daytime_segment_udf(`TIME OCC`) AS `DAYTIME SEGMENT` \
            FROM crimes \
            WHERE `Premis Desc` = 'STREET' \
        ) \
        GROUP BY `DAYTIME SEGMENT` \
        ORDER BY crime_total DESC"

# Perform query
street_crimes_by_daytime = spark.sql(query)

street_crimes_by_daytime.show()

# Saves output to hdfs
street_crimes_by_daytime.write.csv("./query2-SQL.csv", header=True, mode="overwrite")

spark.stop()