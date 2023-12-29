from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.functions import udf
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Firearm Crimes and Average Distance per Year - SQL API")

# Register UDF (get_distance)
get_distance_udf = udf(get_distance)
spark.udf.register("get_distance", get_distance_udf)

# Import data
crime_df = import_crime_data(spark)
police_stations_df = import_police_stations_data(spark)

# Register DataFrames as temporary SQL tables
crime_df.createOrReplaceTempView("crimes")
police_stations_df.createOrReplaceTempView("police_stations")

# Filter Crime Data - Keep crimes involving the use of any form of firearms
firearms_crimes_query = \
    "SELECT \
        DR_NO, \
        AREA, \
        EXTRACT(YEAR FROM `DATE OCC`) AS year, \
        `Weapon Used Cd`, \
        LAT, \
        LON \
    FROM crimes \
    WHERE `Weapon Used Cd` LIKE '1__'\
    ORDER BY DR_NO"

firearms_crimes_data = spark.sql(firearms_crimes_query)
firearms_crimes_data.createOrReplaceTempView("firearm_crimes")

# Join firearm crimes and police stations using PREC/AREA 
firearms_crimes_police_stations_join_query = \
    "SELECT fc.DR_NO, fc.AREA, fc.year, fc.`Weapon Used Cd`, fc.LAT, fc.LON, ps.Y as PS_LAT, ps.X as PS_LON, \
        get_distance(fc.LAT, fc.LON, PS_LAT, PS_LON) as distance \
    FROM police_stations ps \
    INNER JOIN firearm_crimes fc ON \
    ps.PREC = fc.AREA \
    ORDER BY fc.DR_NO"

firearms_crimes_police_stations_data = spark.sql(firearms_crimes_police_stations_join_query)
firearms_crimes_police_stations_data.createOrReplaceTempView("firearm_crimes_police_stations")

# Find average distance and total crimes for each year
average_distance_and_total_crimes_per_year_query = \
    "SELECT year, AVG(distance) as average_distance, COUNT(*) AS total_crimes \
    FROM firearm_crimes_police_stations \
    GROUP BY year \
    ORDER BY year"

average_distance_and_total_crimes_per_year = spark.sql(average_distance_and_total_crimes_per_year_query)
average_distance_and_total_crimes_per_year.show()

# Save output to hdfs
average_distance_and_total_crimes_per_year.write.csv("./query4a1-SQL.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()