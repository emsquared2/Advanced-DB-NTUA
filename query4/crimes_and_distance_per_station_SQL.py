from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.functions import udf
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Weapon Crimes and Average Distance per Police Station - SQL API")

# Register UDF (get_distance)
get_distance_udf = udf(get_distance)
spark.udf.register("get_distance", get_distance_udf)

# Import data
crime_df = import_crime_data(spark)
police_stations_df = import_police_stations_data(spark)

# Register DataFrames as temporary SQL tables
crime_df.createOrReplaceTempView("crimes")
police_stations_df.createOrReplaceTempView("police_stations")

# Filter Crime Data - Keep crimes involving the use of any form of weapon
weapon_crimes_query = \
    "SELECT \
        DR_NO, \
        AREA, \
        `Weapon Used Cd`, \
        LAT, \
        LON \
    FROM crimes \
    WHERE `Weapon Used Cd` IS NOT NULL\
    ORDER BY DR_NO"

weapon_crimes = spark.sql(weapon_crimes_query)
weapon_crimes.createOrReplaceTempView("weapon_crimes")

# Join weapon crimes and police stastions using PREC/AREA 
weapon_crimes_police_stations_join_query = \
    "SELECT  ps.DIVISION, ps.PREC, ps.Y as PS_LAT, ps.X as PS_LON, wc.LAT, wc.LON, wc.DR_NO, wc.`Weapon Used Cd`, \
        get_distance(wc.LAT, wc.LON, PS_LAT, PS_LON) as distance \
    FROM police_stations ps \
    INNER JOIN weapon_crimes wc ON \
    ps.PREC = wc.AREA"

weapon_crimes_police_stations_data = spark.sql(weapon_crimes_police_stations_join_query)
weapon_crimes_police_stations_data.createOrReplaceTempView("weapon_crimes_police_stations")

# Find average distance and total crimes for each police station
average_distance_and_total_crimes_per_police_station_query = \
    "SELECT DIVISION, AVG(distance) as average_distance, COUNT(*) AS total_crimes \
    FROM weapon_crimes_police_stations \
    GROUP BY DIVISION \
    ORDER BY total_crimes DESC"

average_distance_and_total_crimes_per_police_station = spark.sql(average_distance_and_total_crimes_per_police_station_query)
average_distance_and_total_crimes_per_police_station.show()

# Save output to hdfs
average_distance_and_total_crimes_per_police_station.write.csv("./query4b1-SQL.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()