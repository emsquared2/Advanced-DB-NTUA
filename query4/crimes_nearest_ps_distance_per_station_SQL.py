from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Weapon Crimes and Average Distance Nearest to each Police Station - SQL API")

# Register UDF (get_distance)
get_distance_udf = udf(get_distance, DoubleType())
spark.udf.register("get_distance", get_distance, DoubleType())

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
    WHERE `Weapon Used Cd` IS NOT NULL AND \
          (LAT <> 0 AND LON <> 0) \
    ORDER BY DR_NO"

weapon_crimes = spark.sql(weapon_crimes_query)
weapon_crimes.createOrReplaceTempView("weapon_crimes")


# For each police station, identify the crimes that occurred in closest proximity to that specific police station

# Find nearest police station for each weapon crime
weapon_crimes_police_stations_join_query = \
    "WITH Crimes_PS_with_RankedDistances AS ( \
        SELECT wc.DR_NO, \
            wc.AREA, \
            wc.`Weapon Used Cd`, \
            wc.LAT, wc.LON, \
            ps.Y as PS_LAT, ps.X as PS_LON, \
            ps.DIVISION, \
            get_distance(wc.LAT, wc.LON, PS_LAT, PS_LON) as distance, \
            RANK() OVER (PARTITION BY wc.DR_NO ORDER BY get_distance(wc.LAT, wc.LON, ps.Y, ps.X)) AS DistanceRank \
        FROM police_stations ps \
        CROSS JOIN weapon_crimes wc \
    ) \
    SELECT DR_NO, AREA, `Weapon Used Cd`, LAT, LON, PS_LAT, PS_LON, DIVISION, distance \
    FROM Crimes_PS_with_RankedDistances \
    WHERE DistanceRank = 1"

weapon_crimes_police_stations_data = spark.sql(weapon_crimes_police_stations_join_query)
weapon_crimes_police_stations_data.createOrReplaceTempView("weapon_crimes_police_stations")

# Find average distance and total crimes for police station
average_distance_to_nearest_police_station_and_total_crimes_per_police_station_query = \
    "SELECT DIVISION, ROUND(AVG(distance), 3) || ' km' as average_distance, COUNT(*) AS total_crimes \
    FROM weapon_crimes_police_stations \
    GROUP BY DIVISION \
    ORDER BY total_crimes DESC"

average_distance_to_nearest_police_station_and_total_crimes_per_police_station = spark.sql(average_distance_to_nearest_police_station_and_total_crimes_per_police_station_query)
average_distance_to_nearest_police_station_and_total_crimes_per_police_station.show(10)

# Save output to hdfs
average_distance_to_nearest_police_station_and_total_crimes_per_police_station.write.csv("./query4b2-SQL.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()