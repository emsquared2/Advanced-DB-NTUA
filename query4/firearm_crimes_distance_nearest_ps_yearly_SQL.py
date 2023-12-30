from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.functions import udf
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Firearm Crimes and Average Distance (from Nearest Police Station) per Year - SQL API")

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
firearm_crimes_query = \
    "SELECT \
        DR_NO, \
        AREA, \
        EXTRACT(YEAR FROM `DATE OCC`) AS year, \
        `Weapon Used Cd`, \
        LAT, \
        LON \
    FROM crimes \
    WHERE `Weapon Used Cd` LIKE '1__' AND \
          (LAT <> 0 AND LON <> 0) \
    ORDER BY DR_NO"

firearm_crimes_data = spark.sql(firearm_crimes_query)
firearm_crimes_data.createOrReplaceTempView("firearm_crimes")

# Find nearest police station for each firearm crime
firearm_crimes_police_stations_join_query = \
    "WITH Crimes_PS_with_RankedDistances AS ( \
        SELECT fc.DR_NO, \
            fc.AREA, \
            fc.year, \
            fc.`Weapon Used Cd`, \
            fc.LAT, fc.LON, \
            ps.Y as PS_LAT, ps.X as PS_LON, \
            get_distance(fc.LAT, fc.LON, PS_LAT, PS_LON) as distance, \
            RANK() OVER (PARTITION BY fc.DR_NO ORDER BY get_distance(fc.LAT, fc.LON, ps.Y, ps.X)) AS DistanceRank \
        FROM police_stations ps \
        CROSS JOIN firearm_crimes fc \
    ) \
    SELECT DR_NO, AREA, year, `Weapon Used Cd`, LAT, LON, PS_LAT, PS_LON, distance \
    FROM Crimes_PS_with_RankedDistances \
    WHERE DistanceRank = 1"

firearm_crimes_police_stations_data = spark.sql(firearm_crimes_police_stations_join_query)
firearm_crimes_police_stations_data.createOrReplaceTempView("firearm_crimes_police_stations")

# Find average distance and total crimes for each year
average_distance_and_total_crimes_per_year_query = \
    "SELECT year, AVG(distance) as average_distance, COUNT(*) AS total_crimes \
    FROM firearm_crimes_police_stations \
    GROUP BY year \
    ORDER BY year"

average_distance_and_total_crimes_per_year = spark.sql(average_distance_and_total_crimes_per_year_query)
average_distance_and_total_crimes_per_year.show()

# Save output to hdfs
average_distance_and_total_crimes_per_year.write.csv("./query4a2-SQL.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()