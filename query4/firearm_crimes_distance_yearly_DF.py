from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.functions import udf, year, avg, count, concat, lit, round
from pyspark.sql.types import DoubleType
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Firearm Crimes and Average Distance per Year - DataFrame API")

# Import data
crime_df = import_crime_data(spark)
police_stations_df = import_police_stations_data(spark)

get_distance_udf = udf(get_distance, DoubleType())

# Filter Crime Data - Keep crimes involving the use of any form of firearms
firearms_crimes_df = crime_df.filter((crime_df["Weapon Used Cd"].like("1__")) & \
                                     (crime_df["LAT"] != 0) & \
                                     (crime_df["LON"] != 0)) \
                             .orderBy("DR_NO") \
                             .select("DR_NO", "AREA", year(crime_df["DATE OCC"]).alias("year"), "Weapon Used Cd", "LAT", "LON")

# Join firearm crimes and police stations using PREC/AREA
firearms_crimes_police_stations_df = firearms_crimes_df \
                                        .join( \
                                            # Comment out to use a specific join strategy (broadcast, merge, shuffle_hash, shuffle_replicate_nl).
                                            #police_stations_df.hint("broadcast") # we choose to broadcast the smallest dataframe \  
                                            #police_stations_df.hint("merge") \
                                            #police_stations_df.hint("shuffle_hash") \
                                            # police_stations_df.hint("shuffle_replicate_nl") # not recommended here\  
                                            police_stations_df \
                                                .withColumnRenamed("Y", "PS_LAT") \
                                                .withColumnRenamed("X", "PS_LON"), \
                                            firearms_crimes_df["AREA"] == police_stations_df["PREC"], \
                                            "inner" \
                                        ) \
                                        .orderBy("DR_NO") \
                                        .select("DR_NO", "AREA", "year", "Weapon Used Cd","LAT", "LON", "PS_LAT", "PS_LON", \
                                            get_distance_udf("LAT", "LON", "PS_LAT", "PS_LON").alias("distance"))

# Find average distance and total crimes for each year
average_distance_and_total_crimes_per_year = firearms_crimes_police_stations_df \
                                                .groupBy("year") \
                                                .agg(
                                                    avg("distance").alias("average_distance"), \
                                                    count("*").alias("total_crimes") \
                                                ) \
                                                .withColumn( 
                                                    "average_distance",
                                                    concat(round("average_distance", 3).cast("string"), lit(" km"))
                                                ) \
                                                .orderBy("year") \
                                                .select("year", "average_distance", "total_crimes")

average_distance_and_total_crimes_per_year.show()

# Save output to hdfs
average_distance_and_total_crimes_per_year.write.csv("./output/query4/query4a1-DataFrame.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()