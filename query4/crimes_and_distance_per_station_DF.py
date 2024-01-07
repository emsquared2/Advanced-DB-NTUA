from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.functions import udf, avg, count, concat, lit, round
from pyspark.sql.types import DoubleType
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Weapon Crimes and Average Distance per Year - DataFrame API")

# Import data
crime_df = import_crime_data(spark)
police_stations_df = import_police_stations_data(spark)

get_distance_udf = udf(get_distance, DoubleType())

# Filter Crime Data - Keep crimes involving the use of any form of weapon
weapon_crimes_df = crime_df.filter((crime_df["Weapon Used Cd"].isNotNull()) & \
                                     (crime_df["LAT"] != 0) & \
                                     (crime_df["LON"] != 0)) \
                           .orderBy("DR_NO") \
                           .select("DR_NO", "AREA", "Weapon Used Cd", "LAT", "LON")


# Join weapon crimes and police stations using PREC/AREA
weapon_crimes_police_stations_df = weapon_crimes_df \
                                    .join( \
                                        # Comment out to use a specific join strategy (broadcast, merge, shuffle_hash, shuffle_replicate_nl).
                                        #police_stations_df.hint("broadcast") # we choose to broadcast the smallest dataframe\  
                                        #police_stations_df.hint("merge") \
                                        #police_stations_df.hint("shuffle_hash") \
                                        # police_stations_df.hint("shuffle_replicate_nl") #  not recommended here \ 
                                        police_stations_df \
                                            .withColumnRenamed("Y", "PS_LAT") \
                                            .withColumnRenamed("X", "PS_LON"), \
                                        police_stations_df["PREC"] == weapon_crimes_df["AREA"], \
                                        "inner" \
                                    ) \
                                    .select("DIVISION", "PREC", "PS_LAT", "PS_LON", "LAT", "LON", "DR_NO", "Weapon Used Cd", \
                                        get_distance_udf("LAT", "LON", "PS_LAT", "PS_LON").alias("distance"))

# Find average distance and total crimes for each police station
average_distance_and_total_crimes_per_police_station = weapon_crimes_police_stations_df \
                                                        .groupBy("DIVISION") \
                                                        .agg( \
                                                            avg("distance").alias("average_distance"), \
                                                            count("*").alias("total_crimes") \
                                                        ) \
                                                        .withColumn( 
                                                                "average_distance",
                                                                concat(round("average_distance", 3).cast("string"), lit(" km"))
                                                        ) \
                                                        .orderBy("total_crimes", ascending=False) \
                                                        .selectExpr("DIVISION as division", "average_distance", "total_crimes")

average_distance_and_total_crimes_per_police_station.show()

# Save output to hdfs
average_distance_and_total_crimes_per_police_station.write.csv("./output/query4/query4b1-DF.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()
