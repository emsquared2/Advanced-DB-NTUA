from import_data import import_crime_data, import_police_stations_data
from SparkSession import create_spark_session
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, avg, count, col, rank, desc, concat, lit, round
from calculate_distance import get_distance

# Create Spark session
spark = create_spark_session("Total Firearm Crimes and Average Distance (from Nearest Police Station) per Year - Dataframe API")

# Import data
crime_df = import_crime_data(spark)
police_stations_df = import_police_stations_data(spark)

# Register UDF (get_distance)
get_distance_udf = udf(get_distance, DoubleType())

window = Window.partitionBy("DR_NO") \
               .orderBy("distance")

filtered_crimes_df = crime_df.filter((crime_df["Weapon Used Cd"].isNotNull()) & \
                                     (crime_df["LAT"] != 0) & \
                                     (crime_df["LON"] != 0)) \
                             .select("DR_NO", "LAT", "LON")

crimes_police_dist_df = filtered_crimes_df.crossJoin(police_stations_df.select("DIVISION", "Y", "X")) \
                                           .withColumn('distance', get_distance_udf("LAT", "LON", "Y", "X")) \
                                           .withColumn("distance rank", rank().over(window)) \
                                           .filter(col("distance rank") == 1) \
                                           .select("DIVISION", "distance")

# Find average distance and total crimes for each police station
avg_dist_nearest_ps_df = crimes_police_dist_df.groupBy("DIVISION") \
                                              .agg( 
                                                  avg("distance").alias("average distance"), \
                                                  count("*").alias("total crimes") \
                                              ) \
                                              .withColumn( 
                                                "average_distance",
                                                concat(round("average_distance", 3).cast("string"), lit(" km"))
                                              ) \
                                              .orderBy("total_crimes", ascending=False) \
                                              .selectExpr("DIVISION as division", "average_distance", "total_crimes")

avg_dist_nearest_ps_df.show()

# Save output to hdfs
avg_dist_nearest_ps_df.write.csv("./query4b2-DataFrame.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()