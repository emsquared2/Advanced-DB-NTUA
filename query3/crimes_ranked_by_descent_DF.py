from import_data import import_crime_data, import_income_data, import_revgeocoding_data
from SparkSession import create_spark_session
from pyspark.sql.functions import year, col, count, desc

# Create Spark session
spark = create_spark_session("Crimes Ranked by Victim Descent - DataFrame API")

# Import data
crime_df = import_crime_data(spark)
income_df = import_income_data(spark)
revgeocoding_df = import_revgeocoding_data(spark)


# Filter Crime Data for the year 2015 with known Victim Age and Descent
crime_2015_filtered_df = crime_df.filter( \
                                   (year(crime_df["DATE OCC"]) == 2015) & \
                                   (crime_df["Vict Age"].isNotNull() & (crime_df["Vict Age"] != 0)) & \
                                   (crime_df["Vict Descent"].isNotNull() & (crime_df["Vict Descent"] != 'X')) \
                                 ) \
                                 .select("Vict Descent", "LAT", "LON") \
                                 .orderBy("DR_NO")

# Join income data and revgeocoding data using zip code
income_revgeocoding_df = revgeocoding_df.join( \
                                          # Comment out to use a specific join strategy (broadcast, merge, shuffle_hash, shuffle_replicate_nl).
                                          #income_df.hint("broadcast"), # we choose to broadcast the smallest dataframe \   
                                          #income_df.hint("merge"), \
                                          #income_df.hint("shuffle_hash"), \
                                          # income_df.hint("shuffle_replicate_nl"), # not recommended here \  
                                          income_df, \
                                          revgeocoding_df["ZIPcode"] == income_df["Zip Code"], \
                                          "inner" \
                                        ) \
                                        .select("LAT", "LON", "ZIPcode", "Estimated Median Income")

income_revgeocoding_df.explain()

# Join filtered crimes and income_revgeocoding using LAT & LON
crimes_income_ZIPcode_df = crime_2015_filtered_df.join( \
                                                        # Comment out to use a specific join strategy (broadcast, merge, shuffle_hash, shuffle_replicate_nl).
                                                        # income_revgeocoding_df.hint("broadcast"), # we choose to broadcast the smallest dataframe \  
                                                        #income_revgeocoding_df.hint("merge"), \
                                                        #income_revgeocoding_df.hint("shuffle_hash"), \
                                                        # income_revgeocoding_df.hint("shuffle_replicate_nl"), # not recommended here \  
                                                        income_revgeocoding_df, \
                                                        (crime_2015_filtered_df["LAT"] == income_revgeocoding_df["LAT"]) & \
                                                        (crime_2015_filtered_df["LON"] == income_revgeocoding_df["LON"]), \
                                                        "inner" \
                                                 ) \
                                                 .orderBy(desc("Estimated Median Income")) \
                                                 .select("Vict Descent", income_revgeocoding_df["LAT"], income_revgeocoding_df["LON"], "ZIPcode", "Estimated Median Income")

crimes_income_ZIPcode_df.explain()

# Find top 3 ZIP Codes by income
top_incomes_df = crimes_income_ZIPcode_df.select("ZIPcode", "Estimated Median Income") \
                                         .distinct() \
                                         .orderBy(desc("Estimated Median Income")) \
                                         .limit(3) \
                                         .select("ZIPcode")

# Find last 3 ZIP Codes by income
last_incomes_df = crimes_income_ZIPcode_df.select("ZIPcode", "Estimated Median Income") \
                                          .distinct() \
                                          .orderBy("Estimated Median Income") \
                                          .limit(3) \
                                          .select("ZIPcode")

filtered_zips_df = top_incomes_df.union(last_incomes_df)

# Find total number of victims grouped by Victim Descent
crimes_ranked_by_descent_df = crimes_income_ZIPcode_df.join( \
                                                            # Comment out to use a specific join strategy (broadcast, merge, shuffle_hash, shuffle_replicate_nl).
                                                            # filtered_zips_df.hint("broadcast"), # we choose to broadcast the smallest dataframe \ 
                                                            # filtered_zips_df.hint("merge"), \
                                                            #filtered_zips_df.hint("shuffle_hash"), \
                                                            # filtered_zips_df.hint("shuffle_replicate_nl"), # not recommended here \  
                                                            filtered_zips_df, \
                                                             crimes_income_ZIPcode_df["ZIPcode"] == filtered_zips_df["ZIPcode"], \
                                                             "inner" \
                                                        ) \
                                                        .groupBy("Vict Descent") \
                                                        .agg(count("*").alias("total_crimes")) \
                                                        .orderBy(desc("total_crimes"))

crimes_ranked_by_descent_df.show()

crimes_ranked_by_descent_df.explain()

# Save output to HDFS
crimes_ranked_by_descent_df.write.csv("./output/query3/query3-DF.csv", header=True, mode="overwrite")

# Stop Spark session
spark.stop()