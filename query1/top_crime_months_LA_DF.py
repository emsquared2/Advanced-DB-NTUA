from pyspark.sql.functions import year, month, count, desc, row_number, col
from pyspark.sql.window import Window
from import_data import import_crime_data
from SparkSession import create_spark_session

spark = create_spark_session("Top 3 months with the highest number of recorded crimes, in LA, per Year - DataFrame API")

crime_df = import_crime_data(spark)

# Create new year & month column
crime_df = crime_df.withColumn('year', year(crime_df["DATE OCC"])) \
            .withColumn('month', month(crime_df["DATE OCC"]))

window = Window.partitionBy("year") \
                .orderBy(desc("crime_total"))

# Group by year and month, count the tuples to get crime count per month - year
grouped_data = crime_df.groupBy("year", "month") \
                .agg(count("*") \
                .alias("crime_total")) \
                .orderBy("year", "month")

# Get only top 3 crime count months per year
ranked_data = grouped_data.withColumn("#", row_number().over(window))
top3_months = ranked_data.filter(col("#") <= 3)

top3_months.show(42)

# Saves output to hdfs
top3_months.write.csv("./query1-DF_output.csv", header=True, mode="overwrite")

spark.stop()