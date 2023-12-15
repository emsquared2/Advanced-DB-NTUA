from pyspark.sql.functions import year, month, count, desc, row_number, col, collect_list, explode
from pyspark.sql.window import Window
from import_data import import_crime_data

crime_df = import_crime_data()

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

top3_months.show()

# Saves output to hdfs
top3_months.write.csv("./output.csv", header=True, mode="overwrite")

