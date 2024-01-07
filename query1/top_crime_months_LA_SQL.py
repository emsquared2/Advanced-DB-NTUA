from import_data import import_crime_data
from SparkSession import create_spark_session

# Create Spark session
spark = create_spark_session("Top 3 months with the highest number of recorded crimes, in LA, per year - SQL API")

# Import crime data
crime_df = import_crime_data(spark)

# To utilize as SQL tables
crime_df.createOrReplaceTempView("crimes")

# Prepare the SQL query
query = "WITH RankedCrimeData AS ( \
            SELECT \
                EXTRACT(YEAR FROM `DATE OCC`) AS year, \
                EXTRACT(MONTH FROM `DATE OCC`) AS month, \
                COUNT(*) AS crimes_total, \
                RANK() OVER (PARTITION BY EXTRACT(YEAR FROM `DATE OCC`) ORDER BY COUNT(*) DESC) AS crime_rank \
            FROM crimes \
            GROUP BY \
                EXTRACT(YEAR FROM `DATE OCC`), \
                EXTRACT(MONTH FROM `DATE OCC`) \
        ) \
        SELECT year, month, crimes_total, crime_rank \
        FROM RankedCrimeData \
        WHERE crime_rank <= 3 \
        ORDER BY year ASC, crimes_total DESC"

# Perform query
top3_months = spark.sql(query)

top3_months.show(42)

# Saves output to hdfs
top3_months.write.csv("./output/query1/query1-SQL.csv", header=True, mode="overwrite")

spark.stop()