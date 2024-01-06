from import_data import import_crime_data
from SparkSession import create_spark_session

spark = create_spark_session("Crime Data")

crime_df = import_crime_data(spark)

# Get row count
rows = crime_df.count()
print(f"DataFrame Rows count : {rows}")

# Print the schema
crime_df.printSchema()