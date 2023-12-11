from import_data import import_crime_data

crime_df = import_crime_data()

# Get row count
rows = crime_df.count()
print(f"DataFrame Rows count : {rows}")

crime_df.printSchema()