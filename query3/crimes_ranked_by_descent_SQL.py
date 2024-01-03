from import_data import import_crime_data, import_income_data, import_revgeocoding_data
from SparkSession import create_spark_session

# Create Spark session
spark = create_spark_session("Crimes Ranked by Victim Descent - SQL API")

# Import data
crime_df = import_crime_data(spark)
income_df = import_income_data(spark)
revgeocoding_df = import_revgeocoding_data(spark)

# Register DataFrames as temporary SQL tables
crime_df.createOrReplaceTempView("crimes")
income_df.createOrReplaceTempView("incomes")
revgeocoding_df.createOrReplaceTempView("revgeocoding")


# Filter Crime Data for the year 2015 with known Victim Age and Descent 
crime_data_2015_filter_query = \
    "SELECT `Vict Descent` as `Victim Descent`, LAT, LON \
    FROM crimes \
    WHERE ( \
        EXTRACT(YEAR FROM `DATE OCC`) = 2015 AND \
        `Vict Descent` IS NOT NULL AND `Vict Descent` <> 'X' AND \
        `Vict Age` IS NOT NULL AND `Vict Age` <> '0' \
    ) \
    ORDER BY DR_NO"

# Join income data and revgeocding data using zip code
income_revgeocoding_data_query = \
    "SELECT revgc.LAT, revgc.LON, revgc.ZIPcode, in.`Estimated Median Income` as Income \
    FROM revgeocoding revgc \
    INNER JOIN incomes in ON revgc.ZIPcode = in.`Zip Code`"


#Perform queries
filtered_crime_data = spark.sql(crime_data_2015_filter_query)
income_revgeocoding_data = spark.sql(income_revgeocoding_data_query)


filtered_crime_data.createOrReplaceTempView("filtered_crimes")
income_revgeocoding_data.createOrReplaceTempView("income_revgc")


# Join filtered crimes and income_revgeocoding using LAT & LON
crimes_income_ZIPcode_query = \
    "SELECT fc.`Victim Descent`, fc.LAT, fc.LON, ir.ZIPcode, ir.Income \
    FROM filtered_crimes fc \
    INNER JOIN income_revgc ir ON \
    fc.LAT = ir.LAT AND fc.LON = ir.LON \
    ORDER BY ir.Income Desc"

crimes_income_ZIPcode_data = spark.sql(crimes_income_ZIPcode_query)

crimes_income_ZIPcode_data.createOrReplaceTempView("crimes_income_ZIPcode")

# Find top 3 and last 3 ZIP Codes by income
filtered_ZIPcodes_query = " \
            SELECT ZIPcode \
            FROM ( \
                SELECT DISTINCT ZIPcode, Income \
                FROM crimes_income_ZIPcode \
                ORDER BY Income DESC \
                LIMIT 3 \
            ) AS top_incomes \
            UNION \
            SELECT ZIPcode \
            FROM ( \
                SELECT DISTINCT ZIPcode, Income \
                FROM crimes_income_ZIPcode \
                ORDER BY Income ASC \
                LIMIT 3 \
            ) AS last_incomes"

filtered_ZIPcodes = spark.sql(filtered_ZIPcodes_query)

filtered_ZIPcodes.createOrReplaceTempView("filtered_zip_codes")

# Find total number of victims grouped by Victim Descent
crimes_ranked_by_descent_query = \
    "WITH CrimesRankedByDescent AS ( \
        SELECT ciz.`Victim Descent`, ciz.ZIPcode, ciz.Income \
        FROM crimes_income_ZIPcode ciz \
        INNER JOIN filtered_zip_codes fzc ON ciz.ZIPcode = fzc.ZIPcode \
    ) \
    SELECT `Victim Descent`, COUNT(*) AS total_crimes \
    FROM CrimesRankedByDescent \
    GROUP BY `Victim Descent` \
    ORDER BY total_crimes DESC"


crimes_ranked_by_descent = spark.sql(crimes_ranked_by_descent_query)

crimes_ranked_by_descent.show()

# Save output to hdfs
crimes_ranked_by_descent.write.csv("./query3-SQL_output.csv", header=True, mode="overwrite")

# Stop Spark Session
spark.stop()    