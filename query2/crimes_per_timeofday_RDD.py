from SparkSession import create_spark_session_rdd
from import_data import import_crime_data_rdd

sc = create_spark_session_rdd("Crime Rates Across Daytime Segments - RDD API")

crime_rdd = import_crime_data_rdd(sc)

# Function that identifies the day segment of a certain time
def daytime_segment(time):
    try:
        hours = int(time[0:2])
    except ValueError:
        return 'UNKNOWN'
    if 5 <= hours < 12:
        return 'MORNING'
    elif 12 <= hours < 16:
        return 'AFTERNOON'
    elif 16 <= hours < 21:
        return 'EVENING'
    else:
        return 'NIGHT'


crime_rdd = crime_rdd.filter(lambda line: line[15] == 'STREET') \
                     .map(lambda line: (daytime_segment(line[3]), 1)) \
                     .reduceByKey(lambda x, y: x + y) \
                     .sortBy(lambda x: x[1], ascending=False)

print(crime_rdd.collect())

# Saves output to hdfs
crime_rdd.saveAsTextFile("./query2-RDD_output.txt")

sc.stop()
