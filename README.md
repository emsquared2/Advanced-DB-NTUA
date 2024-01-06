# LA Crimes - Advanced DB Course Project NTUA
## Overview
A semester team project for the Advanced Database Topics course of the Electrical & Computer Engineering class of the National Technical University of Athens. In this project we explore big data querying techniques using the LA Crimes Dataset from 2010 to present. Using Apache Spark and HDFS we set up a cluster environment to run our data analysis jobs in our own personal cluster. 

## Prerequisites
To replicate our project in your own cluster you need to already have established the following tools:
- ~okeanos-knossos **VM resources** or other personal VMs
- **Apache Spark** on your nodes
- **HDFS** for a distributed file system in your cluster

## Usage
Under the `query` folders you can find the code for each query implemented. For more information about each query you can find under the `Docs` folder. 

Before using the scripts to run each query with the specified characteristics firstly you have to save the datasets in the HDFS. To do that you can run the following command which downloads the crime data directly to your HDFS:
```console
$ curl https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD | hadoop fs -put - hdfs://okeanos-master:54310/your/path/crime-data-from-2010-to-2019.csv
$ curl https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD | hadoop fs -put - hdfs://okeanos-master:54310/your/path/crime-data-from-2020-to-present.csv
```
You can save the data wherever you want inside your HDFS but remember to also change the include path as follows:
```python
# Contents of /utils/import_data

37.
38. crime_df1 = spark.read.csv("hdfs://okeanos-master:54310/your/path/crime-data-from-2010-to-2019.csv", header=True, schema=crime_schema)  
39. crime_df2 = spark.read.csv("hdfs://okeanos-master:54310/your/path/crime-data-from-2020-to-present.csv", header=True, schema=crime_schema)
40.
...
55. crime_rdd1 = spark.textFile("hdfs://okeanos-master:54310/your/path/crime-data-from-2010-to-2019.csv").map(lambda x: parse_csv(x))
...
60. crime_rdd2 = spark.textFile("hdfs://okeanos-master:54310/your/path/crime-data-from-2020-to-present.csv").map(lambda x: parse_csv(x))
...
78. income_df = spark.read.csv("hdfs://okeanos-master:54310/your/path/LA_income_2015.csv", header=True, schema=income_schema)
...
91. revgeocoding_df = spark.read.csv("hdfs://okeanos-master:54310/your/path/revgecoding.csv", header=True, schema=revgeocoding_schema)
...
107. police_stations_df = spark.read.csv("hdfs://okeanos-master:54310/your/path/LAPD_Police_Stations.csv", header=True, schema=police_stations_schema)
108.
```
To run one of the queries you run each script like this:
```console
$ ./scripts/run_query1.sh API_TYPE MODE
Usage: ./scripts/run_query1.sh <API_TYPE> <MODE>

API_TYPE: Specify the type of API (DF, SQL) used in the filename.
MODE: Specify the mode in which the job will be run (client or cluster).
```
So to run query1 with a Dataframe implementation on cluster mode just run:
```console
$ ./scripts/run_query1.sh DF cluster
```

> Some queries offer an implementation for RDD too (it is specified on the Usage: of that script) and some offer configuration to the executors used. Change the script command accordingly to use those features too.

For client mode the query output can be seen directly in the console.

For cluster mode on the other hand the output is saved in an output file in the HDFS. To make a local copy you can run the below command:
```console
$ hadoop fs -getmerge hdfs://okeanos-master:54310/query ./desired_filename.txt
```

## Authors
- [Alexandros Ionitsa](https://github.com/alex1on)
- [Manos Emmanouilidis](https://github.com/emsquared2)


