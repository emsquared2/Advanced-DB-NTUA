#!/bin/bash

# Get the directory of the script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
main_dir="$(dirname "$script_dir")"
cd "$script_dir"

# Function to run the Spark job using spark-submit
run_spark_job() {
    py_file_options="--py-files $main_dir/utils/import_data.py,$main_dir/utils/SparkSession.py"

    api_type=$1
    mode=$2

    # Check if the module exists
    module_name="$main_dir/query2/crimes_per_timeofday_${api_type}.py"
    if [ -e "$module_name" ]; then
        # Build the spark-submit command based on the specified mode
        if [ "$mode" == "client" ]; then
            spark-submit $py_file_options "$module_name"
        elif [ "$mode" == "cluster" ]; then
            spark-submit --deploy-mode cluster --num-executors 4 $py_file_options "$module_name"
        else
            echo "Invalid mode. Please specify 'client' or 'cluster'."
            print_usage
        fi
    else
        echo "Error: Module ${module_name} not found."
    fi
}

# Function to print usage information
print_usage() {
    echo "Usage: $0 <API_TYPE> <MODE>"
    echo "API_TYPE: Specify the type of API (DF, SQL, RDD) used in the filename."
    echo "MODE: Specify the mode in which the job will be run (client or cluster)."
}

# Check the number of command-line arguments
if [ "$#" -ne 2 ]; then
    echo "Error: Invalid number of arguments."
    print_usage
else
    # Get API type and mode from command-line arguments
    api_type=$1
    mode=$2

    # Run the Spark job based on user input
    run_spark_job $api_type $mode
fi
