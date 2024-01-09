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
    executors=$3
    
    # Check if the module exists
    module_name="$main_dir/query3/crimes_ranked_by_descent_${api_type}.py"
    if [ -e "$module_name" ]; then
        if [[ ( $executors -ge 2 )  &&  ( $executors -le 4 ) ]]; then
            # Build the spark-submit command based on the specified mode
            if [ "$mode" == "client" ]; then
                spark-submit --num-executors $executors $py_file_options "$module_name"
            elif [ "$mode" == "cluster" ]; then
                spark-submit --deploy-mode cluster --num-executors $executors $py_file_options "$module_name"
            else
                echo "Invalid mode. Please specify 'client' or 'cluster'."
                print_usage
            fi
        else
            echo "Invalid executor number. Please specify a number between 2 and 4."
        fi
    else
        echo "Error: Module ${module_name} not found."
    fi
}

# Function to print usage information
print_usage() {
    echo "Usage: $0 <API_TYPE> <MODE> <EXEC>"
    echo "API_TYPE: Specify the type of API (DF, SQL) used in the filename."
    echo "MODE: Specify the mode in which the job will be run (client or cluster)."
    echo "EXEC: Specify the number of executors (valid values 2 - 4)."
}

# Check the number of command-line arguments
if [ "$#" -ne 3 ]; then
    echo "Error: Invalid number of arguments."
    print_usage
else
    # Get API type and mode from command-line arguments
    api_type=$1
    mode=$2
    executors=$3

    # Run the Spark job based on user input
    run_spark_job $api_type $mode $executors
fi
