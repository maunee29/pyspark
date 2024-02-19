from pyspark.sql import SparkSession
import argparse

from pyspark.sql.types import IntegerType

def salbysum(input_table, output_dir):
    spark = SparkSession.builder.appName("BigQuerySalaryByDepartment").getOrCreate()

    # Read the data from BigQuery table
    bqDF = spark.read.format("bigquery").option("table", input_table).load()

    bqDF2 = bqDF.withColumn("salaryasnum", bqDF["salary"].cast(IntegerType()))

    # Group results by dept_name and calculate the sum of the salaries and order alphabetically
    sumDF = bqDF2.groupby("dept_name").sum("salaryasnum")
    sumDF = sumDF.orderBy("dept_name")


    # Write result to the specified output directory, sumsalbydept
    output_path = f"{output_dir}/sumsalbydept"
    sumDF.write.format("csv").mode("overwrite").save(output_path)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="PySpark DataFrame Salary Count")
    parser.add_argument("input_table", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("output_dir", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Call the salbysum function with the provided input file and output directory
    salbysum(args.input_table, args.output_dir)
