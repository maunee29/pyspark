import argparse
from pyspark.sql import SparkSession

def calculatesalary(input_file, output_dir):
    # Step 1: Initialize SparkSession
    spark = SparkSession.builder.appName("SalaryCount").getOrCreate()

    # Step 2: Read the input data from the file
    df = spark.read.csv(input_file, header="true")
    df.show()
    df.printSchema() #prints the type for the column

    df.createOrReplaceTempView("num_tab")
    salbydept = spark.sql("SELECT sum(salary) FROM num_tab group by dept_name")
    salbydept.show()

   # df1 = spark.sql("SELECT dept_name, sum(salary) FROM num_tab")

    spark.stop()


if __name__ == "__main__":
    # Create the argument parser
    parser = argparse.ArgumentParser(description="PySpark DataFrame Salary Count")
    parser.add_argument("input_file", help="Path to the input data file")
    parser.add_argument("output_dir", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Call the calculatesalary function with the provided input file and output directory
    calculatesalary(args.input_file, args.output_dir)
