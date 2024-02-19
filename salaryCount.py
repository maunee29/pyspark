import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import IntegerType
def calculatesalary(input_file, output_dir):
    # Step 1: Initialize SparkSession
    spark = SparkSession.builder.appName("SalaryCount").getOrCreate()

    # Step 2: Read the input data from the file
    print("Read data and separate by columns")
    df = spark.read.csv(input_file, header="true")
    df.show()
    df.printSchema() #prints the type for the column

    print("Change Salary column from String to Integer")
    df_num = df.withColumn("salarynum", df["salary"].cast(IntegerType()))
    df_num.show()
    print("Group by dept name and sum the salary and then order alplabetically")
    df_group = df_num.groupBy("dept_name").sum("salarynum").orderBy("dept_name").show()
    #df_group.show()


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
