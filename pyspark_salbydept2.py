import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def read_bigquery_table():
    spark = SparkSession.builder.appName("ReadBigQueryTable").getOrCreate()

    bqDF = spark.read.format("bigquery").option("table", "notional-plasma-410003:hr.empdata").load()

    return bqDF


def process_data_and_save(bqDF, output_dir):
    bqDF2 = bqDF.withColumn("salaryasnum", bqDF["salary"].cast(IntegerType()))
    sumDF = bqDF2.groupby("dept_name").sum("salary")

    output_file = output_dir + "/salbydep.csv"
    sumDF.write.format("csv").mode("overwrite").option("header", "true").save(output_file)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="PySpark DataFrame Salary Count")
    parser.add_argument("output_dir", help="Path to the output directory")
    args = parser.parse_args()

    # Read BigQuery table
    bqDF = read_bigquery_table()

    # Process data and save to output directory
    process_data_and_save(bqDF, args.output_dir)
