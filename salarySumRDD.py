import argparse
from tokenize import tokenize

from pyspark import SparkConf, SparkContext
from pyspark.python.pyspark.shell import sc

from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import IntegerType


#def addone(word):
 #   return (word, 1)


def calculatesalary(input_file, output_dir):
    # Step 1: Initialize SparkSession
    conf = SparkConf().setAppName("salarybyDept_rdd").setMaster("local")
    sc = SparkContext(conf=conf)

    # Step 2: Read the input data from the file
    rdd1 = sc.textFile(input_file)
    # rdd1.show()

    words_rdd = rdd1.map(tokenize)
    # words_rdd.show()

    words_pairs_rdd = words_rdd.map(lambda word: (word, 1))
    #words_pairs_rdd = words_rdd.map(addone)       this line does the same thing as the above line

    words_counts_rdd = words_pairs_rdd.reduceByKey(lambda a, b: a + b)
    word_count = words_counts_rdd


    # df1 = df.withColumn("salarynum", df["salary"].cast(IntegerType()))
    # df1.show()
    # df1.printSchema()

    # df1 = df1.groupBy("dept_name").sum("salarynum").orderBy("dept_name").show()
    # df1.show()

    sc.stop()


if __name__ == "__main__":
    # Create the argument parser
    parser = argparse.ArgumentParser(description="PySpark DataFrame Salary Count")
    parser.add_argument("input_file", help="Path to the input data file")
    parser.add_argument("output_dir", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Call the calculatesalary function with the provided input file and output directory
    calculatesalary(args.input_file, args.output_dir)
