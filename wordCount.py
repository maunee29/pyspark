import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def word_count(input_file, output_dir):
    # Step 1: Initialize SparkSession
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Step 2: Read the input data from the file
    df = spark.read.text(input_file)


    print("This is the dataframe after reading from file")
    df.show()
    # Step 3: Tokenize the text and perform word count
    df1 = df.select((split(col("value"), " ")).alias("word"))
    print("This is the dataframe after split")
    df1.show()
    words_df = df.select(explode(split(col("value"), " ")).alias("word"))
    print("This is the dataframe after explode")
    words_df.show()
    word_count_df = words_df.groupBy("word").count(" ")
    print("This is the dataframe after count")
    word_count_df.show()
    # Step 4: Write the word count DataFrame to the output directory
    word_count_df.write.csv(output_dir, header=True, mode="overwrite")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    # Create the argument parser
    parser = argparse.ArgumentParser(description="PySpark DataFrame Word Count")
    parser.add_argument("input_file", help="Path to the input data file")
    parser.add_argument("output_dir", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()

    # Call the word_count function with the provided input file and output directory
    word_count(args.input_file, args.output_dir)
