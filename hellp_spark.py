import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("test").set("spark.eventLog.enabled", "false")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.sql('''select 'spark' as hello''')
df.show()
