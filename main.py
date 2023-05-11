from pyspark import SparkConf
from pyspark.shell import spark
from pyspark.sql import SparkSession, Window
from datetime import datetime, date
import pyspark.sql.types as t
import pyspark.sql.functions as f
#import task1
from task1 import task1


def main():
  spark_session = (SparkSession.builder
                   .master("local")
                   .appName("imdb app")
                   .config(conf=SparkConf())
                   .getOrCreate())

  # movies_df = spark_session.read.csv(path)
  task1(spark_session)

if __name__ == "__main__":
  main()