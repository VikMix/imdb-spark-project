from pyspark import SparkConf
from pyspark.sql import SparkSession

from task1 import task1
from task2 import task2
from task3 import task3
from task4 import task4
from task5 import task5
from task6 import task6
from task7 import task7


def main():
  spark_session = (SparkSession.builder
                   .master("local")
                   .appName("imdb app")
                   .config(conf=SparkConf())
                   .getOrCreate())

  # task1(spark_session)
  # name_df = task2(spark_session)
  tb_df = task3(spark_session)
  # task4(spark_session, tb_df, name_df)
  # task5(spark_session, tb_df)
  # task6(spark_session, tb_df)
  task7(spark_session, tb_df)


if __name__ == "__main__":
  main()
