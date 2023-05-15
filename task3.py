import settings
import pyspark.sql.types as t
from pyspark.sql import functions as f
import columns as c
from read_write import write



# tb_df:DataFrame
def task3(spark_session):
  tb_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                            t.StructField("titleType", t.StringType(), True),
                            t.StructField("primaryTitle", t.StringType(), True),
                            t.StructField("originalTitle", t.StringType(), True),
                            t.StructField("isAdult", t.IntegerType(), True),
                            t.StructField("startYear", t.IntegerType(), True),
                            t.StructField("endYear", t.IntegerType(), True),
                            t.StructField("runtimeMinutes", t.IntegerType(), True),
                            t.StructField("genres", t.StringType(), True),
                            ])
  tb_df = spark_session.read.csv(settings.path_title_basics,
                                 header='True',
                                 nullValue=r'\N',
                                 schema=tb_schema,
                                 sep=r'\t')
  # tb_df.show(truncate=False)
  df = (tb_df.select(c.tb_primaryTitle, c.tb_originalTitle, c.tb_runtimeMinutes)
        .filter((f.col(c.tb_runtimeMinutes) > 120) & (f.col(c.tb_titleType) == "movie")))
  write(df, settings.directory_to_write3)
  return tb_df
