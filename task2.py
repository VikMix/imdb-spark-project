import settings
import pyspark.sql.types as t
from pyspark.sql import functions as f
import columns as c
from read_write import write


# name_df: DataFrame
def task2(spark_session):
  name_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                              t.StructField('primaryName', t.StringType(), False),
                              t.StructField('birthYear', t.IntegerType(), True),
                              t.StructField('deathYear', t.IntegerType(), True),
                              t.StructField('primaryProfession', t.StringType(), True),
                              t.StructField('knownForTitles', t.StringType(), True),
                              ])

  name_df = spark_session.read.csv(settings.path_name_basics,
                                   header='True',
                                   nullValue=r'\N',
                                   schema=name_schema,
                                   sep=r'\t')
  bydf = name_df.select(f.col(c.primaryName)).where((f.col(c.birthYear) >= 1800) & (f.col(c.birthYear) < 1901))
  write(bydf, settings.directory_to_write2)
  return name_df
