import settings
import pyspark.sql.types as t
import pyspark.sql.functions as f
import columns as c
from read_write import write


def task1(spark_session):
  title_sсhema = t.StructType([t.StructField('titleId', t.StringType(), False),
                               t.StructField('ordering', t.IntegerType(), False),
                               t.StructField('title', t.StringType(), False),
                               t.StructField('region', t.StringType(), True),
                               t.StructField('language', t.StringType(), True),
                               t.StructField('types', t.StringType(), True),
                               t.StructField('attributes', t.StringType(), True),
                               t.StructField('isOriginalTitle', t.IntegerType(), True)
                               ])

  title_ua_df = spark_session.read.csv(settings.path_title_akas,
                                       header='True',
                                       nullValue=r'\N',
                                       schema=title_sсhema,
                                       sep=r'\t')
  df=title_ua_df.select(f.col(c.title)).where(f.col(c.region) == 'UA')
  write(df, settings.directory_to_write1)
