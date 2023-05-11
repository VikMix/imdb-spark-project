#import main
import pyspark.sql.types as t
import pyspark.sql.functions as f
from main import spark_session
from read_write import write
title_sсhema=t.StructType([t.StructField ('titleId',t.StringType(), False),
                         t.StructField ('ordering', t.IntegerType(), False),
                         t.StructField ('title', t.StringType(), False),
                         t.StructField ('region', t.StringType(), True),
                         t.StructField ('language', t.StringType(), True),
                         t.StructField ('types', t.StringType(), True),
                         t.StructField ('attributes', t.StringType(), True),
                         t.StructField ('isOriginalTitle', t.IntegerType(), True)
                          ])

title_ua_df=spark_session.read.csv(path,
                                header='True',
                                nullValue=r'\N',
                                schema=title_sсhema,
                                sep=r'\t')
directory_to_write = "C:\Rabota\Prof2IT\DataMining\project\out1"
def task1(df):
  df.filter().withColumn
  title_ua_df.select(f.col('title')).where(f.col('region') == 'UA').show(50, truncate=False)
  write(df, directory_to_write)