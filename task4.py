import settings as s
import pyspark.sql.types as t
import pyspark.sql.functions as f
import columns as c
from read_write import write

def task4(spark_session,tb_df, name_df):
  title_principals_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                          t.StructField("ordering", t.IntegerType(), True),
                                          t.StructField("nconst", t.StringType(), True),
                                          t.StructField("category", t.StringType(), True),
                                          t.StructField("job", t.StringType(), True),
                                          t.StructField("characters", t.StringType(), True),
                                          ])
  tp_df=spark_session.read.csv(s.path_title_principals,
                               header='True',
                               nullValue=r'\N',
                               schema=title_principals_schema,
                               sep=r'\t')

  res_df = name_df.join(tp_df, c.nconst)\
                  .join(tb_df,c.tconst)\
                  .select(c.primaryName, c.tb_primaryTitle, c.tp_characters)
  write(res_df, s.directory_to_write4)
  return tp_df