import settings as s
import pyspark.sql.types as t
import pyspark.sql.functions as f
import columns as c
from read_write import write
from pyspark.sql import Window as w

def task6(spark_session,tb_df):
  title_episode_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                       t.StructField("parentTconst", t.StringType(), True),
                                       t.StructField("seasonNumber", t.IntegerType(), True),
                                       t.StructField("episodeNumber", t.IntegerType(), True),
                                       ])
  title_episode_df=spark_session.read.csv(s.path_title_episode,
                                          header='True',
                                          nullValue=r'\N',
                                          schema=title_episode_schema,
                                          sep=r'\t')
  #title_episode_df.show(200, truncate=False)
  all_episode_df = title_episode_df.groupBy(c.te_parentTconst).count().orderBy(f.desc("count"))

  episode_df = (all_episode_df
                .join(tb_df.filter(f.col(c.tb_titleType) == "tvSeries"),f.col(c.te_parentTconst) == f.col(c.tconst))
                .orderBy(f.desc("count"), c.tb_originalTitle)).limit(50)
  #episode_df.show(60)

  write(episode_df, s.directory_to_write6)
  return title_episode_df

