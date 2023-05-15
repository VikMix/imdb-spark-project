import settings as s
import pyspark.sql.types as t
import pyspark.sql.functions as f
import columns as c
from read_write import write
from pyspark.sql import Window as w


def task7(spark_session, tb_df):
  title_ratings_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                       t.StructField("averageRating", t.DoubleType(), True),
                                       t.StructField("numVotes", t.IntegerType(), True),
                                       ])
  title_ratings_df = spark_session.read.csv(s.path_title_rating,
                                            header='True',
                                            nullValue=r'\N',
                                            schema=title_ratings_schema,
                                            sep=r'\t')

  # Get 10 titles of the most popular movies/series etc. by each decade
  tb_df = tb_df.filter(f.col(c.tb_startYear).isNotNull())
  title_ratings_df = title_ratings_df.select(c.tconst, c.tr_averageRating)
  most_popular_df = tb_df.join(title_ratings_df, c.tconst)
  most_popular_df = (most_popular_df.withColumn(c.decade, 10*f.floor(f.col(c.tb_startYear) / 10).cast(t.IntegerType()))
                     .orderBy(c.decade))
  window = w.partitionBy(c.decade).orderBy(f.desc(f.col(c.tr_averageRating)))
  most_popular_df = (most_popular_df.withColumn(c.ten_most_popular, f.row_number().over(window))
                     .where(f.col(c.ten_most_popular) <= 10)
                     .select(c.tb_primaryTitle, c.tb_startYear, c.tb_endYear,
                             c.tr_averageRating, c.decade, c.ten_most_popular))
  write(most_popular_df, s.directory_to_write7)
