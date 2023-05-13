import settings as s
import pyspark.sql.types as t
import pyspark.sql.functions as f
import columns as c
from read_write import write
from pyspark.sql import Window as w

def task5(spark_session,tb_df):
  title_akas_schema = t.StructType([t.StructField("titleId", t.StringType(), False),
                                    t.StructField("ordering", t.IntegerType(), False),
                                    t.StructField("title", t.StringType(), False),
                                    t.StructField("region", t.StringType(), True),
                                    t.StructField("language", t.StringType(), True),
                                    t.StructField("types", t.StringType(), True),
                                    t.StructField("attributes", t.StringType(), True),
                                    t.StructField("isOriginalTitle", t.IntegerType(), True),
                                    ])
  title_akas_df=spark_session.read.csv(s.path_title_akas,
                               header='True',
                               nullValue=r'\N',
                               schema=title_akas_schema,
                               sep=r'\t')

  title_ratings_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                       t.StructField("averageRating", t.DoubleType(), True),
                                       t.StructField("numVotes", t.IntegerType(), True),
                                       ])
  title_ratings_df = spark_session.read.csv(s.path_title_rating,
                                         header='True',
                                         nullValue=r'\N',
                                         schema=title_ratings_schema,
                                         sep=r'\t')

  akas_df = title_akas_df.filter(f.col(c.region).isNotNull())
  title_df= tb_df.filter((f.col(c.tb_isAdult) == 1) & f.col(c.tb_titleType).isNotNull())

  akas_df = akas_df.select(f.col(c.titleId), f.col(c.region))
  #akas_df.show()
  title_df = title_df.select(f.col(c.tconst), f.col(c.tb_isAdult), f.col(c.tb_titleType))
  adult_per_region_df = akas_df.join(title_df, f.col(c.tconst) == f.col(c.titleId))
  #adalt_per_region_df.show()
  region_window = (w.partitionBy(c.region).orderBy(c.region))
  adult_df = adult_per_region_df.withColumn(c.adult_per_region, f.sum(f.col(c.tb_isAdult)).over(region_window))
  #adult_df.show()

  type_window = (w.partitionBy(c.region, c.tb_titleType).orderBy(c.region, c.tb_titleType))
  adult_df = adult_df.withColumn(c.adult_per_title_type, f.count(f.col(c.tconst)).over(type_window))
  adult_df = adult_df.orderBy(f.desc(c.adult_per_region), f.desc(c.adult_per_title_type))
  adult_df = adult_df.join(title_ratings_df, c.tconst)
  rate_window = (w.partitionBy(c.region, c.tb_titleType)
                 .orderBy(f.desc(c.adult_per_region), f.desc(c.adult_per_title_type), f.desc(c.tr_averageRating)))
  adult_df = adult_df.withColumn(c.top_adult, f.row_number().over(rate_window))
  adult_df = adult_df.orderBy(f.desc(c.adult_per_region), f.desc(c.adult_per_title_type), f.desc(c.tr_averageRating))
  adult_df = adult_df.where(f.col(c.top_adult) <= 100)
  #adult_df.show(150,truncate=False)

  write(adult_df, s.directory_to_write5)
  return title_akas_df
