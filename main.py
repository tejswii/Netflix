from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('NF').getOrCreate()

credits_df = spark.read.option('header', 'true').csv('raw_credits.csv', inferSchema=True)
titles_df = spark.read.option('header', 'true').csv('raw_titles.csv', inferSchema=True)

#credits_df = spark.table("credits")
#titles_df = spark.table("titles")

result_df1 = (
    credits_df.join(titles_df, credits_df.id == titles_df.id, "inner")
    .filter((col("role") == "ACTOR") & (col("imdb_score").isNotNull()) & (col("type") == "MOVIE"))
    .select(
        col("name"),
        col("role"),
        col("title"),
        col("type"),
        col("release_year"),
        col("imdb_score"),
        row_number().over(Window.partitionBy("release_year").orderBy(col("imdb_score").asc())).alias("Rank")
    )
    .filter(col("Rank") < 2)
    .orderBy(col("release_year"), col("Rank"))
)

result_df1.show(60)

result_df2 = (
    credits_df.join(titles_df, credits_df.id == titles_df.id, "inner")
    .filter((col("role") == "ACTOR") & (col("imdb_score").isNotNull()) & (col("type") == "SHOW"))
    .select(
        col("name"),
        col("role"),
        col("title"),
        col("type"),
        col("release_year"),
        col("imdb_score"),
        row_number().over(Window.partitionBy("release_year").orderBy(col("imdb_score").asc())).alias("Rank")
    )
    .filter(col("Rank") < 2)
    .orderBy(col("release_year"), col("Rank"))
)

result_df2.show(60)

result_df3 = (
    credits_df.join(titles_df, credits_df.id == titles_df.id, "inner")
    .filter((col("role") == "ACTOR") & (col("imdb_score").isNotNull()))
    .select(
        col("name"),
        col("role"),
        col("title"),
        col("type"),
        col("release_year"),
        col("imdb_score"),
        row_number().over(Window.partitionBy("release_year").orderBy(col("imdb_score").asc())).alias("Rank")
    )
    .filter(col("Rank") < 2)
    .orderBy(col("release_year"), col("Rank"))
)

result_df3.show(150)
