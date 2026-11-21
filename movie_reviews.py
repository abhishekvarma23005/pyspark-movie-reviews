from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim

spark = SparkSession.builder.appName("IMDb Movie Reviews").getOrCreate()

df = spark.read.csv(
    "dataset/IMDB Dataset.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"',
)

print("schema:")
df.printSchema()

print("first 5 rows:")
df.show(5, truncate=100)

clean_df = df.filter(
    col("review").isNotNull() & (trim(col("review")) != "")
)

print("total reviews:", clean_df.count())

print("reviews by sentiment:")
clean_df.groupBy("sentiment").count().show()

transformed_df = clean_df.withColumn("review_length", length(col("review")))

print("reviews longer than 500 characters:")
transformed_df.filter(col("review_length") > 500).show(5, truncate=100)

transformed_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "output/cleaned_reviews"
)

print("saved cleaned reviews to output/cleaned_reviews")

spark.stop()
