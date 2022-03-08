import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='shan-bigdata-6c83179d1393.json'

spark = SparkSession.builder.enableHiveSupport().appName('movie_etl').getOrCreate()

path = 'gs://shan_movie_bucket/movies.csv'
output_path = 'gs://shan_movie_bucket/movies.parquet'

df = spark.read.csv(path, header=True).select(col('title').alias('movie_name'),col('genres').alias('movie_type'))
df.write.parquet(output_path, mode='overwrite')

	
	
	