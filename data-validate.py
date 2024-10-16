from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BeeAnalysis").getOrCreate()

df1 = spark.read.csv("gs://bee-mspb-data/D1_sensor_data.csv", header=True, inferSchema=True)