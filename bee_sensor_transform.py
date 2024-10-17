from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def ingest_data(file_path_sensor1, file_path_sensor2):
    # initialize Spark session with GCS connector
    spark = SparkSession.builder.appName("BeeAnalysis").getOrCreate()
    sensor_df1 = spark.read.csv(file_path_sensor1, header=True, inferSchema=True)
    # sensor_df1.show()

    sensor_df2 = spark.read.csv(file_path_sensor2, header=True, inferSchema=True)
    # sensor_df2.show()

    return sensor_df1, sensor_df2


def config_gcs_connector():

    # Configure GCS credentials
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.enable", "true"
    )
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        "/home/ad/Bee-info/bee-info-438715-0827c6f7150c.json",
    )


def transform(sensor_df1, sensor_df2):
    sensor_df1.printSchema()
    sensor_df2.printSchema()

    # Add 'sensorid' column to each DataFrame
    sensor_df1 = sensor_df1.withColumn("sensorid", lit(1))  # For sensor data 1
    sensor_df2 = sensor_df2.withColumn("sensorid", lit(2))  # For sensor data 2

    df = sensor_df1.union(sensor_df2)
    return df


def data_pipeline(FILE_PATH_SENSOR1, FILE_PATH_SENSOR2):
    # config_gcs_connector()
    sensor_df1, sensor_df2 = ingest_data(FILE_PATH_SENSOR1, FILE_PATH_SENSOR2)
    df = transform(sensor_df1, sensor_df2)
    df.show_head()
