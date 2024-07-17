import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Assignment1")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Loading rideshare and taxi lookup data
    rideshare_data_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/rideshare_data.csv"
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/taxi_zone_lookup.csv"

    # Converting rideshare and taxi_zone_lookup data to dataframes
    rideshare_data_df = spark.read.option("header", "true").csv(rideshare_data_path)
    taxi_zone_lookup_df = spark.read.option("header", "true").csv(taxi_zone_lookup_path)

    # Joining Pickup Details to rideshare data
    rideshare_pickup_df = rideshare_data_df.join(taxi_zone_lookup_df, rideshare_data_df["pickup_location"] == taxi_zone_lookup_df["LocationID"], "inner").select(rideshare_data_df["*"], taxi_zone_lookup_df["Borough"].alias("Pickup_Borough"), taxi_zone_lookup_df["Zone"].alias("Pickup_Zone"), taxi_zone_lookup_df["service_zone"].alias("Pickup_service_zone"))

    # Joining Dropoff Details to Rideshare data that has pickup details
    rideshare_pickup_dropoff_df = rideshare_pickup_df.join(taxi_zone_lookup_df, rideshare_pickup_df["dropoff_location"] == taxi_zone_lookup_df["LocationID"], "inner").select(rideshare_pickup_df["*"], taxi_zone_lookup_df["Borough"].alias("Dropoff_Borough"), taxi_zone_lookup_df["Zone"].alias("Dropoff_Zone"), taxi_zone_lookup_df["service_zone"].alias("Dropoff_service_zone"))

    # Converting Unix timestamp to date format
    rideshare_pickup_dropoff_df = rideshare_pickup_dropoff_df.withColumn("date", date_format(from_unixtime(rideshare_pickup_dropoff_df["date"]), "yyyy-MM-dd"))

    # Printing one row for sanity testing
    rideshare_pickup_dropoff_df.limit(1).show(truncate=False)

    # Printing schema for task 1
    rideshare_pickup_dropoff_df.printSchema()
    
    count = rideshare_pickup_dropoff_df.count()
    print("Number of rows in the schema: ", count)


    spark.stop()