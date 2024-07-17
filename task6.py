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
        .appName("Assignment1-task6")\
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


    # Group by 'pickup_location' and 'time_of_day', then count
    trip_counts = rideshare_pickup_dropoff_df.groupBy('Pickup_Borough', 'time_of_day').count().withColumnRenamed('count', 'trip_count')
    
    # Filter for trip counts greater than 0 and less than 1000
    filtered_trip_counts = trip_counts.filter((col('trip_count') > 0) & (col('trip_count') < 1000))
    
    filtered_trip_counts.show(100, truncate=False)


    # Part 2
    evening_trips = rideshare_pickup_dropoff_df.filter(rideshare_pickup_dropoff_df['time_of_day'] == 'evening')

    # Group by 'pickup_location' and count the number of records in each group
    evening_trip_counts = evening_trips.groupBy('Pickup_Borough', 'time_of_day').count()
    
    evening_trip_counts.show(100, truncate=False)


    # Part 3
    # Filter trips that start from Brooklyn and end at Staten Island
    trips_brooklyn_staten = rideshare_pickup_dropoff_df.filter(
        (rideshare_pickup_dropoff_df['Pickup_Borough'] == "Brooklyn") &
        (rideshare_pickup_dropoff_df['Dropoff_Borough'] == "Staten Island")
    )

    # Then select only the required columns to print
    result_trips = trips_brooklyn_staten.select('Pickup_Borough', 'Dropoff_Borough', 'Pickup_Zone')
    
    print("--------------------------------------------------")
    print(f"Number of trips from Brooklyn to Staten Island are : {trips_brooklyn_staten.count()}")
    
    # Show 10 sample results
    result_trips.show(10, truncate=False)


    spark.stop()