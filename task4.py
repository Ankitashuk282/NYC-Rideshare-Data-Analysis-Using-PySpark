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
from pyspark.sql.functions import col, avg, desc
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Assignment1-task4")\
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

    # Loading rideshare data
    rideshare_data_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/rideshare_data.csv"

    # Converting rideshare and taxi_zone_lookup data to dataframes
    rideshare_data_df = spark.read.option("header", "true").csv(rideshare_data_path)

    # Group by 'time_of_day' and calculate the average 'driver_total_pay'
    average_driver_total_pay = rideshare_data_df.groupBy("time_of_day").agg(avg("driver_total_pay").alias("average_driver_total_pay"))
    sorted_average_driver_total_pay = average_driver_total_pay.sort(desc("average_driver_total_pay"))
    sorted_average_driver_total_pay.show()

    
    # Question Part 2
    # Calculate the average 'trip_length' for each 'time_of_day'
    average_trip_length = rideshare_data_df.groupBy("time_of_day") \
        .agg(avg("trip_length").alias("average_trip_length"))
    
    # Sort the results by 'average_trip_length' in descending order
    sorted_average_trip_length = average_trip_length.sort(desc("average_trip_length"))
    sorted_average_trip_length.show()


    # Part 3
    # We join the resultant dataframes from previous results based on time of the day 
    # and using that to calculate average earning per mile
    joined_df = average_driver_total_pay.join(average_trip_length, on=["time_of_day"])
    average_earning_per_mile_df = joined_df.withColumn("average_earning_per_mile", joined_df["average_driver_total_pay"] / joined_df["average_trip_length"])
    
    final_df = average_earning_per_mile_df.select("time_of_day", "average_earning_per_mile")
    sorted_final_df = final_df.sort(desc("average_earning_per_mile"))
    sorted_final_df.show()

    
    spark.stop()