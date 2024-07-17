import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, row_number
from pyspark.sql.functions import desc, to_date, month, count
from pyspark.sql.functions import concat_ws, col, sum
from pyspark.sql.window import Window

from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Assignment1-task2")\
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

    print("Data Reporsitory Bucket: ", s3_data_repository_bucket)
    print("S3 Endpoint: ", s3_endpoint_url)
    print("S3 Access Key Id: ", s3_access_key_id)
    print("S3 Secret Access Key: ", s3_secret_access_key) 
    print("Bucket Name: ", s3_bucket)

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

    rideshare_pickup_dropoff_df = rideshare_pickup_dropoff_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    # Extract the month from the 'date' column
    rideshare_pickup_dropoff_df = rideshare_pickup_dropoff_df.withColumn('month', month('date'))

    # Part 1
    # Group by 'business' and 'month', and count the number of trips and save data to personal clsuter
    trip_counts = rideshare_pickup_dropoff_df.groupBy('business', 'month').count()
    
    output_path = "s3a://" + s3_bucket + "/task2/trip_counts_by_business_each_month"
    trip_counts.write.option("header", "true").csv(output_path)
    Show the results
    trip_counts.show()    

    # Part 2
    rideshare_pickup_dropoff_df = rideshare_pickup_dropoff_df.withColumn("rideshare_profit", rideshare_pickup_dropoff_df["rideshare_profit"].cast("float")).withColumn("driver_total_pay", rideshare_pickup_dropoff_df["driver_total_pay"].cast("float"))
    
    # Group by business and month, then sum up the profits
    profit_by_business_month = rideshare_pickup_dropoff_df.groupBy("business", "month").agg(sum("rideshare_profit").alias("total_profit")).orderBy("business", "month")

    output_path = "s3a://" + s3_bucket + "/task2/profit_by_business_each_month"
    profit_by_business_month.write.option("header", "true").csv(output_path)
    
    profit_by_business_month.show()


    # Part 3
    # Group by business and month, then sum up the driver's earnings
    driver_earnings_by_business_month = rideshare_pickup_dropoff_df.groupBy("business", "month") \
                                   .agg(sum("driver_total_pay").alias("total_earnings")) \
                                   .orderBy("business", "month")
    
    driver_earnings_by_business_month.show()

    output_path = "s3a://" + s3_bucket + "/task2/driver_earnings_each_month"
    driver_earnings_business_month.write.option("header", "true").csv(output_path)
    
    spark.stop()






