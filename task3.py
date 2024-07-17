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
        .appName("Assignment1-task3")\
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

    rideshare_pickup_dropoff_df = rideshare_pickup_dropoff_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))


    # Extract the month directly from the 'date' 
    pickup_location_counts = rideshare_pickup_dropoff_df.groupBy(month("date").alias("month"), "Pickup_Borough").count().withColumnRenamed("count", "trip_count")
    
    # Use a window function to partition the data by month and order by the trip_count in descending order
    windowSpec = Window.partitionBy("month").orderBy(desc("trip_count"))
    pickup_location_ranked = pickup_location_counts.withColumn("rank", row_number().over(windowSpec))
    
    # Filter to get top 5 pickup locations for each month
    top_pickup_locations = pickup_location_ranked.filter(pickup_location_ranked.rank <= 5)
    top_pickup_locations = top_pickup_locations.orderBy("month", desc("trip_count"))
    
    # Select and rename the columns as per the requirement before showing the result
    top_pickup_locations.select("Pickup_Borough", "month", "trip_count").show(30, truncate=False)


    # Get drop-off location counts
    dropoff_location_counts = rideshare_pickup_dropoff_df.groupBy(month("date").alias("month"), "Dropoff_Borough").count().withColumnRenamed("count", "trip_count")
    
    dropoff_location_ranked = dropoff_location_counts.withColumn("rank", row_number().over(windowSpec))
    
    # Filter to get top 5 pickup locations for each month
    top_dropoff_locations = dropoff_location_ranked.filter(dropoff_location_ranked.rank <= 5)
    top_driopoff_locations = top_dropoff_locations.orderBy("month", desc("trip_count"))
    
    # Select and rename the columns as per the requirement before showing the result
    top_dropoff_locations.select("Dropoff_Borough", "month", "trip_count").show(30, truncate=False)


    # Add a 'Route' column by concatenating 'Pickup Borough' and 'Dropoff_Borough'
    rideshare_df =rideshare_pickup_dropoff_df.withColumn("Route", concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough")))
    
    route_profits = rideshare_df.groupBy("Route").agg(sum("driver_total_pay").alias("total_profit")).orderBy(col("total_profit").desc())

    # Step 4: Limit to top 30
    top_30_routes = route_profits.limit(30)
    
    # Show the results
    top_pickup_locations.select("Pickup_Borough", "month", "trip_count").show(30, truncate=False)
    top_dropoff_locations.select("Dropoff_Borough", "month", "trip_count").show(30, truncate=False)
    top_30_routes.show(30, truncate=False)


    spark.stop()