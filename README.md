Key Achievements in Rideshare Data Analysis Project:

1. **Data Integration and Preprocessing**
   - Merged NYC rideshare data with taxi zone information using PySpark, enhancing the dataset with pickup and drop-off details.
   - Converted Unix timestamps to a readable date format, optimizing data for further analysis and ensuring seamless integration.

2. **Data Aggregation and Visualization**
   - Conducted monthly aggregation of trips, platform profits, and driver earnings using PySpark's groupby and aggregation functions.
   - Visualized data using Matplotlib, identifying key trends such as Uber's dominant market share and Lyft's financial challenges in NYC.

3. **Advanced Analytical Insights**
   - Identified top pickup and drop-off locations and the most profitable routes using groupby and window functions.
   - Analyzed average driver earnings, trip lengths, and detected anomalies in wait times, providing actionable insights for strategic decision-making.
  
**Task 1: Merging datasets:**
**1.1 & 1.2**

- Firstly, I load the rideshare_data.csv and taxi_zone_lookup.csv using the path and file provided in the assignment and convert them into dataframes.
- I tested with sample_data, and printed the results to verify that the data was loaded correctly.
- The aim of this task is to add both pickup and dropoff details to the rideshare_data.
- To achieve this, I joined the pickup details to the rideshare_data using pickup_location from rideshare_data and mapping it to the LocationID from taxi_zone_lookup data.
- Now, to add the dropoff details to rideshare_data, I make another join to the dataframe obtained from the last step; but this time joining rideshare_data to taxi_zone_lookup_data on dropoff_location and LocationID.
- For this task, I’m using the built-in ‘join’ function from PySpark.
-  Printed the new schema on console. (Screenshot attached for your reference)
  
**1.3**
- To convert Unix timestamp to “yyyy-MM-dd” format, I’ve used from_unixtime method from the Pyspark.sql library. This is used to convert unixtime into a string of provided format
- I did not convert the type from string to date as the only objective of the task is to convert unix time to “yyyy-MM-dd” format.

**Task 2: Aggregation on Data:**
**2.1**
• To count the no. of trips for each business in each month we group the data by business and month
and then count these values.
• I used groupby function for grouping.
• Then, I downloaded this data into a local cluster in csv format using write.option API from pyspark
dataframe.
• Then, I used this csv file to visualize using Matplotlib.
• I used Google colab for my visualizations.
**2.2**
• This was to calculate platforms profit for each business each month.
• To do this we group the data by business and month and then sum the profits using
rideshare_profit field.
• Then, I downloaded this data into a local cluster in csv format using write.option API from pyspark
dataframe.
• Then, I used this csv file to visualize using Matplotlib.
**2.3**
• This was to calculate drivers earnings for each business in each month.
• To do this we group the data by business and month and then sum the drivers earnings for each
group.
• Then, I downloaded this data into a local cluster in csv format using write.option API from pyspark
dataframe.
• Then, I used this csv file to visualize using Matplotlib.
**2.4**
• The data shows that uber has much more market share than Lyft.
• Uber is a profitable company during these months in NYC.
• Lyft is making huge losses in NYC in these months and the losses are increasing every month.
• Lyft should think about pivoting their strategies in NYC at least.
• Uber has been growing in these months and they need to continue doing what they’re doing.
**Task 3: Top-K Processing:**
**3.1 & 3.2**
• For this problem we needed to fetch top 5 pickup and drop-off locations for each month. To solve this, we needed to get the count of trips being picked-up/dropped-off from each borough every month.
• To achieve that, I used groupBy functions to make groups of months and trip counts for each borough that was used for pickup.
• Then I used a window function to classify the data by month and sort them in descending order of trip count.
• As we needed to show top 5 pickup locations for each month, I used the filter function to filter out top 5 results and display them.
• Similar approach was used for picking up top 5 drop-off locations for each month.
**3.3**
• To calculate 30 earnest routes, I first added a route column by concatenating pickup borough with dropoff borough.
• Then I grouped the whole data by making use of the above column by using groupby function.
• And then I calculated each routes profit by adding the driver_total_pay for every route.
• Then I sorted it in descending order and picked up the top 30 by using limit function.
**3.4**

From the results of this task, the following observations are made:
• Most rideshare trips are to and from Manhattan (both most pickups and most drop-offs are from
here)
• The most profitable route is within Manhattan (i.e., from Manhattan to Manhattan); it’s almost
double of the second most popular route
• Also, the most popular routes are within the same borough. (Manhattan to Manhattan, Brooklyn to
Brooklyn, Queens to Queens)
Potential Strategies:
• From the observations, it looks like that most people in NY are using rideshares for shorter
distances (within borough). We can maybe run some promos for users to use it for longer distances
• The company is earning the most from Manhattan and the demand is also huge. So encourage
more drivers to drive in Manhattan.
**Task 4: Average of Data:**
**4.1**
• The task is to calculate the average of driver earnings during different times of the day.
• For this, I grouped the results by time of the day and then calculated the average of the
driver_total_pay for each time of the day and printed the results.
• One method to calculate this was to sum driver_total_pay for a time of a day and then divide by the
count to calculate average.
• But I have used the aggregate (agg) and average function to calculate the same.
**4.2**
• With the same approach as in 4.1, firstly I grouped the results by time of the day and then
calculated the average of the trip length (average_trip_length).
**4.3**
• I joined the resultant dataframes from previous results based on time of the day .
• Then I used that to calculate average earning per mile.
• Average earning per mile is calculated as Total earning/Total miles.
**Task 5: Finding anomalies:**
**5.1**
• Firstly, I filtered the data based on month and choose only data from January.
• Then, I grouped the data by day and calculated average waiting time for each day.
• Then I sorted the data by day for visualization.
• I downloaded the dataframe to my own cluster in csv format and then to my local machine.
• I combined all csv files into one and used Google colab and Matplotlib for creating the histogram.
**5.2**
• The only day when the average waiting time exceed 300 seconds was 1st January.
**5.3**
• The average waiting time was the longest on 1st January as it is New years day which is a Holiday and a lot of people go out on that day.
• Since, people go out there’s also a lot of traffic which eventually increases wait time.
• Also, a lot of people drink on that day which increases the demand of cabs, in turn increasing the
average wait time.
**Task 6: Filtering Data:**
**6.1**
• In this task, we need to find the trip counts greater than 0 and less than 1000 for different 'Pickup_Borough' at different 'time_of_day'.
• To achieve this, we grouped the data by pickup_location and time_of_day and then counted each group.
**6.2**
• This task was to calculate the number of trips for each Pickup Borugh in the evening time.
• To do this, I filtered the data obtained from previous task and applied a filter to select “evening” as time of the day.
• Then we grouped by Pickup_borough and displayed the results.
**6.3**
• This task was to calculate the number of trips from Brooklyn to Staten Island.
• To do this, I applied two filters: one for pickup borough to be Brooklyn and second for dropoff borough to be Staten Island.
• Then I selected the required columns according to the question and displayed the results.
**Task 7: Routes Analysis:**
• I analysed top 10 popular routes and to do so in terms of the trip count, I first created a new column to
concatenate the Pickup_Zone and Dropoff_Zone into the format of ‘Pickup_Zone to Dropoff_Zone’.
• Then I aggregated the data to count the no. of trips taken for each unique route for both Uber and Lyft. I chose for both Uber and Lyft as it was required to show number of trips for both the companies along with the final count.
• Then I summed these counts to get a total count per route.
• In the final step, I determined the top 10 most popular routes by sorting them in descending order and selecting the top 10.
   
