#!/usr/bin/env python
# import libraries
import requests
from bs4 import BeautifulSoup
from pyspark import SparkFiles
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
import time
from time import strftime
from time import gmtime
import sys


start = time.time()
# Spark_context represents interface to a running spark cluster manager
sc = SparkContext('local')
# The entry point to programming Spark with the Dataset and DataFrame API.

spark = SparkSession(sc)

print('Welcome')

# In this loop we enter the year and month we want to extract. 
# If the year or the month is bad entry the code will ask to enter it again.
# Enter ex to exit from python
while True:
    
    year = input('Please enter any year between 2009 and 2021 to extract or ex to exit: ')
    if year == 'ex':
                   sys.exit()
    try:
        value = int(year)
    except ValueError:
        print("Oops!  That was no valid year.  Try again...")
        continue

    if int(year) in range(2009,2022):
        while True:
            month = input('Please enter any month between 1 and 12 to extract or ex to exit: ').zfill(2)
            if month == 'ex':
                   sys.exit()
            try:
                value = int(month)
            except ValueError:
                print("Oops!  That was no valid month.  Try again...")
                continue
            if int(month) in range(1,13):
                        break
            else:
                print("Oops!  That was no valid month.  Try again...")

        break
    else:
        print("Oops!  That was no valid year.  Try again...")


    

    
def get_url(year,month):

    """
 
    This function inspect the TLC Trip Record Data website, 
    and get the URL of the dataset we want to extract .
  
    Parameters:
    year (str): The year that we interested in
    month (str): The month that we interested in
    Returns:
    URL: Dataset URL
  
    """
    
    dest = f'{year}-{month}'
    # get()  method sends a GET request to the specified url
    r = requests.get('https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page')
    # pulling data out of HTML and XML files
    soup = BeautifulSoup(r.text, 'lxml')
    #extracting all the URLs found within a page’s ‘a’
    for link in soup.find_all('a'):
        # check if the url has the exact year and month we interested in and return it
        if dest  in link.get('href') and 'yellow' in link.get('href'):
            return (link.get('href'))
# call the function get_url 
url = get_url(year,month)
#Add a file to be downloaded 
spark.sparkContext.addFile(url)
# read the dataset and save it as dataframe
df = spark.read.csv(SparkFiles.get(url.split('trip+data/')[1]), header=True, inferSchema= True)
# calculate percentile 0.95
percent_95=df.selectExpr('percentile_approx(trip_distance, 0.95)').collect()
percent_95=percent_95[0][:][0]
#filter the dataframe depend on the trip distance that exceed the percentile 0.95
df_filter=df.filter(df.trip_distance >=percent_95)
# select the columns we intrested in 
df_filter=df_filter.select("PULocationID","DOLocationID","trip_distance")
# save url that provides us the match zone id with zone name and associated borough name
url_zone = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
#Add a file to be downloaded 

spark.sparkContext.addFile(url_zone)
# read the dataset and save it as dataframe
df_zone = spark.read.csv(SparkFiles.get("taxi+_zone_lookup.csv"), header=True, inferSchema= True)
# merge the 2 dataframes
df_zone= df_zone.join(df_filter,df_zone.LocationID ==  df_filter.DOLocationID,"inner")

# groupBy the dataframe and calculate the number of trips
# sort the results from the greatest number of trips to the smallest number of trips
# return the greatest 10 number of trips
df_zone.groupBy("Borough",'Zone').count().sort(desc('count')).select(F.col("count").alias("Trips"),"Borough",'Zone').show(10)


end = time.time()
tiempo = end - start
# return the execution time. 
print(f'The execution time of the code is %s' % strftime("%H:%M:%S", gmtime(tiempo)))

