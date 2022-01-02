# nyc_taxi
I used python language to solve this problem. 
First, I imported the libraries needed to solve this problem. Some of them are the third library, such as:
1- Requests: The requests module allows you to send HTTP requests using Python. The HTTP request returns a Response Object with all the response data (content, encoding, status, etc.). 

2- Beautiful Soup: A library makes it easy to scrape information from web pages. It sits atop an HTML or XML parser, providing Pythonic idioms for iterating, searching, and modifying the parse tree. 

3- Pyspark: it is a Python API for Spark that lets us analyze large-scale data.

Second, I extract the data by inspecting the website that contains the dataset by the libraries Requests and Beautiful Soup. Then, I read the data using Pyspark. After that, I calculated the percentile 0.95 of the trip distance and filtered the dataframe depending on the trip distance that exceeded the percentile 0.95. 

Finally, I did some aggregation functions to return the greatest 10 number of trips.

To run the code:

1- Download the nyc_taxi.py script

2- Open a terminal and type the word python, or python3 if you have both versions, followed by the script path, just like this:

python /"script_path"/nyc_taxi.py
