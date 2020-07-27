"""
Project: pytrends - https://pypi.org/project/pytrends/

Description: Unoffical Google Trends API.

provides a simple interface for automating the download of Google Trends data.

Changes need to be made to the API every time Google makes changes to their backend.

Installation:

Go to the Python terminal and execute to following code:
--pip install pytrends

Requirements:

The module was written to work with both Python 2.7+ and Python 3.4+ interpreters.

Requires: Requests, lxml, Pandas packages. Checks are made automatically during the the process
for the availability of these modules.

"""

# Import the Python Google Search Trends API Package.
from pytrends.request import TrendReq
import pandas as pd
import time
import os

# Connecting to Google.
pytrends = TrendReq(hl='en-UK', tz=0)

# Alternative proxy connection if you are blocked due to Google's limit:
"""
pytrends = TrendReq(hl='en-US', tz=0, timeout=(10,25), proxies=['https://34.203.233.13:80',],
                    retries=2, backoff_factor=0.1, requests_args={'verify':False})

Timeout: Request to external servers should have a time attached, in case the server
does not respond in a timely manner.  By defualtm requests do not time out unless a 
timeout is explicitly set. 

Connect: the connect timeout is the number of seconds requests will wait for the client to
establish a connection to a remote machine call on the socket. Best practice is to set a timeout
slightly larger than a multiple of 3. This is the default TCP packet retransmission window. 

Read: once the client has connected to the server and sent the HTTP requestm the read timeout is 
the number of seconds the client will wait for the server to respond. The number of seconds the 
client will wait between bytes sent from the server. 99.9% of cases, this is the time before the 
server sends the first byte.  

https://requests.readthedocs.io/en/master/user/advanced/#timeouts
"""
# The keyword list collection can take up to 5 keywords.
kw_list = ["covid"]

# The Paylod - (Requester).



# GET DATA FOR INTEREST OVERTIME

# Define the parameters for the payload requester for 30-days.
pytrends.build_payload(kw_list, cat=0, timeframe='today 1-m', geo='GB', gprop='')

# Execute the payload request.
pd_iot_thirty_days = pytrends.interest_over_time()

# Remove index.
pd_iot_thirty_days.reset_index(inplace=True)

# Rename API Columns.
pd_iot_thirty_days.rename(columns={'date': 'Date', 'covid': 'Value'}, inplace=True)

# Add additional columns required for the report.
pd_iot_thirty_days["Label"] = pd_iot_thirty_days["Value"]
pd_iot_thirty_days["Range"] = "Last-30-Days"

# Sort pd_iot_thirty_days By Date
pd_iot_thirty_days.sort_values(by=["Date"], inplace=True, ascending=True)

# Print returned output for the pd_iot_thirty_days payload request.
print("")
print("Interest Over Time For The Last 30-days:")
print(pd_iot_thirty_days[["Date", "Value", "Label", "Range"]])



# Define the parameters for the payload requester for 90-days.
pytrends.build_payload(kw_list, cat=0, timeframe='today 3-m', geo='GB', gprop='')

# Execute the payload request.
pd_iot_ninety_days = pytrends.interest_over_time()

# Remove index.
pd_iot_ninety_days.reset_index(inplace=True)

# Rename API Columns.
pd_iot_ninety_days.rename(columns={'date': 'Date', 'covid': 'Value'}, inplace=True)

# Add additional columns required for the report.
pd_iot_ninety_days["Label"] = pd_iot_ninety_days["Value"]
pd_iot_ninety_days["Range"] = "Last-90-Days"

# Sort pd_iot_thirty_days By Date
pd_iot_ninety_days.sort_values(by=["Date"], inplace=True, ascending=True)

# Print returned output for the pd_iot_ninety_days payload request.
print("")
print("Interest Over Time For The Last 90-days:")
print(pd_iot_ninety_days[["Date", "Value", "Label", "Range"]])



# Merge Interest Over Time DataFrame 30 and 90 days together.
pd_iot_collection = [pd_iot_thirty_days[["Date", "Value", "Label", "Range"]], pd_iot_ninety_days[["Date", "Value", "Label", "Range"]]]

# Concat the collection into a single DataFrame.
pd_iot = pd.concat(pd_iot_collection)

# Print returned final output DataFrame.
print("")
print("Interest Over Time Combined:")
print(pd_iot)

# CREATE A UNIQUE DIRECTORY BASED ON DATE TIME STAMPS TO STORE PAYLOADS RECEIVED AT THE END OF THE DATA PIPELINE.

# Map the individual date time elements.
year, month, day, hour, min, sec = map(int, time.strftime("%Y %m %d %H %M %S").split())

# Create a unique directory name to store the payload.
unique_directory = str(year) + str(month) + str(day) + "_" + str(hour) + str(min) + str(sec)

# Create the directory.
parent_directory = '/Users/julianmuscatdoublesin/PycharmProjects/PythonLibrary/ds_tableau_public/'

# Create a unique directory name to store the payload.
unique_directory = str(year) + str(month) + str(day) + "_" + str(hour) + str(min) + str(sec)

# Create the path for the new unique directory.
full_path = os.path.join(parent_directory, unique_directory)

# Create the directory.
os.mkdir(full_path)

print("")
print("New Folder Created: "+ full_path)

#STORE PAYLOADS RECEIVED AT THE END OF THE DATA PIPELINE. into a CSV file.
filename = 'multiTimeline.csv'
pd_iot.to_csv(full_path + "/" + filename, index=False, header=True)

print("")
print("Stored Interest Over Time Payload:")
print("Filename: " + filename)



# GET DATA FOR INTEREST BY REGION

# Define the parameters for the payload requester for 30-days.
pytrends.build_payload(kw_list, cat=0, timeframe='today 1-m', geo='GB', gprop='')

# Execute the payload request.
pd_ibr_thirty_days = pytrends.interest_by_region(resolution='GB', inc_low_vol=True, inc_geo_code=False)

# Remove index.
pd_ibr_thirty_days.reset_index(inplace=True)

# Rename API Columns.
pd_ibr_thirty_days.rename(columns={'geoName': 'Region', 'covid': 'Value'}, inplace=True)

# Add additional columns required for the report.
pd_ibr_thirty_days["Country"] = "United Kingdom"
pd_ibr_thirty_days["Label"] = pd_ibr_thirty_days["Value"]
pd_ibr_thirty_days["Range"] = "Last-30-Days"

# Change column order.
pd_ibr_thirty_days = pd_ibr_thirty_days[['Country', 'Region', 'Value', 'Label', 'Range']]

# Sort pd_iot_thirty_days By Date
pd_ibr_thirty_days.sort_values(by=["Value"], inplace=True, ascending=False)

# Print returned output for the pd_ibr_thirty_days payload request.
print("")
print("Sub-Region Interest Over Time For The Last 30-days:")
print(pd_ibr_thirty_days)



# Define the parameters for the payload requester for 90-days.
pytrends.build_payload(kw_list, cat=0, timeframe='today 3-m', geo='GB', gprop='')

# Execute the payload request.
pd_ibr_ninety_days = pytrends.interest_by_region(resolution='GB', inc_low_vol=True, inc_geo_code=False)

# Remove index.
pd_ibr_ninety_days.reset_index(inplace=True)

# Rename API Columns.
pd_ibr_ninety_days.rename(columns={'geoName': 'Region', 'covid': 'Value'}, inplace=True)

# Add additional columns required for the report.
pd_ibr_ninety_days["Country"] = "United Kingdom"
pd_ibr_ninety_days["Label"] = pd_ibr_ninety_days["Value"]
pd_ibr_ninety_days["Range"] = "Last-90-Days"

# Change column order.
pd_ibr_ninety_days = pd_ibr_ninety_days[['Country', 'Region', 'Value', 'Label', 'Range']]

# Sort pd_iot_thirty_days By Date
pd_ibr_ninety_days.sort_values(by=["Value"], inplace=True, ascending=False)

# Print returned output for the pd_ibr_ninety_days payload request.
print("")
print("Sub-Region Interest Over Time For The Last 90-days:")
print(pd_ibr_ninety_days)



# Merge Interest By Region DataFrame 30 and 90 days together.
pd_ibr_collection = [pd_ibr_thirty_days, pd_ibr_ninety_days]

# Concat the collection into a single DataFrame.
pd_ibr = pd.concat(pd_ibr_collection)

# Print returned final output DataFrame.
print("")
print("Interest By Region Combined:")
print(pd_ibr)

#STORE PAYLOADS RECEIVED AT THE END OF THE DATA PIPELINE. into a CSV file.
filename = 'geoMap.csv'
pd_ibr.to_csv(full_path + "/" + filename, index=False, header=True)

print("")
print("Stored Interest By Region Payload:")
print("Filename: " + filename)



# GET DATA FOR RELATED SEARCH TERMS

# Define the parameters for the payload requester for 30-days.
pytrends.build_payload(kw_list, cat=0, timeframe='today 1-m', geo='GB', gprop='')

# Execute the payload request.
dict_srch_thirty_days = pytrends.related_queries()

# Convert dictionary: dict_srch_thirty_days into DataFrame: pd_srch_thirty_days.
pd_srch_thirty_days = pd.DataFrame(dict_srch_thirty_days["covid"]["top"])

# Rename API Columns.
pd_srch_thirty_days.rename(columns={'query': 'Keyword', 'value': 'Value'}, inplace = True)

# Add additional columns required for the report.
pd_srch_thirty_days["Range"] = "Last-30-Days"

# Sort pd_iot_thirty_days By Date
pd_srch_thirty_days.sort_values(by=["Value"], inplace=True, ascending=False)

# Print returned output for the pd_iot_thirty_days payload request.
print("")
print("Top Related Search Terms The Last 30-days:")
print(pd_srch_thirty_days)



# Define the parameters for the payload requester for 90-days.
pytrends.build_payload(kw_list, cat=0, timeframe='today 3-m', geo='GB', gprop='')

# Execute the payload request.
dict_srch_ninety_days = pytrends.related_queries()

# Convert dictionary: dict_srch_ninety_days into DataFrame: pd_srch_ninety_days.
pd_srch_ninety_days = pd.DataFrame(dict_srch_ninety_days["covid"]["top"])

# Rename API Columns.
pd_srch_ninety_days.rename(columns={'query': 'Keyword', 'value': 'Value'}, inplace = True)

# Add additional columns required for the report.
pd_srch_ninety_days["Range"] = "Last-90-Days"

# Sort pd_iot_thirty_days By Date
pd_srch_ninety_days.sort_values(by=["Value"], inplace=True, ascending=False)

# Print returned output for the pd_iot_ninety_days payload request.
print("")
print("Top Related Search Terms The Last 90-days:")
print(pd_srch_ninety_days)



# Merge Related Search Terms DataFrame 30 and 90 days together.
pd_srch_collection = [pd_srch_thirty_days, pd_srch_ninety_days]

# Concat the collection into a single DataFrame.
pd_srch = pd.concat(pd_srch_collection)

# Print returned final output DataFrame.
print("")
print("Top Related Search Terms Combined:")
print(pd_srch)

#STORE PAYLOADS RECEIVED AT THE END OF THE DATA PIPELINE. into a CSV file.
filename = 'relatedQueries.csv'
pd_srch.to_csv(full_path + "/" + filename, index=False, header=True)

print("")
print("Stored Related Search Terms Payload:")
print("Filename: " + filename)