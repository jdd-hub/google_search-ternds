"""
Project: pytrends - https://pypi.org/project/pytrends/

Description: Unofficial Google Trends API.

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
does not respond in a timely manner.  By default requests do not time out unless a 
timeout is explicitly set. 

Connect: the connect timeout is the number of seconds requests will wait for the client to
establish a connection to a remote machine call on the socket. Best practice is to set a timeout
slightly larger than a multiple of 3. This is the default TCP packet retransmission window. 

Read: once the client has connected to the server and sent the HTTP request the read timeout is 
the number of seconds the client will wait for the server to respond. The number of seconds the 
client will wait between bytes sent from the server. 99.9% of cases, this is the time before the 
server sends the first byte.  

https://requests.readthedocs.io/en/master/user/advanced/#timeouts
"""
# The keyword list collection can take up to 5 keywords.
kw_list = ["covid"]


# GET DATA FOR INTEREST OVERTIME

def get_interest_over_time(month):

    # Set value for time_frame.
    time_frame = "today 1-m"

    if month == 1:
        time_frame = "today 1-m"
    elif month == 3:
        time_frame = "today 3-m"

    # Define the parameters for the payload.
    pytrends.build_payload(kw_list, cat=0, timeframe=time_frame, geo='GB', gprop='')

    # Execute the payload request.
    pd_iot = pytrends.interest_over_time()

    # Remove index.
    pd_iot.reset_index(inplace=True)

    # Rename API Columns.
    pd_iot.rename(columns={'date': 'Date', 'covid': 'Value'}, inplace=True)

    # Add additional columns required for the report.
    pd_iot["Label"] = pd_iot["Value"]

    # Set value for days label.
    if month == 1:
        pd_iot["Range"] = "Last-30-Days"
    elif month == 3:
        pd_iot["Range"] = "Last-90-Days"

    # Sort the DataFrame.
    pd_iot.sort_values(by=["Date"], inplace=True, ascending=True)

    # Finally return the processed payload as a DateFrame.
    return pd_iot[["Date", "Value", "Label", "Range"]]


# GET DATA FOR RELATED SEARCH TERMS

def get_related_queries(month):

    # Set value for time_frame.
    time_frame = "today 1-m"

    if month == 1:
        time_frame = "today 1-m"
    elif month == 3:
        time_frame = "today 3-m"

    # Define the parameters for the payload.
    pytrends.build_payload(kw_list, cat=0, timeframe=time_frame, geo='GB', gprop='')

    # Execute the payload request.
    dict_srch = pytrends.related_queries()

    # Convert dictionary into DataFrame.
    pd_srch = pd.DataFrame(dict_srch["covid"]["top"])

    # Rename API Columns.
    pd_srch.rename(columns={'query': 'Keyword', 'value': 'Value'}, inplace=True)

    # Add additional columns required for the report.

    # Set value for days label.
    if month == 1:
        pd_srch["Range"] = "Last-30-Days"
    elif month == 3:
        pd_srch["Range"] = "Last-90-Days"

    # Sort the DataFrame.
    pd_srch.sort_values(by=["Value"], inplace=True, ascending=False)

    # Finally return the processed payload as a DateFrame.
    return pd_srch


# GET DATA FOR INTEREST BY REGION

def get_interest_by_region(month):

    # Set value for time_frame.
    month = month

    time_frame = "today 1-m"

    if month == 1:
        time_frame = "today 1-m"
    elif month == 3:
        time_frame = "today 3-m"

    # Define the parameters for the payload.
    pytrends.build_payload(kw_list, cat=0, timeframe=time_frame, geo='GB', gprop='')

    # Execute the payload request.
    pd_ibr = pytrends.interest_by_region(resolution='GB', inc_low_vol=True, inc_geo_code=False)

    # Remove index.
    pd_ibr.reset_index(inplace=True)

    # Rename API Columns.
    pd_ibr.rename(columns={'geoName': 'Region', 'covid': 'Value'}, inplace=True)

    # Add additional columns required for the report.
    pd_ibr["Country"] = "United Kingdom"
    pd_ibr["Label"] = pd_ibr["Value"]

    # Set value for days label.
    if month == 1:
        pd_ibr["Range"] = "Last-30-Days"
    elif month == 3:
        pd_ibr["Range"] = "Last-90-Days"

    # Change column order.
    pd_ibr = pd_ibr[['Country', 'Region', 'Value', 'Label', 'Range']]

    # Sort the DataFrame.
    pd_ibr.sort_values(by=["Value"], inplace=True, ascending=False)

    # Finally return the processed payload as a DateFrame.
    return pd_ibr


# CONCAT BOTH DATAFRAMED PAYLOADS
def concat_payloads(pd_payload_one, pd_payload_two):

    # Concat the collection into a single DataFrame.
    # Finally return the processed payload as a DateFrame.
    return pd.concat([pd_payload_one, pd_payload_two])


def create_unique_directory():

    # Create a unique directory based on current date and time stamp.
    # Stores payloads received at the end of the data pipeline.

    # Map the individual date time elements.
    year, month, day, hour, min, sec = map(int, time.strftime("%Y %m %d %H %M %S").split())

    # Create a unique directory name.
    unique_directory = str(year) + str(month) + str(day) + "_" + str(hour) + str(min) + str(sec)

    # Create the path for the new unique directory.
    full_path = os.path.join(parent_directory, unique_directory)

    # Create the directory.
    os.mkdir(full_path)

    # Finally return the full path to the new directory.
    return full_path + "/"


# =====================================================================================================================

# 1. CREATE THE DIRECTORY.

# Set the root for the unique directory locations.
parent_directory = '/Users/julianmuscatdoublesin/PycharmProjects/PythonLibrary/ds_tableau_public/'

# Call the function that creates the unique directory for the current session.
directory = create_unique_directory()

# Display returned path for verification.
print("")
print("New Folder Created: " + directory)

# 2. GET INTEREST OVER TIME

# Get 30-day payload.
pd_iot_thirty = get_interest_over_time(1)

# Display returned payload for verification.
print("")
print("Interest Over Time For The Last 30-days:")
print(pd_iot_thirty)

# Get 90-day payload.
pd_iot_ninety = get_interest_over_time(3)

# Display returned payload for verification.
print("")
print("Interest Over Time For The Last 90-days:")
print(pd_iot_ninety)

# Concat both 30 and 90-day payloads into a single DataFrame.
pd_iot_concat = concat_payloads(pd_iot_thirty, pd_iot_ninety)

# Display the final output for verification.
print("")
print("Interest Over Time Combined:")
print(pd_iot_concat)

# Finally store the combined payloads into a CSV file.
filename = 'multiTimeline.csv'
pd_iot_concat.to_csv(directory + filename, index=False, header=True)

# Display conformation for storage operation. The final step of the process.
print("")
print("Stored Interest Over Time Payload:")
print("Filename: " + filename)

# 3. GET INTEREST BY REGION

# Get 30-day payload.
pd_ibr_thirty = get_interest_by_region(1)

# Display returned payload for verification.
print("")
print("Regional Interest Over Time For The Last 30-days:")
print(pd_ibr_thirty)

# Get 90-day payload.
pd_ibr_ninety = get_interest_by_region(3)

# Display returned payload for verification.
print("")
print("Regional Interest Over Time For The Last 90-days:")
print(pd_ibr_ninety)

# Concat both 30 and 90-day payloads into a single DataFrame.
pd_ibr_concat = concat_payloads(pd_ibr_thirty, pd_ibr_ninety)

# Display the final output for verification.
print("")
print("Regional Interest Over Time Combined:")
print(pd_ibr_concat)

# Finally store the combined payloads into a CSV file.
filename = 'geoMap.csv'
pd_ibr_concat.to_csv(directory + filename, index=False, header=True)

# Display conformation for storage operation. The final step of the process.
print("")
print("Stored Regional Interest Over Time Payload:")
print("Filename: " + filename)

# 4. GET RELATED SEARCH TERMS

# Get 30-day payload.
pd_srch_thirty = get_related_queries(1)

# Display returned payload for verification.
print("")
print("Top Related Search Terms The Last 30-days:")
print(pd_srch_thirty)

# Get 90-day payload.
pd_srch_ninety = get_related_queries(3)

# Display returned payload for verification.
print("")
print("Top Related Search Terms The Last 90-days:")
print(pd_srch_ninety)


# Concat both 30 and 90-day payloads into a single DataFrame.
pd_srch_concat = concat_payloads(pd_srch_thirty, pd_srch_ninety)

# Display returned payload for verification.
print("")
print("Top Related Search Terms Combined:")
print(pd_srch_concat)

# Finally store the combined payloads into a CSV file.
filename = 'relatedQueries.csv'
pd_srch_concat.to_csv(directory + filename, index=False, header=True)

# Display conformation for storage operation. The final step of the process.
print("")
print("Stored Related Search Terms Payload:")
print("Filename: " + filename)