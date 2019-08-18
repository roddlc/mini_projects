# packages
import pandas as pd
import numpy as np
#from sodapy import Socrata
import os
import json, requests
from datetime import datetime


# display all columns and rows
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# ===============================================================================================
# pull rideshare data from Chicago's open data API by week


# lists of start and end dates to search
list_of_start_dates = ["01T00:00:00.000", "08T00:00:00.000", "15T00:00:00.000", "22T00:00:00.000", "29T00:00:00.000"]
list_of_end_dates = ["07T23:45:00.000", "14T23:45:00.000", "21T23:45:00.000", "28T23:45:00.000", "30T23:45:00.000"]


# printing date spans
for start, end in zip(list_of_start_dates, list_of_end_dates):
    print("start: " + start + " --- " + "end: " + end)


# since there are millions of records in the month of November 2018, this for loop will iterate over
# individual weeks to make the process more manageable. The result will be five CSV files containing
# the data for the span of days defined in the start and end dates listed above
for start, end in zip(list_of_start_dates, list_of_end_dates):

    # define the URL to query the API for the count of records between dates
    # this will allow the search URL to pull all the records for the time span
    count_request = f"https://data.cityofchicago.org/resource/m6dm-c72p.json?$select=count(trip_start_timestamp)\
    &$where=trip_start_timestamp%20between%20%272018-11-{start}%27%20and%20%272018-11-{end}%27"

    # use python's requests packages to make the request
    count_json = requests.get(count_request)

    # print count_json to see if request succeeded; if it succeeded response == 200
    print(count_json)

    # extract the count value from the json
    count = [value for value in count_json.json()[0].values()]
    count = pd.to_numeric(count[0])

    # define search url (includes start, end, and count) to request the week's data
    search = f"https://data.cityofchicago.org/resource/m6dm-c72p.json?$where=trip_start_timestamp between '2018-11-{start}'\
    and '2018-11-{end}'&$limit={count}"

    # request data (will be received in json format)
    r = requests.get(search)

    # convert json to dataframe
    tmp_df = pd.DataFrame(r.json())

    # remove unnecessary cols to save some memory
    tmp_df = tmp_df.drop(["dropoff_centroid_location", "pickup_centroid_location"], axis = 1)

    # convert "trip_end_timestamp" and "trip_start_timestamp" to datetime64 object
    tmp_df["trip_end_timestamp"] = pd.to_datetime(tmp_df["trip_end_timestamp"])
    tmp_df["trip_start_timestamp"] = pd.to_datetime(tmp_df["trip_start_timestamp"])

    # create two new columns that contain the day of the week and hour of the day for the "trip_start_timestamp"
    tmp_df["trip_start_day_of_week"] = tmp_df["trip_start_timestamp"].dt.strftime("%A")
    tmp_df["trip_start_hour_of_day"] = tmp_df["trip_start_timestamp"].dt.strftime("%H")

    # write to csv
    tmp_df.to_csv(f"data/rideshare_nov{start[:2]}_to_nov{end[:2]}.csv", index = False)

    # print to track progress
    print(f"File beginning on Nov {start[:2]} has finished")

    # since each week takes several minutes, have computer say it as well
    os.system(f'say "File beginning on November {start[:2]} has finished"')


#END