import dataiku
import pandas as pd
import requests
from pandas.io.json import json_normalize
import datetime
from dateutil import parser
from co2_converter_common import date_chunk
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
from co2_converter_common import parse_wkt_point, get_geopoint_column_names, merge_w_nearest_keys

# Inputs
input_names = get_input_names_for_role('input_ds')
input_datasets = [dataiku.Dataset(name) for name in input_names]
input_dataset = input_datasets[0]

# Outputs
output_names = get_output_names_for_role('output_ds')
output_datasets = [dataiku.Dataset(name) for name in output_names]
output_dataset = output_datasets[0]

# Load input DSS dataset as a Pandas dataframe
input_df = input_dataset.get_dataframe()

# Load input Parameters:
date_column_name = get_recipe_config().get('date_column_name')
coordinates = get_recipe_config().get('coordinates')
API_ENDPOINT = 'https://api.electricitymap.org/v3/power-breakdown/past-range'
API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")

user_selected_columns = get_recipe_config().get('user_selected_columns')

# # Check if columns are in input dataset
columns_names = input_df.columns

extracted_geopoint, extracted_longitude, extracted_latitude = get_geopoint_column_names(columns_names)

# Parse Geopoint to longitude and latitude:
input_df[extracted_geopoint] = input_df[coordinates].apply(lambda point: parse_wkt_point(point))
input_df[extracted_longitude] = input_df[extracted_geopoint].apply(lambda point: point[0])
input_df[extracted_latitude] = input_df[extracted_geopoint].apply(lambda point: point[1])

# Input validation:

# ## Date Column:
if date_column_name not in columns_names:
    raise Exception("Not able to find the '%s' column" % date_column_name)

# ## coordinates column:
if coordinates not in columns_names:
    raise Exception("Not able to find the '%s' column" % coordinates)

# # Check input data validity:

# API token validity:
if API_TOKEN is None:
    raise Exception("No electricityMap API token found.")

# setup request
session = requests.session()

# GroupBy latitude, longitude to retrieve only one API call per coordinates:
dates_per_unique_latitude_longitudes = input_df.groupby([extracted_longitude, extracted_latitude])[date_column_name].unique()
data = pd.DataFrame()

# for each unique location location and with 10 days chunks (to avoid API limit):

for index_latitude_longitude, dates in enumerate(dates_per_unique_latitude_longitudes):

    # Parse dates:
    now = datetime.datetime.utcnow()
    max_date = parser.parse(str(dates.max()))
    min_date = parser.parse(str(dates.min()))

    # Get only the day from the dates:
    min_date_day = min_date.strftime("%Y-%m-%d")
    max_date_day = max_date.strftime("%Y-%m-%d")

    # As the API is limited to 10 days, I create chunks of dates:
    chunked_dates = date_chunk(min_date_day, max_date_day, 10)

    for index_chunked_dates in range(len(chunked_dates)):
        params = {
            'lat': dates_per_unique_latitude_longitudes.index[index_latitude_longitude][1],
            'lon': dates_per_unique_latitude_longitudes.index[index_latitude_longitude][0],
            'start': chunked_dates[index_chunked_dates][0],
            'end': chunked_dates[index_chunked_dates][-1]
        }

        # make request and create df dataframe with response from API:
        response = session.get(API_ENDPOINT, params=params, auth=('auth-token', API_TOKEN))
        response_json = response.json()
        chunked_data = json_normalize(response_json['data'])
        chunked_data['latitude'] = dates_per_unique_latitude_longitudes.index[index_latitude_longitude][1]
        chunked_data['longitude'] = dates_per_unique_latitude_longitudes.index[index_latitude_longitude][0]
        data = data.append(chunked_data, ignore_index=True)

    # Filtering on the columns selected by the user:
    data_to_return = data[['datetime', 'latitude', 'longitude']]
    for user_selected_column in user_selected_columns:
        column_list = [column for column in data.columns if column.startswith(user_selected_column)]
        user_selected_data = data[column_list]
        data_to_return = pd.concat((data_to_return, user_selected_data), axis=1)

    # rename datetime in e-mix_date_time:
    data_to_return = data_to_return.rename(columns={
        "datetime": "e-mix_date_time", "latitude": extracted_latitude, "longitude": extracted_longitude}
    )

    # join on date with input_df:
    # convert emix_date_time to datetime format:
    data_to_return['e-mix_date_time'] = pd.to_datetime(data_to_return['e-mix_date_time'])

    # convert DateColName to datetime format:
    input_df[date_column_name] = pd.to_datetime(input_df[date_column_name])

    output_df = merge_w_nearest_keys(input_df, data_to_return, date_column_name, 'e-mix_date_time', by=[extracted_latitude, extracted_longitude])

    # drop extracted columns (not needed):
    output_df.drop([extracted_geopoint, extracted_latitude, extracted_longitude], axis=1, inplace=True)

# ##################################### output ######################################

# Write output
output_dataset.write_with_schema(output_df)
