import pandas as pd
import requests
import datetime
from dateutil import parser
import co2_converter_common as ccc
from dataiku.customrecipe import get_recipe_config

try:
    from pandas import json_normalize
except:
    from pandas.io.json import json_normalize

# Get input parameters:
input_df, output_dataset, columns_names = ccc.get_input_output()
coordinates = ccc.get_coordinates(input_df)
date_column_name = ccc.get_date_column_name(input_df)
extracted_geopoint, extracted_longitude, extracted_latitude = ccc.get_geopoint_column_names(columns_names)
user_selected_columns = get_recipe_config().get('user_selected_columns')

API_ENDPOINT = 'https://api.electricitymap.org/v3/power-breakdown/past-range'
API_TOKEN = ccc.get_api_token()

# Date is not in the future:
input_df[date_column_name] = pd.to_datetime(input_df[date_column_name], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
now = datetime.datetime.utcnow()
if max(input_df[date_column_name]).timestamp() > now.timestamp():
    raise Exception("Date is in the future. Please check your input dataset or use the CO2 forecast component.")

# Parse Geopoint to longitude and latitude:
input_df[extracted_geopoint] = input_df[coordinates].apply(lambda point: ccc.parse_wkt_point(point))
input_df[extracted_longitude] = input_df[extracted_geopoint].apply(lambda point: point[0])
input_df[extracted_latitude] = input_df[extracted_geopoint].apply(lambda point: point[1])

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
    chunked_dates = ccc.date_chunk(min_date_day, max_date_day, 10)

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

    output_df = ccc.merge_w_nearest_keys(input_df, data_to_return, date_column_name, 'e-mix_date_time', by=[extracted_latitude, extracted_longitude])

    # drop extracted columns (not needed):
    output_df.drop([extracted_geopoint, extracted_latitude, extracted_longitude], axis=1, inplace=True)

# ##################################### output ######################################

# Write output
output_dataset.write_with_schema(output_df)
