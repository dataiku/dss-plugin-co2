
import dataiku
import pandas as pd
import requests
from io import StringIO
from pandas.io.json import json_normalize
import datetime
from dateutil import parser
from co2_converter_common import date_chunk, parse_wkt_point, merge_w_nearest_keys, get_geopoint_column_names
from dataiku.customrecipe import get_input_names_for_role, get_recipe_config, get_output_names_for_role

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
api_provider = get_recipe_config().get('api_provider')
date_column_name = get_recipe_config().get('date_column_name')
consumption_column_name = get_recipe_config().get('consumption_column_name')

# API endpoint parameters conditions:
if api_provider == 'RTE':
    API_ENDPOINT = 'https://opendata.reseaux-energies.fr/api/records/1.0/download/'

if api_provider == 'ElectricityMap':
    API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/past-range'
    API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")
    coordinates = get_recipe_config().get('coordinates')

# Input validation:

# # Check if columns are in input dataset
columns_names = input_df.columns

# ## Date Column:
if date_column_name not in columns_names:
    raise Exception("Not able to find the '%s' column" % date_column_name)

# ## Consumption Column:
if consumption_column_name not in columns_names:
    raise Exception("Not able to find the '%s' column" % consumption_column_name)

# Date is not in the future:
input_df[date_column_name] = pd.to_datetime(input_df[date_column_name], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
now = datetime.datetime.utcnow()
if max(input_df[date_column_name]).timestamp() > now.timestamp():
    raise Exception("Date is in the future. Please check your input dataset or use the CO2 forecast component.")

# ## Coordinates column:
if api_provider == 'ElectricityMap':
    if coordinates not in columns_names:
        raise Exception("Not able to find the '%s' column" % coordinates)

    # # Check if geopoint column has no empty values:
    if input_df[coordinates].isnull().values.any():
        raise ValueError('Empty coordinates found. Please check your input dataset.')

    # # Check if geopoint looks like a valid geopoint:
    if not input_df[coordinates].str.startswith('POINT(').all():
        raise ValueError('Invalid coordinates. Geopoint format required: POINT(longitude latitude)')

# # Check input data validity:
    # API token validity:
    if API_TOKEN is None:
        raise Exception("No electricityMap API token found.")

extracted_geopoint, extracted_longitude, extracted_latitude = get_geopoint_column_names(columns_names)

# setup request
session = requests.session()


# ##################################### RTE ######################################
if api_provider == 'RTE':
    # Get MinDate and MaxDate to compute the number of rows to be requested:
    min_date = min(input_df[date_column_name])
    max_date = max(input_df[date_column_name])

    # Parameters for API call:
    params = {
        'dataset': 'eco2mix-national-cons-def',
        'timezone': 'Europe/Paris',
        'q': 'date_heure:['+str(min_date.isoformat())+' TO '+str(max_date.isoformat())+']'
    }

    # make request and create df dataframe with response from API:
    response = session.get(API_ENDPOINT, params=params)
    data_buffer = StringIO(str(response.content, 'utf-8'))
    data = pd.read_csv(data_buffer, sep=';')

    # keep only date_heure and taux_co2 from df:
    column_names_to_keep = ['date_heure', 'taux_co2']
    data = data[column_names_to_keep]
    data = data.dropna()

    # rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    data = data.rename(columns={"date_heure": "co2_date_time", "taux_co2": "carbon_intensity"})

    # convert co2_date_time to datetime format:
    data['co2_date_time'] = pd.to_datetime(data['co2_date_time'], utc=True)

    # join on closest dates with input_df:
    output_df = merge_w_nearest_keys(input_df, data, date_column_name, 'co2_date_time')


# ##################################### ElectricityMap ######################################

if api_provider == 'ElectricityMap':

    input_df[extracted_geopoint] = input_df[coordinates].apply(lambda point: parse_wkt_point(point))
    input_df[extracted_longitude] = input_df[extracted_geopoint].apply(lambda point: point[0])
    input_df[extracted_latitude] = input_df[extracted_geopoint].apply(lambda point: point[1])

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

    # Drop and rename df columns before joining

    # keep only date_heure and taux_co2 from df:
    column_names_to_keep = ['datetime', 'carbonIntensity', 'latitude', 'longitude']
    data = data[column_names_to_keep]
    data = data.dropna()

    # rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    # renaming latitude and longitude to extracted_latitude and extracted_longitude for join_asof
    data = data.rename(columns={
        "datetime": "co2_date_time", "carbonIntensity": "carbon_intensity", "latitude": extracted_latitude, "longitude": extracted_longitude}
    )

    # convert co2_date_time to datetime format:
    data['co2_date_time'] = pd.to_datetime(data['co2_date_time'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)

    # join on latitude, longitude and closest dates with input_df:
    output_df = merge_w_nearest_keys(input_df, data, date_column_name, 'co2_date_time', by=[extracted_latitude, extracted_longitude])

    # drop extracted columns (not needed):
    output_df.drop([extracted_geopoint, extracted_latitude, extracted_longitude], axis=1, inplace=True)

# ##################################### output ######################################

# compute co2emission:
# taux_co2 'standard' unit is in g/kwh, but I choose to convert the result into kgeqCO2:
output_df["co2_emission"] = (output_df[consumption_column_name] * output_df['carbon_intensity']) / 1000

# Write output
output_dataset.write_with_schema(output_df)
