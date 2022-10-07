import pandas as pd
import requests
from io import StringIO
from pandas.io.json import json_normalize
import datetime
from dateutil import parser
from dataiku.customrecipe import get_recipe_config
import co2_converter_common as ccc

# Get input parameters:
input_df, output_dataset, columns_names = ccc.get_input_output()
api_provider = get_recipe_config().get('api_provider', 'RTE')
date_column_name = ccc.get_date_column_name(input_df)
consumption_column_name = ccc.get_consumption_column_name(input_df)
extracted_geopoint, extracted_longitude, extracted_latitude = ccc.get_geopoint_column_names(columns_names)

# API endpoint parameters conditions:
if api_provider == 'RTE':
    API_ENDPOINT = 'https://opendata.reseaux-energies.fr/api/records/1.0/download/'

elif api_provider == 'ElectricityMap':
    coordinates = ccc.get_coordinates(input_df)
    API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/past-range'
    API_TOKEN = ccc.get_api_token()
else:
    ValueError("No API provider was selected")


# Date is not in the future:
input_df[date_column_name] = pd.to_datetime(input_df[date_column_name], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
now = datetime.datetime.utcnow()
if max(input_df[date_column_name]).timestamp() > now.timestamp():
    raise Exception("Date is in the future. Please check your input dataset or use the CO2 forecast component.")

# setup request:
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
    output_df = ccc.merge_w_nearest_keys(input_df, data, date_column_name, 'co2_date_time')


# ##################################### ElectricityMap ######################################

if api_provider == 'ElectricityMap':

    input_df[extracted_geopoint] = input_df[coordinates].apply(lambda point: ccc.parse_wkt_point(point))
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

    # Drop and rename data columns before joining:
    column_names_to_keep = ['datetime', 'carbonIntensity', 'latitude', 'longitude']
    data = data[column_names_to_keep]
    data = data.dropna()

    data = data.rename(columns={
        "datetime": "co2_date_time", "carbonIntensity": "carbon_intensity", "latitude": extracted_latitude, "longitude": extracted_longitude}
    )

    # convert co2_date_time to datetime format:
    data['co2_date_time'] = pd.to_datetime(data['co2_date_time'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)

    # join on latitude, longitude and closest dates with input_df:
    output_df = ccc.merge_w_nearest_keys(input_df, data, date_column_name, 'co2_date_time', by=[extracted_latitude, extracted_longitude])

    # drop extracted columns (not needed):
    output_df.drop([extracted_geopoint, extracted_latitude, extracted_longitude], axis=1, inplace=True)

# ##################################### output ######################################

# compute co2emission:
# taux_co2 'standard' unit is in g/kwh, but I choose to convert the result into kgeqCO2:
output_df["co2_emission"] = (output_df[consumption_column_name] * output_df['carbon_intensity']) / 1000

# Write output
output_dataset.write_with_schema(output_df)
