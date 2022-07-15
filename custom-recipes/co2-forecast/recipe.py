
import dataiku
import pandas as pd
import requests
from pandas.io.json import json_normalize
from dataiku.customrecipe import get_input_names_for_role, get_recipe_config, get_output_names_for_role
from co2_converter_common import parse_wkt_point, get_geopoint_column_names

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


API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/forecast'
API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")
coordinates = get_recipe_config().get('coordinates')

# Input validation:

# # Check if columns are in input dataset
columns_names = input_df.columns

# ## Coordinates column:

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


extracted_geopoint, extracted_longitude, extracted_latitude = get_geopoint_column_names(columns_names)
# Parse Geopoint to longitude and latitude:
input_df[extracted_geopoint] = input_df[coordinates].apply(lambda point: parse_wkt_point(point))
input_df[extracted_longitude] = input_df[extracted_geopoint].apply(lambda point: point[0])
input_df[extracted_latitude] = input_df[extracted_geopoint].apply(lambda point: point[1])

# GroupBy latitude, longitude to retrieve only one API call per coordinates:
unique_latitudes_longitudes = input_df[[extracted_longitude, extracted_latitude]].drop_duplicates().reset_index(drop=True)
data = pd.DataFrame()

# for each unique location location:

for index, unique_latitude_longitude in unique_latitudes_longitudes.iterrows():
    params = {
        'lat': unique_latitudes_longitudes[extracted_latitude][index],
        'lon': unique_latitudes_longitudes[extracted_longitude][index],
    }

    # make request and create df dataframe with response from API:
    response = session.get(API_ENDPOINT, params=params, auth=('auth-token', API_TOKEN))
    response_json = response.json()
    data_buffer = json_normalize(response_json["forecast"])
    data_buffer['latitude'] = unique_latitudes_longitudes[extracted_latitude][index]
    data_buffer['longitude'] = unique_latitudes_longitudes[extracted_longitude][index]
    data_buffer[coordinates] = input_df[coordinates][index]

    data = data.append(data_buffer, ignore_index=True)

# Drop and rename df columns before joining
# keep only on date_heure and taux_co2 from df:
column_names_to_keep = ['datetime', 'carbonIntensity', 'latitude', 'longitude', coordinates]
data = data[column_names_to_keep]
data = data.dropna()

data = data.rename(columns={"datetime": "co2_date_time", "carbonIntensity": "carbon_intensity"})

# Write output
output_dataset.write_with_schema(data)
