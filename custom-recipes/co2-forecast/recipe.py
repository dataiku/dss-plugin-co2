import pandas as pd
import requests
from pandas.io.json import json_normalize
from dataiku.customrecipe import get_recipe_config
import co2_converter_common as ccc

# Get input parameters:
input_df, output_dataset, columns_names = ccc.get_input_output()
coordinates = ccc.get_coordinates(input_df)
api_provider = get_recipe_config().get('api_provider')
extracted_geopoint, extracted_longitude, extracted_latitude = ccc.get_geopoint_column_names(columns_names)

# API endpoint:
API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/forecast'
API_TOKEN = ccc.get_api_token()

# setup request:
session = requests.session()

# Parse Geopoint to longitude and latitude:
input_df[extracted_geopoint] = input_df[coordinates].apply(lambda point: ccc.parse_wkt_point(point))
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

# Drop and rename data columns before joining:
column_names_to_keep = ['datetime', 'carbonIntensity', 'latitude', 'longitude', coordinates]
data = data[column_names_to_keep]
data = data.dropna()

data = data.rename(columns={"datetime": "co2_date_time", "carbonIntensity": "carbon_intensity"})

# Write output
output_dataset.write_with_schema(data)
