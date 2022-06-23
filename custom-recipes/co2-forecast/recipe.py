
import dataiku
import pandas as pd
import requests
from pandas.io.json import json_normalize
from dateutil import parser
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


API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/forecast'
API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")
coordinates = get_recipe_config().get('Coordinates')

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

# # Check if extracted_geopoint_longitude and extracted_geopoint_latitudes columns names are not already used:
if 'extracted_geopoint_longitude' not in columns_names or 'extracted_geopoint_longitude' not in columns_names:
    extracted_longitude = 'extracted_geopoint_longitude'
    extracted_latitude = 'extracted_geopoint_latitude'
else:
    extracted_longitude = 'extracted_geopoint_longitude_1'
    extracted_latitude = 'extracted_geopoint_latitude_1'

# setup request
r = requests.session()

# Converting geopoint to longitude and latitude to fit the API endpoint:
input_df["extracted_geopoint"] = input_df[coordinates].str.replace(r'[POINT()]', '', regex=True)
input_df["extracted_geopoint"] = input_df["extracted_geopoint"].str.split(" ", expand=False)
split_df = pd.DataFrame(input_df["extracted_geopoint"].tolist(), columns=[extracted_longitude, extracted_latitude])
input_df = input_df.drop(columns="extracted_geopoint")
input_df = pd.concat([input_df, split_df], axis=1)

# GroupBy latitude, longitude to retrieve only one API call per coordinates:
uniquelatlon = input_df[[extracted_longitude, extracted_latitude]].drop_duplicates().reset_index(drop=True)
df = pd.DataFrame()

# for each unique location location:

for index, x in uniquelatlon.iterrows():
    params = {
        'lat': uniquelatlon[extracted_latitude][index],
        'lon': uniquelatlon[extracted_longitude][index],
    }

    # make request and create df dataframe with response from API:
    response = r.get(API_ENDPOINT, params=params, auth=('auth-token', API_TOKEN))
    dictr = response.json()
    dfa = json_normalize(dictr["forecast"])
    dfa['latitude'] = uniquelatlon[extracted_latitude][index]
    dfa['longitude'] = uniquelatlon[extracted_longitude][index]
    dfa[coordinates] = input_df[coordinates][index]

    df = df.append(dfa, ignore_index=True)

# Drop and rename df columns before joining
# keep only on date_heure and taux_co2 from df:
col_list = ['datetime', 'carbonIntensity', 'latitude', 'longitude', coordinates]
df = df[col_list]
df = df.dropna()

# renaming latitude and longitude to extracted_latitude and extracted_longitude for join_asof
df = df.rename(columns={"datetime": "co2_date_time", "carbonIntensity": "carbon_intensity"})


# Write output
output_dataset.write_with_schema(df)