
import dataiku
import pandas as pd
import requests
from io import StringIO
from pandas.io.json import json_normalize
import datetime
from dateutil import parser
from co2_converter_common import date_chunk
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
APIProvider = get_recipe_config().get('APIProvider')
DateColName = get_recipe_config().get('DateColName')
ConsumptionColName = get_recipe_config().get('ConsumptionColName')

# API endpoint parameters conditions:
if APIProvider == 'RTE':
    API_ENDPOINT = 'https://opendata.reseaux-energies.fr/api/records/1.0/download/'

if APIProvider == 'ElectricityMap':
    API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/past-range'
    API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")
    coordinates = get_recipe_config().get('Coordinates')

# Input validation:

# # Check if columns are in input dataset
columns_names = input_df.columns

# ## Date Column:
if DateColName not in columns_names:
    raise Exception("Not able to find the '%s' column" % DateColName)

# ## Consumption Column:
if ConsumptionColName not in columns_names:
    raise Exception("Not able to find the '%s' column" % ConsumptionColName)

# ## Coordinates column:
if APIProvider == 'ElectricityMap':
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

    # Date is not in the future:
    input_df[DateColName] = pd.to_datetime(input_df[DateColName], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
    now = datetime.datetime.utcnow()
    if max(input_df[DateColName]).timestamp() > now.timestamp():
        raise Exception("Date is in the future. Please check your input dataset or use the CO2 forecast component.")

# # Check if extracted_geopoint_longitude and extracted_geopoint_latitudes columns names are not already used:
if 'extracted_geopoint_longitude' not in columns_names or 'extracted_geopoint_longitude' not in columns_names:
    extracted_longitude = 'extracted_geopoint_longitude'
    extracted_latitude = 'extracted_geopoint_latitude'
else:
    extracted_longitude = 'extracted_geopoint_longitude_1'
    extracted_latitude = 'extracted_geopoint_latitude_1'

# setup request
r = requests.session()


# ##################################### RTE ######################################
if APIProvider == 'RTE':
    # Get MinDate and MaxDate to compute the number of rows to be requested:
    min_date = min(input_df[DateColName])
    max_date = max(input_df[DateColName])

    # Parameters for API call:
    params = {
        'dataset': 'eco2mix-national-cons-def',
        'timezone': 'Europe/Paris',
        'q': 'date_heure:['+str(min_date.isoformat())+' TO '+str(max_date.isoformat())+']'
    }

    # make request and create df dataframe with response from API:
    response = r.get(API_ENDPOINT, params=params)
    data = StringIO(str(response.content, 'utf-8'))
    df = pd.read_csv(data, sep=';')

    # keep only on date_heure and taux_co2 from df:
    col_list = ['date_heure', 'taux_co2']
    df = df[col_list]
    df = df.dropna()

    # rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    df = df.rename(columns={"date_heure": "co2_date_time", "taux_co2": "carbon_intensity"})

    # convert co2_date_time to datetime format:
    df['co2_date_time'] = pd.to_datetime(df['co2_date_time'], utc=True)

    # join on date with input_df:
    output_df = pd.merge_asof(input_df.sort_values(by=DateColName), df.sort_values(by='co2_date_time'), left_on=DateColName, right_on='co2_date_time')

# ##################################### ElectricityMap ######################################

if APIProvider == 'ElectricityMap':

    # Converting geopoint to longitude and latitude to fit the API endpoint:
    input_df["extracted_geopoint"] = input_df[coordinates].str.replace(r'[POINT()]', '', regex=True)
    input_df["extracted_geopoint"] = input_df["extracted_geopoint"].str.split(" ", expand=False)
    split_df = pd.DataFrame(input_df["extracted_geopoint"].tolist(), columns=[extracted_longitude, extracted_latitude])
    input_df = input_df.drop(columns="extracted_geopoint")
    input_df = pd.concat([input_df, split_df], axis=1)

    # GroupBy latitude, longitude to retrieve only one API call per coordinates:
    uniquelatlon = input_df.groupby([extracted_longitude, extracted_latitude])[DateColName].unique()
    df = pd.DataFrame()

    # for each unique location location and with 10 days chunks (to avoid API limit):

    for index, x in enumerate(uniquelatlon):
        MinDate = x.min()
        MaxDate = x.max()

        # Parse dates:
        now = datetime.datetime.utcnow()
        max_date = parser.parse(str(MaxDate))
        min_date = parser.parse(str(MinDate))

        # Get only the day from the dates:
        MinDateDay = min_date.strftime("%Y-%m-%d")
        MaxDateDay = max_date.strftime("%Y-%m-%d")

        # As the API is limited to 10 days, I create chunks of dates:
        chunked_dates = date_chunk(MinDateDay, MaxDateDay, 10)

        for i in range(len(chunked_dates)):
            params = {
                'lat': uniquelatlon.index[index][1],
                'lon': uniquelatlon.index[index][0],
                'start': chunked_dates[i][0],
                'end': chunked_dates[i][-1]
            }
    
            # make request and create df dataframe with response from API:
            response = r.get(API_ENDPOINT, params=params, auth=('auth-token', API_TOKEN))
            dictr = response.json()
            dfa = json_normalize(dictr['data'])
            dfa['latitude'] = uniquelatlon.index[index][1]
            dfa['longitude'] = uniquelatlon.index[index][0]

            df = df.append(dfa, ignore_index=True)

    # Drop and rename df columns before joining

    # keep only on date_heure and taux_co2 from df:
    col_list = ['datetime', 'carbonIntensity', 'latitude', 'longitude']
    df = df[col_list]
    df = df.dropna()

    # rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    # renaming latitude and longitude to extracted_latitude and extracted_longitude for join_asof
    df = df.rename(columns={"datetime": "co2_date_time", "carbonIntensity": "carbon_intensity", "latitude": extracted_latitude, "longitude": extracted_longitude})

    # join on date with input_df:

    # convert co2_date_time to datetime format:
    df['co2_date_time'] = pd.to_datetime(df['co2_date_time'], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)

    output_df = pd.merge_asof(
        input_df.sort_values(by=[DateColName]),
        df.sort_values(by=['co2_date_time']),
        by=[extracted_latitude, extracted_longitude],
        left_on=[DateColName],
        right_on=['co2_date_time']
    )

    # drop lat and lon columns (not needed):
    output_df.drop([extracted_latitude, extracted_longitude], axis=1, inplace=True)

# ##################################### output ######################################

# compute co2emission:
# taux_co2 'standard' unit is in g/kwh, but I choose to convert the result into kgeqCO2:
output_df["co2_emission"] = (output_df[ConsumptionColName] * output_df['carbon_intensity']) / 1000

# Write output
output_dataset.write_with_schema(output_df)