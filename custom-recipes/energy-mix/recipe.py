import dataiku
import pandas as pd
import requests
from pandas.io.json import json_normalize
import datetime
from co2_converter_common import date_chunk
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config

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
DateColName = get_recipe_config().get('DateColName')
lat = get_recipe_config().get('LatColName')
lon = get_recipe_config().get('LonColName')
API_ENDPOINT = 'https://api.electricitymap.org/v3/power-breakdown/past-range'
API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")

ColToRetrieve = get_recipe_config().get('ColsToRetrieve')


# Input validation:

# # Check if columns are in input dataset
columns_names = input_df.columns

# ## Date Column:
if DateColName not in columns_names:
    raise Exception("Not able to find the '%s' column" % DateColName)


# ## Latitude and longitude Column:

if lat not in columns_names:
    raise Exception("Not able to find the '%s' column" % lat)

if lon not in columns_names:
    raise Exception("Not able to find the '%s' column" % lon)


# # Check input data validity:

# ##Latitude and longitude

if input_df[lat].min() < -90:
    raise Exception("Latitude value is below -90.")
if input_df[lat].max() > 90:
    raise Exception("Latitude value is over 90.")

if input_df[lon].min() < -180:
    raise Exception("longitude value is below -180.")
if input_df[lon].max() > 180:
    raise Exception("longitude value is over 180.")

# API token validity:
if API_TOKEN is None:
    raise Exception("No electricityMap API token found.")


# setup request
r = requests.session()

# ##################################### ElectricityMap ######################################

# GroupBy
uniquelatlon = input_df.groupby([lat, lon])[DateColName].unique()
df = pd.DataFrame()

# for each location and with 10 days chunks
for index, x in enumerate(uniquelatlon):
    MinDate = x.min()
    MaxDate = x.max()

    now = datetime.datetime.now().isoformat()
    max_date = MaxDate.isoformat()

    # Splitting like a goret because the date format between date and time is not consistent: Once it's 'T' once it's ' '.
    # I don't have the time to understand why. I guess I should convert to datetime format but it didn't worked when I tried

    MinDateDay = str(MinDate)[0:10]
    MaxDateDay = str(MaxDate)[0:10]

    # As the API is limited to 10 days, I create chunks of dates:
    chunked_dates = date_chunk(MinDateDay, MaxDateDay, 10)

    for i in range(len(chunked_dates)):

        params = {
            'lat': uniquelatlon.index[index][0],
            'lon': uniquelatlon.index[index][1],
            'start': chunked_dates[i][0],
            'end': chunked_dates[i][-1]
        }

        # make request and create df dataframe with response from API:
        response = r.get(API_ENDPOINT, params=params, auth=('auth-token', API_TOKEN))
        dictr = response.json()
        dfa = json_normalize(dictr['data'])
        dfa['latitude'] = uniquelatlon.index[index][0]
        dfa['longitude'] = uniquelatlon.index[index][1]

        df = df.append(dfa, ignore_index=True)

    if(max_date > now):

        # Change api endpoint:
        API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/forecast'

        # Change the parameters.
        params = {
            'lat': uniquelatlon.index[index][0],
            'lon': uniquelatlon.index[index][1],
        }

        # Same as before: make request and create df dataframe with response from API:
        response = r.get(API_ENDPOINT, params=params, auth=('auth-token', API_TOKEN))
        dictr = response.json()
        dfa = json_normalize(dictr['forecast'])
        dfa['latitude'] = uniquelatlon.index[index][0]
        dfa['longitude'] = uniquelatlon.index[index][1]

        df = df.append(dfa, ignore_index=True)

    # Filtering on the columns selected by the user:
    df_col = df[['datetime', 'latitude', 'longitude']]
    for i in ColToRetrieve:
        list = [col for col in df.columns if col.startswith(i)]
        dfa = df[list]
        df_col = pd.concat((df_col, dfa), axis=1)

    # Drop and rename df columns before joining

    # rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    df_col = df_col.rename(columns={"datetime": "e-mix_date_time"})

    # join on date with input_df:
    # convert emix_date_time to datetime format:
    df_col['e-mix_date_time'] = pd.to_datetime(df_col['e-mix_date_time'])

    # convert DateColName to datetime format:
    input_df[DateColName] = pd.to_datetime(input_df[DateColName])
    output_df = pd.merge_asof(
        input_df.sort_values(by=[DateColName]),
        df_col.sort_values(by=['e-mix_date_time']),
        by=[lat, lon],
        left_on=[DateColName],
        right_on=['e-mix_date_time']
    )


# ##################################### output ######################################

# Write output
output_dataset.write_with_schema(output_df)
