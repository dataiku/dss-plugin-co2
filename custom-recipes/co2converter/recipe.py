
# import the classes for accessing DSS objects from the recipe
from pandas.core.tools.datetimes import DatetimeScalarOrArrayConvertible
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import requests
from io import StringIO
from pandas.io.json import json_normalize
import datetime


# Import the helpers for custom recipes
from dataiku.customrecipe import *

# Inputs
input_names = get_input_names_for_role('input_ds')
input_datasets  = [dataiku.Dataset(name) for name in input_names]
input_dataset = input_datasets[0]

# Outputs
output_names = get_output_names_for_role('output_ds')
output_datasets = [dataiku.Dataset(name) for name in output_names]
output_dataset=output_datasets[0]

#load input DSS dataset as a Pandas dataframe
input_df = input_dataset.get_dataframe()


#parameters:
if get_recipe_config()['APIProvider'] == 'RTE':
    API_ENDPOINT = 'https://opendata.reseaux-energies.fr/api/records/1.0/download/'

if get_recipe_config()['APIProvider'] == 'ElectricityMap':
    
    API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/past-range'
    API_TOKEN = get_recipe_config().get("api_configuration_preset").get("APITOKEN")
    lat = get_recipe_config()['LatColName']
    lon = get_recipe_config()['LonColName']
    


DateColName = get_recipe_config()['DateColName']
ConsumptionColName = get_recipe_config()['ConsumptionColName']

def date_chunk(start, end, chunk_size):
     # Set the range
    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")

    # You can have the difference in days with this :
    delta = (end_date - start_date)
    diff_days = delta.days

    # Create the list of date
    date_list = [ (start_date + (datetime.timedelta(days=1) * x)).strftime("%Y-%m-%d")  for x in range(0, diff_days + 1)]
    
    chunked_list = list()
    date_list_size = len(date_list)
    
    for i in range(0, len(date_list), chunk_size):
 
        if(date_list_size < (2* chunk_size)):
            size1 = date_list_size // 2
            size2 = date_list_size - size1
            
            chunked_list.append(date_list[i:i+size1])
            chunked_list.append(date_list[i+size1:i+date_list_size])
            return chunked_list
        else:
            chunked_list.append(date_list[i:i + chunk_size])
    
        date_list_size -= chunk_size
        

    return chunked_list


# setup request
r = requests.session()


###################################### RTE ######################################
#'eco2mix-national-tr' -> tr= temps reel, de H-2 à M-2
#'eco2mix-national-cons-def' -> historique consolidé 2012 à M-1
#To do: Add a test on the date: if date < currentdate - 1 month -> eco2mix-national-tr else eco2mix-national-cons-def

if get_recipe_config()['APIProvider'] == 'RTE':
    #Get MinDate and MaxDate to compute the number of rows to be requested:
    MinDate = min(input_df[DateColName])
    MaxDate = max(input_df[DateColName])

    #Modify MinDate and MaxDate to have the right format for the query
    min_date = MinDate.isoformat()
    max_date = MaxDate.isoformat()

    #Convert DateColName to datetime format: 
    input_df[DateColName] =  pd.to_datetime(input_df[DateColName])
    
    #Parameters for API call: 
    params = {
    'dataset': 'eco2mix-national-cons-def',
    'timezone': 'Europe/Paris',
    'q':'date_heure:['+min_date+' TO '+max_date+']'
    }
    
    # make request and create df dataframe with response from API:
    response = r.get(API_ENDPOINT, params=params)
    data = StringIO(str(response.content,'utf-8')) 
    df=pd.read_csv(data,sep=';')
    
    # keep only on date_heure and taux_co2 from df:
    col_list = ['date_heure', 'taux_co2']
    df = df[col_list]
    df = df.dropna()

    #rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    df = df.rename(columns={"date_heure": "co2_date_time", "taux_co2": "carbon_intensity"})

    #convert co2_date_time to datetime format: 
    df['co2_date_time'] =  pd.to_datetime(df['co2_date_time'], utc = True)
    
    #join on date with input_df:
    output_df = pd.merge_asof(input_df.sort_values(by=DateColName), df.sort_values(by='co2_date_time'), left_on=DateColName, right_on='co2_date_time')


###################################### ElectricityMap ######################################


if get_recipe_config()['APIProvider'] == 'ElectricityMap':

    #GroupBy
    uniquelatlon = input_df.groupby([lat, lon])[DateColName].unique()
    df =  pd.DataFrame()
    #for each location and with 10 days chunks

    for index, x in enumerate(uniquelatlon):
        MinDate = x.min()
        MaxDate = x.max()
        
        now = datetime.datetime.now().isoformat()
        max_date = MaxDate.isoformat()
        
        
        #Splitting like a goret because the date format between date and time is not consistent: Once it's 'T' once it's ' '.
        #I don't have the time to understand why. I guess I should convert to datetime format but it didn't worked when I tried
        
        MinDateDay = str(MinDate)[0:10]
        MaxDateDay = str(MaxDate)[0:10]

        #As the API is limited to 10 days, I create chunks of dates:
        chunked_dates = date_chunk(MinDateDay,MaxDateDay,10)
        
        for i in range(len(chunked_dates)):
            
            params = {
            'lat': uniquelatlon.index[index][0],
            'lon': uniquelatlon.index[index][1],
            'start': chunked_dates[i][0],
            'end': chunked_dates[i][-1]
            }

            # make request and create df dataframe with response from API:
            response = r.get(API_ENDPOINT, params=params, auth=('auth-token',API_TOKEN))
            dictr = response.json()
            dfa = json_normalize(dictr['data'])
            dfa['latitude'] = uniquelatlon.index[index][0]
            dfa['longitude'] = uniquelatlon.index[index][1]
        
            df = df.append(dfa,ignore_index=True)
            

            
        if(max_date > now):
            
            #Change api endpoint:
            API_ENDPOINT = 'https://api.electricitymap.org/v3/carbon-intensity/forecast'
    
            #Change the parameters. 
            params = {
            'lat': uniquelatlon.index[index][0],
            'lon': uniquelatlon.index[index][1],
            }
            
            #Same as before: make request and create df dataframe with response from API:
            response = r.get(API_ENDPOINT, params=params, auth=('auth-token',API_TOKEN))
            dictr = response.json()
            dfa = json_normalize(dictr['forecast'])
            dfa['latitude'] = uniquelatlon.index[index][0]
            dfa['longitude'] = uniquelatlon.index[index][1]
        
            df = df.append(dfa,ignore_index=True)

            
            
    
 #Drop and rename df columns before joining
 
    # keep only on date_heure and taux_co2 from df:
    col_list = ['datetime', 'carbonIntensity','latitude','longitude']
    df = df[col_list]
    df = df.dropna()
    
    #rename date_heure in co2_dateTime and taux_co2 in carbon_intensity
    df = df.rename(columns={"datetime": "co2_date_time", "carbonIntensity": "carbon_intensity"})
    
    #join on date with input_df:

    #convert co2_date_time to datetime format:
    df['co2_date_time'] =  pd.to_datetime(df['co2_date_time'])

    #convert DateColName to datetime format: 
    input_df[DateColName] =  pd.to_datetime(input_df[DateColName])

    output_df = pd.merge_asof(input_df.sort_values(by=[DateColName]), df.sort_values(by=['co2_date_time']), by=[lat,lon], left_on=[DateColName], right_on=['co2_date_time'])


###################################### output ######################################

#compute co2emission:
#taux_co2 'standard' unit is in g/kwh, but I choose to convert the result into kgeqCO2:
output_df["co2_emission"] = (output_df[ConsumptionColName] * output_df['carbon_intensity']) / 1000

# Write output
output_dataset.write_with_schema(output_df)