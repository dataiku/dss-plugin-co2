import dataiku
import datetime
import pandas as pd
import re
from dataiku.customrecipe import get_input_names_for_role, get_recipe_config, get_output_names_for_role


def date_chunk(start, end, chunk_size):
    # Set the range
    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")

    # Compute the difference in days
    delta = (end_date - start_date)
    diff_days = delta.days

    # Create the list of date
    date_list = [(start_date + (datetime.timedelta(days=1) * x)).strftime("%Y-%m-%d") for x in range(0, diff_days + 1)]

    if diff_days < 1:
        date_list = [[start_date.strftime("%Y-%m-%d"), (end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]]
        return date_list

    chunked_list = list()
    date_list_size = len(date_list)

    for i in range(0, len(date_list), chunk_size):

        if(date_list_size < (2 * chunk_size)):
            size1 = date_list_size // 2

            chunked_list.append(date_list[i:i+size1])
            chunked_list.append(date_list[i+size1:i+date_list_size])
            return chunked_list
        else:
            chunked_list.append(date_list[i:i + chunk_size])

        date_list_size -= chunk_size

    return chunked_list


def parse_wkt_point(point):
    # Paris latitude 48.856614 longitude 2.352222
    # Point(longitude latitude)
    # Paris = Point(2.352222 48.856614) correct
    match = re.search(r'\(\s?(\S+)\s+(\S+)\s?\)', point)
    # regex from https://gis.stackexchange.com/questions/246504/wkt-to-some-object-that-will-give-me-longitude-and-latitude-properties
    if match:
        try:
            longitude = float(match.group(1))
            latitude = float(match.group(2))
        except:
            return [None, None]
        if (-90 <= latitude <= 90) and (-180 <= longitude <= 180):
            return [longitude, latitude]
    return [None, None]


def merge_w_nearest_keys(left, right, left_on, right_on, by=None):
    return pd.merge_asof(left.sort_values(by=left_on), right.sort_values(by=right_on), by=by, left_on=left_on, right_on=right_on)


def get_geopoint_column_names(columns_names):
    # # Check if extracted_geopoint_longitude, extracted_geopoint_latitudes and extracted_geopoint columns names are not already used:
    if 'extracted_geopoint_longitude' not in columns_names or 'extracted_geopoint_longitude' or 'extracted_geopoint' not in columns_names:
        extracted_geopoint = 'extracted_geopoint'
        extracted_longitude = 'extracted_geopoint_longitude'
        extracted_latitude = 'extracted_geopoint_latitude'
    else:
        extracted_geopoint = 'extracted_geopoint_42'
        extracted_longitude = 'extracted_geopoint_longitude_42'
        extracted_latitude = 'extracted_geopoint_latitude_42'

    return extracted_geopoint, extracted_longitude, extracted_latitude


def get_input_output():
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
    columns_names = input_df.columns
    return input_df, output_dataset, columns_names


def get_coordinates(input_df):
    coordinates = get_recipe_config().get('coordinates')
    if not coordinates:
        raise ValueError("No coordinate column was selected")
    columns_names = input_df.columns
    if coordinates not in columns_names:
        raise ValueError("Not able to find the '%s' column" % coordinates)
    if input_df[coordinates].isnull().values.any():
        raise ValueError('Empty coordinates found. Please check your input dataset.')
    if not input_df[coordinates].str.startswith('POINT(').all():
        raise ValueError('Invalid coordinates. Geopoint format required: POINT(longitude latitude)')

    return coordinates


def get_api_token():
    api_token = get_recipe_config().get("api_configuration_preset").get("APITOKEN")
    if not api_token:
        raise ValueError("No electricityMap API token found.")
    return api_token


def get_date_column_name(input_df):
    date_column_name = get_recipe_config().get('date_column_name')
    if not date_column_name:
        raise ValueError("No date column was selected")
    columns_names = input_df.columns
    if date_column_name not in columns_names:
        raise ValueError("Not able to find the '%s' column" % date_column_name)
    return date_column_name


def get_consumption_column_name(input_df):
    consumption_column_name = get_recipe_config().get('consumption_column_name')
    if not consumption_column_name:
        raise ValueError("No consumption column was selected")
    columns_names = input_df.columns
    if consumption_column_name not in columns_names:
        raise ValueError("Not able to find the '%s' column" % consumption_column_name)
    return consumption_column_name