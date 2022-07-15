import datetime
import pandas as pd
import re


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
    return pd.merge_asof(left.sort_values(by=left_on), right.sort_values(by=right_on),by=by, left_on=left_on, right_on=right_on)


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
