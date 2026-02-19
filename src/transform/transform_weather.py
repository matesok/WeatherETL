import json
import logging

import pandas as pd

from src.utils.files_utils import RAW_DIR, PROCESSED_DIR, find_files_paths


def transform_batch(directory_name):
    path = str(RAW_DIR / directory_name / "*.json")
    files_to_process = find_files_paths(path)
    transformed_data = []
    for file in files_to_process:
        try:
            with open(file) as json_file:
                raw_data = json.load(json_file)
                transformed_data.append(transform_single_weather_data(raw_data))
        except Exception as e:
            logging.error(f'Failed to process {file}: {e}')

    prepare_transformed_file(directory_name, transformed_data)
    return directory_name


def prepare_transformed_file(processing_hour, transformed_data):
    df = pd.DataFrame(transformed_data)
    df.drop_duplicates(subset=['city_id', 'date', 'time'], inplace=True)

    processing_hour = processing_hour + ".parquet"
    df.to_parquet(path=PROCESSED_DIR / processing_hour, engine='pyarrow')


def transform_single_weather_data(raw_data):
    metadata = raw_data['metadata']
    data_dic = raw_data['data']
    weather_list = data_dic.get('weather', [])
    weather_dic = weather_list[0] if weather_list else  {}
    main_dic = data_dic['main']
    wind_dic = data_dic['wind']
    clouds_dic = data_dic['clouds']
    rain_dic = data_dic.get('rain', {})
    snow_dic = data_dic.get('snow', {})
    dt_utc = pd.to_datetime(data_dic['dt'], unit='s', utc=True)
    dt_local = (dt_utc + pd.to_timedelta(data_dic['timezone'], unit='s'))

    return {
        #dim metadata
        'fetch_id': metadata['fetch_id'],
        'fetch_ts': metadata['fetched_ts'],
        'source': metadata['source'],
        # dim location
        'city_id': data_dic['id'],
        'city_name': data_dic['name'],
        'lat': data_dic['coord']['lat'],
        'lon': data_dic['coord']['lon'],
        # dim weather condition
        'condition_id': weather_dic.get('id'),
        'main_weather': weather_dic.get('main'),
        'weather_description': weather_dic.get('description'),
        # dim date
        'date': dt_local.date(),
        'day_of_week': dt_local.dayofweek,
        'month': dt_local.month,
        'quarter': dt_local.quarter,
        'year': dt_local.year,
        #dim time
        'time': dt_local.time(),
        'hour': dt_local.hour,
        'minute': dt_local.minute,
        'second': dt_local.second,
        #fact
        'temperature_c': main_dic['temp'],
        'temperature_feels_like_c': main_dic['feels_like'],
        'pressure': main_dic['pressure'],
        'humidity': main_dic['humidity'],
        'wind_speed': wind_dic['speed'],
        'clouds_percent': clouds_dic['all'],
        'rain_1h': rain_dic.get('1h', 0),
        'snow_1h': snow_dic.get('1h', 0),
    }
