import datetime
import json
import logging
from io import BytesIO

import pandas as pd

from src.utils.files_utils import RAW_DIR, PROCESSED_DIR
from src.aws.aws_s3 import list_files, fetch_file, upload_file


def transform_batch(s3_directory):
    # path = str(RAW_DIR / directory_name / "*.json")
    # files_to_process = find_files_paths(path)
    s3_files = list_files(s3_directory)
    transformed_data = []
    for file in s3_files:
        try:
            data = fetch_file(file)
            raw_data = json.loads(data)
            transformed_data.append(transform_single_weather_data(raw_data))
        except Exception as e:
            logging.error(f'Failed to process {file}: {e}')

    return prepare_transformed_file(s3_directory, transformed_data)


def prepare_transformed_file(processing_hour, transformed_data):
    final_file_name = processing_hour.replace(str(RAW_DIR) + '/', '')
    hour_dir = final_file_name
    df = pd.DataFrame(transformed_data)
    df.drop_duplicates(subset=['city_id', 'date', 'time'], inplace=True)

    final_file_name = datetime.datetime.now().strftime('%f') + '.parquet'
    buf = BytesIO()
    df.to_parquet(buf, engine='pyarrow')
    buf.seek(0)
    print('transformed file prepared')

    return upload_file(buf, PROCESSED_DIR, hour_dir, final_file_name)


def transform_single_weather_data(raw_data):
    metadata = raw_data['metadata']
    data_dic = raw_data['data']
    weather_list = data_dic.get('weather', [])
    weather_dic = weather_list[0] if weather_list else {}
    main_dic = data_dic['main']
    wind_dic = data_dic['wind']
    clouds_dic = data_dic['clouds']
    rain_dic = data_dic.get('rain', {})
    snow_dic = data_dic.get('snow', {})
    dt_utc = pd.to_datetime(data_dic['dt'], unit='s', utc=True)
    dt_local = (dt_utc + pd.to_timedelta(data_dic['timezone'], unit='s'))

    return {
        'fetch_id': metadata['fetch_id'],
        'fetch_ts': metadata['fetched_ts'],
        'source': metadata['source'],
        'city_id': data_dic['id'],
        'city_name': data_dic['name'],
        'lat': data_dic['coord']['lat'],
        'lon': data_dic['coord']['lon'],
        'condition_id': weather_dic.get('id'),
        'main_weather': weather_dic.get('main'),
        'weather_description': weather_dic.get('description'),
        'date': dt_local.date(),
        'day': dt_local.day,
        'day_of_week': dt_local.dayofweek,
        'month': dt_local.month,
        'quarter': dt_local.quarter,
        'year': dt_local.year,
        'time': dt_local.time(),
        'hour': dt_local.hour,
        'minute': dt_local.minute,
        'second': dt_local.second,
        'temperature_c': main_dic['temp'],
        'temperature_feels_like_c': main_dic['feels_like'],
        'pressure': main_dic['pressure'],
        'humidity': main_dic['humidity'],
        'wind_speed': wind_dic['speed'],
        'clouds_percent': clouds_dic['all'],
        'rain_1h': rain_dic.get('1h', 0),
        'snow_1h': snow_dic.get('1h', 0),
    }
