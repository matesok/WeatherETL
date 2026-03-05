import datetime
import json
import logging
from io import BytesIO

import pandas as pd
from pandas import Series

from src.utils.files_utils import RAW_DIR, PROCESSED_DIR
from src.aws.aws_s3 import list_files, fetch_file, upload_file, get_bucket

FINAL_COLUMNS = {
    'metadata_fetch_id': 'fetch_id',
    'metadata_fetched_ts': 'fetch_ts',
    'metadata_source': 'source',
    'data_id': 'city_id',
    'data_name': 'city_name',
    'data_coord_lat': 'lat',
    'data_coord_lon': 'lon',
    'data_weather_id': 'condition_id',
    'data_weather_main': 'main_weather',
    'data_weather_description': 'weather_description',
    'data_main_temp': 'temperature_c',
    'data_main_feels_like': 'temperature_feels_like_c',
    'data_main_pressure': 'pressure',
    'data_main_humidity': 'humidity',
    'data_wind_speed': 'wind_speed',
    'data_clouds_all': 'clouds_percent',
    'data_rain_1h': 'rain_1h',
    'data_snow_1h': 'snow_1h',
    'date': 'date',
    'time': 'time'
}


def transform_batch(s3_directory):
    s3_files = list_files(s3_directory)
    bucket = get_bucket()
    # s3_files_spark_path = f's3a://{bucket}/{s3_directory}/*.json'
    # df = spark.read.json(s3_files_spark_path, schema=get_struct())
    # df.printSchema()
    # df.show(2, truncate=False)
    # transformed_data = transform_data(df)
    transformed_data = []
    raw_records = []
    for file in s3_files:
        try:
            data = fetch_file(file)
            raw_records.append(json.loads(data))
        except Exception as e:
            logging.error(f'Failed to process {file}: {e}')

    if not raw_records:
        logging.warning(f'No valid records found in {s3_directory}')
        return None

    transformed_data = transform_raw_records(raw_records)

    return prepare_transformed_file(s3_directory, transformed_data)


def transform_raw_records(raw_records):
    df = pd.json_normalize(raw_records, sep='_', )

    df['data_weather'] = (
        df['data_weather']
        .apply(lambda x: x[0] if isinstance(x, list) else {})
    )
    weather_df = pd.json_normalize(df['data_weather']).add_prefix('data_weather_')
    df.drop('data_weather', axis=1, inplace=True)

    df = pd.concat([df, weather_df], axis=1)

    df['date_local'] = (
            pd.to_datetime(df['data_dt'], unit='s', utc=True)
            + pd.to_timedelta(df['data_timezone'], unit='s')
    )
    df['date'] = df['date_local'].dt.date
    df['time'] = df['date_local'].dt.time

    df['data_rain_1h'] = df['data_rain_1h'].fillna(0.0) if 'data_rain_1h' in df.columns else 0.0
    df['data_snow_1h'] = df['data_snow_1h'].fillna(0.0) if 'data_snow_1h' in df.columns else 0.0

    final_data = df[df.columns.intersection(FINAL_COLUMNS.keys())]

    final_data.rename(columns=FINAL_COLUMNS, inplace=True)

    return final_data


def prepare_transformed_file(processing_hour, df):
    final_file_name = processing_hour.replace(str(RAW_DIR) + '/', '')
    hour_dir = final_file_name
    df.drop_duplicates(subset=['city_id', 'date', 'time'], inplace=True)

    final_file_name = datetime.datetime.now().strftime('%f') + '.parquet'
    buf = BytesIO()
    df.to_parquet(buf, engine='pyarrow')
    buf.seek(0)
    print('transformed file prepared')

    return upload_file(buf, PROCESSED_DIR, hour_dir, final_file_name)
